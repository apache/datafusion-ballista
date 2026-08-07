// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use nix::fcntl::{Flock, FlockArg};
use std::fs::{File, OpenOptions};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Reserve a free TCP port by binding to :0 and immediately releasing it.
///
/// Inherently racy, but adequate here: the child binds within milliseconds and
/// the tests are the only thing running.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

/// Open a child process log file for append.
///
/// Appending (rather than truncating) matters for Task 6's kill/restart
/// scenarios: a restarted executor reuses the same log path, and the prior
/// process's output is the evidence of why it died. It must not be wiped out
/// by the replacement process starting up.
fn open_log(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// Locate a binary built by this crate.
///
/// The profile directory is taken from the running test executable
/// (`<target>/<profile>/deps/<test>`) rather than inferred from the profile's
/// settings. Inferring it from `cfg!(debug_assertions)` only works for the
/// stock `dev` and `release` profiles: CI builds under `--profile ci`, which
/// inherits `dev` but turns debug assertions off, so the binaries land in
/// `target/ci/` while the inference points at `target/release/`. Deriving the
/// directory from `current_exe` is correct for any profile and honours
/// `CARGO_TARGET_DIR` for free.
fn binary(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().expect("locate the test executable");
    path.pop(); // deps/
    path.pop(); // <target>/<profile>/
    path.push(name);
    assert!(
        path.exists(),
        "{} not found at {}. Run `cargo build -p ballista-chaos --bins` first.",
        name,
        path.display()
    );
    path
}

/// One supervised executor process.
///
/// `child` is used by this task's `kill_executor`/`executor_is_alive`.
pub(crate) struct ExecutorHandle {
    pub(crate) child: Child,
    #[allow(dead_code)]
    pub(crate) port: u16,
    #[allow(dead_code)]
    pub(crate) grpc_port: u16,
    pub(crate) work_dir: PathBuf,
}

pub struct TestClusterBuilder {
    executors: usize,
    executor_timeout_seconds: u64,
    expire_interval_seconds: u64,
    task_max_failures: usize,
    stage_max_failures: usize,
    concurrent_tasks: usize,
    no_executors_grace_seconds: u64,
}

impl Default for TestClusterBuilder {
    fn default() -> Self {
        Self {
            executors: 2,
            // Ballista's defaults are 180s/15s, which would make an executor-kill
            // scenario take three minutes to even notice the death.
            executor_timeout_seconds: 5,
            expire_interval_seconds: 1,
            task_max_failures: 4,
            stage_max_failures: 4,
            concurrent_tasks: 4,
            // Ballista's default is 30s. A short grace makes the total-loss
            // scenario fail the job a second or so after the reap.
            no_executors_grace_seconds: 1,
        }
    }
}

impl TestClusterBuilder {
    pub fn executors(mut self, n: usize) -> Self {
        self.executors = n;
        self
    }

    /// How long the scheduler waits on a missing heartbeat before declaring the
    /// executor lost. Scenario E raises this deliberately to isolate the
    /// FetchPartitionError path from the ExecutorLost path.
    pub fn executor_timeout_seconds(mut self, seconds: u64) -> Self {
        self.executor_timeout_seconds = seconds;
        self
    }

    /// How long the scheduler waits, after losing its last executor, for one to
    /// (re)register before failing the running jobs (see #2029).
    pub fn no_executors_grace_seconds(mut self, seconds: u64) -> Self {
        self.no_executors_grace_seconds = seconds;
        self
    }

    pub fn task_max_failures(mut self, n: usize) -> Self {
        self.task_max_failures = n;
        self
    }

    pub fn concurrent_tasks(mut self, n: usize) -> Self {
        self.concurrent_tasks = n;
        self
    }

    pub async fn start(self) -> Result<TestCluster, String> {
        // Held for the cluster's whole lifetime, so only one cluster exists in
        // this process at a time. `--test-threads=1` gives the same guarantee,
        // but nothing forces a caller (CI runs a plain `cargo test` over the
        // whole workspace) to pass it, and a dozen concurrent clusters exhaust
        // ports and CPU and fail for reasons that have nothing to do with the
        // scenario under test. Making the harness enforce its own requirement
        // is more robust than documenting a flag.
        let cluster_lock = cluster_lock().lock_owned().await;

        // The mutex above only serializes within one process. `cargo test`
        // runs test binaries sequentially, but nothing else does: a second
        // cargo invocation in another shell, an IDE's background test run, or
        // a runner like nextest (which parallelizes binaries) would put two
        // multi-process clusters on the machine at once, and on a two-core CI
        // runner that is enough for executors to miss the registration
        // deadline. flock is advisory and machine-wide, so the second process
        // blocks here until the first one's cluster is gone.
        let machine_lock = tokio::task::spawn_blocking(machine_lock)
            .await
            .map_err(|e| format!("acquire machine-wide cluster lock: {e}"))??;

        let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
        let log_dir = temp.path().join("logs");
        std::fs::create_dir_all(&log_dir).map_err(|e| e.to_string())?;
        let scheduler_port = free_port();

        let scheduler_log = log_dir.join("scheduler.log");
        let scheduler_stdout = open_log(&scheduler_log).map_err(|e| {
            format!("open scheduler log {}: {e}", scheduler_log.display())
        })?;
        let scheduler_stderr = open_log(&scheduler_log).map_err(|e| {
            format!("open scheduler log {}: {e}", scheduler_log.display())
        })?;

        let mut scheduler = Command::new(binary("chaos-scheduler"));
        scheduler
            .env("CHAOS_SCHEDULER_PORT", scheduler_port.to_string())
            .env(
                "CHAOS_EXECUTOR_TIMEOUT_SECONDS",
                self.executor_timeout_seconds.to_string(),
            )
            .env(
                "CHAOS_EXPIRE_INTERVAL_SECONDS",
                self.expire_interval_seconds.to_string(),
            )
            .env(
                "CHAOS_TASK_MAX_FAILURES",
                self.task_max_failures.to_string(),
            )
            .env(
                "CHAOS_STAGE_MAX_FAILURES",
                self.stage_max_failures.to_string(),
            )
            .env(
                "CHAOS_NO_EXECUTORS_GRACE_SECONDS",
                self.no_executors_grace_seconds.to_string(),
            )
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
            )
            .stdout(Stdio::from(scheduler_stdout))
            .stderr(Stdio::from(scheduler_stderr));
        let scheduler = scheduler
            .spawn()
            .map_err(|e| format!("spawn scheduler: {e}"))?;

        let mut cluster = TestCluster {
            scheduler,
            scheduler_port,
            executors: Vec::new(),
            temp,
            log_dir,
            builder: self,
            _cluster_lock: cluster_lock,
            _machine_lock: machine_lock,
        };

        // Executors dial the scheduler over gRPC the moment they start. If they
        // are spawned before the scheduler has bound its port, some of them get
        // GrpcConnectionError and exit — the scheduler never sees them, and
        // await_executors then burns its full 120s deadline waiting for ghost
        // registrations. Gate the executor spawns on scheduler readiness.
        cluster.await_scheduler_ready().await?;

        for i in 0..cluster.builder.executors {
            cluster.spawn_executor(i)?;
        }

        let n = cluster.builder.executors;
        cluster.await_executors(n).await?;
        Ok(cluster)
    }
}

/// A multi-process Ballista cluster under test. Every child is killed on drop.
pub struct TestCluster {
    scheduler: Child,
    scheduler_port: u16,
    pub(crate) executors: Vec<ExecutorHandle>,
    temp: tempfile::TempDir,
    log_dir: PathBuf,
    builder: TestClusterBuilder,
    /// Serializes clusters across the whole test process; see
    /// [`TestClusterBuilder::start`]. `Drop for TestCluster` reaps every child
    /// before this field is dropped, so the next cluster never starts until the
    /// previous one's processes are gone.
    _cluster_lock: OwnedMutexGuard<()>,
    /// Serializes clusters across test *processes* on the same machine; see
    /// [`TestClusterBuilder::start`].
    _machine_lock: Flock<File>,
}

/// The process-wide lock guaranteeing one cluster at a time.
fn cluster_lock() -> Arc<Mutex<()>> {
    static LOCK: OnceLock<Arc<Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(Mutex::new(()))).clone()
}

/// The machine-wide lock guaranteeing one cluster at a time across processes.
///
/// Blocks until acquired, so call it from a blocking-friendly context. The
/// kernel releases a flock when its file handle closes, so a SIGKILLed test
/// process cannot leave the lock stuck.
fn machine_lock() -> Result<Flock<File>, String> {
    let path = std::env::temp_dir().join("ballista-chaos-cluster.lock");
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| format!("open lock file {}: {e}", path.display()))?;
    Flock::lock(file, FlockArg::LockExclusive)
        .map_err(|(_, e)| format!("flock {}: {e}", path.display()))
}

impl TestCluster {
    pub fn builder() -> TestClusterBuilder {
        TestClusterBuilder::default()
    }

    /// The Ballista client URL.
    pub fn scheduler_url(&self) -> String {
        format!("df://127.0.0.1:{}", self.scheduler_port)
    }

    /// The scheduler REST base URL. gRPC and REST share one port.
    pub fn rest_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.scheduler_port)
    }

    /// The shared directory for fixtures and fault budgets. Every executor can
    /// read it, which is what makes the fault budget cluster-wide.
    pub fn shared_dir(&self) -> &std::path::Path {
        self.temp.path()
    }

    /// Directory containing each child process's stdout/stderr log
    /// (`scheduler.log`, `executor-{index}.log`). When a scenario fails, these
    /// logs are the evidence of what the scheduler and executors were doing.
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }

    pub(crate) fn spawn_executor(&mut self, index: usize) -> Result<(), String> {
        let port = free_port();
        let grpc_port = free_port();
        let work_dir = self.temp.path().join(format!("executor-{index}"));
        std::fs::create_dir_all(&work_dir).map_err(|e| e.to_string())?;

        // Appends rather than truncates: a respawn at this same index (Task 6's
        // kill/restart scenarios) must not erase the log of the process that
        // just died.
        let executor_log = self.log_dir.join(format!("executor-{index}.log"));
        let executor_stdout = open_log(&executor_log).map_err(|e| {
            format!("open executor {index} log {}: {e}", executor_log.display())
        })?;
        let executor_stderr = open_log(&executor_log).map_err(|e| {
            format!("open executor {index} log {}: {e}", executor_log.display())
        })?;

        let child = Command::new(binary("chaos-executor"))
            .env("CHAOS_EXECUTOR_PORT", port.to_string())
            .env("CHAOS_EXECUTOR_GRPC_PORT", grpc_port.to_string())
            .env("CHAOS_SCHEDULER_PORT", self.scheduler_port.to_string())
            .env(
                "CHAOS_CONCURRENT_TASKS",
                self.builder.concurrent_tasks.to_string(),
            )
            .env("CHAOS_WORK_DIR", work_dir.display().to_string())
            .env("CHAOS_HEARTBEAT_SECONDS", "1")
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
            )
            .stdout(Stdio::from(executor_stdout))
            .stderr(Stdio::from(executor_stderr))
            .spawn()
            .map_err(|e| format!("spawn executor {index}: {e}"))?;

        if self.executors.len() > index {
            self.executors[index] = ExecutorHandle {
                child,
                port,
                grpc_port,
                work_dir,
            };
        } else {
            self.executors.push(ExecutorHandle {
                child,
                port,
                grpc_port,
                work_dir,
            });
        }
        Ok(())
    }

    /// Block until the scheduler is accepting REST/gRPC connections.
    ///
    /// Returns early with the log tail if the scheduler process has already
    /// exited, so a startup crash surfaces immediately instead of masquerading
    /// as a 30s connect timeout.
    async fn await_scheduler_ready(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(Some(status)) = self.scheduler.try_wait() {
                return Err(format!(
                    "scheduler exited before becoming ready: {status}\n{}",
                    self.log_tails()
                ));
            }
            if reqwest::get(format!("{}/api/executors", self.rest_url()))
                .await
                .and_then(|r| r.error_for_status())
                .is_ok()
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "timed out waiting for scheduler to accept connections\n{}",
                    self.log_tails()
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Block until `n` executors have registered with the scheduler.
    ///
    /// The deadline is generous because a loaded CI runner starts these child
    /// processes slowly; a healthy cluster returns in a couple of seconds
    /// regardless. On timeout the error carries every child's log tail — the
    /// only evidence of a startup failure CI would otherwise throw away.
    pub async fn await_executors(&self, n: usize) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Ok(count) = self.registered_executors().await
                && count >= n
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "timed out waiting for {n} executors to register\n{}",
                    self.log_tails()
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// The last lines of every child process log, for timeout diagnostics.
    pub fn log_tails(&self) -> String {
        let mut out = String::new();
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&self.log_dir)
            .map(|d| d.filter_map(|e| e.ok().map(|e| e.path())).collect())
            .unwrap_or_default();
        paths.sort();
        for path in paths {
            let content = std::fs::read_to_string(&path).unwrap_or_default();
            let tail: Vec<&str> = content.lines().rev().take(20).collect();
            out.push_str(&format!(
                "--- {} (last {} lines) ---\n",
                path.display(),
                tail.len()
            ));
            for line in tail.into_iter().rev() {
                out.push_str(line);
                out.push('\n');
            }
        }
        out
    }

    pub async fn diagnostics(&self, job_id: &str) -> String {
        let stages = self
            .stages(job_id)
            .await
            .ok()
            .and_then(|stages| serde_json::to_string_pretty(&stages).ok())
            .unwrap_or_else(|| "stage summary unavailable".to_string());
        format!("--- stages ---\n{stages}\n{}", self.log_tails())
    }

    /// Block until the scheduler considers exactly `n` executors registered.
    ///
    /// Unlike `await_executors` (which waits for *at least* `n`, the right
    /// condition when growing a cluster), a SIGKILLed executor is not dropped
    /// from `/api/executors` the instant it dies — the scheduler keeps
    /// listing it until its heartbeat times out (`executor_timeout_seconds`).
    /// A scenario that kills an executor and wants to observe the scheduler
    /// actually reaping it (rather than just transiently over-counting) needs
    /// to wait for the count to come down to `n` exactly, not merely reach it.
    pub async fn await_executor_count(&self, n: usize) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Ok(count) = self.registered_executors().await
                && count == n
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "timed out waiting for exactly {n} registered executors"
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// How many executors the scheduler currently considers registered.
    pub async fn registered_executors(&self) -> Result<usize, String> {
        let body: serde_json::Value =
            reqwest::get(format!("{}/api/executors", self.rest_url()))
                .await
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
        Ok(body.as_array().map(|a| a.len()).unwrap_or(0))
    }

    /// The id of the single job the scheduler currently knows about.
    ///
    /// The harness runs one query at a time, so "the running job" is unambiguous.
    pub async fn running_job_id(&self) -> Result<String, String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let body: serde_json::Value =
                reqwest::get(format!("{}/api/jobs", self.rest_url()))
                    .await
                    .map_err(|e| e.to_string())?
                    .json()
                    .await
                    .map_err(|e| e.to_string())?;

            if let Some(job) = body.as_array().and_then(|jobs| jobs.first())
                && let Some(id) = job.get("job_id").and_then(|v| v.as_str())
            {
                return Ok(id.to_string());
            }
            if Instant::now() > deadline {
                return Err("timed out waiting for a job to appear".to_string());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn stages(&self, job_id: &str) -> Result<serde_json::Value, String> {
        reqwest::get(format!("{}/api/job/{job_id}/stages", self.rest_url()))
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())
    }

    /// Block until `stage_id` has at least one task in state Running.
    ///
    /// This is what lets a kill land *while the stage is genuinely executing*,
    /// rather than after an arbitrary sleep that may fire too early or too late.
    pub async fn await_stage_running(
        &self,
        job_id: &str,
        stage_id: usize,
    ) -> Result<(), String> {
        self.await_stage_task_state(job_id, stage_id, "Running")
            .await
    }

    /// Block until any task in any stage is Running.
    ///
    /// Planner-agnostic sync point: the static and adaptive (AQE) planners
    /// number and materialize stages differently, so rather than target a
    /// specific stage id we wait until the job is genuinely executing a task
    /// somewhere. Used where the scenario only needs a kill to land mid-flight.
    pub async fn await_any_stage_running(&self, job_id: &str) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let stages = self.stages(job_id).await?;
            let running =
                stages
                    .get("stages")
                    .and_then(|s| s.as_array())
                    .is_some_and(|stages| {
                        stages.iter().any(|stage| {
                            stage.get("tasks").and_then(|t| t.as_array()).is_some_and(
                                |tasks| {
                                    tasks.iter().any(|t| {
                                        t.get("status").and_then(|s| s.as_str())
                                            == Some("Running")
                                    })
                                },
                            )
                        })
                    });
            if running {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(
                    "timed out waiting for any stage to start running".to_string()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Block until every task in `stage_id` is Successful.
    pub async fn await_stage_successful(
        &self,
        job_id: &str,
        stage_id: usize,
    ) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let stages = self.stages(job_id).await?;
            if let Some(stage) = find_stage(&stages, stage_id)
                && let Some(tasks) = stage.get("tasks").and_then(|t| t.as_array())
            {
                let all_ok = !tasks.is_empty()
                    && tasks.iter().all(|t| {
                        t.get("status").and_then(|s| s.as_str()) == Some("Successful")
                    });
                if all_ok {
                    return Ok(());
                }
            }
            if Instant::now() > deadline {
                return Err(format!("timed out waiting for stage {stage_id} to succeed"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn await_stage_task_state(
        &self,
        job_id: &str,
        stage_id: usize,
        state: &str,
    ) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let stages = self.stages(job_id).await?;
            if let Some(stage) = find_stage(&stages, stage_id)
                && let Some(tasks) = stage.get("tasks").and_then(|t| t.as_array())
            {
                let hit = tasks
                    .iter()
                    .any(|t| t.get("status").and_then(|s| s.as_str()) == Some(state));
                if hit {
                    return Ok(());
                }
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "timed out waiting for a {state} task in stage {stage_id}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// The scheduler's short categorical job status: "Queued", "Running",
    /// "Completed", "Failed", or "Invalid".
    ///
    /// The brief's original version read the JSON field literally named
    /// `job_status`, but that field (`JobResponse::job_status` /
    /// `handlers::format_job_status`'s second return value) is actually a long
    /// human-readable sentence, e.g. "Completed. Produced 1 partition
    /// containing 50 rows. Elapsed time: 49 ms." — never `"Successful"` as the
    /// original doc comment claimed (a completed job reports `"Completed"`,
    /// not `"Successful"`), and not stable/matchable for assertions. The short
    /// categorical value the doc comment actually promises lives in the
    /// sibling `status` field, so this reads that one instead. Verified against
    /// a live cluster: `{"status": "Completed", "job_status": "Completed.
    /// Produced 1 partition containing 50 rows. Elapsed time: 49 ms.", ...}`.
    pub async fn job_status(&self, job_id: &str) -> Result<String, String> {
        let body: serde_json::Value =
            reqwest::get(format!("{}/api/job/{job_id}", self.rest_url()))
                .await
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
        Ok(body
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string())
    }

    /// SIGKILL an executor. Not SIGTERM: a graceful shutdown would let the
    /// executor deregister, which is a different (and much easier) code path
    /// than the crash we are trying to test.
    pub fn kill_executor(&mut self, index: usize) -> Result<(), String> {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;

        let pid = self.executors[index].child.id();
        kill(Pid::from_raw(pid as i32), Signal::SIGKILL).map_err(|e| e.to_string())?;
        let _ = self.executors[index].child.wait();
        Ok(())
    }

    pub async fn await_successful_shuffle_output(
        &self,
        job_id: &str,
    ) -> Result<(usize, usize), String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let stages = self.stages(job_id).await?;
            for stage_id in running_stage_ids_with_successful_tasks(&stages) {
                if let Some(executor_index) =
                    self.executor_with_shuffle_output(job_id, stage_id)
                {
                    return Ok((executor_index, stage_id));
                }
            }
            if self.job_status(job_id).await.unwrap_or_default() == "Completed" {
                return Err(format!(
                    "job {job_id} completed before a running shuffle-writing stage could be targeted\n{}",
                    self.log_tails()
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "timed out waiting for successful shuffle output for job {job_id}\n{}",
                    self.log_tails()
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn executor_with_shuffle_output(
        &self,
        job_id: &str,
        stage_id: usize,
    ) -> Option<usize> {
        self.executors
            .iter()
            .enumerate()
            .find_map(|(index, executor)| {
                let stage_dir = executor.work_dir.join(job_id).join(stage_id.to_string());
                dir_has_entries(&stage_dir).then_some(index)
            })
    }

    /// Start a fresh executor process in the given slot and wait for it to register.
    pub async fn restart_executor(&mut self, index: usize) -> Result<(), String> {
        let expected = self.executors.len();
        self.spawn_executor(index)?;
        self.await_executors(expected).await
    }

    /// Whether an executor process is still alive.
    pub fn executor_is_alive(&mut self, index: usize) -> bool {
        matches!(self.executors[index].child.try_wait(), Ok(None))
    }
}

/// Stage ids come back from the REST API as strings.
fn find_stage(stages: &serde_json::Value, stage_id: usize) -> Option<&serde_json::Value> {
    stages.get("stages")?.as_array()?.iter().find(|s| {
        s.get("stage_id").and_then(|v| v.as_str()) == Some(stage_id.to_string().as_str())
    })
}

fn dir_has_entries(path: &Path) -> bool {
    std::fs::read_dir(path)
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false)
}

fn running_stage_ids_with_successful_tasks(stages: &serde_json::Value) -> Vec<usize> {
    stages
        .get("stages")
        .and_then(|s| s.as_array())
        .into_iter()
        .flatten()
        .filter(|stage| {
            stage.get("stage_status").and_then(|v| v.as_str()) == Some("Running")
                && stage
                    .get("tasks")
                    .and_then(|tasks| tasks.as_array())
                    .into_iter()
                    .flatten()
                    .any(|task| {
                        task.get("status").and_then(|status| status.as_str())
                            == Some("Successful")
                    })
        })
        .filter_map(|stage| stage.get("stage_id")?.as_str()?.parse().ok())
        .collect()
}

/// If `child` already exited with a non-zero status, log the path of its
/// output so a human investigating a failed scenario knows where to look.
/// Then make sure it is actually gone.
fn reap(child: &mut Child, log_path: &Path) {
    if let Ok(Some(status)) = child.try_wait()
        && !status.success()
    {
        log::warn!(
            "process exited with {status}; see log at {}",
            log_path.display()
        );
    }
    let _ = child.kill();
    let _ = child.wait();
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for (index, executor) in self.executors.iter_mut().enumerate() {
            let log_path = self.log_dir.join(format!("executor-{index}.log"));
            reap(&mut executor.child, &log_path);
        }
        let scheduler_log = self.log_dir.join("scheduler.log");
        reap(&mut self.scheduler, &scheduler_log);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cluster_starts_with_the_requested_executors_registered() {
        let cluster = TestCluster::builder()
            .executors(2)
            // This canary only checks that registration happened; it kills
            // nothing, so the default short reap timeout buys it nothing and
            // lets a CPU-starved executor be reaped mid-startup on a loaded CI
            // runner before the snapshot below.
            .executor_timeout_seconds(30)
            .start()
            .await
            .expect("cluster must start");

        // The scheduler's own view is the source of truth: if the executors did
        // not register, every later scenario would silently run single-executor.
        let executors: serde_json::Value =
            reqwest::get(format!("{}/api/executors", cluster.rest_url()))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();

        assert_eq!(
            executors.as_array().map(|a| a.len()),
            Some(2),
            "expected 2 registered executors, got {executors:?}"
        );
    }

    #[tokio::test]
    async fn killed_executor_is_reaped_and_can_be_restarted() {
        let mut cluster = TestCluster::builder()
            .executors(2)
            .executor_timeout_seconds(5)
            .start()
            .await
            .unwrap();

        assert_eq!(cluster.registered_executors().await.unwrap(), 2);

        cluster.kill_executor(0).unwrap();

        // The scheduler must notice the missing heartbeat and drop the executor.
        // With the defaults (180s timeout, 60s heartbeat) this would never happen
        // inside a test; it works only because the harness turns both down.
        cluster
            .await_executor_count(1)
            .await
            .expect("scheduler never reaped the killed executor");

        cluster.restart_executor(0).await.unwrap();
        assert_eq!(
            cluster.registered_executors().await.unwrap(),
            2,
            "restarted executor must re-register"
        );
    }
}
