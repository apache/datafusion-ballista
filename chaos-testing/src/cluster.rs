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

use std::fs::{File, OpenOptions};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

/// Locate a binary built by this crate, honouring CARGO_TARGET_DIR.
fn binary(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // workspace root
    let target =
        std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    path.push(target);
    path.push(if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    });
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
/// `child` is used by this task's `kill_executor`/`executor_is_alive`. `port`,
/// `grpc_port`, and `work_dir` are still not read anywhere yet — no code in
/// this task needed to target an executor by its network address or inspect
/// its working directory — so they keep a narrower `#[allow(dead_code)]` than
/// the struct-wide one Task 5 left; a later scenario that needs to address a
/// specific executor's port or inspect its shuffle files can drop it then.
pub(crate) struct ExecutorHandle {
    pub(crate) child: Child,
    #[allow(dead_code)]
    pub(crate) port: u16,
    #[allow(dead_code)]
    pub(crate) grpc_port: u16,
    #[allow(dead_code)]
    pub(crate) work_dir: PathBuf,
}

pub struct TestClusterBuilder {
    executors: usize,
    executor_timeout_seconds: u64,
    expire_interval_seconds: u64,
    task_max_failures: usize,
    stage_max_failures: usize,
    concurrent_tasks: usize,
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

    pub fn task_max_failures(mut self, n: usize) -> Self {
        self.task_max_failures = n;
        self
    }

    pub async fn start(self) -> Result<TestCluster, String> {
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
        };

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

    /// Block until `n` executors have registered with the scheduler.
    pub async fn await_executors(&self, n: usize) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(count) = self.registered_executors().await
                && count >= n
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("timed out waiting for {n} executors to register"));
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

    /// The scheduler's view of the job's status ("Running", "Successful", "Failed").
    pub async fn job_status(&self, job_id: &str) -> Result<String, String> {
        let body: serde_json::Value =
            reqwest::get(format!("{}/api/job/{job_id}", self.rest_url()))
                .await
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
        Ok(body
            .get("job_status")
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

    // TEMP-VERIFY: throwaway shape-verification test, removed before commit.
    #[tokio::test]
    async fn temp_verify_json_shapes() {
        use crate::fixture::Fixture;
        use ballista::prelude::{SessionConfigExt, SessionContextExt};
        use datafusion::execution::SessionStateBuilder;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let cluster = TestCluster::builder().executors(2).start().await.unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let fixture = Fixture::write(data_dir.path()).await.unwrap();

        let url = cluster.scheduler_url();
        let session_config = SessionConfig::new_with_ballista().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            .build();
        let ctx = SessionContext::remote_with_state(&url, state)
            .await
            .unwrap();
        for stmt in fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }

        let ctx2 = ctx.clone();
        let handle = tokio::spawn(async move {
            ctx2.sql(Fixture::baseline_query())
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
        });

        let job_id = cluster.running_job_id().await.unwrap();
        eprintln!("TEMP-VERIFY job_id = {job_id}");

        let jobs_raw: serde_json::Value =
            reqwest::get(format!("{}/api/jobs", cluster.rest_url()))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        eprintln!("TEMP-VERIFY /api/jobs = {jobs_raw:#}");

        cluster.await_stage_running(&job_id, 1).await.unwrap();
        let stages_raw: serde_json::Value =
            reqwest::get(format!("{}/api/job/{job_id}/stages", cluster.rest_url()))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        eprintln!(
            "TEMP-VERIFY /api/job/{{id}}/stages (while stage 1 running) = {stages_raw:#}"
        );

        cluster.await_stage_successful(&job_id, 1).await.unwrap();
        let status = cluster.job_status(&job_id).await.unwrap();
        eprintln!("TEMP-VERIFY job_status after stage 1 success = {status}");

        handle.await.unwrap();
        let final_status = cluster.job_status(&job_id).await.unwrap();
        eprintln!("TEMP-VERIFY job_status after job completion = {final_status}");
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
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if cluster.registered_executors().await.unwrap_or(2) == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "scheduler never reaped the killed executor"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        cluster.restart_executor(0).await.unwrap();
        assert_eq!(
            cluster.registered_executors().await.unwrap(),
            2,
            "restarted executor must re-register"
        );
    }
}
