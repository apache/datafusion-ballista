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

//! A Kubernetes (kind) backend for the chaos harness.
//!
//! Where [`crate::cluster::TestCluster`] spawns the scheduler and executors as
//! local OS processes, [`K8sCluster`] runs them as pods in a `kind` cluster: the
//! scheduler as a Deployment behind a `ClusterIP` Service, and the executors as
//! a labelled Deployment (so a later scenario can `scale`/`delete pod` them).
//!
//! The fixture is shared through a `hostPath` volume mounted into both pods.
//! The harness writes the parquet under [`fixture_dir`] on the host; kind's
//! `extraMounts` bind that path into the node, and each pod `hostPath`-mounts
//! it, so the path string is identical on host, node, and pod and the
//! schema-inferring `CREATE EXTERNAL TABLE ... LOCATION` that
//! `Fixture::register_sql` emits resolves the same everywhere — no object store.
//! The directory lives under `$HOME` rather than `/tmp`: Docker Desktop reliably
//! shares the home directory into its VM (and thus the kind node), whereas the
//! VM's `/tmp` is not the host's. Because kind is single-node, a rescheduled pod
//! re-mounts the same directory, so the fixture survives executor kills.
//!
//! The harness process runs outside the cluster, so it reaches the scheduler's
//! gRPC + REST (both on one port) through a `kubectl port-forward`. Results are
//! fetched through the scheduler's embedded flight proxy
//! (`advertise_flight_sql_endpoint`), so the client never contacts executor pod
//! IPs directly.
//!
//! This backend shells out to `kubectl`; it assumes a `kind` cluster already
//! exists, `kubectl` is on `PATH` pointed at it, and the chaos image has been
//! `kind load`ed. See `chaos-testing/k8s/` and the crate README for the runbook.

use std::collections::VecDeque;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Directory shared between the harness and the pods, holding the fixture.
///
/// Defaults to `$HOME/.ballista-chaos-fixtures` and can be overridden with
/// `CHAOS_FIXTURE_DIR` (the run script sets it so the kind `extraMounts` and
/// this backend agree). A path under `$HOME` is used rather than `/tmp` because
/// Docker Desktop reliably shares the home directory into its VM (and thus into
/// the kind node), whereas the VM has its own `/tmp` that is not the host's.
/// The path is identical on host, node, and pod, so the schema-inferring
/// `CREATE EXTERNAL TABLE ... LOCATION` that `Fixture::register_sql` emits
/// resolves the same everywhere.
fn fixture_dir() -> String {
    std::env::var("CHAOS_FIXTURE_DIR").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        format!("{home}/.ballista-chaos-fixtures")
    })
}

const CHAOS_IMAGE: &str = "ballista-chaos:test";
const SCHEDULER_PORT: u16 = 50050;
/// Port the executor pods serve `/healthz` + `/readyz` on for the k8s probes.
const EXECUTOR_HEALTH_PORT: u16 = 50053;
const EXECUTOR_DEPLOYMENT: &str = "ballista-executor";

/// Marker file dropped in any fixture directory the harness manages, so a later
/// run can tell a directory it owns (safe to clear) from a foreign one.
const FIXTURE_MARKER: &str = ".ballista-chaos-fixture";

/// How long the port-forward supervisor waits before restarting a forward that
/// exited or failed to spawn, so a hard-failing forward does not hot-loop.
const PORT_FORWARD_RETRY_DELAY: Duration = Duration::from_millis(500);

/// How many lines of port-forward stderr to retain for diagnostics.
const PORT_FORWARD_STDERR_LINES: usize = 20;

/// Per-process counter so each `K8sCluster::start` gets a distinct namespace,
/// even if several run in one process (`--test-threads=1` serialises them today,
/// but this keeps namespaces unique if that ever changes or a start is retried).
static NS_SEQ: AtomicU32 = AtomicU32::new(0);

/// How an executor pod is removed.
#[derive(Clone, Copy, Debug)]
pub enum KillMode {
    /// `kubectl delete pod` — SIGTERM plus the termination grace period, so the
    /// executor's graceful-shutdown path runs (the path a raw process `SIGKILL`
    /// can never reach).
    Graceful,
    /// `kubectl delete pod --grace-period=0 --force` — an abrupt loss, the
    /// closest k8s analogue of the process harness's `SIGKILL`.
    Forced,
}

/// A Ballista cluster running as pods in a kind cluster.
pub struct K8sCluster {
    namespace: String,
    scheduler_local_port: u16,
    port_forward: PortForward,
    shared_dir: PathBuf,
}

impl K8sCluster {
    /// Deploy a scheduler + `executors` executor pods, wait until all executors
    /// have registered, and open a port-forward to the scheduler.
    pub async fn start(executors: usize) -> Result<Self, String> {
        require_kubectl()?;

        // One cluster per process; --test-threads=1 keeps it to one at a time.
        // The counter guards against collisions if that ever changes.
        let namespace = format!(
            "chaos-{}-{}",
            std::process::id(),
            NS_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let shared_dir = PathBuf::from(fixture_dir());

        // Ensure the shared dir exists, then clear its *contents* so a previous
        // run's fixture (e.g. one written by an older build with a different
        // schema) cannot leak into this run. This matters for local,
        // non-ephemeral use; on CI the runner is fresh. We clear the contents
        // rather than the directory itself: it is the bind-mount root, and
        // removing it can sever the mount so pod writes no longer reach the node.
        //
        // `guard_fixture_dir` refuses to clear a directory the harness does not
        // own, so a stray `CHAOS_FIXTURE_DIR` cannot turn this into a recursive
        // delete of something that matters; `write_marker` then re-stamps the
        // now-empty directory as ours for the next run.
        std::fs::create_dir_all(&shared_dir)
            .map_err(|e| format!("create shared dir {}: {e}", shared_dir.display()))?;
        guard_fixture_dir(&shared_dir)?;
        clear_dir_contents(&shared_dir)?;
        write_marker(&shared_dir)?;

        let manifests = render_manifests(&namespace, executors, &shared_dir);
        kubectl_apply(&manifests).await?;

        // Guard so the namespace is torn down even if a later step fails.
        let guard = NamespaceGuard {
            namespace: namespace.clone(),
        };

        kubectl(&[
            "-n",
            &namespace,
            "rollout",
            "status",
            "deploy/ballista-scheduler",
            "--timeout=120s",
        ])
        .await?;

        let scheduler_local_port = free_port()?;
        let port_forward = PortForward::spawn(&namespace, scheduler_local_port);

        let cluster = Self {
            namespace,
            scheduler_local_port,
            port_forward,
            shared_dir,
        };

        cluster.await_executors(executors).await?;

        // Everything is up; keep the namespace (transfer ownership to `cluster`).
        std::mem::forget(guard);
        Ok(cluster)
    }

    /// `df://…` endpoint for the `ballista` client, via the port-forward.
    pub fn scheduler_url(&self) -> String {
        format!("df://127.0.0.1:{}", self.scheduler_local_port)
    }

    /// `http://…` endpoint for the scheduler REST API, via the port-forward.
    pub fn rest_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.scheduler_local_port)
    }

    /// The host directory shared into every pod; write the fixture here.
    pub fn shared_dir(&self) -> &Path {
        &self.shared_dir
    }

    /// Block until `n` executors have registered with the scheduler.
    pub async fn await_executors(&self, n: usize) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Ok(count) = self.registered_executors().await
                && count == n
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                self.dump_diagnostics().await;
                return Err(format!(
                    "timed out waiting for {n} executors to register with the scheduler"
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Print pod status and scheduler/executor logs to stderr — invoked when a
    /// wait times out, so a failed run is diagnosable even though the namespace
    /// is torn down afterwards. Set `CHAOS_KEEP_NS=1` to keep the namespace for
    /// manual `kubectl` inspection.
    pub async fn dump_diagnostics(&self) {
        eprintln!("==> chaos k8s diagnostics for namespace {}", self.namespace);
        for args in [
            vec!["-n", &self.namespace, "get", "pods", "-o", "wide"],
            vec![
                "-n",
                &self.namespace,
                "logs",
                "-l",
                "app=ballista-scheduler",
                "--tail=40",
            ],
            vec![
                "-n",
                &self.namespace,
                "logs",
                "-l",
                "app=ballista-executor",
                "--tail=40",
                "--prefix",
            ],
        ] {
            match kubectl(&args).await {
                Ok(out) => eprintln!("$ kubectl {}\n{out}", args.join(" ")),
                Err(e) => eprintln!("$ kubectl {} -> {e}", args.join(" ")),
            }
        }

        // The port-forward carries every out-of-cluster call (client, executor
        // poll, results), so its stderr is often the first sign of why a wait
        // failed — a dropped or pod-restart-killed forward, for example.
        let forward_stderr = self.port_forward.recent_stderr();
        if !forward_stderr.is_empty() {
            eprintln!(
                "--- kubectl port-forward stderr (last {PORT_FORWARD_STDERR_LINES} lines) ---\n{forward_stderr}"
            );
        }
    }

    /// How many executors the scheduler currently considers registered.
    pub async fn registered_executors(&self) -> Result<usize, String> {
        // A short timeout so a stalled port-forward surfaces as a retryable
        // error in the polling loop rather than hanging the whole wait.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| e.to_string())?;
        let body: serde_json::Value = client
            .get(format!("{}/api/executors", self.rest_url()))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(body.as_array().map(|a| a.len()).unwrap_or(0))
    }

    /// Scale the executor Deployment. `0` is a total loss that stays lost (the
    /// controller does not recreate the pods); scaling back up recovers.
    ///
    /// Not yet exercised by a scenario — this is the k8s primitive the executor
    /// kill/loss scenarios (the #2029 follow-ups) will drive; the baseline test
    /// only needs a healthy cluster. Kept here so the backend is complete.
    pub async fn scale_executors(&self, replicas: usize) -> Result<(), String> {
        kubectl(&[
            "-n",
            &self.namespace,
            "scale",
            &format!("deploy/{EXECUTOR_DEPLOYMENT}"),
            &format!("--replicas={replicas}"),
        ])
        .await
        .map(|_| ())
    }

    /// Delete one executor pod. The Deployment reschedules a replacement (a fresh
    /// executor with a new id), exercising k8s rescheduling plus Ballista's
    /// executor-loss recovery.
    pub async fn kill_one_executor(&self, mode: KillMode) -> Result<(), String> {
        let pods = self.pods_by_label("app=ballista-executor").await?;
        let name = pods
            .into_iter()
            .next()
            .ok_or_else(|| "no executor pods found".to_string())?;
        let mut args = vec!["-n", &self.namespace, "delete", "pod", &name];
        if matches!(mode, KillMode::Forced) {
            args.extend_from_slice(&["--grace-period=0", "--force"]);
        }
        kubectl(&args).await.map(|_| ())
    }

    async fn pods_by_label(&self, label: &str) -> Result<Vec<String>, String> {
        let out = kubectl(&[
            "-n",
            &self.namespace,
            "get",
            "pods",
            "-l",
            label,
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ])
        .await?;
        Ok(out.split_whitespace().map(|s| s.to_string()).collect())
    }
}

impl Drop for K8sCluster {
    fn drop(&mut self) {
        // `PortForward`'s own Drop stops the supervisor task and kills the
        // running forward, so we only need to tear down the namespace here.
        delete_namespace(&self.namespace);
    }
}

/// Deletes a namespace on drop; `mem::forget`ten once startup fully succeeds.
struct NamespaceGuard {
    namespace: String,
}

impl Drop for NamespaceGuard {
    fn drop(&mut self) {
        delete_namespace(&self.namespace);
    }
}

/// Best-effort namespace teardown, skipped when `CHAOS_KEEP_NS` is set so a
/// failed run can be inspected with `kubectl`.
fn delete_namespace(namespace: &str) {
    if std::env::var_os("CHAOS_KEEP_NS").is_some() {
        eprintln!(
            "CHAOS_KEEP_NS set: leaving namespace {namespace} in place; \
             delete it with `kubectl delete namespace {namespace}`"
        );
        return;
    }
    let _ = Command::new("kubectl")
        .args([
            "delete",
            "namespace",
            namespace,
            "--wait=false",
            "--ignore-not-found",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Refuse to wipe a fixture directory the harness does not own.
///
/// [`K8sCluster::start`] clears `CHAOS_FIXTURE_DIR` on every run, so a typo or an
/// env var inherited from another tool could otherwise turn the clear into a
/// recursive delete of a directory that matters. Two cheap checks close that:
/// never operate on the filesystem root or a bare home directory, and treat a
/// non-empty directory without our [`FIXTURE_MARKER`] as foreign and leave it
/// untouched. An empty directory (a fresh CI runner, or a path the operator just
/// created) is fine — [`write_marker`] stamps it as ours afterwards.
fn guard_fixture_dir(dir: &Path) -> Result<(), String> {
    if dir == Path::new("/") {
        return Err("refusing to use '/' as the fixture directory".to_string());
    }
    if let Some(home) = std::env::var_os("HOME")
        && !home.is_empty()
        && dir == Path::new(&home)
    {
        return Err(format!(
            "refusing to use the home directory {} as the fixture directory; \
             set CHAOS_FIXTURE_DIR to a dedicated path",
            dir.display()
        ));
    }

    // A directory we created carries the marker; it is safe to clear.
    if dir.join(FIXTURE_MARKER).exists() {
        return Ok(());
    }

    // Otherwise only proceed if it is empty — a non-empty, unmarked directory was
    // presumably not created by the harness, so clearing it could delete
    // something that matters.
    let is_empty = std::fs::read_dir(dir)
        .map_err(|e| format!("read fixture dir {}: {e}", dir.display()))?
        .next()
        .is_none();
    if is_empty {
        Ok(())
    } else {
        Err(format!(
            "refusing to clear fixture directory {}: it is not empty and has no \
             {FIXTURE_MARKER} marker, so it was not created by the chaos harness. \
             Point CHAOS_FIXTURE_DIR at a dedicated directory, or clear it manually.",
            dir.display()
        ))
    }
}

/// Stamp `dir` as harness-owned so a later run knows it is safe to clear.
fn write_marker(dir: &Path) -> Result<(), String> {
    let marker = dir.join(FIXTURE_MARKER);
    std::fs::write(
        &marker,
        "This directory is managed by the Ballista chaos harness (K8sCluster).\n\
         It is cleared on each run; do not store anything here.\n",
    )
    .map_err(|e| format!("write fixture marker {}: {e}", marker.display()))
}

/// Remove everything *inside* `dir` without removing `dir` itself. `dir` is a
/// bind-mount root, so deleting it can sever the mount; deleting only its
/// entries is safe and leaves the mount intact.
fn clear_dir_contents(dir: &Path) -> Result<(), String> {
    for entry in std::fs::read_dir(dir)
        .map_err(|e| format!("read shared dir {}: {e}", dir.display()))?
    {
        let entry = entry.map_err(|e| format!("read dir entry: {e}"))?;
        let path = entry.path();
        let is_dir = entry.file_type().map_err(|e| e.to_string())?.is_dir();
        let result = if is_dir {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        result.map_err(|e| format!("remove {}: {e}", path.display()))?;
    }
    Ok(())
}

fn require_kubectl() -> Result<(), String> {
    Command::new("kubectl")
        .arg("version")
        .arg("--client")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| format!("kubectl not found on PATH: {e}"))
        .and_then(|s| {
            s.success()
                .then_some(())
                .ok_or_else(|| "`kubectl version --client` failed".to_string())
        })?;

    // This backend creates and deletes namespaces, so refuse to run unless the
    // current context is a kind cluster — a guard against pointing it at a real
    // cluster by accident. kind names its context `kind-<cluster>`.
    let output = Command::new("kubectl")
        .args(["config", "current-context"])
        .output()
        .map_err(|e| format!("read kubectl current-context: {e}"))?;
    let context = String::from_utf8_lossy(&output.stdout);
    let context = context.trim();
    if !context.starts_with("kind-") {
        return Err(format!(
            "refusing to run: current kubectl context is {context:?}, not a kind \
             cluster (expected a `kind-` prefix). Point kubectl at a kind cluster, \
             e.g. `kubectl config use-context kind-ballista-chaos`."
        ));
    }
    Ok(())
}

/// Reserve a free local TCP port for the port-forward.
fn free_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("bind ephemeral port: {e}"))?;
    listener
        .local_addr()
        .map(|a| a.port())
        .map_err(|e| e.to_string())
}

/// A supervised `kubectl port-forward` to the scheduler Service.
///
/// The forward carries everything the harness does from outside the cluster —
/// the `df://` client connection, the `/api/executors` poll, and query results
/// coming back through the scheduler's flight proxy. `kubectl port-forward`
/// resolves the Service to a single pod when it starts and never re-resolves,
/// and long-lived forwards also drop on their own from apiserver hiccups or idle
/// timeouts. A single unmonitored forward would therefore turn a scheduler pod
/// restart — or simply a long-running suite — into an opaque "connection
/// refused". This owns a background task that restarts the forward whenever it
/// exits and keeps a tail of its stderr for diagnostics.
struct PortForward {
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    stderr: Arc<Mutex<VecDeque<String>>>,
}

impl PortForward {
    /// Start supervising a forward from `local_port` to the scheduler Service in
    /// `namespace`. The forward is (re)established in the background, so callers
    /// should reach the scheduler through a retrying poll rather than assuming it
    /// is immediately up.
    fn spawn(namespace: &str, local_port: u16) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let stderr = Arc::new(Mutex::new(VecDeque::new()));
        let task = tokio::spawn(supervise_port_forward(
            namespace.to_string(),
            local_port,
            shutdown_rx,
            stderr.clone(),
        ));
        Self {
            shutdown: Some(shutdown_tx),
            task: Some(task),
            stderr,
        }
    }

    /// The most recent port-forward stderr lines, newest last, for diagnostics.
    fn recent_stderr(&self) -> String {
        self.stderr
            .lock()
            .map(|buf| buf.iter().cloned().collect::<Vec<_>>().join("\n"))
            .unwrap_or_default()
    }
}

impl Drop for PortForward {
    fn drop(&mut self) {
        // Ask the supervisor to stop; aborting the task then drops its child,
        // which is `kill_on_drop`, so the running forward is killed even if the
        // shutdown signal is not observed before the runtime goes away.
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Record one stderr line into the bounded ring buffer, dropping the oldest.
fn record_port_forward_stderr(buf: &Arc<Mutex<VecDeque<String>>>, line: String) {
    if let Ok(mut buf) = buf.lock() {
        while buf.len() >= PORT_FORWARD_STDERR_LINES {
            buf.pop_front();
        }
        buf.push_back(line);
    }
}

/// Supervisor loop: (re)spawn `kubectl port-forward` until asked to stop.
async fn supervise_port_forward(
    namespace: String,
    local_port: u16,
    mut shutdown: oneshot::Receiver<()>,
    stderr: Arc<Mutex<VecDeque<String>>>,
) {
    loop {
        let spawn_result = tokio::process::Command::new("kubectl")
            .args([
                "-n",
                &namespace,
                "port-forward",
                "svc/ballista-scheduler",
                &format!("{local_port}:{SCHEDULER_PORT}"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn();

        let mut child = match spawn_result {
            Ok(child) => child,
            Err(e) => {
                record_port_forward_stderr(
                    &stderr,
                    format!("spawn kubectl port-forward failed: {e}"),
                );
                tokio::select! {
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(PORT_FORWARD_RETRY_DELAY) => continue,
                }
            }
        };

        // Drain the forward's stderr into the ring buffer. The task ends on its
        // own when the child exits and its pipe closes.
        if let Some(pipe) = child.stderr.take() {
            let stderr = stderr.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(pipe).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    record_port_forward_stderr(&stderr, line);
                }
            });
        }

        tokio::select! {
            _ = &mut shutdown => {
                let _ = child.kill().await;
                return;
            }
            status = child.wait() => {
                record_port_forward_stderr(
                    &stderr,
                    match status {
                        Ok(status) => {
                            format!("kubectl port-forward exited ({status}); restarting")
                        }
                        Err(e) => {
                            format!("kubectl port-forward wait failed: {e}; restarting")
                        }
                    },
                );
                // Brief backoff so a forward that fails immediately does not
                // hot-loop; bail out early if we are shutting down.
                tokio::select! {
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(PORT_FORWARD_RETRY_DELAY) => {}
                }
            }
        }
    }
}

/// Run `kubectl` with the given args, returning stdout on success.
async fn kubectl(args: &[&str]) -> Result<String, String> {
    let output = tokio::process::Command::new("kubectl")
        .args(args)
        .output()
        .await
        .map_err(|e| format!("run kubectl {args:?}: {e}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(format!(
            "kubectl {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// `kubectl apply` a rendered manifest by piping it to stdin.
async fn kubectl_apply(manifests: &str) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;

    let mut child = tokio::process::Command::new("kubectl")
        .args(["apply", "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn kubectl apply: {e}"))?;

    child
        .stdin
        .take()
        .expect("stdin piped")
        .write_all(manifests.as_bytes())
        .await
        .map_err(|e| format!("write manifests to kubectl: {e}"))?;

    let output = child
        .wait_with_output()
        .await
        .map_err(|e| format!("wait for kubectl apply: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "kubectl apply failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// Render the namespace + scheduler (Deployment + Service) + executor Deployment.
/// `mount` is the fixture directory, bind-mounted into both pods (see
/// [`fixture_dir`]); it must match the kind `extraMounts` path.
fn render_manifests(
    namespace: &str,
    executors: usize,
    mount: &std::path::Path,
) -> String {
    let mount = mount.display();
    format!(
        r#"
apiVersion: v1
kind: Namespace
metadata:
  name: {namespace}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ballista-scheduler
  namespace: {namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ballista-scheduler
  template:
    metadata:
      labels:
        app: ballista-scheduler
    spec:
      containers:
        - name: scheduler
          image: {CHAOS_IMAGE}
          imagePullPolicy: Never
          command: ["/root/chaos-scheduler"]
          env:
            - name: CHAOS_SCHEDULER_PORT
              value: "{SCHEDULER_PORT}"
            - name: CHAOS_BIND_HOST
              value: "0.0.0.0"
            - name: CHAOS_EXTERNAL_HOST
              value: "ballista-scheduler"
            - name: CHAOS_ADVERTISE_FLIGHT_PROXY
              value: ""
            - name: CHAOS_EXECUTOR_TIMEOUT_SECONDS
              value: "5"
            - name: CHAOS_EXPIRE_INTERVAL_SECONDS
              value: "1"
            - name: RUST_LOG
              value: "info"
          ports:
            - containerPort: {SCHEDULER_PORT}
          # Both probes use /healthz, not /readyz: executors reach the scheduler
          # through the Service below, and a Service only routes to Ready pods.
          # The scheduler's /readyz gates on registered executors, so a /readyz
          # readiness probe would deadlock (no endpoints -> executors can't
          # register -> never ready). /healthz reports process liveness, which is
          # all the Service needs to start routing.
          readinessProbe:
            httpGet:
              path: /healthz
              port: {SCHEDULER_PORT}
            periodSeconds: 2
            failureThreshold: 3
          livenessProbe:
            httpGet:
              path: /healthz
              port: {SCHEDULER_PORT}
            periodSeconds: 10
            failureThreshold: 3
          volumeMounts:
            - name: fixtures
              mountPath: {mount}
      volumes:
        - name: fixtures
          hostPath:
            path: {mount}
            type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: ballista-scheduler
  namespace: {namespace}
spec:
  selector:
    app: ballista-scheduler
  ports:
    - port: {SCHEDULER_PORT}
      targetPort: {SCHEDULER_PORT}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {EXECUTOR_DEPLOYMENT}
  namespace: {namespace}
spec:
  replicas: {executors}
  selector:
    matchLabels:
      app: ballista-executor
  template:
    metadata:
      labels:
        app: ballista-executor
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: executor
          image: {CHAOS_IMAGE}
          imagePullPolicy: Never
          command: ["/root/chaos-executor"]
          env:
            - name: CHAOS_SCHEDULER_HOST
              value: "ballista-scheduler"
            - name: CHAOS_SCHEDULER_PORT
              value: "{SCHEDULER_PORT}"
            - name: CHAOS_EXECUTOR_PORT
              value: "50051"
            - name: CHAOS_EXECUTOR_GRPC_PORT
              value: "50052"
            - name: CHAOS_EXECUTOR_HEALTH_PORT
              value: "{EXECUTOR_HEALTH_PORT}"
            - name: CHAOS_BIND_HOST
              value: "0.0.0.0"
            - name: CHAOS_EXECUTOR_EXTERNAL_HOST
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: CHAOS_HEARTBEAT_SECONDS
              value: "1"
            - name: RUST_LOG
              value: "info"
          ports:
            - containerPort: {EXECUTOR_HEALTH_PORT}
          # The executor is reached by pod IP (not a readiness-gated Service), so
          # /readyz here is safe and meaningful: it reports SERVICE_UNAVAILABLE
          # until the first heartbeat lands, then 200. Liveness stays on /healthz
          # (process-alive) so a slow/again-disconnected executor is not killed.
          readinessProbe:
            httpGet:
              path: /readyz
              port: {EXECUTOR_HEALTH_PORT}
            periodSeconds: 2
            failureThreshold: 3
          livenessProbe:
            httpGet:
              path: /healthz
              port: {EXECUTOR_HEALTH_PORT}
            periodSeconds: 10
            failureThreshold: 3
          volumeMounts:
            - name: fixtures
              mountPath: {mount}
      volumes:
        - name: fixtures
          hostPath:
            path: {mount}
            type: DirectoryOrCreate
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_allows_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        guard_fixture_dir(dir.path()).expect("an empty directory is safe to clear");
    }

    #[test]
    fn guard_allows_marked_directory() {
        let dir = tempfile::tempdir().unwrap();
        // A non-empty directory we own (has the marker) is safe to clear.
        write_marker(dir.path()).unwrap();
        std::fs::write(dir.path().join("stale.parquet"), b"old").unwrap();
        guard_fixture_dir(dir.path())
            .expect("a marked directory is owned by the harness");
    }

    #[test]
    fn guard_refuses_foreign_non_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        // Non-empty and unmarked: presumably not ours, so it must be refused.
        std::fs::write(dir.path().join("important.txt"), b"do not delete").unwrap();
        let err = guard_fixture_dir(dir.path())
            .expect_err("an unmarked non-empty directory must be refused");
        assert!(err.contains("not created by the chaos harness"), "{err}");
    }

    #[test]
    fn guard_refuses_root_and_home() {
        assert!(guard_fixture_dir(Path::new("/")).is_err());
        if let Some(home) = std::env::var_os("HOME").filter(|h| !h.is_empty()) {
            assert!(guard_fixture_dir(Path::new(&home)).is_err());
        }
    }

    #[test]
    fn marker_round_trips_through_guard() {
        // After a clear+mark cycle, the directory is still recognised as ours
        // even though clearing removed the previous marker.
        let dir = tempfile::tempdir().unwrap();
        write_marker(dir.path()).unwrap();
        std::fs::write(dir.path().join("part-0.parquet"), b"x").unwrap();
        guard_fixture_dir(dir.path()).unwrap();
        clear_dir_contents(dir.path()).unwrap();
        write_marker(dir.path()).unwrap();
        assert!(dir.path().join(FIXTURE_MARKER).exists());
        guard_fixture_dir(dir.path()).unwrap();
    }
}
