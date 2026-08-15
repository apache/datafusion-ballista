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

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

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
    port_forward: Child,
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
        std::fs::create_dir_all(&shared_dir)
            .map_err(|e| format!("create shared dir {}: {e}", shared_dir.display()))?;
        clear_dir_contents(&shared_dir)?;

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
        let port_forward = spawn_port_forward(&namespace, scheduler_local_port)?;

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
        let _ = self.port_forward.kill();
        let _ = self.port_forward.wait();
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

fn spawn_port_forward(namespace: &str, local_port: u16) -> Result<Child, String> {
    Command::new("kubectl")
        .args([
            "-n",
            namespace,
            "port-forward",
            "svc/ballista-scheduler",
            &format!("{local_port}:{SCHEDULER_PORT}"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("spawn kubectl port-forward: {e}"))
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
