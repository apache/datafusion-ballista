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

use crate::config::BallistaConfig;
use crate::error::{BallistaError, Result};
use crate::extension::SessionConfigExt;
use crate::serde::protobuf::KeyValuePair;
use crate::serde::scheduler::PartitionStats;

use datafusion::arrow::array::{ArrayRef, AsArray, GenericByteViewArray};
use datafusion::arrow::datatypes::ByteViewType;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, metrics};
use futures::StreamExt;
use log::{error, info};
use std::any::Any;
use std::borrow::Cow;
use std::future::Future;
use std::io::BufWriter;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs::File, pin::Pin};
use tokio::net::TcpSocket;
use tonic::codegen::StdError;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Endpoint, Error, Server};

/// Configuration for gRPC client connections.
///
/// This struct holds timeout and keep-alive settings that are applied
/// when establishing gRPC connections from executors to schedulers or
/// between distributed components.
///
/// # Examples
///
/// ```
/// use ballista_core::config::BallistaConfig;
/// use ballista_core::utils::GrpcClientConfig;
///
/// let ballista_config = BallistaConfig::default();
/// let grpc_config = GrpcClientConfig::from(&ballista_config);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrpcClientConfig {
    /// Connection timeout in seconds
    pub connect_timeout_seconds: u64,
    /// Request timeout in seconds
    pub timeout_seconds: u64,
    /// TCP keep-alive interval in seconds
    pub tcp_keepalive_seconds: u64,
    /// HTTP/2 keep-alive ping interval in seconds
    pub http2_keepalive_interval_seconds: u64,
    /// Should client use tls
    pub use_tls: bool,
    /// Returns the maximum message size for gRPC clients in bytes.
    pub max_message_size: usize,
    /// Number of retries for IO operations.
    pub io_retries_times: u8,
    /// Wait time in milliseconds between IO retries.
    pub io_retry_wait_time_ms: u64,
    /// HTTP/2 initial connection-level flow-control window in bytes. 0 = tonic default.
    pub initial_connection_window_size: u32,
    /// HTTP/2 initial stream-level flow-control window in bytes. 0 = tonic default.
    pub initial_stream_window_size: u32,
}

impl From<&BallistaConfig> for GrpcClientConfig {
    fn from(config: &BallistaConfig) -> Self {
        Self {
            connect_timeout_seconds: config.grpc_client_connect_timeout_seconds() as u64,
            timeout_seconds: config.grpc_client_timeout_seconds() as u64,
            tcp_keepalive_seconds: config.grpc_client_tcp_keepalive_seconds() as u64,
            http2_keepalive_interval_seconds: config
                .grpc_client_http2_keepalive_interval_seconds()
                as u64,
            use_tls: config.use_tls(),
            max_message_size: config.grpc_client_max_message_size(),
            io_retries_times: config.io_retries_times() as u8,
            io_retry_wait_time_ms: config.io_retry_wait_time_ms() as u64,
            initial_connection_window_size: config
                .grpc_client_initial_connection_window_size(),
            initial_stream_window_size: config.grpc_client_initial_stream_window_size(),
        }
    }
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout_seconds: 20,
            timeout_seconds: 20,
            tcp_keepalive_seconds: 3600,
            http2_keepalive_interval_seconds: 300,
            use_tls: false,
            max_message_size: 16 * 1024 * 1024,
            io_retries_times: 3,
            io_retry_wait_time_ms: 3000,
            initial_connection_window_size: 67108864,
            initial_stream_window_size: 16777216,
        }
    }
}

/// Configuration for gRPC server.
///
/// This struct holds timeout and keep-alive settings that are applied
/// when creating gRPC servers in executors and schedulers.
///
/// # Examples
///
/// ```
/// use ballista_core::utils::GrpcServerConfig;
///
/// let server_config = GrpcServerConfig::default();
/// let server = ballista_core::utils::create_grpc_server(&server_config);
/// ```
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// Request timeout in seconds
    pub timeout_seconds: u64,
    /// TCP keep-alive interval in seconds
    pub tcp_keepalive_seconds: u64,
    /// HTTP/2 keep-alive ping interval in seconds
    pub http2_keepalive_interval_seconds: u64,
    /// HTTP/2 keep-alive ping timeout in seconds
    pub http2_keepalive_timeout_seconds: u64,
    /// Length of the listening socket's accept queue, for servers bound with
    /// [`create_grpc_server_incoming`].
    ///
    /// The kernel caps it at `net.core.somaxconn` (4096 on Linux 5.4 and
    /// later). A connection that arrives while the queue is full is delayed
    /// by a SYN retransmit, and under Linux SYN cookies can be reset once the
    /// client starts sending, so the queue should hold the largest burst a
    /// peer can produce. A wide pipelined job opens a connection from every
    /// reader partition to every executor it reads from, all at once.
    pub listen_backlog: u32,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 20,
            tcp_keepalive_seconds: 3600,
            http2_keepalive_interval_seconds: 300,
            http2_keepalive_timeout_seconds: 20,
            listen_backlog: 4096,
        }
    }
}

/// Default session builder using the provided configuration
pub fn default_session_builder(
    config: SessionConfig,
) -> datafusion::common::Result<SessionState> {
    use crate::extension::{
        ballista_aggregate_functions, ballista_scalar_functions,
        ballista_window_functions,
    };

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build()?))
        .with_optimizer_rules(crate::optimizer::ballista_default_optimizer_rules())
        .with_scalar_functions(ballista_scalar_functions())
        .with_aggregate_functions(ballista_aggregate_functions())
        .with_window_functions(ballista_window_functions())
        .build();

    Ok(state)
}

/// Creates a default session configuration with Ballista extensions.
pub fn default_config_producer() -> SessionConfig {
    SessionConfig::new_with_ballista()
}

/// Creates [IpcWriteOptions] using the compression codec configured in the BallistaConfig
pub fn create_write_options(
    compression_type: Option<CompressionType>,
) -> std::result::Result<IpcWriteOptions, DataFusionError> {
    IpcWriteOptions::default()
        .try_with_compression(compression_type)
        .map_err(|err| {
            DataFusionError::Internal(format!("Failed to set compression codec: {err}"))
        })
}

/// Compacts `Utf8View` / `BinaryView` columns whose data buffers hold more
/// than twice the bytes their views reference.
///
/// The IPC writer serializes every data buffer of a view array, including
/// bytes no view points at. Operators such as TopK, `SortPreservingMergeExec`
/// and `LIMIT` can emit a few rows that still reference the data buffers of
/// every batch those rows came from, so writing them as is can turn a 20-row
/// result into tens of MB. Dense columns are left alone, because compacting
/// them would copy every string to save at most half the bytes. Arrow's
/// `BatchCoalescer` uses the same threshold before copying strings.
pub(crate) fn compact_sparse_view_columns(batch: RecordBatch) -> Result<RecordBatch> {
    let mut compacted = false;
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| -> ArrayRef {
            if let Some(a) = c.as_string_view_opt()
                && is_sparse(a)
            {
                compacted = true;
                Arc::new(a.gc())
            } else if let Some(a) = c.as_binary_view_opt()
                && is_sparse(a)
            {
                compacted = true;
                Arc::new(a.gc())
            } else {
                Arc::clone(c)
            }
        })
        .collect();
    if !compacted {
        return Ok(batch);
    }
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Whether a view array's data buffers hold more than twice the bytes its
/// views reference.
fn is_sparse<T: ByteViewType + ?Sized>(array: &GenericByteViewArray<T>) -> bool {
    let buffered: usize = array.data_buffers().iter().map(|b| b.len()).sum();
    buffered > 2 * array.total_buffer_bytes_used()
}

/// Stream data to disk in Arrow IPC format.
///
/// Batches are read from the async stream and forwarded through a bounded
/// channel to a `spawn_blocking` task that performs all synchronous file I/O,
/// keeping the tokio worker thread unblocked.
pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &Path,
    disk_write_metric: &metrics::Time,
    channel_capacity: usize,
    compression_type: Option<CompressionType>,
) -> Result<PartitionStats> {
    let schema = stream.schema();
    let path_owned = path.to_owned();
    let write_metric = disk_write_metric.clone();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(channel_capacity);

    let handle = tokio::task::spawn_blocking(move || -> Result<u64> {
        let file = BufWriter::new(File::create(&path_owned).map_err(|e| {
            error!("Failed to create partition file at {:?}: {e:?}", path_owned);
            BallistaError::IoError(e)
        })?);

        let options = create_write_options(compression_type)?;

        let mut writer =
            StreamWriter::try_new_with_options(file, schema.as_ref(), options)?;

        while let Some(batch) = rx.blocking_recv() {
            let timer = write_metric.timer();
            writer.write(&compact_sparse_view_columns(batch)?)?;
            timer.done();
        }
        let timer = write_metric.timer();
        writer.finish()?;
        timer.done();
        Ok(std::fs::metadata(&path_owned).map(|m| m.len()).unwrap_or(0))
    });

    let mut num_rows = 0;
    let mut num_batches = 0;

    let stream_err = loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                num_batches += 1;
                num_rows += batch.num_rows();
                if tx.send(batch).await.is_err() {
                    break None;
                }
            }
            Some(Err(e)) => break Some(e),
            None => break None,
        }
    };
    drop(tx);

    let write_result = handle
        .await
        .map_err(|e| BallistaError::General(format!("Disk writer task failed: {e}")))?;

    if let Some(e) = stream_err {
        if let Err(write_err) = &write_result {
            error!("Disk writer also failed: {write_err}");
        }
        return Err(e.into());
    }
    let num_bytes = write_result?;

    Ok(PartitionStats::new(
        Some(num_rows as u64),
        Some(num_batches),
        Some(num_bytes),
    ))
}

/// Collects all record batches from a stream into a vector.
pub async fn collect_stream(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Creates a gRPC client connection with the specified configuration.
pub async fn create_grpc_client_connection<D>(
    dst: D,
    config: &GrpcClientConfig,
) -> std::result::Result<Channel, Error>
where
    D: std::convert::TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    let endpoint = tonic::transport::Endpoint::new(dst)?
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds))
        .timeout(Duration::from_secs(config.timeout_seconds))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
        .http2_keep_alive_interval(Duration::from_secs(
            config.http2_keepalive_interval_seconds,
        ))
        // Use a fixed timeout for keep-alive pings to keep configuration simple
        // since this is a standalone configuration
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);
    endpoint.connect().await
}

/// Creates a gRPC client endpoint (without connecting) for customization.
/// This is typically used when TLS or other custom configuration is needed.
/// If `config` is provided, standard timeout and keepalive settings are applied.
pub fn create_grpc_client_endpoint<D>(
    dst: D,
    config: Option<&GrpcClientConfig>,
) -> std::result::Result<Endpoint, Error>
where
    D: std::convert::TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    let endpoint = tonic::transport::Endpoint::new(dst)?;
    if let Some(config) = config {
        let mut endpoint = endpoint
            .connect_timeout(Duration::from_secs(config.connect_timeout_seconds))
            .timeout(Duration::from_secs(config.timeout_seconds))
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
            .http2_keep_alive_interval(Duration::from_secs(
                config.http2_keepalive_interval_seconds,
            ))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true);
        if config.initial_connection_window_size > 0 {
            endpoint = endpoint.initial_connection_window_size(Some(
                config.initial_connection_window_size,
            ));
        }
        if config.initial_stream_window_size > 0 {
            endpoint = endpoint
                .initial_stream_window_size(Some(config.initial_stream_window_size));
        }
        Ok(endpoint)
    } else {
        Ok(endpoint)
    }
}

/// Creates a gRPC server builder with the specified configuration.
pub fn create_grpc_server(config: &GrpcServerConfig) -> Server {
    Server::builder()
        .timeout(Duration::from_secs(config.timeout_seconds))
        // No limit on the streams a connection carries. Pipelined readers
        // multiplex the streams of every task on an executor over one
        // connection to each producer executor, and a stream the client had
        // to queue behind a limit would leave its producer blocked on a full
        // queue.
        .max_concurrent_streams(None)
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
        .http2_keepalive_interval(Some(Duration::from_secs(
            config.http2_keepalive_interval_seconds,
        )))
        .http2_keepalive_timeout(Some(Duration::from_secs(
            config.http2_keepalive_timeout_seconds,
        )))
}

/// Binds a gRPC server's listening socket, for use with tonic's
/// `serve_with_incoming` / `serve_with_incoming_shutdown`. Unlike tonic's
/// `serve`, which binds lazily inside the future it returns, the socket is
/// listening by the time this returns — so use this whenever a peer may be
/// told to connect as soon as the server is started.
///
/// tonic ignores the builder's `tcp_nodelay` and `tcp_keepalive` when serving
/// from a pre-bound listener, so this applies the same values that
/// [`create_grpc_server`] sets, for the same reasons. The remaining settings
/// still come from the builder.
///
/// The socket listens with `config.listen_backlog` rather than the 128 that
/// tonic's own `serve` and `TcpIncoming::bind` get from the standard library,
/// which a burst of connections from a wide cluster overflows at once.
///
/// # Panics
///
/// The listener is registered with the Tokio reactor, so this must be called
/// from within a Tokio runtime.
pub fn create_grpc_server_incoming(
    addr: SocketAddr,
    config: &GrpcServerConfig,
) -> Result<TcpIncoming> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    // The standard library's bind sets this too, so a restarted server can
    // rebind its port while old connections linger in TIME_WAIT.
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(config.listen_backlog)?;
    Ok(TcpIncoming::from(listener)
        .with_nodelay(Some(true))
        .with_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds))))
}

/// Recursively collects metrics from an execution plan and all its children.
pub fn collect_plan_metrics(plan: &dyn ExecutionPlan) -> Vec<MetricsSet> {
    let mut metrics_array = Vec::<MetricsSet>::new();
    if let Some(metrics) = plan.metrics() {
        metrics_array.push(metrics);
    }
    plan.children().iter().for_each(|c| {
        collect_plan_metrics(c.as_ref())
            .into_iter()
            .for_each(|e| metrics_array.push(e))
    });
    metrics_array
}

/// Given an interval in seconds, get the time in seconds before now
pub fn get_time_before(interval_seconds: u64) -> u64 {
    let now_epoch_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    now_epoch_ts
        .checked_sub(Duration::from_secs(interval_seconds))
        .unwrap_or_else(|| Duration::from_secs(0))
        .as_secs()
}

/// Current time since UNIX EPOCH. In milliseconds.
pub fn get_current_time() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis()
}

/// Base marker used in place of a configuration value that may be a secret.
///
/// Logged redactions also include the original value's byte length.
pub const REDACTED: &str = "<redacted>";

/// Parts of a configuration key that mark its value as a possible secret.
const SENSITIVE_KEY_PARTS: [&str; 6] = [
    "secret",
    "password",
    "token",
    "credential",
    "api_key",
    "private_key",
];

/// Returns `value` as it may appear in a log line: unchanged, or a redaction
/// marker containing its byte length if `key` looks like it names a secret.
///
/// Session configuration travels as plain key/value pairs, including config
/// extensions Ballista knows nothing about, so the key name is all there is to
/// go on. A key is sensitive if it contains `secret`, `password`, `token`,
/// `credential`, `api_key` or `private_key`, ignoring case.
pub fn redact_config_value<'a>(key: &str, value: &'a str) -> Cow<'a, str> {
    let key = key.to_ascii_lowercase();
    if SENSITIVE_KEY_PARTS.iter().any(|part| key.contains(part)) {
        Cow::Owned(format!("<redacted (length: {})>", value.len()))
    } else {
        Cow::Borrowed(value)
    }
}

/// Copies `pairs` for logging, with the values of sensitive keys replaced as
/// [`redact_config_value`] describes.
pub fn redact_key_value_pairs(pairs: &[KeyValuePair]) -> Vec<KeyValuePair> {
    pairs
        .iter()
        .map(|KeyValuePair { key, value }| KeyValuePair {
            key: key.clone(),
            value: value
                .as_deref()
                .map(|value| redact_config_value(key, value).to_string()),
        })
        .collect()
}

/// A graceful shutdown that outlasted its bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainTimedOut {
    /// The bound that elapsed.
    pub bound: Duration,
}

impl std::fmt::Display for DrainTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "graceful shutdown timed out after {} seconds",
            self.bound.as_secs()
        )
    }
}

impl std::error::Error for DrainTimedOut {}

impl From<DrainTimedOut> for BallistaError {
    fn from(timed_out: DrainTimedOut) -> Self {
        BallistaError::General(timed_out.to_string())
    }
}

/// Waits for the in-flight work of a shutting-down component to finish, but no
/// longer than `bound`. A zero `bound` waits for as long as it takes.
///
/// On timeout this stops waiting and returns [`DrainTimedOut`]. The work isn't
/// cancelled: a server's connection tasks are detached, so they keep running
/// until the process exits.
pub async fn drain_or_time_out<T>(
    work: impl Future<Output = T>,
    bound: Duration,
) -> std::result::Result<T, DrainTimedOut> {
    if bound.is_zero() {
        return Ok(work.await);
    }
    match tokio::time::timeout(bound, work).await {
        Ok(output) => Ok(output),
        Err(_) => {
            let timed_out = DrainTimedOut { bound };
            log::warn!("{timed_out}; no longer waiting for work still in flight");
            Err(timed_out)
        }
    }
}

/// Waits until `active` reports no work, polling it a few times a second.
/// `active` has to count work from the moment it is accepted, or an idle reading
/// can miss work that is still being registered.
pub async fn wait_until_idle(active: impl Fn() -> usize) {
    while active() != 0 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Calls a stop function once, either when asked to or, failing that, when
/// dropped, so a future that is abandoned still stops its server.
struct Stopper<S: FnMut()> {
    stop: S,
    stopped: bool,
}

impl<S: FnMut()> Stopper<S> {
    fn new(stop: S) -> Self {
        Self {
            stop,
            stopped: false,
        }
    }

    fn stop(&mut self) {
        if !self.stopped {
            self.stopped = true;
            (self.stop)();
        }
    }
}

impl<S: FnMut()> Drop for Stopper<S> {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Runs `serve` until it ends or `shutdown` completes, then winds it down
/// within `bound`:
///
/// 1. `begin_drain` runs, and the component keeps serving until the future it
///    returns completes. This is where it stops taking new work and waits for
///    what it has, for example with [`wait_until_idle`]. A component with
///    nothing to wait for returns a ready future.
/// 2. `stop` tells the server to stop accepting connections, and `serve` gets
///    the rest of `bound` to finish its in-flight requests.
///
/// `shutdown` is called again once the wind-down has started, and a second
/// signal ends it at once with an error.
///
/// `stop` runs once, however the call ends: when `serve` ends first, when
/// `bound` elapses, on a second signal, and when the future is dropped before
/// it finishes. A timeout is an error, see [`drain_or_time_out`].
pub async fn serve_until_shutdown<E, Idle, Signal>(
    serve: impl Future<Output = std::result::Result<(), E>>,
    mut shutdown: impl FnMut() -> Signal,
    begin_drain: impl FnOnce() -> Idle,
    stop: impl FnMut(),
    bound: Duration,
) -> Result<()>
where
    Idle: Future<Output = ()>,
    Signal: Future,
    BallistaError: From<E>,
{
    let mut stop = Stopper::new(stop);
    tokio::pin!(serve);

    tokio::select! {
        result = &mut serve => {
            stop.stop();
            return result.map_err(BallistaError::from);
        }
        _ = shutdown() => {}
    }

    let idle = begin_drain();
    let wind_down = async {
        tokio::select! {
            result = &mut serve => return result,
            () = idle => {}
        }
        stop.stop();
        serve.await
    };
    let outcome = tokio::select! {
        outcome = drain_or_time_out(wind_down, bound) => Some(outcome),
        _ = shutdown() => None,
    };
    // Reached after a timeout or a second signal too, when `wind_down` may
    // never have stopped the server.
    stop.stop();
    match outcome {
        Some(outcome) => outcome?.map_err(BallistaError::from),
        None => {
            log::warn!("Received a second shutdown signal; stopping now");
            Err(BallistaError::General(
                "shutdown cut short by a second signal".to_string(),
            ))
        }
    }
}

/// A signal that asks the process to shut down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// Ctrl-C.
    CtrlC,
    /// The platform's request to terminate, see [`terminate_signal`].
    Terminate,
}

impl std::fmt::Display for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ShutdownSignal::CtrlC => "ctrl-c event",
            ShutdownSignal::Terminate => "terminate signal",
        })
    }
}

/// Completes when the platform asks the process to terminate: SIGTERM on Unix,
/// and Ctrl-Break or closing the console on Windows. Other platforms never
/// complete.
///
/// This is what `docker stop` and the Kubernetes kubelet send. Fails if the
/// signal can't be listened for.
///
/// On Windows the process is only notified of the close, and Windows ends a
/// console process about 5 seconds after that, even if it is still shutting
/// down. A system shutdown notifies only services, which receive it as a
/// control request rather than a console event, so it doesn't complete this
/// future for a console process, although the console-shutdown event is
/// listened for in case it is delivered.
pub async fn terminate_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::terminate())?.recv().await;
        Ok(())
    }
    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_break, ctrl_close, ctrl_shutdown};
        let mut brk = ctrl_break()?;
        let mut close = ctrl_close()?;
        let mut shutdown = ctrl_shutdown()?;
        tokio::select! {
            _ = brk.recv() => {}
            _ = close.recv() => {}
            _ = shutdown.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    std::future::pending().await
}

/// Completes on Ctrl-C or [`terminate_signal`], saying which arrived.
///
/// If a signal can't be listened for, that branch never completes, rather than
/// signalling a shutdown at startup.
pub async fn shutdown_signal() -> ShutdownSignal {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("failed to listen for Ctrl-C: {e}");
            std::future::pending::<()>().await;
        }
    };
    let terminate = async {
        if let Err(e) = terminate_signal().await {
            error!("failed to listen for the terminate signal: {e}");
            std::future::pending::<()>().await;
        }
    };

    let signal = tokio::select! {
        () = ctrl_c => ShutdownSignal::CtrlC,
        () = terminate => ShutdownSignal::Terminate,
    };
    info!("Received {signal}, shutting down");
    signal
}

/// Extract a human-readable message from a `catch_unwind` panic payload.
/// `panic!("literal")` yields `&'static str`; `panic!("{x}")` yields
/// `String`. Anything else falls back to a placeholder.
pub fn panic_payload_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BinaryViewArray, StringViewArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    /// Strings too long to be inlined in their views, so every value lives in
    /// a data buffer.
    fn long_strings(n: usize) -> StringViewArray {
        StringViewArray::from_iter_values(
            (0..n).map(|i| format!("a string too long to inline {i:04}")),
        )
    }

    fn data_buffer_bytes<T: ByteViewType + ?Sized>(
        array: &GenericByteViewArray<T>,
    ) -> usize {
        array.data_buffers().iter().map(|b| b.len()).sum()
    }

    #[test]
    fn compacts_sparse_view_columns() {
        let binaries = BinaryViewArray::from_iter_values(
            (0..1000).map(|i| format!("a value too long to inline {i:04}").into_bytes()),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8View, false),
            Field::new("b", DataType::BinaryView, false),
        ]));
        // 2 rows backed by data buffers that hold all 1000 values
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(long_strings(1000)), Arc::new(binaries)],
        )
        .unwrap()
        .slice(10, 2);

        let compacted = compact_sparse_view_columns(batch.clone()).unwrap();

        assert_eq!(compacted, batch);
        assert_eq!(
            data_buffer_bytes(compacted.column(0).as_string_view()),
            batch.column(0).as_string_view().total_buffer_bytes_used()
        );
        assert_eq!(
            data_buffer_bytes(compacted.column(1).as_binary_view()),
            batch.column(1).as_binary_view().total_buffer_bytes_used()
        );
    }

    #[test]
    fn leaves_dense_view_columns_alone() {
        let strings: ArrayRef = Arc::new(long_strings(1000));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::clone(&strings)]).unwrap();

        let result = compact_sparse_view_columns(batch).unwrap();

        assert!(
            Arc::ptr_eq(result.column(0), &strings),
            "a dense column must not be copied"
        );
    }

    #[test]
    fn redacts_values_of_sensitive_keys() {
        for key in [
            "s3.secret_access_key",
            "s3.session_token",
            "secret_access_key",
            "custom.db_password",
            "custom.CLIENT_SECRET",
            "custom.credential.option.jwt",
            "custom.api_key",
            "custom.private_key",
        ] {
            assert_eq!(
                redact_config_value(key, "value"),
                "<redacted (length: 5)>",
                "{key}"
            );
        }
    }

    #[test]
    fn keeps_values_of_other_keys() {
        for key in [
            "s3.access_key_id",
            "s3.region",
            "ballista.job.name",
            "datafusion.execution.target_partitions",
        ] {
            assert_eq!(redact_config_value(key, "value"), "value", "{key}");
        }
    }

    #[test]
    fn redacts_key_value_pairs_without_changing_keys_or_unset_values() {
        let pair = |key: &str, value: Option<&str>| KeyValuePair {
            key: key.to_string(),
            value: value.map(str::to_string),
        };

        let redacted = redact_key_value_pairs(&[
            pair("s3.secret_access_key", Some("SECRET")),
            pair("s3.session_token", None),
            pair("s3.region", Some("us-east-1")),
        ]);

        assert_eq!(
            redacted,
            vec![
                pair("s3.secret_access_key", Some("<redacted (length: 6)>")),
                pair("s3.session_token", None),
                pair("s3.region", Some("us-east-1")),
            ]
        );
    }

    #[test]
    fn test_grpc_client_config_from_ballista_config() {
        let ballista_config = BallistaConfig::default();
        let grpc_config = GrpcClientConfig::from(&ballista_config);

        // Verify the conversion picks up the right values
        assert_eq!(
            grpc_config.connect_timeout_seconds,
            ballista_config.grpc_client_connect_timeout_seconds() as u64
        );
        assert_eq!(
            grpc_config.timeout_seconds,
            ballista_config.grpc_client_timeout_seconds() as u64
        );
        assert_eq!(
            grpc_config.tcp_keepalive_seconds,
            ballista_config.grpc_client_tcp_keepalive_seconds() as u64
        );
        assert_eq!(
            grpc_config.http2_keepalive_interval_seconds,
            ballista_config.grpc_client_http2_keepalive_interval_seconds() as u64
        );
    }

    #[test]
    fn test_create_grpc_client_endpoint_with_config() {
        let config = GrpcClientConfig {
            connect_timeout_seconds: 10,
            timeout_seconds: 30,
            tcp_keepalive_seconds: 1800,
            http2_keepalive_interval_seconds: 150,
            use_tls: false,
            max_message_size: 16 * 1024 * 1024,
            io_retries_times: 3,
            io_retry_wait_time_ms: 3000,
            initial_connection_window_size: 67108864,
            initial_stream_window_size: 16777216,
        };
        let result = create_grpc_client_endpoint("http://localhost:50051", Some(&config));
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_grpc_client_endpoint_invalid_url() {
        let result = create_grpc_client_endpoint("not a valid url", None);
        assert!(result.is_err());
    }

    /// The point of binding up front is that the port is reachable before
    /// anything is served on it, so a peer told to connect back cannot arrive
    /// too early.
    #[tokio::test]
    async fn test_create_grpc_server_incoming_binds_eagerly() {
        let incoming = create_grpc_server_incoming(
            "127.0.0.1:0".parse().unwrap(),
            &GrpcServerConfig::default(),
        )
        .expect("bind");
        let addr = incoming.local_addr().expect("local addr");

        // `incoming` is never handed to a server, and yet:
        tokio::net::TcpStream::connect(addr)
            .await
            .expect("port is already listening");
    }

    /// Binding eagerly means a port conflict surfaces here, as an error, rather
    /// than later inside the spawned server task.
    #[tokio::test]
    async fn test_create_grpc_server_incoming_port_in_use() {
        let first = create_grpc_server_incoming(
            "127.0.0.1:0".parse().unwrap(),
            &GrpcServerConfig::default(),
        )
        .expect("bind");
        let addr = first.local_addr().expect("local addr");

        let result = create_grpc_server_incoming(addr, &GrpcServerConfig::default());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_stalled_drain_times_out_at_the_bound() {
        let bound = Duration::from_millis(50);
        let result = drain_or_time_out(std::future::pending::<()>(), bound).await;
        assert_eq!(result, Err(DrainTimedOut { bound }));
    }

    #[tokio::test]
    async fn a_zero_bound_waits_for_the_drain_to_finish() {
        let slow = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            "finished after the wait"
        };
        let result = drain_or_time_out(slow, Duration::ZERO).await;
        assert_eq!(result, Ok("finished after the wait"));
    }

    #[tokio::test]
    async fn a_finished_drain_returns_its_output() {
        let result = drain_or_time_out(async { 7 }, Duration::from_secs(10)).await;
        assert_eq!(result, Ok(7));
    }

    #[test]
    fn a_timed_out_drain_converts_to_an_error_naming_the_bound() {
        let error = BallistaError::from(DrainTimedOut {
            bound: Duration::from_secs(10),
        });
        assert!(
            error
                .to_string()
                .contains("graceful shutdown timed out after 10 seconds"),
            "{error}"
        );
    }

    /// Sends SIGTERM to this process once `signal` is listening, and returns
    /// what `signal` completed with.
    #[cfg(unix)]
    async fn after_sigterm<T>(signal: impl Future<Output = T>) -> T {
        let mut signal = Box::pin(signal);
        // The first poll registers the listener, so the signal can't arrive
        // before it is ready and take the default action instead.
        assert!(futures::poll!(signal.as_mut()).is_pending());
        let status = std::process::Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .expect("kill runs");
        assert!(status.success());
        tokio::time::timeout(Duration::from_secs(10), signal)
            .await
            .expect("the signal must arrive")
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sigterm_completes_the_terminate_signal() {
        assert!(after_sigterm(terminate_signal()).await.is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sigterm_is_reported_as_the_shutdown_signal() {
        assert_eq!(
            after_sigterm(shutdown_signal()).await,
            ShutdownSignal::Terminate
        );
    }

    /// What a component did while `serve_until_shutdown` wound it down.
    #[derive(Default)]
    struct Events(std::sync::Mutex<Vec<&'static str>>);

    impl Events {
        fn record(&self, event: &'static str) {
            self.0.lock().unwrap().push(event);
        }

        fn all(&self) -> Vec<&'static str> {
            self.0.lock().unwrap().clone()
        }
    }

    /// A shutdown that signals once, like a user pressing Ctrl-C once.
    fn one_signal() -> impl FnMut() -> futures::future::BoxFuture<'static, ()> {
        let mut calls = 0;
        move || {
            calls += 1;
            let first = calls == 1;
            Box::pin(async move {
                if !first {
                    std::future::pending::<()>().await
                }
            })
        }
    }

    #[tokio::test]
    async fn a_failing_server_is_stopped_and_its_error_returned() {
        let events = Events::default();
        let result = serve_until_shutdown(
            async { Err::<(), _>(BallistaError::General("bind failed".to_string())) },
            std::future::pending::<()>,
            || async { events.record("drain") },
            || events.record("stop"),
            Duration::from_secs(10),
        )
        .await;

        assert!(matches!(result, Err(BallistaError::General(m)) if m == "bind failed"));
        assert_eq!(events.all(), vec!["stop"]);
    }

    #[tokio::test]
    async fn the_server_keeps_serving_until_the_drain_is_done() {
        let events = Events::default();
        let server = async {
            // Ends once told to stop, like a server draining its connections.
            while !events.all().contains(&"stop") {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<(), BallistaError>(())
        };
        let result = serve_until_shutdown(
            server,
            one_signal(),
            || async {
                events.record("drain started");
                tokio::time::sleep(Duration::from_millis(100)).await;
                events.record("drain done");
            },
            || events.record("stop"),
            Duration::from_secs(10),
        )
        .await;

        assert!(result.is_ok());
        let events = events.all();
        assert_eq!(events.first(), Some(&"drain started"));
        let stopped_at = events.iter().position(|e| *e == "stop").unwrap();
        let drained_at = events.iter().position(|e| *e == "drain done").unwrap();
        assert!(
            drained_at < stopped_at,
            "stopped before the drain: {events:?}"
        );
    }

    #[tokio::test]
    async fn a_drain_that_outlasts_the_bound_times_out_and_still_stops() {
        let events = Events::default();
        let result = serve_until_shutdown(
            std::future::pending::<std::result::Result<(), BallistaError>>(),
            one_signal(),
            std::future::pending::<()>,
            || events.record("stop"),
            Duration::from_millis(50),
        )
        .await;

        assert!(
            matches!(&result, Err(BallistaError::General(m)) if m.contains("timed out")),
            "{result:?}"
        );
        assert_eq!(events.all(), vec!["stop"]);
    }

    #[tokio::test]
    async fn dropping_the_future_stops_the_server() {
        let events = Events::default();
        let mut call = Box::pin(serve_until_shutdown(
            std::future::pending::<std::result::Result<(), BallistaError>>(),
            std::future::pending::<()>,
            || async {},
            || events.record("stop"),
            Duration::from_secs(10),
        ));
        assert!(futures::poll!(call.as_mut()).is_pending());
        assert!(events.all().is_empty(), "nothing has asked it to stop yet");

        drop(call);

        assert_eq!(events.all(), vec!["stop"]);
    }

    #[tokio::test]
    async fn a_second_signal_cuts_the_drain_short() {
        let events = Events::default();
        let signals = std::sync::atomic::AtomicUsize::new(0);
        // The first signal is there at once, the second a moment later.
        let shutdown = || {
            let nth = signals.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if nth > 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            serve_until_shutdown(
                std::future::pending::<std::result::Result<(), BallistaError>>(),
                shutdown,
                std::future::pending::<()>,
                || events.record("stop"),
                Duration::from_secs(60),
            ),
        )
        .await
        .expect("the second signal must not wait for the bound");

        assert!(
            matches!(&result, Err(BallistaError::General(m)) if m.contains("second signal")),
            "{result:?}"
        );
        assert_eq!(events.all(), vec!["stop"]);
    }

    #[tokio::test]
    async fn idle_is_the_first_reading_of_no_active_work() {
        let polls = std::sync::Mutex::new(vec![2, 1, 0, 5].into_iter());
        let active = || polls.lock().unwrap().next().unwrap_or(0);
        tokio::time::timeout(Duration::from_secs(10), wait_until_idle(active))
            .await
            .expect("must become idle");
        assert_eq!(polls.lock().unwrap().next(), Some(5), "stops at the zero");
    }
}
