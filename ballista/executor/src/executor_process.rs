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

//! Ballista Executor Process

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, UNIX_EPOCH};

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::registry::BallistaFunctionRegistry;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{error, info, warn};
use tempfile::TempDir;
use tokio::fs::DirEntry;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::{fs, time};
use uuid::Uuid;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;

use ballista_core::config::{LogRotationPolicy, TaskSchedulingPolicy};
use ballista_core::error::BallistaError;
use ballista_core::extension::{EndpointOverrideFn, SessionConfigExt};
use ballista_core::serde::protobuf::executor_resource::Resource;
use ballista_core::serde::protobuf::executor_status::Status;
use ballista_core::serde::protobuf::{
    ExecutorRegistration, ExecutorResource, ExecutorSpecification, ExecutorStatus,
    ExecutorStoppedParams, HeartBeatParams, scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::serde::{
    BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use ballista_core::utils::{
    GrpcClientConfig, GrpcServerConfig, create_grpc_client_endpoint, create_grpc_server,
    default_config_producer, get_time_before,
};
use ballista_core::{BALLISTA_VERSION, ConfigProducer, RuntimeProducer};

use crate::execution_engine::ExecutionEngine;
use crate::executor::{Executor, TasksDrainedFuture};
use crate::executor_server::TERMINATING;
use crate::flight_service::BallistaFlightService;
use crate::metrics::LoggingMetricsCollector;
use crate::shutdown::Shutdown;
use crate::shutdown::ShutdownNotifier;
use crate::{ArrowFlightServerProvider, terminate};
use crate::{execution_loop, executor_server};

/// Configuration for the executor process.
///
/// This struct contains all settings needed to run an executor, including
/// network configuration, scheduler connection details, resource limits,
/// and optional overrides for customizing executor behavior.
pub struct ExecutorProcessConfig {
    /// Local IP address for binding executor services.
    pub bind_host: String,
    /// External hostname/IP advertised to other components for connectivity.
    pub external_host: Option<String>,
    /// Port for the Arrow Flight service.
    pub port: u16,
    /// Port for the executor's gRPC service.
    pub grpc_port: u16,
    /// Hostname of the scheduler to connect to.
    pub scheduler_host: String,
    /// Port of the scheduler's gRPC service.
    pub scheduler_port: u16,
    /// Timeout in seconds for establishing scheduler connection.
    pub scheduler_connect_timeout_seconds: u16,
    /// Maximum number of concurrent tasks this executor can run.
    pub concurrent_tasks: usize,
    /// Task scheduling policy (pull-staged or push-staged).
    pub task_scheduling_policy: TaskSchedulingPolicy,
    /// Directory for storing log files.
    pub log_dir: Option<String>,
    /// Directory for storing temporary shuffle data.
    pub work_dir: Option<String>,
    /// Log level configuration for specific modules.
    pub special_mod_log_level: String,
    /// Whether to include thread info in log output.
    pub print_thread_info: bool,
    /// Log file rotation policy.
    pub log_rotation_policy: LogRotationPolicy,
    /// Time-to-live in seconds for job data before cleanup.
    pub job_data_ttl_seconds: u64,
    /// Interval in seconds between cleanup runs.
    pub job_data_clean_up_interval_seconds: u64,
    /// The maximum size of a decoded message
    pub grpc_max_decoding_message_size: u32,
    /// The maximum size of an encoded message
    pub grpc_max_encoding_message_size: u32,
    /// gRPC server timeout configuration
    pub grpc_server_config: GrpcServerConfig,
    /// Interval in seconds between heartbeat messages.
    pub executor_heartbeat_interval_seconds: u64,
    /// Optional execution engine to use to execute physical plans, will default to
    /// DataFusion if none is provided.
    pub override_execution_engine: Option<Arc<dyn ExecutionEngine>>,
    /// Overrides default function registry
    pub override_function_registry: Option<Arc<BallistaFunctionRegistry>>,
    /// [RuntimeProducer] override option
    pub override_runtime_producer: Option<RuntimeProducer>,
    /// [ConfigProducer] override option
    pub override_config_producer: Option<ConfigProducer>,
    /// [PhysicalExtensionCodec] override option
    pub override_logical_codec: Option<Arc<dyn LogicalExtensionCodec>>,
    /// [PhysicalExtensionCodec] override option
    pub override_physical_codec: Option<Arc<dyn PhysicalExtensionCodec>>,
    /// [ArrowFlightServerProvider] implementation override option
    pub override_arrow_flight_service: Option<Arc<ArrowFlightServerProvider>>,
    /// Override function for customizing gRPC client endpoints before they are used
    pub override_create_grpc_client_endpoint: Option<EndpointOverrideFn>,
}

impl ExecutorProcessConfig {
    /// Generates a prefix for log file names based on executor host and port.
    pub fn log_file_name_prefix(&self) -> String {
        format!(
            "executor_{}_{}",
            self.external_host
                .clone()
                .unwrap_or_else(|| "localhost".to_string()),
            self.port
        )
    }
}

impl Default for ExecutorProcessConfig {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".into(),
            external_host: None,
            port: 50051,
            grpc_port: 50052,
            scheduler_host: "localhost".into(),
            scheduler_port: 50050,
            scheduler_connect_timeout_seconds: 0,
            concurrent_tasks: std::thread::available_parallelism().unwrap().get(),
            task_scheduling_policy: Default::default(),
            log_dir: None,
            work_dir: None,
            special_mod_log_level: "INFO".into(),
            print_thread_info: true,
            log_rotation_policy: Default::default(),
            job_data_ttl_seconds: 604800,
            job_data_clean_up_interval_seconds: 0,
            grpc_max_decoding_message_size: 16777216,
            grpc_max_encoding_message_size: 16777216,
            grpc_server_config: Default::default(),
            executor_heartbeat_interval_seconds: 60,
            override_execution_engine: None,
            override_function_registry: None,
            override_runtime_producer: None,
            override_config_producer: None,
            override_logical_codec: None,
            override_physical_codec: None,
            override_arrow_flight_service: None,
            override_create_grpc_client_endpoint: None,
        }
    }
}

/// Starts the main executor process with the given configuration.
///
/// This function initializes all executor components including:
/// - Arrow Flight service for shuffle data transfer
/// - gRPC service for task management
/// - Scheduler connection and registration
/// - Heartbeat and cleanup background tasks
///
/// The function blocks until the executor receives a shutdown signal
/// (SIGTERM, Ctrl+C, or stop request from scheduler).
pub async fn start_executor_process(
    opt: Arc<ExecutorProcessConfig>,
) -> ballista_core::error::Result<()> {
    let addr = format!("{}:{}", opt.bind_host, opt.port);
    let address = addr.parse().map_err(|e: std::net::AddrParseError| {
        BallistaError::Configuration(e.to_string())
    })?;

    let scheduler_host = opt.scheduler_host.clone();
    let scheduler_port = opt.scheduler_port;
    let scheduler_url = format!("http://{scheduler_host}:{scheduler_port}");

    let work_dir = if let Some(work_dir) = opt.work_dir.clone() {
        work_dir
    } else if let Some(temp_dir) = TempDir::new()?.path().to_str().map(ToOwned::to_owned)
    {
        temp_dir
    } else {
        return Err(BallistaError::Configuration(
            "Unable to bind work dir".to_string(),
        ));
    };

    let concurrent_tasks = if opt.concurrent_tasks == 0 {
        // use all available cores if no concurrency level is specified
        std::thread::available_parallelism().unwrap().get()
    } else {
        opt.concurrent_tasks
    };

    // assign this executor an unique ID
    let executor_id = Uuid::new_v4().to_string();
    info!("Executor starting ... (Datafusion Ballista {BALLISTA_VERSION})");
    info!("Executor id: {executor_id}");
    info!("Executor working directory: {work_dir}");
    info!("Executor number of concurrent tasks: {concurrent_tasks}");

    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        host: opt.external_host.clone(),
        port: opt.port as u32,
        grpc_port: opt.grpc_port as u32,
        specification: Some(ExecutorSpecification {
            resources: vec![ExecutorResource {
                resource: Some(Resource::TaskSlots(concurrent_tasks as u32)),
            }],
        }),
    };

    // put them to session config
    let metrics_collector = Arc::new(LoggingMetricsCollector::default());
    let config_producer = opt
        .override_config_producer
        .clone()
        .unwrap_or_else(|| Arc::new(default_config_producer));

    let wd = work_dir.clone();
    let runtime_producer: RuntimeProducer =
        opt.override_runtime_producer.clone().unwrap_or_else(|| {
            Arc::new(move |_| {
                let runtime_env = RuntimeEnvBuilder::new()
                    .with_temp_file_path(wd.clone())
                    .build()?;
                Ok(Arc::new(runtime_env))
            })
        });

    let logical = opt
        .override_logical_codec
        .clone()
        .unwrap_or_else(|| Arc::new(BallistaLogicalExtensionCodec::default()));

    let physical = opt
        .override_physical_codec
        .clone()
        .unwrap_or_else(|| Arc::new(BallistaPhysicalExtensionCodec::default()));

    let default_codec: BallistaCodec<
        datafusion_proto::protobuf::LogicalPlanNode,
        datafusion_proto::protobuf::PhysicalPlanNode,
    > = BallistaCodec::new(logical, physical);

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime_producer,
        config_producer,
        opt.override_function_registry.clone().unwrap_or_default(),
        metrics_collector,
        concurrent_tasks,
        opt.override_execution_engine.clone(),
    ));

    let connect_timeout = opt.scheduler_connect_timeout_seconds as u64;
    let session_config = (executor.config_producer)();
    let ballista_config = session_config.ballista_config();
    let grpc_config = GrpcClientConfig::from(&ballista_config);
    let connection = if connect_timeout == 0 {
        let mut endpoint = create_grpc_client_endpoint(scheduler_url, Some(&grpc_config))
            .map_err(|_| {
                BallistaError::GrpcConnectionError(
                    "Could not create endpoint to scheduler".to_string(),
                )
            })?;

        if let Some(ref override_fn) = opt.override_create_grpc_client_endpoint {
            endpoint = override_fn(endpoint).map_err(|_| {
                BallistaError::GrpcConnectionError(
                    "Failed to apply endpoint override".to_string(),
                )
            })?;
        }

        endpoint.connect().await.map_err(|_| {
            BallistaError::GrpcConnectionError(
                "Could not connect to scheduler".to_string(),
            )
        })
    } else {
        // this feature was added to support docker-compose so that we can have the executor
        // wait for the scheduler to start, or at least run for 10 seconds before failing so
        // that docker-compose's restart policy will restart the container.
        let start_time = Instant::now().elapsed().as_secs();
        let mut x = None;
        while x.is_none()
            && Instant::now().elapsed().as_secs() - start_time < connect_timeout
        {
            match create_grpc_client_endpoint(scheduler_url.clone(), Some(&grpc_config)) {
                Ok(mut endpoint) => {
                    if let Some(ref override_fn) =
                        opt.override_create_grpc_client_endpoint
                    {
                        match override_fn(endpoint) {
                            Ok(overridden_endpoint) => endpoint = overridden_endpoint,
                            Err(e) => {
                                warn!(
                                    "Failed to apply endpoint override to scheduler at {scheduler_url} ({e}); retrying ..."
                                );
                                tokio::time::sleep(time::Duration::from_millis(500))
                                    .await;
                                continue;
                            }
                        }
                    }

                    match endpoint.connect().await {
                        Ok(connection) => {
                            info!("Connected to scheduler at {scheduler_url}");
                            x = Some(connection);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to connect to scheduler at {scheduler_url} ({e}); retrying ..."
                            );
                            tokio::time::sleep(time::Duration::from_millis(500)).await;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to create endpoint to scheduler at {scheduler_url} ({e}); retrying ..."
                    );
                    tokio::time::sleep(time::Duration::from_millis(500)).await;
                }
            }
        }
        match x {
            Some(conn) => Ok(conn),
            _ => Err(BallistaError::General(format!(
                "Timed out attempting to connect to scheduler at {scheduler_url}"
            ))),
        }
    }?;

    let mut scheduler = SchedulerGrpcClient::new(connection)
        .max_encoding_message_size(opt.grpc_max_encoding_message_size as usize)
        .max_decoding_message_size(opt.grpc_max_decoding_message_size as usize);

    let scheduler_policy = opt.task_scheduling_policy;
    let job_data_ttl_seconds = opt.job_data_ttl_seconds;

    // Graceful shutdown notification
    let shutdown_notification = ShutdownNotifier::new();

    if opt.job_data_clean_up_interval_seconds > 0 {
        let mut interval_time =
            time::interval(Duration::from_secs(opt.job_data_clean_up_interval_seconds));

        let mut shuffle_cleaner_shutdown = shutdown_notification.subscribe_for_shutdown();
        let shuffle_cleaner_complete = shutdown_notification.shutdown_complete_tx.clone();

        tokio::spawn(async move {
            // As long as the shutdown notification has not been received
            while !shuffle_cleaner_shutdown.is_shutdown() {
                tokio::select! {
                    _ = interval_time.tick() => {
                            if let Err(e) = clean_shuffle_data_loop(&work_dir, job_data_ttl_seconds).await
                        {
                            error!("Ballista executor fail to clean_shuffle_data {e:?}")
                        }
                        },
                    _ = shuffle_cleaner_shutdown.recv() => {
                        if let Err(e) = clean_all_shuffle_data(&work_dir).await
                        {
                            error!("Ballista executor fail to clean_shuffle_data {e:?}")
                        } else {
                            info!("Shuffle data cleaned.");
                        }
                        drop(shuffle_cleaner_complete);
                        return;
                    }
                };
            }
        });
    }

    let mut service_handlers: FuturesUnordered<JoinHandle<Result<(), BallistaError>>> =
        FuturesUnordered::new();

    // Channels used to receive stop requests from Executor grpc service.
    let (stop_send, mut stop_recv) = mpsc::channel::<bool>(10);

    match scheduler_policy {
        TaskSchedulingPolicy::PushStaged => {
            service_handlers.push(
                //If there is executor registration error during startup, return the error and stop early.
                executor_server::startup(
                    scheduler.clone(),
                    opt.clone(),
                    executor.clone(),
                    default_codec,
                    stop_send,
                    &shutdown_notification,
                )
                .await?,
            );
        }
        _ => {
            service_handlers.push(tokio::spawn(execution_loop::poll_loop(
                scheduler.clone(),
                executor.clone(),
                default_codec,
            )));
        }
    };
    let shutdown = shutdown_notification.subscribe_for_shutdown();
    let override_flight = opt.override_arrow_flight_service.clone();

    service_handlers.push(match override_flight {
        None => {
            info!("Starting built-in arrow flight service");
            flight_server_task(
                address,
                shutdown,
                opt.grpc_max_encoding_message_size as usize,
                opt.grpc_max_decoding_message_size as usize,
                opt.grpc_server_config.clone(),
            )
            .await
        }
        Some(flight_provider) => {
            info!("Starting custom, user provided, arrow flight service");
            (flight_provider)(address, shutdown, opt.grpc_server_config.clone())
        }
    });

    let tasks_drained = TasksDrainedFuture(executor);

    // Concurrently run the service checking and listen for the `shutdown` signal and wait for the stop request coming.
    // The check_services runs until an error is encountered, so under normal circumstances, this `select!` statement runs
    // until the `shutdown` signal is received or a stop request is coming.
    let (notify_scheduler, stop_reason) = tokio::select! {
        service_val = check_services(&mut service_handlers) => {
            let msg = format!("executor services stopped with reason {service_val:?}");
            info!("{msg:?}");
            (true, msg)
        },
        _ = signal::ctrl_c() => {
            let msg = "executor received ctrl-c event.".to_string();
             info!("{msg:?}");
            (true, msg)
        },
        _ = terminate::sig_term() => {
            let msg = "executor received terminate signal.".to_string();
             info!("{msg:?}");
            (true, msg)
        },
        _ = stop_recv.recv() => {
            (false, "".to_string())
        },
    };

    // Set status to fenced
    info!("setting executor to TERMINATING status");
    TERMINATING.store(true, Ordering::Release);

    if notify_scheduler {
        // Send a heartbeat to update status of executor to `Fenced`. This should signal to the
        // scheduler to no longer schedule tasks on this executor
        if let Err(error) = scheduler
            .heart_beat_from_executor(HeartBeatParams {
                executor_id: executor_id.clone(),
                metrics: vec![],
                status: Some(ExecutorStatus {
                    status: Some(Status::Terminating(String::default())),
                }),
                metadata: Some(ExecutorRegistration {
                    id: executor_id.clone(),
                    host: opt.external_host.clone(),
                    port: opt.port as u32,
                    grpc_port: opt.grpc_port as u32,
                    specification: Some(ExecutorSpecification {
                        resources: vec![ExecutorResource {
                            resource: Some(Resource::TaskSlots(concurrent_tasks as u32)),
                        }],
                    }),
                }),
            })
            .await
        {
            error!("error sending heartbeat with fenced status: {error:?}");
        }

        // TODO we probably don't need a separate rpc call for this....
        if let Err(error) = scheduler
            .executor_stopped(ExecutorStoppedParams {
                executor_id,
                reason: stop_reason,
            })
            .await
        {
            error!("ExecutorStopped grpc failed: {error:?}");
        }

        // Wait for tasks to drain
        tasks_drained.await;
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let ShutdownNotifier {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = shutdown_notification;

    // When `notify_shutdown` is dropped, all components which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all related components to finish the shutdown processing.
    let _ = shutdown_complete_rx.recv().await;
    info!("Executor stopped.");
    Ok(())
}

// Arrow flight service
async fn flight_server_task(
    address: SocketAddr,
    mut grpc_shutdown: Shutdown,
    max_encoding_message_size: usize,
    max_decoding_message_size: usize,
    grpc_server_config: GrpcServerConfig,
) -> JoinHandle<Result<(), BallistaError>> {
    tokio::spawn(async move {
        info!(
            "Built-in arrow flight server listening on: {address:?} max_encoding_size: {max_encoding_message_size} max_decoding_size: {max_decoding_message_size}"
        );

        let server_future = create_grpc_server(&grpc_server_config)
            .add_service(
                FlightServiceServer::new(BallistaFlightService::new())
                    .max_decoding_message_size(max_decoding_message_size)
                    .max_encoding_message_size(max_encoding_message_size),
            )
            .serve_with_shutdown(address, grpc_shutdown.recv());

        server_future.await.map_err(|e| {
            error!("Could not start built-in arrow flight server.");
            BallistaError::TonicError(e)
        })
    })
}

// Check the status of long running services
async fn check_services(
    service_handlers: &mut FuturesUnordered<JoinHandle<Result<(), BallistaError>>>,
) -> Result<(), BallistaError> {
    loop {
        match service_handlers.next().await {
            Some(result) => match result {
                // React to "inner_result", i.e. propagate as BallistaError
                Ok(inner_result) => match inner_result {
                    Ok(()) => (),
                    Err(e) => return Err(e),
                },
                // React to JoinError
                Err(e) => return Err(BallistaError::TokioError(e)),
            },
            None => {
                info!("service handlers are all done with their work!");
                return Ok(());
            }
        }
    }
}

/// This function will be scheduled periodically for cleanup the job shuffle data left on the executor.
/// Only directories will be checked cleaned.
async fn clean_shuffle_data_loop(
    work_dir: &str,
    seconds: u64,
) -> ballista_core::error::Result<()> {
    let mut dir = fs::read_dir(work_dir).await?;
    let mut to_deleted = Vec::new();
    while let Some(child) = dir.next_entry().await? {
        if let Ok(metadata) = child.metadata().await {
            let child_path = child.path().into_os_string();
            // only delete the job dir
            if metadata.is_dir() {
                match satisfy_dir_ttl(child, seconds).await {
                    Err(e) => {
                        error!(
                            "Fail to check ttl for the directory {child_path:?} due to {e:?}"
                        )
                    }
                    Ok(false) => to_deleted.push(child_path),
                    Ok(_) => {}
                }
            } else {
                warn!(
                    "{child_path:?} under the working directory is a not a directory and will be ignored when doing cleanup"
                )
            }
        } else {
            error!("Fail to get metadata for file {:?}", child.path())
        }
    }
    info!(
        "The directories {to_deleted:?} that have not been modified for {seconds:?} seconds so that they will be deleted"
    );
    for del in to_deleted {
        if let Err(e) = fs::remove_dir_all(&del).await {
            error!("Fail to remove the directory {del:?} due to {e}");
        }
    }
    Ok(())
}

/// This function will clean up all shuffle data on this executor
async fn clean_all_shuffle_data(work_dir: &str) -> ballista_core::error::Result<()> {
    let mut dir = fs::read_dir(work_dir).await?;
    let mut to_deleted = Vec::new();
    while let Some(child) = dir.next_entry().await? {
        if let Ok(metadata) = child.metadata().await {
            // only delete the job dir
            if metadata.is_dir() {
                to_deleted.push(child.path().into_os_string())
            }
        } else {
            error!("Can not get metadata from file: {child:?}")
        }
    }

    info!("The work_dir {:?} will be deleted", &to_deleted);
    for del in to_deleted {
        if let Err(e) = fs::remove_dir_all(&del).await {
            error!("Fail to remove the directory {del:?} due to {e}");
        }
    }
    Ok(())
}

/// Remove a job directory under work_dir.
/// Used by both push-based (gRPC handler) and pull-based (poll loop) cleanup.
pub(crate) async fn remove_job_dir(
    work_dir: &str,
    job_id: &str,
) -> ballista_core::error::Result<()> {
    let work_path = PathBuf::from(&work_dir);
    let job_path = work_path.join(job_id);

    // Match legacy behavior: If the job path does not exist, return OK
    if !tokio::fs::try_exists(&job_path).await.unwrap_or(false) {
        return Ok(());
    }

    if !job_path.is_dir() {
        return Err(BallistaError::General(format!(
            "Path {job_path:?} is not a directory"
        )));
    } else if !is_subdirectory(job_path.as_path(), work_path.as_path()) {
        return Err(BallistaError::General(format!(
            "Path {job_path:?} is not a subdirectory of {work_path:?}"
        )));
    }

    info!("Remove data for job {:?}", job_id);

    tokio::fs::remove_dir_all(&job_path).await.map_err(|e| {
        BallistaError::General(format!("Failed to remove {job_path:?} due to {e}"))
    })?;

    Ok(())
}

// Check whether the path is the subdirectory of the base directory
pub(crate) fn is_subdirectory(path: &Path, base_path: &Path) -> bool {
    let path = match path.canonicalize() {
        Ok(p) => p,
        Err(_) => return false,
    };
    let base = match base_path.canonicalize() {
        Ok(b) => b,
        Err(_) => return false,
    };

    path.parent().is_some_and(|p| p.starts_with(&base))
}

/// Checks if a directory should be retained based on its TTL.
///
/// Returns `Ok(true)` if the directory or any of its contents have been
/// modified within the TTL period, meaning it should be kept.
/// Returns `Ok(false)` if the directory is older than the TTL and can be deleted.
pub async fn satisfy_dir_ttl(
    dir: DirEntry,
    ttl_seconds: u64,
) -> ballista_core::error::Result<bool> {
    let cutoff = get_time_before(ttl_seconds);

    let mut to_check = vec![dir];
    while let Some(dir) = to_check.pop() {
        // Check the ttl for the current directory first
        if dir
            .metadata()
            .await?
            .modified()?
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
            > cutoff
        {
            return Ok(true);
        }
        // Check its children
        let mut children = fs::read_dir(dir.path()).await?;
        while let Some(child) = children.next_entry().await? {
            let metadata = child.metadata().await?;
            if metadata.is_dir() {
                to_check.push(child);
            } else if metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs()
                > cutoff
            {
                return Ok(true);
            };
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use crate::executor_process::is_subdirectory;
    use std::path::{Path, PathBuf};

    use super::clean_shuffle_data_loop;
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_executor_clean_up() {
        let work_dir = TempDir::new().unwrap();
        let work_dir_clone = work_dir.path().to_owned();
        let job_dir = work_dir_clone.join("job_id");
        let file_path = job_dir.as_path().join("tmp.csv");
        let data = "Jorge,2018-12-13T12:12:10.011Z\n\
                    Andrew,2018-11-13T17:11:10.011Z";
        fs::create_dir(job_dir).unwrap();
        File::create(&file_path)
            .expect("creating temp file")
            .write_all(data.as_bytes())
            .expect("writing data");

        let work_dir_cloned = work_dir_clone.clone();

        let count1 = fs::read_dir(&work_dir_clone).unwrap().count();
        assert_eq!(count1, 1);
        let mut handles = vec![];
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            clean_shuffle_data_loop(work_dir_cloned.to_str().unwrap(), 1)
                .await
                .unwrap();
        }));
        futures::future::join_all(handles).await;
        let count2 = fs::read_dir(work_dir_clone).unwrap().count();
        assert_eq!(count2, 0);
    }

    #[tokio::test]
    async fn test_arrow_flight_provider_ergonomics() {
        let config = crate::executor_process::ExecutorProcessConfig {
            override_arrow_flight_service: Some(std::sync::Arc::new(
                move |address, mut grpc_shutdown, ballista_config| {
                    tokio::spawn(async move {
                        log::info!(
                            "custom arrow flight server listening on: {address:?}"
                        );

                        let server_future = ballista_core::utils::create_grpc_server(
                            &ballista_config,
                        )
                        .add_service(
                            arrow_flight::flight_service_server::FlightServiceServer::new(
                                crate::flight_service::BallistaFlightService::new(),
                            ),
                        )
                        .serve_with_shutdown(address, grpc_shutdown.recv());

                        server_future.await.map_err(|e| {
                            log::error!("Could not start built-in arrow flight server.");
                            ballista_core::error::BallistaError::TonicError(e)
                        })
                    })
                },
            )),
            ..Default::default()
        };

        assert!(config.override_arrow_flight_service.is_some());
    }

    #[tokio::test]
    async fn test_is_subdirectory() {
        let base_dir = TempDir::new().unwrap();
        let base_dir = base_dir.path();

        // Normal correct one
        {
            let job_path = prepare_testing_job_directory(base_dir, "job_a");
            assert!(is_subdirectory(&job_path, base_dir));
        }

        // Empty job id
        {
            let job_path = prepare_testing_job_directory(base_dir, "");
            assert!(!is_subdirectory(&job_path, base_dir));

            let job_path = prepare_testing_job_directory(base_dir, ".");
            assert!(!is_subdirectory(&job_path, base_dir));
        }

        // Malicious job id
        {
            let job_path = prepare_testing_job_directory(base_dir, "..");
            assert!(!is_subdirectory(&job_path, base_dir));
        }
    }
    fn prepare_testing_job_directory(base_dir: &Path, job_id: &str) -> PathBuf {
        let mut path = base_dir.to_path_buf();
        path.push(job_id);
        if !path.exists() {
            fs::create_dir(&path).unwrap();
        }
        path
    }
}
