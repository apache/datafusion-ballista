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

//! Executor gRPC server for push-based task scheduling.
//!
//! This module implements the executor-side gRPC service that receives tasks
//! from the scheduler in push-based scheduling mode. It handles task execution,
//! heartbeat communication, and status reporting.

use ballista_core::BALLISTA_VERSION;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use log::{debug, error, info, warn};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use ballista_core::error::BallistaError;
use ballista_core::extension::EndpointOverrideFn;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{
    CancelTasksParams, CancelTasksResult, ExecutorMetric, ExecutorStatus,
    HeartBeatParams, LaunchMultiTaskParams, LaunchMultiTaskResult, LaunchTaskParams,
    LaunchTaskResult, RegisterExecutorParams, RemoveJobDataParams, RemoveJobDataResult,
    StopExecutorParams, StopExecutorResult, TaskStatus, UpdateTaskStatusParams,
    executor_grpc_server::{ExecutorGrpc, ExecutorGrpcServer},
    executor_metric, executor_status,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::serde::scheduler::PartitionId;
use ballista_core::serde::scheduler::TaskDefinition;

use ballista_core::serde::scheduler::from_proto::{
    get_task_definition, get_task_definition_vec,
};
use ballista_core::utils::{create_grpc_client_endpoint, create_grpc_server};

use dashmap::DashMap;
use datafusion::execution::TaskContext;
use datafusion_proto::{logical_plan::AsLogicalPlan, physical_plan::AsExecutionPlan};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

use crate::cpu_bound_executor::DedicatedExecutor;
use crate::executor::Executor;
use crate::executor_process::{ExecutorProcessConfig, remove_job_dir};
use crate::shutdown::ShutdownNotifier;
use crate::{TaskExecutionTimes, as_task_status};

type ServerHandle = JoinHandle<Result<(), BallistaError>>;
type SchedulerClients = Arc<DashMap<String, SchedulerGrpcClient<Channel>>>;

/// Wrap TaskDefinition with its curator scheduler id for task update to its specific curator scheduler later
#[derive(Debug)]
struct CuratorTaskDefinition {
    scheduler_id: String,
    task: TaskDefinition,
}

/// Wrap TaskStatus with its curator scheduler id for task update to its specific curator scheduler later
#[derive(Debug)]
struct CuratorTaskStatus {
    scheduler_id: String,
    task_status: TaskStatus,
}

/// Starts the executor gRPC server and registers with the scheduler.
///
/// This function initializes the push-based task scheduling infrastructure:
/// - Creates the executor gRPC server for receiving tasks
/// - Registers the executor with the scheduler
/// - Starts the heartbeat loop
/// - Starts the task runner pool
///
/// Returns a handle to the server task that can be awaited for completion.
pub async fn startup<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    mut scheduler: SchedulerGrpcClient<Channel>,
    config: Arc<ExecutorProcessConfig>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
    stop_send: mpsc::Sender<bool>,
    shutdown_noti: &ShutdownNotifier,
) -> Result<ServerHandle, BallistaError> {
    let channel_buf_size = executor.concurrent_tasks * 50;
    let (tx_task, rx_task) = mpsc::channel::<CuratorTaskDefinition>(channel_buf_size);
    let (tx_task_status, rx_task_status) =
        mpsc::channel::<CuratorTaskStatus>(channel_buf_size);

    let executor_server = ExecutorServer::new(
        scheduler.clone(),
        executor.clone(),
        ExecutorEnv {
            tx_task,
            tx_task_status,
            tx_stop: stop_send,
        },
        codec,
        config.grpc_max_encoding_message_size as usize,
        config.grpc_max_decoding_message_size as usize,
        config.override_create_grpc_client_endpoint.clone(),
    );

    // 1. Start executor grpc service
    let server = {
        let executor_meta = executor.metadata.clone();
        let addr = format!("{}:{}", config.bind_host, executor_meta.grpc_port);
        let addr = addr.parse().unwrap();
        let grpc_server_config = config.grpc_server_config.clone();

        info!(
            "Ballista v{BALLISTA_VERSION} Rust Executor Grpc Server listening on {addr:?}"
        );
        let server = ExecutorGrpcServer::new(executor_server.clone())
            .max_encoding_message_size(config.grpc_max_encoding_message_size as usize)
            .max_decoding_message_size(config.grpc_max_decoding_message_size as usize);
        let mut grpc_shutdown = shutdown_noti.subscribe_for_shutdown();
        tokio::spawn(async move {
            let shutdown_signal = grpc_shutdown.recv();
            let grpc_server_future = create_grpc_server(&grpc_server_config)
                .add_service(server)
                .serve_with_shutdown(addr, shutdown_signal);
            grpc_server_future.await.map_err(|e| {
                error!("Tonic error, Could not start Executor Grpc Server.");
                BallistaError::TonicError(e)
            })
        })
    };

    // 2. Do executor registration
    // TODO the executor registration should happen only after the executor grpc server started.
    let executor_server = Arc::new(executor_server);
    match register_executor(&mut scheduler, executor.clone()).await {
        Ok(_) => {
            info!("Executor registration succeed");
        }
        Err(error) => {
            error!("Executor registration failed due to: {error}");
            // abort the Executor Grpc Future
            server.abort();
            return Err(error);
        }
    };

    // 3. Start Heartbeater loop
    {
        let heartbeater = Heartbeater::new(executor_server.clone());
        heartbeater.start(shutdown_noti, config.executor_heartbeat_interval_seconds);
    }

    // 4. Start TaskRunnerPool loop
    {
        let task_runner_pool = TaskRunnerPool::new(executor_server.clone());
        task_runner_pool.start(rx_task, rx_task_status, shutdown_noti);
    }

    Ok(server)
}

#[allow(clippy::clone_on_copy)]
async fn register_executor(
    scheduler: &mut SchedulerGrpcClient<Channel>,
    executor: Arc<Executor>,
) -> Result<(), BallistaError> {
    let result = scheduler
        .register_executor(RegisterExecutorParams {
            metadata: Some(executor.metadata.clone()),
        })
        .await?;
    if result.into_inner().success {
        Ok(())
    } else {
        Err(BallistaError::General(
            "Executor registration failed!!!".to_owned(),
        ))
    }
}

/// The executor's gRPC server that handles incoming task requests.
///
/// This server implements the ExecutorGrpc trait and manages task execution,
/// cancellation, and status reporting for push-based scheduling.
#[derive(Clone)]
pub struct ExecutorServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    /// Timestamp when the server was started (milliseconds since epoch).
    _start_time: u128,
    /// The executor instance that runs tasks.
    executor: Arc<Executor>,
    /// Environment containing task communication channels.
    executor_env: ExecutorEnv,
    /// Codec for serializing/deserializing execution plans.
    codec: BallistaCodec<T, U>,
    /// gRPC client for the scheduler this executor registered with.
    scheduler_to_register: SchedulerGrpcClient<Channel>,
    /// Cache of scheduler clients for communicating with multiple schedulers.
    schedulers: SchedulerClients,
    /// Maximum size for outgoing gRPC messages.
    grpc_max_encoding_message_size: usize,
    /// Maximum size for incoming gRPC messages.
    grpc_max_decoding_message_size: usize,
    override_create_grpc_client_endpoint: Option<EndpointOverrideFn>,
}

#[derive(Clone)]
struct ExecutorEnv {
    /// Receive `TaskDefinition` from rpc then send to CPU bound tasks pool `dedicated_executor`.
    tx_task: mpsc::Sender<CuratorTaskDefinition>,
    /// Receive `TaskStatus` from CPU bound tasks pool `dedicated_executor` then use rpc send back to scheduler.
    tx_task_status: mpsc::Sender<CuratorTaskStatus>,
    /// Receive stop executor request from rpc.
    tx_stop: mpsc::Sender<bool>,
}

unsafe impl Sync for ExecutorEnv {}

/// Global flag indicating whether the executor is terminating. This should be
/// set to `true` when the executor receives a shutdown signal
pub static TERMINATING: AtomicBool = AtomicBool::new(false);

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorServer<T, U> {
    fn new(
        scheduler_to_register: SchedulerGrpcClient<Channel>,
        executor: Arc<Executor>,
        executor_env: ExecutorEnv,
        codec: BallistaCodec<T, U>,
        grpc_max_encoding_message_size: usize,
        grpc_max_decoding_message_size: usize,
        override_create_grpc_client_endpoint: Option<EndpointOverrideFn>,
    ) -> Self {
        Self {
            _start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            executor,
            executor_env,
            codec,
            scheduler_to_register,
            schedulers: Default::default(),
            grpc_max_encoding_message_size,
            grpc_max_decoding_message_size,
            override_create_grpc_client_endpoint,
        }
    }

    async fn get_scheduler_client(
        &self,
        scheduler_id: &str,
    ) -> Result<SchedulerGrpcClient<Channel>, BallistaError> {
        let scheduler = self.schedulers.get(scheduler_id).map(|value| value.clone());
        // If channel does not exist, create a new one
        if let Some(scheduler) = scheduler {
            Ok(scheduler)
        } else {
            let scheduler_url = format!("http://{scheduler_id}");
            let mut endpoint = create_grpc_client_endpoint(scheduler_url, None)?;

            if let Some(ref override_fn) = self.override_create_grpc_client_endpoint {
                endpoint = override_fn(endpoint).map_err(|e| {
                    BallistaError::GrpcConnectionError(format!(
                        "Failed to customize endpoint for scheduler {scheduler_id}: {e}"
                    ))
                })?;
            }

            let connection = endpoint.connect().await?;
            let scheduler = SchedulerGrpcClient::new(connection)
                .max_encoding_message_size(self.grpc_max_encoding_message_size)
                .max_decoding_message_size(self.grpc_max_decoding_message_size);

            {
                self.schedulers
                    .insert(scheduler_id.to_owned(), scheduler.clone());
            }

            Ok(scheduler)
        }
    }

    /// 1. First Heartbeat to its registration scheduler, if successful then return; else go next.
    /// 2. Heartbeat to schedulers which has launching tasks to this executor until one succeeds
    async fn heartbeat(&self) {
        let status = if TERMINATING.load(Ordering::Acquire) {
            executor_status::Status::Terminating(String::default())
        } else {
            executor_status::Status::Active(String::default())
        };

        let heartbeat_params = HeartBeatParams {
            executor_id: self.executor.metadata.id.clone(),
            metrics: self.get_executor_metrics(),
            status: Some(ExecutorStatus {
                status: Some(status),
            }),
            metadata: Some(self.executor.metadata.clone()),
        };
        let mut scheduler = self.scheduler_to_register.clone();
        match scheduler
            .heart_beat_from_executor(heartbeat_params.clone())
            .await
        {
            Ok(_) => {
                return;
            }
            Err(e) => {
                warn!(
                    "Fail to update heartbeat to its registration scheduler due to {e:?}"
                );
            }
        };

        for mut item in self.schedulers.iter_mut() {
            let scheduler_id = item.key().clone();
            let scheduler = item.value_mut();

            match scheduler
                .heart_beat_from_executor(heartbeat_params.clone())
                .await
            {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    warn!(
                        "Fail to update heartbeat to scheduler {scheduler_id} due to {e:?}"
                    );
                }
            }
        }
    }

    /// This method should not return Err. If task fails, a failure task status should be sent
    /// to the channel to notify the scheduler.
    async fn run_task(&self, task_identity: String, curator_task: CuratorTaskDefinition) {
        let start_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        info!("Start to run task {task_identity}");
        let task = curator_task.task;

        let task_id = task.task_id;
        let job_id = task.job_id;
        let stage_id = task.stage_id;
        let stage_attempt_num = task.stage_attempt_num;
        let partition_id = task.partition_id;
        let plan = task.plan;

        let part = PartitionId {
            job_id: job_id.clone(),
            stage_id,
            partition_id,
        };

        let query_stage_exec = self
            .executor
            .execution_engine
            .create_query_stage_exec(
                job_id.clone(),
                stage_id,
                plan,
                &self.executor.work_dir,
            )
            .unwrap();

        let task_context = {
            let function_registry = task.function_registry;
            let runtime = self.executor.produce_runtime(&task.session_config).unwrap();

            Arc::new(TaskContext::new(
                Some(task_identity.clone()),
                task.session_id,
                task.session_config,
                function_registry.scalar_functions.clone(),
                function_registry.aggregate_functions.clone(),
                function_registry.window_functions.clone(),
                runtime,
            ))
        };

        info!("Start to execute shuffle write for task {task_identity}");

        let execution_result = self
            .executor
            .execute_query_stage(
                task_id,
                part.clone(),
                query_stage_exec.clone(),
                task_context,
            )
            .await;
        info!("Done with task {task_identity}");
        debug!("Statistics: {execution_result:?}");

        let plan_metrics = query_stage_exec.collect_plan_metrics();
        let operator_metrics = plan_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()
            .ok();
        let executor_id = &self.executor.metadata.id;

        let end_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let task_execution_times = TaskExecutionTimes {
            launch_time: task.launch_time,
            start_exec_time,
            end_exec_time,
        };

        let task_status = as_task_status(
            execution_result,
            executor_id.clone(),
            task_id,
            stage_attempt_num,
            part,
            operator_metrics,
            task_execution_times,
        );

        let scheduler_id = curator_task.scheduler_id;
        let task_status_sender = self.executor_env.tx_task_status.clone();
        task_status_sender
            .send(CuratorTaskStatus {
                scheduler_id,
                task_status,
            })
            .await
            .unwrap();
    }

    // TODO populate with real metrics
    fn get_executor_metrics(&self) -> Vec<ExecutorMetric> {
        let available_memory = ExecutorMetric {
            metric: Some(executor_metric::Metric::AvailableMemory(u64::MAX)),
        };
        let executor_metrics = vec![available_memory];
        executor_metrics
    }
}

/// Heartbeater will run forever until a shutdown notification received.
struct Heartbeater<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    executor_server: Arc<ExecutorServer<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> Heartbeater<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    fn start(
        &self,
        shutdown_noti: &ShutdownNotifier,
        executor_heartbeat_interval_seconds: u64,
    ) {
        let executor_server = self.executor_server.clone();
        let mut heartbeat_shutdown = shutdown_noti.subscribe_for_shutdown();
        let heartbeat_complete = shutdown_noti.shutdown_complete_tx.clone();
        tokio::spawn(async move {
            info!("Starting heartbeater to send heartbeat the scheduler periodically");
            // As long as the shutdown notification has not been received
            while !heartbeat_shutdown.is_shutdown() {
                executor_server.heartbeat().await;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(executor_heartbeat_interval_seconds)) => {},
                    _ = heartbeat_shutdown.recv() => {
                        info!("Stop heartbeater");
                        drop(heartbeat_complete);
                        return;
                    }
                };
            }
        });
    }
}

/// There are two loop(future) running separately in tokio runtime.
/// First is for sending back task status to scheduler
/// Second is for receiving task from scheduler and run.
/// The two loops will run forever until a shutdown notification received.
struct TaskRunnerPool<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    executor_server: Arc<ExecutorServer<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskRunnerPool<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    fn start(
        &self,
        mut rx_task: mpsc::Receiver<CuratorTaskDefinition>,
        mut rx_task_status: mpsc::Receiver<CuratorTaskStatus>,
        shutdown_noti: &ShutdownNotifier,
    ) {
        //1. loop for task status reporting
        let executor_server = self.executor_server.clone();
        let mut tasks_status_shutdown = shutdown_noti.subscribe_for_shutdown();
        let tasks_status_complete = shutdown_noti.shutdown_complete_tx.clone();
        tokio::spawn(async move {
            info!("Starting the task status reporter");
            // As long as the shutdown notification has not been received
            while !tasks_status_shutdown.is_shutdown() {
                let mut curator_task_status_map: HashMap<String, Vec<TaskStatus>> =
                    HashMap::new();
                // First try to fetch task status from the channel in *blocking* mode
                let maybe_task_status: Option<CuratorTaskStatus> = tokio::select! {
                     task_status = rx_task_status.recv() => task_status,
                    _ = tasks_status_shutdown.recv() => {
                        info!("Stop task status reporting loop");
                        drop(tasks_status_complete);
                        return;
                    }
                };

                let mut fetched_task_num = 0usize;
                if let Some(task_status) = maybe_task_status {
                    let task_status_vec = curator_task_status_map
                        .entry(task_status.scheduler_id)
                        .or_default();
                    task_status_vec.push(task_status.task_status);
                    fetched_task_num += 1;
                } else {
                    info!("Channel is closed and will exit the task status report loop.");
                    drop(tasks_status_complete);
                    return;
                }

                // Then try to fetch by non-blocking mode to fetch as much finished tasks as possible
                loop {
                    match rx_task_status.try_recv() {
                        Ok(task_status) => {
                            let task_status_vec = curator_task_status_map
                                .entry(task_status.scheduler_id)
                                .or_default();
                            task_status_vec.push(task_status.task_status);
                            fetched_task_num += 1;
                        }
                        Err(TryRecvError::Empty) => {
                            info!("Fetched {fetched_task_num} tasks status to report");
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            info!(
                                "Channel is closed and will exit the task status report loop"
                            );
                            drop(tasks_status_complete);
                            return;
                        }
                    }
                }

                for (scheduler_id, tasks_status) in curator_task_status_map.into_iter() {
                    match executor_server.get_scheduler_client(&scheduler_id).await {
                        Ok(mut scheduler) => {
                            if let Err(e) = scheduler
                                .update_task_status(UpdateTaskStatusParams {
                                    executor_id: executor_server
                                        .executor
                                        .metadata
                                        .id
                                        .clone(),
                                    task_status: tasks_status.clone(),
                                })
                                .await
                            {
                                error!(
                                    "Fail to update tasks {tasks_status:?} due to {e:?}"
                                );
                            }
                        }
                        Err(e) => {
                            error!(
                                "Fail to connect to scheduler {scheduler_id} due to {e:?}"
                            );
                        }
                    }
                }
            }
        });

        //2. loop for task fetching and running
        let executor_server = self.executor_server.clone();
        let mut task_runner_shutdown = shutdown_noti.subscribe_for_shutdown();
        let task_runner_complete = shutdown_noti.shutdown_complete_tx.clone();
        tokio::spawn(async move {
            info!("Starting the task runner pool");

            // Use a dedicated executor for CPU bound tasks so that the main tokio
            // executor can still answer requests even when under load
            let dedicated_executor = DedicatedExecutor::new(
                "task_runner",
                executor_server.executor.concurrent_tasks,
            );

            // As long as the shutdown notification has not been received
            while !task_runner_shutdown.is_shutdown() {
                let maybe_task: Option<CuratorTaskDefinition> = tokio::select! {
                     task = rx_task.recv() => task,
                    _ = task_runner_shutdown.recv() => {
                        info!("Stop the task runner pool");
                        drop(task_runner_complete);
                        return;
                    }
                };
                if let Some(curator_task) = maybe_task {
                    let task_identity = format!(
                        "TID {} {}/{}.{}/{}.{}",
                        &curator_task.task.task_id,
                        &curator_task.task.job_id,
                        &curator_task.task.stage_id,
                        &curator_task.task.stage_attempt_num,
                        &curator_task.task.partition_id,
                        &curator_task.task.task_attempt_num,
                    );
                    info!("Received task {:?}", &task_identity);

                    let server = executor_server.clone();
                    dedicated_executor.spawn(async move {
                        server.run_task(task_identity.clone(), curator_task).await;
                    });
                } else {
                    info!("Channel is closed and will exit the task receive loop");
                    drop(task_runner_complete);
                    return;
                }
            }
        });
    }
}

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorGrpc
    for ExecutorServer<T, U>
{
    async fn launch_task(
        &self,
        request: Request<LaunchTaskParams>,
    ) -> Result<Response<LaunchTaskResult>, Status> {
        let LaunchTaskParams {
            tasks,
            scheduler_id,
        } = request.into_inner();
        let task_sender = self.executor_env.tx_task.clone();
        for task in tasks {
            task_sender
                .send(CuratorTaskDefinition {
                    scheduler_id: scheduler_id.clone(),
                    task: get_task_definition(
                        task,
                        self.executor.runtime_producer.clone(),
                        self.executor.produce_config(),
                        self.executor.function_registry.scalar_functions.clone(),
                        self.executor.function_registry.aggregate_functions.clone(),
                        self.executor.function_registry.window_functions.clone(),
                        self.codec.clone(),
                    )
                    .map_err(|e| Status::invalid_argument(format!("{e}")))?,
                })
                .await
                .unwrap();
        }
        Ok(Response::new(LaunchTaskResult { success: true }))
    }

    /// by this interface, it can reduce the deserialization cost for multiple tasks
    /// belong to the same job stage running on the same one executor
    async fn launch_multi_task(
        &self,
        request: Request<LaunchMultiTaskParams>,
    ) -> Result<Response<LaunchMultiTaskResult>, Status> {
        let LaunchMultiTaskParams {
            multi_tasks,
            scheduler_id,
        } = request.into_inner();
        let task_sender = self.executor_env.tx_task.clone();
        for multi_task in multi_tasks {
            let multi_task: Vec<TaskDefinition> = get_task_definition_vec(
                multi_task,
                self.executor.runtime_producer.clone(),
                self.executor.produce_config(),
                self.executor.function_registry.scalar_functions.clone(),
                self.executor.function_registry.aggregate_functions.clone(),
                self.executor.function_registry.window_functions.clone(),
                self.codec.clone(),
            )
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
            for task in multi_task {
                task_sender
                    .send(CuratorTaskDefinition {
                        scheduler_id: scheduler_id.clone(),
                        task,
                    })
                    .await
                    .unwrap();
            }
        }
        Ok(Response::new(LaunchMultiTaskResult { success: true }))
    }

    async fn stop_executor(
        &self,
        request: Request<StopExecutorParams>,
    ) -> Result<Response<StopExecutorResult>, Status> {
        let stop_request = request.into_inner();
        if stop_request.executor_id != self.executor.metadata.id {
            warn!(
                "The executor id {} in request is different from {}. The stop request will be ignored",
                stop_request.executor_id, self.executor.metadata.id
            );
            return Ok(Response::new(StopExecutorResult {}));
        }
        let stop_reason = stop_request.reason;
        let force = stop_request.force;
        info!(
            "Receive stop executor request, reason: {:?}, force {:?}",
            stop_reason, force
        );
        let stop_sender = self.executor_env.tx_stop.clone();
        stop_sender.send(force).await.unwrap();
        Ok(Response::new(StopExecutorResult {}))
    }

    async fn cancel_tasks(
        &self,
        request: Request<CancelTasksParams>,
    ) -> Result<Response<CancelTasksResult>, Status> {
        let task_infos = request.into_inner().task_infos;
        info!("Cancelling tasks for {:?}", task_infos);

        let mut cancelled = true;

        for task in task_infos {
            if let Err(e) = self
                .executor
                .cancel_task(
                    task.task_id as usize,
                    task.job_id,
                    task.stage_id as usize,
                    task.partition_id as usize,
                )
                .await
            {
                error!("Error cancelling task: {e:?}");
                cancelled = false;
            }
        }

        Ok(Response::new(CancelTasksResult { cancelled }))
    }

    async fn remove_job_data(
        &self,
        request: Request<RemoveJobDataParams>,
    ) -> Result<Response<RemoveJobDataResult>, Status> {
        let job_id = request.into_inner().job_id;

        remove_job_dir(&self.executor.work_dir, &job_id)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        Ok(Response::new(RemoveJobDataResult {}))
    }
}

#[cfg(test)]
mod test {}
