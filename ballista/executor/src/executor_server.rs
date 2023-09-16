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

use ballista_core::BALLISTA_VERSION;
use datafusion::config::{ConfigOptions, Extensions};
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};
use tracing::{debug, error, info, warn};

use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::{
    executor_grpc_server::{ExecutorGrpc, ExecutorGrpcServer},
    executor_metric, executor_status,
    scheduler_grpc_client::SchedulerGrpcClient,
    CancelTasksParams, CancelTasksResult, ExecutorMetric, ExecutorStatus,
    HeartBeatParams, LaunchTaskParams, LaunchTaskResult, RegisterExecutorParams,
    RemoveJobDataParams, RemoveJobDataResult, SchedulerLostParams, StopExecutorParams,
    StopExecutorResult, TaskStatus, UpdateTaskStatusParams,
};

use ballista_core::serde::scheduler::TaskDefinition;
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::create_grpc_server;
use dashmap::DashMap;
use datafusion::execution::context::TaskContext;
use datafusion::prelude::SessionConfig;
use datafusion_proto::{
    logical_plan::AsLogicalPlan,
    physical_plan::{from_proto::parse_protobuf_hash_partitioning, AsExecutionPlan},
};
use lazy_static::lazy_static;
use tokio::fs;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

use crate::circuit_breaker::client::{
    CircuitBreakerClient, CircuitBreakerMetadataExtension,
};
use crate::cpu_bound_executor::DedicatedExecutor;
use crate::execution_engine::QueryStageExecutor;
use crate::executor::Executor;
use crate::scheduler_client_registry::SchedulerClientRegistry;
use crate::shutdown::ShutdownNotifier;
use crate::{as_task_status, TaskExecutionTimes};

// Set the max gRPC message size to 64 MiB. This is quite large
// but we have to send execution plans over gRPC and they can be large.
const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub type ServerHandle = JoinHandle<Result<(), BallistaError>>;
type SchedulerClients = Arc<DashMap<String, SchedulerGrpcClient<Channel>>>;

/// Wrap TaskDefinition with its curator scheduler id for task update to its specific curator scheduler later
#[derive(Debug)]
struct CuratorTaskDefinition {
    scheduler_id: String,
    plan: Vec<u8>,
    tasks: Vec<TaskDefinition>,
}

/// Wrap TaskStatus with its curator scheduler id for task update to its specific curator scheduler later
#[derive(Debug)]
struct CuratorTaskStatus {
    scheduler_id: String,
    task_status: Vec<TaskStatus>,
}

pub async fn startup<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    mut scheduler: SchedulerGrpcClient<Channel>,
    bind_host: String,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
    stop_send: mpsc::Sender<bool>,
    shutdown_noti: &ShutdownNotifier,
    default_extensions: Extensions,
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
        default_extensions,
    );

    // 1. Start executor grpc service
    let server = {
        let executor_meta = executor.metadata.clone();
        let addr = format!("{}:{}", bind_host, executor_meta.grpc_port);
        let addr = addr.parse().unwrap();

        info!(
            executor_id = executor_meta.id,
            socket_address = %addr,
            version = BALLISTA_VERSION,
            "starting executor server",
        );

        let server = ExecutorGrpcServer::new(executor_server.clone())
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE);

        let mut grpc_shutdown = shutdown_noti.subscribe_for_shutdown();
        tokio::spawn(async move {
            let shutdown_signal = grpc_shutdown.recv();
            let grpc_server_future = create_grpc_server()
                .add_service(server)
                .serve_with_shutdown(addr, shutdown_signal);
            grpc_server_future.await.map_err(|e| {
                error!(error = %e, "failed to start executor gRPC server");
                BallistaError::TonicError(e)
            })
        })
    };

    // 2. Do executor registration
    // TODO the executor registration should happen only after the executor grpc server started.
    let executor_server = Arc::new(executor_server);
    match register_executor(&mut scheduler, executor.clone()).await {
        Ok(_) => {
            info!("executor registration succeed");
        }
        Err(error) => {
            error!(error = %error, "executor registration failed");
            // abort the Executor Grpc Future
            server.abort();
            return Err(error);
        }
    };

    // 3. Start Heartbeater loop
    {
        let heartbeater = Heartbeater::new(executor_server.clone());
        heartbeater.start(shutdown_noti);
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
            "executor registration failed".to_owned(),
        ))
    }
}

#[derive(Clone)]
pub struct ExecutorServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    _start_time: u128,
    executor: Arc<Executor>,
    executor_env: ExecutorEnv,
    codec: BallistaCodec<T, U>,
    scheduler_to_register: SchedulerGrpcClient<Channel>,
    schedulers: SchedulerClients,
    default_extensions: Extensions,
    circuit_breaker_client: Arc<CircuitBreakerClient>,
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
        default_extensions: Extensions,
    ) -> Self {
        let schedulers: SchedulerClients = Arc::new(DashMap::new());

        let circuit_breaker_client = Arc::new(CircuitBreakerClient::new(
            Duration::from_secs(1),
            schedulers,
        ));

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
            default_extensions,
            circuit_breaker_client,
        }
    }

    async fn get_scheduler_client(
        &self,
        scheduler_id: &str,
    ) -> Result<SchedulerGrpcClient<Channel>, BallistaError> {
        self.schedulers
            .get_or_create_scheduler_client(scheduler_id)
            .await
    }

    /// 1. First Heartbeat to its registration scheduler, if successful then return; else go next.
    /// 2. Heartbeat to schedulers which has launching tasks to this executor until one succeeds
    async fn heartbeat(&self) {
        let status = if TERMINATING.load(Ordering::Acquire) {
            info!(
                executor_id = self.executor.metadata.id,
                "sending heartbeat with TERMINATING status"
            );
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
                        executor_id = self.executor.metadata.id,
                        error = %e,
                        "failed to send heartbeat to registration scheduler"
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
                        scheduler_id,
                        error = %e,
                        "failed to send heartbeat to scheduler"
                    );
                }
            }
        }
    }

    async fn decode_task_inner(
        &self,
        curator_task: &TaskDefinition,
        plan: &[u8],
    ) -> Result<Arc<dyn QueryStageExecutor>, BallistaError> {
        let task = curator_task;
        let task_identity = task_identity(task);
        let mut config =
            ConfigOptions::new().with_extensions(self.default_extensions.clone());
        for (k, v) in &task.props {
            config.set(k, v)?;
        }

        let job_id = task.job_id.clone();
        let stage_id = task.stage_id;
        let attempt = task.stage_attempt_num;

        let circuit_breaker_metadata = CircuitBreakerMetadataExtension {
            job_id: job_id.clone(),
            stage_id: stage_id as u32,
            attempt_number: attempt as u32,
        };

        let session_config = SessionConfig::from(config)
            .with_extension(self.circuit_breaker_client.clone())
            .with_extension(Arc::new(circuit_breaker_metadata));

        let mut task_scalar_functions = HashMap::new();
        let mut task_aggregate_functions = HashMap::new();
        for scalar_func in self.executor.scalar_functions.clone() {
            task_scalar_functions.insert(scalar_func.0, scalar_func.1);
        }
        for agg_func in self.executor.aggregate_functions.clone() {
            task_aggregate_functions.insert(agg_func.0, agg_func.1);
        }

        let task_context = Arc::new(TaskContext::new(
            Some(task_identity),
            task.session_id.clone(),
            session_config,
            task_scalar_functions,
            task_aggregate_functions,
            self.executor.window_functions.clone(),
            self.executor.runtime.clone(),
        ));

        let plan = U::try_decode(plan).and_then(|proto| {
            proto.try_into_physical_plan(
                task_context.deref(),
                &self.executor.runtime,
                self.codec.physical_extension_codec(),
            )
        })?;

        Ok(self.executor.execution_engine.create_query_stage_exec(
            job_id,
            stage_id,
            plan,
            &self.executor.work_dir,
        )?)
    }

    async fn decode_task(
        &self,
        scheduler_id: String,
        curator_task: TaskDefinition,
        plan: &[u8],
    ) -> Result<Arc<dyn QueryStageExecutor>, BallistaError> {
        match self.decode_task_inner(&curator_task, plan).await {
            Ok(exec) => Ok(exec),
            Err(err) => {
                let start_exec_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                let task_status_sender = self.executor_env.tx_task_status.clone();

                let job_id = curator_task.job_id.clone();
                let stage_id = curator_task.stage_id;
                let task_id = curator_task.task_id;
                let partitions = curator_task.partitions.clone();
                let attempt = curator_task.stage_attempt_num;
                let launch_time = curator_task.launch_time;

                error!(job_id, stage_id, task_id, scheduler_id, executor_id = self.executor.metadata.id, error = %err, "failed decode task");
                let task_status = as_task_status(
                    Err(BallistaError::Internal(format!(
                        "failed to decode task: {err}"
                    ))),
                    self.executor.metadata.id.clone(),
                    job_id.clone(),
                    stage_id,
                    task_id,
                    partitions,
                    attempt,
                    None,
                    TaskExecutionTimes {
                        launch_time,
                        start_exec_time,
                        end_exec_time: start_exec_time,
                    },
                );

                if let Err(_e) = task_status_sender
                    .send(CuratorTaskStatus {
                        scheduler_id,
                        task_status: vec![task_status],
                    })
                    .await
                {
                    error!(
                        executor_id = self.executor.metadata.id,
                        job_id,
                        stage_id,
                        task_id,
                        "failed to send task status, channel closed"
                    );
                }

                Err(err)
            }
        }
    }

    async fn run_task_inner(
        &self,
        start_exec_time: u64,
        task_identity: &str,
        scheduler_id: String,
        curator_task: TaskDefinition,
        query_stage_exec: Arc<dyn QueryStageExecutor>,
    ) -> Result<TaskStatus, BallistaError> {
        info!(task_identity, scheduler_id, "running task");
        let task = curator_task;
        let task_props = task.props;

        let mut config =
            ConfigOptions::new().with_extensions(self.default_extensions.clone());
        for (k, v) in task_props {
            config.set(&k, &v)?;
        }

        let job_id = task.job_id;
        let stage_id = task.stage_id;
        let attempt = task.stage_attempt_num;

        let circuit_breaker_metadata = CircuitBreakerMetadataExtension {
            job_id: job_id.clone(),
            stage_id: stage_id as u32,
            attempt_number: attempt as u32,
        };

        let session_config = SessionConfig::from(config)
            .with_extension(self.circuit_breaker_client.clone())
            .with_extension(Arc::new(circuit_breaker_metadata));

        if let Err(e) = self
            .circuit_breaker_client
            .register_scheduler(task_identity.to_owned(), scheduler_id.clone())
        {
            error!(executor_id = self.executor.metadata.id,task_identity, scheduler_id, error = %e, "failed to register circuit breaker");
        }

        let mut task_scalar_functions = HashMap::new();
        let mut task_aggregate_functions = HashMap::new();
        // TODO combine the functions from Executor's functions and TaskDefintion's function resources
        for scalar_func in self.executor.scalar_functions.clone() {
            task_scalar_functions.insert(scalar_func.0, scalar_func.1);
        }
        for agg_func in self.executor.aggregate_functions.clone() {
            task_aggregate_functions.insert(agg_func.0, agg_func.1);
        }

        let session_id = task.session_id;
        let runtime = self.executor.runtime.clone();
        let task_context = Arc::new(TaskContext::new(
            Some(task_identity.to_string()),
            session_id,
            session_config,
            task_scalar_functions,
            task_aggregate_functions,
            self.executor.window_functions.clone(),
            runtime.clone(),
        ));

        let shuffle_output_partitioning = parse_protobuf_hash_partitioning(
            task.output_partitioning.as_ref(),
            task_context.as_ref(),
            query_stage_exec.schema().as_ref(),
        )?;

        let task_id = task.task_id;
        let partitions = task.partitions;

        info!(
            executor_id = self.executor.metadata.id,
            task_identity, "executing shuffle write"
        );

        let execution_result = self
            .executor
            .execute_query_stage(
                &job_id,
                task_id,
                task_id,
                &partitions,
                query_stage_exec.clone(),
                task_context,
                shuffle_output_partitioning,
            )
            .await;

        info!(
            executor_id = self.executor.metadata.id,
            task_identity, "completed task"
        );
        debug!("Statistics: {:?}", execution_result);

        let plan_metrics = query_stage_exec.collect_plan_metrics();
        let operator_metrics = match plan_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()
        {
            Ok(metrics) => metrics,
            Err(e) => {
                error!(executor_id = self.executor.metadata.id, error = %e, "error serializing task metrics");
                vec![]
            }
        };

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

        Ok(as_task_status(
            execution_result,
            executor_id.clone(),
            job_id,
            stage_id,
            task_id,
            partitions,
            attempt,
            Some(operator_metrics),
            task_execution_times,
        ))
    }

    async fn run_task(
        &self,
        task_identity: &str,
        scheduler_id: String,
        curator_task: TaskDefinition,
        query_stage_exec: Arc<dyn QueryStageExecutor>,
    ) {
        let start_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let task_status_sender = self.executor_env.tx_task_status.clone();

        let job_id = curator_task.job_id.clone();
        let stage_id = curator_task.stage_id;
        let task_id = curator_task.task_id;
        let partitions = curator_task.partitions.clone();
        let attempt = curator_task.stage_attempt_num;
        let launch_time = curator_task.launch_time;

        match self
            .run_task_inner(
                start_exec_time,
                task_identity,
                scheduler_id.clone(),
                curator_task,
                query_stage_exec,
            )
            .await
        {
            Ok(task_status) => {
                if let Err(_e) = task_status_sender
                    .send(CuratorTaskStatus {
                        scheduler_id,
                        task_status: vec![task_status],
                    })
                    .await
                {
                    error!(
                        executor_id = self.executor.metadata.id,
                        task_identity,
                        job_id,
                        stage_id,
                        task_id,
                        "failed to send task status, channel closed"
                    );
                }
            }
            Err(err) => {
                let executor_id = self.executor.metadata.id.clone();

                error!(task_identity, job_id, stage_id, task_id, executor_id, scheduler_id, error = %err, "failed to run task");
                let task_status = as_task_status(
                    Err(err),
                    executor_id,
                    job_id.clone(),
                    stage_id,
                    task_id,
                    partitions,
                    attempt,
                    None,
                    TaskExecutionTimes {
                        launch_time,
                        start_exec_time,
                        end_exec_time: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                    },
                );

                if let Err(_e) = task_status_sender
                    .send(CuratorTaskStatus {
                        scheduler_id,
                        task_status: vec![task_status],
                    })
                    .await
                {
                    error!(
                        executor_id = self.executor.metadata.id,
                        task_identity,
                        job_id,
                        stage_id,
                        task_id,
                        "failed to send task status, channel closed"
                    );
                }
            }
        }
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

    fn start(&self, shutdown_noti: &ShutdownNotifier) {
        let executor_server = self.executor_server.clone();
        let mut heartbeat_shutdown = shutdown_noti.subscribe_for_shutdown();
        let heartbeat_complete = shutdown_noti.shutdown_complete_tx.clone();
        tokio::spawn(async move {
            let executor_id = &executor_server.executor.metadata.id;
            info!(executor_id, "starting executor heartbeat");
            // As long as the shutdown notification has not been received
            while !heartbeat_shutdown.is_shutdown() {
                executor_server.heartbeat().await;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(60000)) => {},
                    _ = heartbeat_shutdown.recv() => {
                        info!(executor_id, "stopping executor heartbeat");
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

fn task_identity(task: &TaskDefinition) -> String {
    format!(
        "TID {} {}/{}.{}/{:?}",
        &task.task_id,
        &task.job_id,
        &task.stage_id,
        &task.stage_attempt_num,
        &task.partitions,
    )
}

lazy_static! {
    static ref STATUS_RETRY_POLICY: Vec<Duration> = vec![Duration::from_millis(10); 3];
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskRunnerPool<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    async fn send_status_update(
        scheduler: &mut SchedulerGrpcClient<Channel>,
        executor_id: &str,
        status: Vec<TaskStatus>,
    ) -> Result<(), Status> {
        let mut retries = STATUS_RETRY_POLICY.iter();

        loop {
            if let Err(e) = scheduler
                .update_task_status(UpdateTaskStatusParams {
                    executor_id: executor_id.to_owned(),
                    task_status: status.clone(),
                    executor_terminating: TERMINATING.load(Ordering::Acquire),
                })
                .await
            {
                warn!(executor_id, error = %e, "failed to update task status");
                if let Some(interval) = retries.next() {
                    if matches!(
                        e.code(),
                        Code::Unknown
                            | Code::Unavailable
                            | Code::Internal
                            | Code::Aborted
                    ) {
                        // we can retry, sleep for specified interval and retry
                        tokio::time::sleep(*interval).await;
                    } else {
                        // error is not retryable, return error
                        return Err(e);
                    }
                } else {
                    // retries are exhausted, return error
                    return Err(e);
                }
            } else {
                return Ok(());
            }
        }
    }

    async fn send_scheduler_lost(
        scheduler: &mut SchedulerGrpcClient<Channel>,
        executor_id: &str,
        scheduler_id: &str,
        status: Vec<TaskStatus>,
    ) -> Result<(), Status> {
        let mut retries = STATUS_RETRY_POLICY.iter();

        loop {
            if let Err(e) = scheduler
                .scheduler_lost(SchedulerLostParams {
                    executor_id: executor_id.to_owned(),
                    scheduler_id: scheduler_id.to_owned(),
                    task_status: status.clone(),
                })
                .await
            {
                warn!(
                    executor_id, scheduler_id, error = %e, "failed to send scheduler lost"
                );
                if let Some(interval) = retries.next() {
                    if matches!(
                        e.code(),
                        Code::Unknown
                            | Code::Unavailable
                            | Code::Internal
                            | Code::Aborted
                    ) {
                        // we can retry, sleep for specified interval and retry
                        tokio::time::sleep(*interval).await;
                    } else {
                        // error is not retryable, return error
                        return Err(e);
                    }
                } else {
                    // retries are exhausted, return error
                    return Err(e);
                }
            } else {
                return Ok(());
            }
        }
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
            let executor_id = &executor_server.executor.metadata.id;
            info!(executor_id, "starting task status reporter");
            // As long as the shutdown notification has not been received
            while !tasks_status_shutdown.is_shutdown() {
                let mut curator_task_status_map: HashMap<String, Vec<TaskStatus>> =
                    HashMap::new();
                // First try to fetch task status from the channel in *blocking* mode
                let maybe_task_status: Option<CuratorTaskStatus> = tokio::select! {
                     task_status = rx_task_status.recv() => task_status,
                    _ = tasks_status_shutdown.recv() => {
                        info!(executor_id, "stopping task status reporter");
                        drop(tasks_status_complete);
                        return;
                    }
                };

                let mut fetched_task_num = 0usize;
                if let Some(task_status) = maybe_task_status {
                    let task_status_vec = curator_task_status_map
                        .entry(task_status.scheduler_id)
                        .or_insert_with(Vec::new);
                    task_status_vec.extend(task_status.task_status);
                    fetched_task_num += 1;
                } else {
                    info!(
                        executor_id,
                        "channel is closed, exiting task status report loop."
                    );
                    drop(tasks_status_complete);
                    return;
                }

                // Then try to fetch by non-blocking mode to fetch as much finished tasks as possible
                loop {
                    match rx_task_status.try_recv() {
                        Ok(task_status) => {
                            let task_status_vec = curator_task_status_map
                                .entry(task_status.scheduler_id)
                                .or_insert_with(Vec::new);
                            fetched_task_num += task_status.task_status.len();
                            task_status_vec.extend(task_status.task_status);
                        }
                        Err(TryRecvError::Empty) => {
                            info!(
                                executor_id,
                                tasks = fetched_task_num,
                                "received task statuses to report",
                            );
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            info!(
                                executor_id,
                                "channel is closed, exiting task status report loop"
                            );
                            drop(tasks_status_complete);
                            return;
                        }
                    }
                }

                for (scheduler_id, tasks_status) in curator_task_status_map.into_iter() {
                    match executor_server.get_scheduler_client(&scheduler_id).await {
                        Ok(mut scheduler) => {
                            let executor_id = &executor_server.executor.metadata.id;
                            if let Err(e) = TaskRunnerPool::<T, U>::send_status_update(
                                &mut scheduler,
                                executor_id,
                                tasks_status.clone(),
                            )
                            .await
                            {
                                let task_ids = tasks_status.iter().map(|status| {
                                    format!(
                                        "{}/{}/{:?}",
                                        status.job_id, status.stage_id, status.partitions
                                    )
                                });
                                error!(
                                    executor_id,
                                    error = %e,
                                    ?task_ids,
                                    "failed to update task status",
                                );

                                if !TERMINATING.load(Ordering::Acquire) {
                                    let mut scheduler =
                                        executor_server.scheduler_to_register.clone();

                                    if let Err(e) = Self::send_scheduler_lost(
                                        &mut scheduler,
                                        &executor_server.executor.metadata.id,
                                        &scheduler_id,
                                        tasks_status,
                                    )
                                    .await
                                    {
                                        error!(executor_id, scheduler_id, error = %e, "failed to send scheduler lost");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                executor_id,
                                scheduler_id,
                                error = %e,
                                "failed to get scheduler client",
                            );

                            let mut scheduler =
                                executor_server.scheduler_to_register.clone();

                            if let Err(e) = Self::send_scheduler_lost(
                                &mut scheduler,
                                &executor_server.executor.metadata.id,
                                &scheduler_id,
                                tasks_status,
                            )
                            .await
                            {
                                error!(executor_id, scheduler_id, error = %e, "failed to send scheduler lost");
                            }
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
            let executor_id = &executor_server.executor.metadata.id;
            info!(executor_id, "starting task runner pool");

            let task_threads =
                num_cpus::get().min(executor_server.executor.concurrent_tasks);

            // Use a dedicated executor for CPU bound tasks so that the main tokio
            // executor can still answer requests even when under load
            let dedicated_executor = DedicatedExecutor::new("task_runner", task_threads);

            // As long as the shutdown notification has not been received
            while !task_runner_shutdown.is_shutdown() {
                let maybe_task: Option<CuratorTaskDefinition> = tokio::select! {
                     task = rx_task.recv() => task,
                    _ = task_runner_shutdown.recv() => {
                        info!(executor_id, "stopping task runner pool");
                        drop(task_runner_complete);
                        return;
                    }
                };
                if let Some(task) = maybe_task {
                    let server = executor_server.clone();
                    let plan = task.plan;
                    let curator_task = task.tasks[0].clone();
                    let scheduler_id = task.scheduler_id.clone();

                    let out: tokio::sync::oneshot::Receiver<
                        Result<Arc<dyn QueryStageExecutor>, BallistaError>,
                    > = dedicated_executor.spawn(async move {
                        server.decode_task(scheduler_id, curator_task, &plan).await
                    });

                    let plan = out.await;

                    let plan = match plan {
                        Ok(Ok(plan)) => plan,
                        Ok(Err(e)) => {
                            error!(
                                executor_id,
                                task_identity = task_identity(&task.tasks[0]),
                                error = %e,
                                "failed to decode task",
                            );
                            continue;
                        }
                        Err(e) => {
                            error!(
                                executor_id,
                                task_identity = task_identity(&task.tasks[0]),
                                error = %e,
                                "failed to receive decoded task",
                            );
                            continue;
                        }
                    };

                    let scheduler_id = task.scheduler_id.clone();

                    for curator_task in task.tasks {
                        let plan = plan.clone();
                        let scheduler_id = scheduler_id.clone();

                        let task_identity = task_identity(&curator_task);
                        info!(executor_id, task_identity, "received task");

                        let server = executor_server.clone();
                        dedicated_executor.spawn(async move {
                            server
                                .run_task(
                                    &task_identity,
                                    scheduler_id,
                                    curator_task,
                                    plan,
                                )
                                .await
                        });
                    }
                } else {
                    info!(
                        executor_id,
                        "channel is closed and will exit the task receive loop"
                    );
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
        // Reject new tasks if in TERMINATING state
        if TERMINATING.load(Ordering::Acquire) {
            return Err(Status::unavailable("executor is terminating"));
        }

        let LaunchTaskParams {
            tasks,
            scheduler_id,
        } = request.into_inner();
        let task_sender = self.executor_env.tx_task.clone();
        for task in tasks {
            let (task_def, plan) = task
                .try_into()
                .map_err(|e| Status::invalid_argument(format!("{e}")))?;

            if task_sender
                .send(CuratorTaskDefinition {
                    scheduler_id: scheduler_id.clone(),
                    plan,
                    tasks: vec![task_def],
                })
                .await
                .is_err()
            {
                warn!(
                    scheduler_id,
                    executor_id = self.executor.metadata.id,
                    "task from scheduler rejected, executor is shutting down"
                );
                return Err(Status::aborted("executor is shutting down"));
            }
        }
        Ok(Response::new(LaunchTaskResult { success: true }))
    }

    async fn stop_executor(
        &self,
        request: Request<StopExecutorParams>,
    ) -> Result<Response<StopExecutorResult>, Status> {
        let stop_request = request.into_inner();
        if stop_request.executor_id != self.executor.metadata.id {
            warn!(
                request_executor_id = stop_request.executor_id,
                executor_id = self.executor.metadata.id,
                "received executor invalid executor ID in stop request, ignoring",
            );
            return Ok(Response::new(StopExecutorResult {}));
        }
        let stop_reason = stop_request.reason;
        let force = stop_request.force;
        info!(
            stop_reason,
            force,
            executor_id = self.executor.metadata.id,
            "receive stop executor request",
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
        info!(
            executor_id = self.executor.metadata.id,
            num_tasks = task_infos.len(),
            "received cancel tasks request"
        );

        let mut cancelled = true;

        for task in task_infos {
            if let Err(e) = self
                .executor
                .cancel_task(task.task_id as usize, task.job_id, task.stage_id as usize)
                .await
            {
                error!(executor_id = self.executor.metadata.id, error = %e, "error cancelling tasks");
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

        let work_dir = PathBuf::from(&self.executor.work_dir);
        let mut path = work_dir.clone();
        path.push(&job_id);

        info!(executor_id = self.executor.metadata.id, job_id, path = %path.display(), "removing job data");

        // Verify it's an existing directory
        if !path.is_dir() {
            return if !path.exists() {
                Ok(Response::new(RemoveJobDataResult {}))
            } else {
                Err(Status::invalid_argument(format!(
                    "path {} is not a directory",
                    path.display()
                )))
            };
        }

        if !is_subdirectory(path.as_path(), work_dir.as_path()) {
            return Err(Status::invalid_argument(format!(
                "path {} is not in the work directory {}",
                path.display(),
                work_dir.display()
            )));
        }

        fs::remove_dir_all(&path).await?;

        Ok(Response::new(RemoveJobDataResult {}))
    }
}

// Check whether the path is the subdirectory of the base directory
fn is_subdirectory(path: &Path, base_path: &Path) -> bool {
    if let (Ok(path), Ok(base_path)) = (path.canonicalize(), base_path.canonicalize()) {
        if let Some(parent_path) = path.parent() {
            parent_path.starts_with(base_path)
        } else {
            false
        }
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::executor_server::is_subdirectory;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_is_subdirectory() {
        let base_dir = TempDir::new().unwrap().into_path();

        // Normal correct one
        {
            let job_path = prepare_testing_job_directory(&base_dir, "job_a");
            assert!(is_subdirectory(&job_path, base_dir.as_path()));
        }

        // Empty job id
        {
            let job_path = prepare_testing_job_directory(&base_dir, "");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));

            let job_path = prepare_testing_job_directory(&base_dir, ".");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));
        }

        // Malicious job id
        {
            let job_path = prepare_testing_job_directory(&base_dir, "..");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));
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
