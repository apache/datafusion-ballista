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

//! Pull-based task execution loop for the executor.
//!
//! This module implements the polling mechanism where executors actively
//! request work from the scheduler, as opposed to push-based scheduling
//! where the scheduler sends tasks to executors.

use crate::cpu_bound_executor::DedicatedExecutor;
use crate::executor::Executor;
use crate::executor_process::remove_job_data;
use crate::{TaskCompletionExtras, TaskExecutionTimes, as_task_status};
use ballista_core::JobId;
use ballista_core::error::BallistaError;
use ballista_core::extension::SessionConfigHelperExt;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{
    PollWorkParams, PollWorkResult, TaskDefinition, TaskStatus,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::serde::scheduler::{ExecutorSpecification, TaskKey};
use ballista_core::utils::redact_key_value_pairs;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::FutureExt;
use log::{debug, error, info, trace, warn};
use std::any::Any;
use std::convert::TryInto;
use std::error::Error;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tonic::codegen::{Body, Bytes, StdError};

/// Idle sleep between polls when polling is the only way to learn of new work.
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Idle sleep when a `poll_now_notify` wake-up is wired and the timer is only
/// a fallback.
const NOTIFIED_IDLE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum time the poll loop waits for a free vcore before polling the
/// scheduler anyway. `poll_work` is what keeps the executor alive under
/// pull-based scheduling, so a fully-busy executor must keep polling (reporting
/// zero free vcores) or the scheduler times it out and resets its tasks. Kept
/// well below the scheduler's executor timeout.
const HEARTBEAT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Main execution loop that polls the scheduler for available tasks.
///
/// Runs indefinitely, periodically asking the scheduler for work. When tasks
/// are received they are executed concurrently and results are reported back
/// to the scheduler.
///
/// Concurrency is bounded by a semaphore. Pass `free_vcores` to supply your
/// own semaphore — useful for sharing a single concurrency limit across
/// multiple poll loops or for observing executor load from outside.
/// Pass `None` to have the loop create a semaphore sized to the executor's
/// configured vcore count.
///
/// A task holds one permit per vcore the scheduler charged for it
/// (`TaskDefinition::vcores_consumed`) until it finishes, so the free count
/// reported on each poll is the number of vcores not in use. The scheduler
/// sizes tasks to that count and, under pull-based scheduling, does not track
/// the executor's vcores in use itself. A task holding fewer permits than
/// vcores would let the executor take on more partitions than it has vcores.
///
/// `poll_now_notify`, when provided, wakes an idle poll loop immediately
/// (typically wired to the scheduler's `on_work_available` callback) instead
/// of waiting out the idle interval. A notification sent mid-poll is not
/// lost: `Notify` stores the permit and the next `notified().await` returns
/// immediately.
///
/// **Shared semaphores**: when one semaphore is shared across loops that
/// connect to different schedulers, each scheduler independently sees the
/// current free capacity and may dispatch up to that many vcores' worth of
/// tasks. The semaphore still caps total concurrent execution — tasks that
/// cannot run immediately wait for capacity — but both schedulers may
/// over-commit relative to what the semaphore can actually admit at once. This
/// is intentional: the semaphore acts as an execution throttle, not a
/// reservation system.
///
/// **Semaphore sizing**: if the provided semaphore has more permits than the
/// executor's thread pool has threads, the partitions admitted beyond that will
/// queue behind running ones. The caller is responsible for sizing the
/// semaphore appropriately for their thread pool. The loop takes the permits
/// available when it starts as the semaphore's size, and never holds more than
/// that many for one task.
///
/// # Panics
///
/// Panics on startup if `free_vcores` is a semaphore with zero permits,
/// which would cause the loop to deadlock immediately.
pub async fn poll_loop<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan, C>(
    mut scheduler: SchedulerGrpcClient<C>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
    poll_now_notify: Option<Arc<Notify>>,
    free_vcores: Option<Arc<Semaphore>>,
    health: crate::health::ExecutorHealth,
) -> Result<(), BallistaError>
where
    C: tonic::client::GrpcService<tonic::body::Body>,
    C::Error: Into<StdError>,
    C::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <C::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    let executor_specification: ExecutorSpecification = executor
        .metadata
        .specification
        .as_ref()
        .unwrap()
        .clone()
        .into();
    let free_vcores = free_vcores.unwrap_or_else(|| {
        Arc::new(Semaphore::new(executor_specification.vcores as usize))
    });
    assert!(
        free_vcores.available_permits() > 0,
        "free_vcores semaphore must have at least one permit; passing a closed or zero-permit semaphore would deadlock the poll loop"
    );
    // The semaphore's size, read before this loop starts a task. At least 1, as
    // asserted above.
    let total_vcores = u32::try_from(free_vcores.available_permits()).unwrap_or(u32::MAX);

    let (task_status_sender, mut task_status_receiver) =
        std::sync::mpsc::channel::<TaskStatus>();
    info!("Starting poll work loop with scheduler");

    let dedicated_executor =
        DedicatedExecutor::new("task_runner", executor_specification.vcores as usize);

    loop {
        // Wait for a vcore permit before asking for new work, but cap the wait
        // so a fully-busy executor still polls the scheduler periodically.
        // `poll_work` is what keeps the executor alive under pull-based
        // scheduling (the scheduler records a heartbeat on every poll; the
        // metrics heartbeats aren't relied on for liveness). If every
        // vcore is held by a task running longer than the scheduler's executor
        // timeout, blocking here indefinitely stops heartbeats, so the scheduler
        // wrongly marks this healthy-but-busy executor dead and resets its
        // in-flight tasks. On timeout we poll anyway below, reporting
        // `num_free_vcores: 0`, so liveness no longer depends on vcore
        // availability.
        match tokio::time::timeout(HEARTBEAT_POLL_INTERVAL, free_vcores.acquire()).await {
            // A vcore is free; release it so the bind below can claim it.
            Ok(Ok(permit)) => drop(permit),
            // Semaphore closed (executor shutting down).
            Ok(Err(_)) => {
                return Err(BallistaError::Internal(
                    "vcore semaphore closed".to_string(),
                ));
            }
            // No free vcore within the interval; poll anyway to stay alive.
            Err(_) => {}
        }

        // Keeps track of whether we received task in last iteration
        // to avoid going in sleep mode between polling
        let mut active_job = false;

        let task_status: Vec<TaskStatus> =
            sample_tasks_status(&mut task_status_receiver).await;

        let poll_work_result: Result<tonic::Response<PollWorkResult>, tonic::Status> =
            scheduler
                .poll_work(PollWorkParams {
                    metadata: Some(executor.metadata.clone()),
                    num_free_vcores: free_vcores.available_permits() as u32,
                    task_status,
                })
                .await;

        match poll_work_result {
            Ok(result) => {
                health.mark_heartbeat_ok();
                let PollWorkResult {
                    tasks,
                    jobs_to_clean,
                } = result.into_inner();
                active_job = !tasks.is_empty();

                // Clean up any state related to the listed jobs
                for cleanup in jobs_to_clean {
                    let job_id = cleanup.job_id.clone().into();
                    let work_dir = executor.work_dir.clone();
                    let remove_stage_ids = cleanup.remove_stage_ids.clone();
                    executor.remove_pipelined_job_data(&job_id, &remove_stage_ids);

                    // In poll-based cleanup, removing job data is fire-and-forget.
                    // Failures here do not affect task execution and are only logged.
                    tokio::spawn(async move {
                        if let Err(e) =
                            remove_job_data(&work_dir, &job_id, &remove_stage_ids).await
                        {
                            error!("failed to remove job data {job_id}: {e}");
                        }
                    });
                }

                for task in tasks {
                    let task_status_sender = task_status_sender.clone();

                    // Hold the task's vcores until it finishes.
                    let permit = free_vcores
                        .clone()
                        .acquire_many_owned(task_permits(&task, total_vcores))
                        .await
                        .map_err(|_| {
                            BallistaError::Internal("vcore semaphore closed".to_string())
                        })?;

                    let start_exec_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    match run_received_task(
                        executor.clone(),
                        permit,
                        task_status_sender.clone(),
                        task.clone(),
                        &codec,
                        &dedicated_executor,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            //
                            // notifying scheduler about task failure
                            // as scheduler expects notification.
                            //

                            let task_key = TaskKey {
                                job_id: task.job_id.clone().into(),
                                stage_id: task.stage_id as usize,
                                task_id: task.task_id as usize,
                            };

                            warn!(
                                "Executor failed to run task: {task_key:?}, error: {e:?}"
                            );

                            let end_exec_time = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis()
                                as u64;

                            let task_execution_times = TaskExecutionTimes {
                                launch_time: task.launch_time,
                                start_exec_time,
                                end_exec_time,
                            };

                            // TODO: MM should we re-try message?
                            if let Err(error) = task_status_sender.send(as_task_status(
                                Err(e),
                                executor.metadata.id.clone(),
                                task.task_attempt_num as usize,
                                task_key,
                                task_execution_times,
                                TaskCompletionExtras::default(),
                            )) {
                                warn!("failed to send task status: {error:?}");
                            };
                        }
                    }
                }
            }
            Err(error) => {
                health.mark_heartbeat_failed();
                warn!(
                    "Executor poll work loop failed. If this continues to happen the Scheduler might be marked as dead. Error: {error}"
                );
            }
        }

        if !active_job {
            match &poll_now_notify {
                Some(notify) => {
                    tokio::select! {
                        () = tokio::time::sleep(NOTIFIED_IDLE_POLL_INTERVAL) => {}
                        () = notify.notified() => {
                            debug!("Received poll_now notification, polling immediately");
                        }
                    }
                }
                None => {
                    tokio::time::sleep(IDLE_POLL_INTERVAL).await;
                }
            }
        }
    }
}

/// Permits a task holds while it runs: one per vcore the scheduler charged for
/// it, so that the free count `poll_loop` reports is the vcores not in use.
///
/// At least 1, as a scheduler that predates `vcores_consumed` sends 0. At most
/// `total_vcores`: waiting for more permits than the semaphore holds never
/// ends, which would stall the loop and with it the heartbeat.
fn task_permits(task: &TaskDefinition, total_vcores: u32) -> u32 {
    let permits = task.vcores_consumed.clamp(1, total_vcores);
    if permits < task.vcores_consumed {
        warn!(
            "Task [TID {}/{}.{}/{}.{}] was charged {} vcores but the executor has {total_vcores}, holding {permits}",
            task.job_id,
            task.stage_id,
            task.stage_attempt_num,
            task.task_id,
            task.task_attempt_num,
            task.vcores_consumed,
        );
    }
    permits
}

/// Tries to get meaningful description from panic-error.
pub(crate) fn any_to_string(any: &Box<dyn Any + Send>) -> String {
    if let Some(s) = any.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = any.downcast_ref::<String>() {
        s.clone()
    } else if let Some(error) = any.downcast_ref::<Box<dyn Error + Send>>() {
        error.to_string()
    } else {
        "Unknown error occurred".to_string()
    }
}

async fn run_received_task<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    executor: Arc<Executor>,
    permit: OwnedSemaphorePermit,
    task_status_sender: Sender<TaskStatus>,
    task: TaskDefinition,
    codec: &BallistaCodec<T, U>,
    dedicated_executor: &DedicatedExecutor,
) -> Result<(), BallistaError> {
    let task_id = task.task_id;
    let task_attempt_num = task.task_attempt_num;
    let job_id: JobId = task.job_id.into();
    let stage_id = task.stage_id;
    let stage_attempt_num = task.stage_attempt_num;
    let task_launch_time = task.launch_time;
    let start_exec_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let task_identity = format!(
        "TID {job_id}/{stage_id}.{stage_attempt_num}/{task_id}.{task_attempt_num}"
    );
    info!("Received task: [{task_identity}]");

    trace!(
        "Received task: [{}], task_properties: {:?}",
        task_identity,
        redact_key_value_pairs(&task.props)
    );
    let session_config = executor.produce_config();
    let session_config = session_config.update_from_key_value_pair(&task.props);

    let task_scalar_functions = executor.function_registry.scalar_functions.clone();
    let task_aggregate_functions = executor.function_registry.aggregate_functions.clone();
    let task_window_functions = executor.function_registry.window_functions.clone();
    let task_higher_order_functions =
        executor.function_registry.higher_order_functions.clone();

    let runtime = executor.produce_runtime_for_session(
        &task.session_id,
        &session_config,
        task.vcores_consumed,
    )?;

    let session_id = task.session_id.clone();
    let task_context = Arc::new(TaskContext::new(
        Some(task_identity.clone()),
        session_id,
        session_config,
        task_scalar_functions,
        task_higher_order_functions,
        task_aggregate_functions,
        task_window_functions,
        runtime.clone(),
    ));

    let plan: Arc<dyn ExecutionPlan> =
        U::try_decode(task.plan.as_slice()).and_then(|proto| {
            proto.try_into_physical_plan(&task_context, codec.physical_extension_codec())
        })?;

    let global_output_partition_ids: Vec<usize> = task
        .global_output_partition_ids
        .iter()
        .map(|p| *p as usize)
        .collect();

    let query_stage_exec = executor.execution_engine.create_query_stage_exec(
        job_id.clone(),
        stage_id as usize,
        task_id as usize,
        global_output_partition_ids,
        plan,
        &executor.work_dir,
        task_context.session_config(),
    )?;
    dedicated_executor.spawn(async move {
        use std::panic::AssertUnwindSafe;
        let key = TaskKey {
            job_id: job_id.clone(),
            stage_id: stage_id as usize,
            task_id: task_id as usize,
        };

        let task_start = Instant::now();
        let execution_result = match AssertUnwindSafe(executor.execute_query_stage(
            key.clone(),
            query_stage_exec.clone(),
            task_context,
        ))
        .catch_unwind()
        .await
        {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(r)) => Err(r),
            Err(r) => {
                error!("Error executing task: {:?}", any_to_string(&r));
                Err(BallistaError::Internal(format!("{:#?}", any_to_string(&r))))
            }
        };

        info!(
            "Finished task : [{task_identity}] in {:?}",
            task_start.elapsed()
        );
        debug!("Task statistics: [{task_identity}] {execution_result:?}");

        let plan_metrics = query_stage_exec.collect_plan_metrics();
        let operator_metrics = plan_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()
            .ok();
        let runtime_stats = query_stage_exec.collect_runtime_stats_reports();
        let column_stats = query_stage_exec.collect_column_stats();
        let runtime_filters = query_stage_exec.collect_runtime_filter_reports();
        // Collect only when the task otherwise succeeded: a failed task's
        // partial state is meaningless, and its own error is the useful one.
        // A collection failure fails the task — these are load-bearing for the
        // downstream stage's prefix merge, so continuing without them would
        // ship a wrong answer that nothing later detects.
        let (execution_result, window_state) = match execution_result {
            Ok(partitions) => match query_stage_exec.collect_window_state_reports() {
                Ok(reports) => (Ok(partitions), reports),
                Err(e) => (Err(e.into()), Vec::new()),
            },
            Err(e) => (Err(e), Vec::new()),
        };

        let end_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let task_execution_times = TaskExecutionTimes {
            launch_time: task_launch_time,
            start_exec_time,
            end_exec_time,
        };

        let _ = task_status_sender.send(as_task_status(
            execution_result,
            executor.metadata.id.clone(),
            stage_attempt_num as usize,
            key,
            task_execution_times,
            TaskCompletionExtras {
                operator_metrics,
                runtime_stats,
                window_state,
                column_stats,
                runtime_filters,
            },
        ));

        // Release the permit after the work is done
        drop(permit);
    });

    Ok(())
}

async fn sample_tasks_status(
    task_status_receiver: &mut Receiver<TaskStatus>,
) -> Vec<TaskStatus> {
    let mut task_status: Vec<TaskStatus> = vec![];

    loop {
        match task_status_receiver.try_recv() {
            Result::Ok(status) => {
                task_status.push(status);
            }
            Err(TryRecvError::Empty) => {
                break;
            }
            Err(TryRecvError::Disconnected) => {
                error!("Task statuses channel disconnected");
            }
        }
    }

    task_status
}

#[cfg(test)]
mod tests {
    use super::poll_loop;
    use crate::execution_engine::{ExecutionEngine, QueryStageExecutor};
    use crate::executor::Executor;
    use crate::health::ExecutorHealth;
    use crate::metrics::LoggingMetricsCollector;
    use ballista_core::JobId;
    use ballista_core::RuntimeProducer;
    use ballista_core::registry::BallistaFunctionRegistry;
    use ballista_core::serde::BallistaCodec;
    use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
    use ballista_core::serde::protobuf::scheduler_grpc_server::{
        SchedulerGrpc, SchedulerGrpcServer,
    };
    use ballista_core::serde::protobuf::{
        CancelJobParams, CancelJobResult, CleanJobDataParams, CleanJobDataResult,
        CreateUpdateSessionParams, CreateUpdateSessionResult, ExecuteQueryParams,
        ExecuteQueryResult, ExecutorRegistration, ExecutorStoppedParams,
        ExecutorStoppedResult, GetJobMetricsParams, GetJobMetricsResult,
        GetJobStatusParams, GetJobStatusResult, HeartBeatParams, HeartBeatResult,
        PollWorkParams, PollWorkResult, RegisterExecutorParams, RegisterExecutorResult,
        RemoveSessionParams, RemoveSessionResult, ShuffleWritePartition, TaskDefinition,
        UpdateTaskStatusParams, UpdateTaskStatusResult,
    };
    use ballista_core::serde::scheduler::ExecutorSpecification;
    use ballista_core::utils::default_config_producer;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::error::Result;
    use datafusion::execution::context::TaskContext;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::MetricsSet;
    use datafusion::prelude::SessionConfig;
    use datafusion_proto::bytes::physical_plan_to_bytes;
    use parking_lot::Mutex;
    use std::fmt::{self, Display};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Notify, Semaphore};
    use tonic::codegen::BoxStream;
    use tonic::{Request, Response, Status};

    /// Vcores of the executor under test.
    const VCORES: usize = 8;

    /// How long to wait for something the test expects to happen. Only
    /// reached when it doesn't.
    const TIMEOUT: Duration = Duration::from_secs(10);

    /// Execution engine whose tasks announce that they started and then run
    /// until released, so a test can look at the executor mid-task. It is its
    /// own query stage: the tasks it creates share its two signals.
    #[derive(Clone, Debug, Default)]
    struct BlockingEngine {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl ExecutionEngine for BlockingEngine {
        fn create_query_stage_exec(
            &self,
            _job_id: JobId,
            _stage_id: usize,
            _task_id: usize,
            _global_output_partition_ids: Vec<usize>,
            _plan: Arc<dyn ExecutionPlan>,
            _work_dir: &str,
            _config: &SessionConfig,
        ) -> Result<Arc<dyn QueryStageExecutor>> {
            Ok(Arc::new(self.clone()))
        }
    }

    impl Display for BlockingEngine {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "BlockingEngine")
        }
    }

    #[async_trait::async_trait]
    impl QueryStageExecutor for BlockingEngine {
        async fn execute_query_stage(
            &self,
            _task_id: usize,
            _context: Arc<TaskContext>,
        ) -> Result<Vec<ShuffleWritePartition>> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(vec![])
        }

        fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
            vec![]
        }
    }

    type GrpcResult<T> = std::result::Result<Response<T>, Status>;

    fn not_called<T>() -> GrpcResult<T> {
        Err(Status::unimplemented("not called by the poll loop"))
    }

    /// A scheduler that hands out one task on the first poll and nothing after.
    struct MockScheduler {
        task: Mutex<Option<TaskDefinition>>,
    }

    #[tonic::async_trait]
    impl SchedulerGrpc for MockScheduler {
        type ExecuteQueryPushStream = BoxStream<GetJobStatusResult>;

        async fn poll_work(
            &self,
            _: Request<PollWorkParams>,
        ) -> GrpcResult<PollWorkResult> {
            Ok(Response::new(PollWorkResult {
                tasks: self.task.lock().take().into_iter().collect(),
                jobs_to_clean: vec![],
            }))
        }

        async fn register_executor(
            &self,
            _: Request<RegisterExecutorParams>,
        ) -> GrpcResult<RegisterExecutorResult> {
            not_called()
        }

        async fn heart_beat_from_executor(
            &self,
            _: Request<HeartBeatParams>,
        ) -> GrpcResult<HeartBeatResult> {
            not_called()
        }

        async fn update_task_status(
            &self,
            _: Request<UpdateTaskStatusParams>,
        ) -> GrpcResult<UpdateTaskStatusResult> {
            not_called()
        }

        async fn create_update_session(
            &self,
            _: Request<CreateUpdateSessionParams>,
        ) -> GrpcResult<CreateUpdateSessionResult> {
            not_called()
        }

        async fn remove_session(
            &self,
            _: Request<RemoveSessionParams>,
        ) -> GrpcResult<RemoveSessionResult> {
            not_called()
        }

        async fn execute_query_push(
            &self,
            _: Request<ExecuteQueryParams>,
        ) -> GrpcResult<Self::ExecuteQueryPushStream> {
            not_called()
        }

        async fn execute_query(
            &self,
            _: Request<ExecuteQueryParams>,
        ) -> GrpcResult<ExecuteQueryResult> {
            not_called()
        }

        async fn get_job_status(
            &self,
            _: Request<GetJobStatusParams>,
        ) -> GrpcResult<GetJobStatusResult> {
            not_called()
        }

        async fn get_job_metrics(
            &self,
            _: Request<GetJobMetricsParams>,
        ) -> GrpcResult<GetJobMetricsResult> {
            not_called()
        }

        async fn executor_stopped(
            &self,
            _: Request<ExecutorStoppedParams>,
        ) -> GrpcResult<ExecutorStoppedResult> {
            not_called()
        }

        async fn cancel_job(
            &self,
            _: Request<CancelJobParams>,
        ) -> GrpcResult<CancelJobResult> {
            not_called()
        }

        async fn clean_job_data(
            &self,
            _: Request<CleanJobDataParams>,
        ) -> GrpcResult<CleanJobDataResult> {
            not_called()
        }
    }

    /// A task the scheduler charged `vcores_consumed` vcores for, with a plan
    /// the executor can decode.
    fn task_definition(vcores_consumed: u32) -> TaskDefinition {
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        TaskDefinition {
            plan: physical_plan_to_bytes(plan)
                .expect("encoding the plan")
                .to_vec(),
            vcores_consumed,
            ..Default::default()
        }
    }

    /// Runs the poll loop of an idle `VCORES`-vcore executor against a scheduler
    /// that hands it one task charged `vcores_consumed` vcores, and asserts that
    /// `expected_free` vcores were free while that task ran. Also checks that
    /// the task gives them all back once it finishes.
    async fn assert_free_vcores_while_task_runs(
        vcores_consumed: usize,
        expected_free: usize,
    ) {
        let engine = BlockingEngine::default();
        let runtime_producer: RuntimeProducer =
            Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())));
        let executor = Arc::new(Executor::new(
            ExecutorRegistration {
                id: "executor".to_string(),
                specification: Some(
                    ExecutorSpecification::default()
                        .with_vcores(VCORES as u32)
                        .into(),
                ),
                ..Default::default()
            },
            "",
            runtime_producer,
            Arc::new(default_config_producer),
            Arc::new(BallistaFunctionRegistry::default()),
            Arc::new(LoggingMetricsCollector::default()),
            VCORES,
            Arc::new(engine.clone()),
        ));

        let scheduler =
            SchedulerGrpcClient::new(SchedulerGrpcServer::new(MockScheduler {
                task: Mutex::new(Some(task_definition(vcores_consumed as u32))),
            }));
        let free_vcores = Arc::new(Semaphore::new(VCORES));
        let handle = tokio::spawn(poll_loop(
            scheduler,
            executor,
            BallistaCodec::default(),
            None,
            Some(free_vcores.clone()),
            ExecutorHealth::new(),
        ));

        tokio::time::timeout(TIMEOUT, engine.started.notified())
            .await
            .expect("the task never started; the poll loop is stuck acquiring vcores");
        let free_while_running = free_vcores.available_permits();

        engine.release.notify_one();
        // All the vcores can be taken at once only after the task gives its own back.
        let _all_back =
            tokio::time::timeout(TIMEOUT, free_vcores.acquire_many(VCORES as u32))
                .await
                .expect("the finished task never gave its vcores back")
                .unwrap();

        handle.abort();
        assert_eq!(
            free_while_running, expected_free,
            "vcores free while a task charged {vcores_consumed} runs on {VCORES} vcores"
        );
    }

    #[tokio::test]
    async fn multi_partition_task_holds_a_permit_per_partition() {
        // The scheduler fills an idle executor's free vcores with a single task.
        // If that task held one permit, the executor would report the rest as
        // free and be handed more work than it has vcores for.
        assert_free_vcores_while_task_runs(VCORES, 0).await;
        assert_free_vcores_while_task_runs(3, VCORES - 3).await;
    }

    #[tokio::test]
    async fn task_holds_at_least_one_permit() {
        // A collapse stage is charged one vcore however many partitions it packs,
        // and a scheduler that predates `vcores_consumed` sends 0.
        assert_free_vcores_while_task_runs(1, VCORES - 1).await;
        assert_free_vcores_while_task_runs(0, VCORES - 1).await;
    }

    #[tokio::test]
    async fn task_never_waits_for_more_permits_than_the_executor_has() {
        // Waiting for permits that don't exist would stall the loop for good,
        // and with it the executor's heartbeat.
        assert_free_vcores_while_task_runs(10 * VCORES, 0).await;
    }
}
