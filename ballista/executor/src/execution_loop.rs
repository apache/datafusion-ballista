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
use crate::executor_process::remove_job_dir;
use crate::{TaskExecutionTimes, as_task_status};
use ballista_core::error::BallistaError;
use ballista_core::extension::SessionConfigHelperExt;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{
    PollWorkParams, PollWorkResult, TaskDefinition, TaskStatus,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::serde::scheduler::{ExecutorSpecification, PartitionId};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::FutureExt;
use log::{debug, error, info, warn};
use std::any::Any;
use std::convert::TryInto;
use std::error::Error;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::codegen::{Body, Bytes, StdError};

/// Main execution loop that polls the scheduler for available tasks.
///
/// This function runs indefinitely, periodically asking the scheduler for
/// work. When tasks are received, they are executed on a dedicated thread
/// pool and results are reported back to the scheduler.
///
/// The loop respects the executor's concurrent task limit via a semaphore,
/// ensuring no more than the configured number of tasks run simultaneously.
pub async fn poll_loop<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan, C>(
    mut scheduler: SchedulerGrpcClient<C>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
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
    let available_task_slots =
        Arc::new(Semaphore::new(executor_specification.task_slots as usize));

    let (task_status_sender, mut task_status_receiver) =
        std::sync::mpsc::channel::<TaskStatus>();
    info!("Starting poll work loop with scheduler");

    let dedicated_executor =
        DedicatedExecutor::new("task_runner", executor_specification.task_slots as usize);

    loop {
        // Wait for task slots to be available before asking for new work
        let permit = available_task_slots.acquire().await.unwrap();
        // Make the slot available again
        drop(permit);

        // Keeps track of whether we received task in last iteration
        // to avoid going in sleep mode between polling
        let mut active_job = false;

        let task_status: Vec<TaskStatus> =
            sample_tasks_status(&mut task_status_receiver).await;

        let poll_work_result: Result<tonic::Response<PollWorkResult>, tonic::Status> =
            scheduler
                .poll_work(PollWorkParams {
                    metadata: Some(executor.metadata.clone()),
                    num_free_slots: available_task_slots.available_permits() as u32,
                    task_status,
                })
                .await;

        match poll_work_result {
            Ok(result) => {
                let PollWorkResult {
                    tasks,
                    jobs_to_clean,
                } = result.into_inner();
                active_job = !tasks.is_empty();

                // Clean up any state related to the listed jobs
                for cleanup in jobs_to_clean {
                    let job_id = cleanup.job_id.clone();
                    let work_dir = executor.work_dir.clone();

                    // In poll-based cleanup, removing job data is fire-and-forget.
                    // Failures here do not affect task execution and are only logged.
                    tokio::spawn(async move {
                        if let Err(e) = remove_job_dir(&work_dir, &job_id).await {
                            error!("failed to remove job dir {job_id}: {e}");
                        }
                    });
                }

                for task in tasks {
                    let task_status_sender = task_status_sender.clone();

                    // Acquire a permit/slot for the task
                    let permit =
                        available_task_slots.clone().acquire_owned().await.unwrap();

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

                            let partition_id = PartitionId {
                                job_id: task.job_id.clone(),
                                stage_id: task.stage_id as usize,
                                partition_id: task.partition_id as usize,
                            };

                            warn!(
                                "Executor failed to run task: {partition_id:?}, error: {e:?}"
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
                                task.task_id as usize,
                                task.task_attempt_num as usize,
                                partition_id,
                                None,
                                task_execution_times,
                            )) {
                                warn!("failed to send task status: {error:?}");
                            };
                        }
                    }
                }
            }
            Err(error) => {
                warn!(
                    "Executor poll work loop failed. If this continues to happen the Scheduler might be marked as dead. Error: {error}"
                );
            }
        }

        if !active_job {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
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
    let job_id = task.job_id;
    let stage_id = task.stage_id;
    let stage_attempt_num = task.stage_attempt_num;
    let task_launch_time = task.launch_time;
    let partition_id = task.partition_id;
    let start_exec_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let task_identity = format!(
        "TID {task_id} {job_id}/{stage_id}.{stage_attempt_num}/{partition_id}.{task_attempt_num}"
    );
    info!("Received task: [{task_identity}]");

    log::trace!(
        "Received task: [{}], task_properties: {:?}",
        task_identity,
        task.props
    );
    let session_config = executor.produce_config();
    let session_config = session_config.update_from_key_value_pair(&task.props);

    let task_scalar_functions = executor.function_registry.scalar_functions.clone();
    let task_aggregate_functions = executor.function_registry.aggregate_functions.clone();
    let task_window_functions = executor.function_registry.window_functions.clone();

    let runtime = executor.produce_runtime(&session_config)?;

    let session_id = task.session_id.clone();
    let task_context = Arc::new(TaskContext::new(
        Some(task_identity.clone()),
        session_id,
        session_config,
        task_scalar_functions,
        task_aggregate_functions,
        task_window_functions,
        runtime.clone(),
    ));

    let plan: Arc<dyn ExecutionPlan> =
        U::try_decode(task.plan.as_slice()).and_then(|proto| {
            proto.try_into_physical_plan(&task_context, codec.physical_extension_codec())
        })?;

    let query_stage_exec = executor.execution_engine.create_query_stage_exec(
        job_id.clone(),
        stage_id as usize,
        plan,
        &executor.work_dir,
    )?;
    dedicated_executor.spawn(async move {
        use std::panic::AssertUnwindSafe;
        let part = PartitionId {
            job_id: job_id.clone(),
            stage_id: stage_id as usize,
            partition_id: partition_id as usize,
        };

        let execution_result = match AssertUnwindSafe(executor.execute_query_stage(
            task_id as usize,
            part.clone(),
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

        info!("Done with task {task_identity}");
        debug!("Statistics: {execution_result:?}");

        let plan_metrics = query_stage_exec.collect_plan_metrics();
        let operator_metrics = plan_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()
            .ok();

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
            task_id as usize,
            stage_attempt_num as usize,
            part,
            operator_metrics,
            task_execution_times,
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
