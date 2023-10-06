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

use async_trait::async_trait;
use ballista_core::serde::protobuf::task_status::Status;
use datafusion::config::{ConfigOptions, Extensions};
use datafusion::physical_plan::ExecutionPlan;

use ballista_core::serde::protobuf::{
    scheduler_grpc_client::SchedulerGrpcClient, PollWorkParams, PollWorkResult,
    TaskDefinition, TaskStatus,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::circuit_breaker::client::{
    CircuitBreakerClient, CircuitBreakerClientConfig, CircuitBreakerMetadataExtension,
};
use crate::cpu_bound_executor::DedicatedExecutor;
use crate::executor::Executor;
use crate::scheduler_client_registry::SchedulerClientRegistry;
use crate::{as_task_status, TaskExecutionTimes};
use ballista_core::error::BallistaError;
use ballista_core::serde::scheduler::ExecutorSpecification;
use ballista_core::serde::BallistaCodec;
use datafusion::execution::context::TaskContext;
use datafusion::prelude::SessionConfig;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::FutureExt;
use log::{debug, error, info, warn};
use std::any::Any;
use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::ops::Deref;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{sync::Arc, time::Duration};
use tonic::transport::Channel;

pub async fn poll_loop<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    mut scheduler: SchedulerGrpcClient<Channel>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
    default_extensions: Extensions,
) -> Result<(), BallistaError> {
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

    let circuit_breaker_client = Arc::new(CircuitBreakerClient::new(
        Arc::new(SingleSchedulerClient {
            scheduler: scheduler.clone(),
        }),
        CircuitBreakerClientConfig::default(),
    ));

    loop {
        // Wait for task slots to be available before asking for new work
        let permit = available_task_slots.acquire().await.unwrap();
        // Make the slot available again
        drop(permit);

        // Keeps track of whether we received task in last iteration
        // to avoid going in sleep mode between polling
        let mut active_job = false;

        let task_status: Vec<TaskStatus> = sample_tasks_status(
            &mut task_status_receiver,
            circuit_breaker_client.clone(),
        )
        .await?;

        let poll_work_result: anyhow::Result<
            tonic::Response<PollWorkResult>,
            tonic::Status,
        > = scheduler
            .poll_work(PollWorkParams {
                metadata: Some(executor.metadata.clone()),
                num_free_slots: available_task_slots.available_permits() as u32,
                task_status,
            })
            .await;

        match poll_work_result {
            Ok(result) => {
                let tasks = result.into_inner().tasks;
                active_job = !tasks.is_empty();

                for task in tasks {
                    let task_status_sender = task_status_sender.clone();

                    // Acquire a permit/slot for the task
                    let permit =
                        available_task_slots.clone().acquire_owned().await.unwrap();

                    let task_id = &task.task_id;
                    let job_id = &task.job_id;
                    let stage_id = &task.stage_id;
                    let stage_attempt_num = &task.stage_attempt_num;
                    let partitions = &task.partitions;

                    let task_identity = create_task_identity(
                        task_id,
                        job_id,
                        stage_id,
                        stage_attempt_num,
                        partitions,
                    );

                    circuit_breaker_client
                        .register_scheduler(task_identity.clone(), "".to_owned())?;

                    match run_received_task(
                        executor.clone(),
                        permit,
                        task_status_sender,
                        task,
                        task_identity,
                        &codec,
                        &dedicated_executor,
                        default_extensions.clone(),
                        circuit_breaker_client.clone(),
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Failed to run task: {:?}", e);
                        }
                    }
                }
            }
            Err(error) => {
                warn!("Executor poll work loop failed. If this continues to happen the Scheduler might be marked as dead. Error: {}", error);
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

fn create_task_identity(
    task_id: &u32,
    job_id: &str,
    stage_id: &u32,
    stage_attempt_num: &u32,
    partitions: &Vec<u32>,
) -> String {
    format!("TID {task_id} {job_id}/{stage_id}.{stage_attempt_num}/{partitions:?}")
}

#[allow(clippy::too_many_arguments)]
async fn run_received_task<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    executor: Arc<Executor>,
    permit: OwnedSemaphorePermit,
    task_status_sender: Sender<TaskStatus>,
    task: TaskDefinition,
    task_identity: String,
    codec: &BallistaCodec<T, U>,
    dedicated_executor: &DedicatedExecutor,
    extensions: Extensions,
    circuit_breaker_client: Arc<CircuitBreakerClient>,
) -> Result<(), BallistaError> {
    let task_id = task.task_id;
    let job_id = task.job_id;
    let stage_id = task.stage_id;
    let attempt = task.stage_attempt_num;
    let stage_attempt_num = task.stage_attempt_num;
    let task_launch_time = task.launch_time;
    let partitions: Vec<usize> = task.partitions.iter().map(|p| *p as usize).collect();
    let start_exec_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    info!("Received task {}", task_identity);

    let mut task_props = HashMap::new();
    for kv_pair in task.props {
        task_props.insert(kv_pair.key, kv_pair.value);
    }

    let mut config = ConfigOptions::new().with_extensions(extensions);
    for (k, v) in task_props {
        config.set(&k, &v)?;
    }

    let circuit_breaker_metadata = CircuitBreakerMetadataExtension {
        job_id: job_id.clone(),
        stage_id,
        attempt_number: attempt,
    };

    let session_config = SessionConfig::from(config)
        .with_extension(circuit_breaker_client)
        .with_extension(Arc::new(circuit_breaker_metadata));

    let mut task_scalar_functions = HashMap::new();
    let mut task_aggregate_functions = HashMap::new();
    let mut task_window_functions = HashMap::new();
    // TODO combine the functions from Executor's functions and TaskDefintion's function resources
    for scalar_func in executor.scalar_functions.clone() {
        task_scalar_functions.insert(scalar_func.0.clone(), scalar_func.1);
    }
    for agg_func in executor.aggregate_functions.clone() {
        task_aggregate_functions.insert(agg_func.0, agg_func.1);
    }
    for window_func in executor.window_functions.clone() {
        task_window_functions.insert(window_func.0, window_func.1);
    }
    let runtime = executor.runtime.clone();
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
            proto.try_into_physical_plan(
                task_context.deref(),
                runtime.deref(),
                codec.physical_extension_codec(),
            )
        })?;

    let shuffle_output_partitioning = parse_protobuf_hash_partitioning(
        task.output_partitioning.as_ref(),
        task_context.as_ref(),
        plan.schema().as_ref(),
    )?;

    let query_stage_exec = executor.execution_engine.create_query_stage_exec(
        job_id.clone(),
        stage_id as usize,
        plan,
        &executor.work_dir,
    )?;
    dedicated_executor.spawn(async move {
        use std::panic::AssertUnwindSafe;

        let execution_result = match AssertUnwindSafe(executor.execute_query_stage(
            &job_id,
            stage_id as usize,
            task_id as usize,
            &partitions,
            query_stage_exec.clone(),
            task_context,
            shuffle_output_partitioning,
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

        info!("Done with task {}", task_identity);
        debug!("Statistics: {:?}", execution_result);

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
            job_id,
            stage_id as usize,
            task_id as usize,
            partitions,
            stage_attempt_num as usize,
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
    circuit_breaker_client: Arc<CircuitBreakerClient>,
) -> Result<Vec<TaskStatus>, BallistaError> {
    let mut task_status: Vec<TaskStatus> = vec![];

    loop {
        let recv = task_status_receiver.try_recv();

        match recv {
            anyhow::Result::Ok(status) => {
                // If there is a status and it is not running
                if status
                    .status
                    .iter()
                    .any(|s| !matches!(s, Status::Running(_)))
                {
                    let task_identity = create_task_identity(
                        &status.task_id,
                        &status.job_id,
                        &status.stage_id,
                        &status.stage_attempt_num,
                        &status.partitions,
                    );

                    circuit_breaker_client.deregister_scheduler(task_identity)?;
                }

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

    Ok(task_status)
}

struct SingleSchedulerClient {
    scheduler: SchedulerGrpcClient<Channel>,
}

#[async_trait]
impl SchedulerClientRegistry for SingleSchedulerClient {
    async fn get_scheduler_client(
        &self,
        _scheduler_id: &str,
    ) -> Result<Option<SchedulerGrpcClient<Channel>>, BallistaError> {
        Ok(Some(self.scheduler.clone()))
    }
    async fn insert_scheduler_client(
        &self,
        _scheduler_id: &str,
        _client: SchedulerGrpcClient<Channel>,
    ) {
        panic!("Tried to insert scheduler into SingleSchedulerClient")
    }
}
