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

use datafusion::physical_plan::ExecutionPlan;

use ballista_core::serde::protobuf::{
    scheduler_grpc_client::SchedulerGrpcClient, PollWorkParams, PollWorkResult,
    TaskDefinition, TaskStatus,
};

use crate::executor::Executor;
use crate::{as_task_status, TaskExecutionTimes};
use ballista_core::error::BallistaError;
use ballista_core::serde::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use ballista_core::serde::scheduler::{ExecutorSpecification, PartitionId};
use ballista_core::serde::{AsExecutionPlan, BallistaCodec};
use ballista_core::utils::collect_plan_metrics;
use datafusion::execution::context::TaskContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use futures::FutureExt;
use log::{debug, error, info, warn};
use std::any::Any;
use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{sync::Arc, time::Duration};
use tonic::transport::Channel;

pub async fn poll_loop<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    mut scheduler: SchedulerGrpcClient<Channel>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
) -> Result<(), BallistaError> {
    let executor_specification: ExecutorSpecification = executor
        .metadata
        .specification
        .as_ref()
        .unwrap()
        .clone()
        .into();
    let available_tasks_slots =
        Arc::new(AtomicUsize::new(executor_specification.task_slots as usize));
    let (task_status_sender, mut task_status_receiver) =
        std::sync::mpsc::channel::<TaskStatus>();
    info!("Starting poll work loop with scheduler");

    loop {
        // Keeps track of whether we received task in last iteration
        // to avoid going in sleep mode between polling
        let mut active_job = false;

        let can_accept_task = available_tasks_slots.load(Ordering::SeqCst) > 0;

        // Don't poll for work if we can not accept any tasks
        if !can_accept_task {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        }

        let task_status: Vec<TaskStatus> =
            sample_tasks_status(&mut task_status_receiver).await;

        let poll_work_result: anyhow::Result<
            tonic::Response<PollWorkResult>,
            tonic::Status,
        > = scheduler
            .poll_work(PollWorkParams {
                metadata: Some(executor.metadata.clone()),
                can_accept_task,
                task_status,
            })
            .await;

        let task_status_sender = task_status_sender.clone();

        match poll_work_result {
            Ok(result) => {
                if let Some(task) = result.into_inner().task {
                    match run_received_tasks(
                        executor.clone(),
                        available_tasks_slots.clone(),
                        task_status_sender,
                        task,
                        &codec,
                    )
                    .await
                    {
                        Ok(_) => {
                            active_job = true;
                        }
                        Err(e) => {
                            warn!("Failed to run task: {:?}", e);
                            active_job = false;
                        }
                    }
                } else {
                    active_job = false
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

async fn run_received_tasks<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    executor: Arc<Executor>,
    available_tasks_slots: Arc<AtomicUsize>,
    task_status_sender: Sender<TaskStatus>,
    task: TaskDefinition,
    codec: &BallistaCodec<T, U>,
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
        "TID {} {}/{}.{}/{}.{}",
        task_id, job_id, stage_id, stage_attempt_num, partition_id, task_attempt_num
    );
    info!("Received task {}", task_identity);
    available_tasks_slots.fetch_sub(1, Ordering::SeqCst);

    let mut task_props = HashMap::new();
    for kv_pair in task.props {
        task_props.insert(kv_pair.key, kv_pair.value);
    }

    let mut task_scalar_functions = HashMap::new();
    let mut task_aggregate_functions = HashMap::new();
    // TODO combine the functions from Executor's functions and TaskDefintion's function resources
    for scalar_func in executor.scalar_functions.clone() {
        task_scalar_functions.insert(scalar_func.0.clone(), scalar_func.1);
    }
    for agg_func in executor.aggregate_functions.clone() {
        task_aggregate_functions.insert(agg_func.0, agg_func.1);
    }
    let runtime = executor.runtime.clone();
    let session_id = task.session_id.clone();
    let task_context = Arc::new(TaskContext::new(
        task_identity.clone(),
        session_id,
        task_props,
        task_scalar_functions,
        task_aggregate_functions,
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

    let shuffle_writer_plan =
        executor.new_shuffle_writer(job_id.clone(), stage_id as usize, plan)?;
    tokio::spawn(async move {
        use std::panic::AssertUnwindSafe;
        let part = PartitionId {
            job_id: job_id.clone(),
            stage_id: stage_id as usize,
            partition_id: partition_id as usize,
        };

        let execution_result = match AssertUnwindSafe(executor.execute_shuffle_write(
            task_id as usize,
            part.clone(),
            shuffle_writer_plan.clone(),
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
        available_tasks_slots.fetch_add(1, Ordering::SeqCst);

        let plan_metrics = collect_plan_metrics(shuffle_writer_plan.as_ref());
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
    });

    Ok(())
}

async fn sample_tasks_status(
    task_status_receiver: &mut Receiver<TaskStatus>,
) -> Vec<TaskStatus> {
    let mut task_status: Vec<TaskStatus> = vec![];

    loop {
        match task_status_receiver.try_recv() {
            anyhow::Result::Ok(status) => {
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
