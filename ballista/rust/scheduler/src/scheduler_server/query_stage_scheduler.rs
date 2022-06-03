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

use std::collections::HashMap;

use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, error, info};

use ballista_core::error::{BallistaError, Result};

use ballista_core::event_loop::{EventAction, EventSender};

use ballista_core::serde::protobuf::{
    job_status, task_status, CompletedJob, CompletedTask, FailedJob, FailedTask,
    JobStatus, RunningJob, TaskStatus,
};
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::{protobuf, AsExecutionPlan, AsLogicalPlan};
use datafusion::physical_plan::ExecutionPlan;

use crate::scheduler_server::event::{QueryStageSchedulerEvent, SchedulerServerEvent};
use crate::state::backend::Keyspace;

use crate::state::executor_manager::ExecutorReservation;
use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    event_sender: Option<EventSender<SchedulerServerEvent>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        event_sender: Option<EventSender<SchedulerServerEvent>>,
    ) -> Self {
        Self {
            state,
            event_sender,
        }
    }

    async fn submit_job(
        &self,
        job_id: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        self.state
            .task_manager
            .submit_job(job_id, session_id, plan)
            .await
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<QueryStageSchedulerEvent> for QueryStageScheduler<T, U>
{
    // TODO
    fn on_start(&self) {
        info!("Starting QueryStageScheduler");
    }

    // TODO
    fn on_stop(&self) {
        info!("Stopping QueryStageScheduler")
    }

    async fn on_receive(
        &self,
        event: QueryStageSchedulerEvent,
    ) -> Result<Option<QueryStageSchedulerEvent>> {
        match event {
            QueryStageSchedulerEvent::JobSubmitted(job_id, _plan) => {
                info!("Job {} submitted", job_id);
                if let Some(sender) = &self.event_sender {
                    let available_tasks = self
                        .state
                        .task_manager
                        .get_available_task_count(&job_id)
                        .await?;

                    let reservations: Vec<ExecutorReservation> = self
                        .state
                        .executor_manager
                        .reserve_slots(available_tasks as u32)
                        .await?
                        .into_iter()
                        .map(|res| res.assign(job_id.clone()))
                        .collect();

                    debug!(
                        "Reserved {} task slots for submitted job {}",
                        reservations.len(),
                        job_id
                    );

                    if let Err(e) = sender
                        .post_event(SchedulerServerEvent::Offer(reservations.clone()))
                        .await
                    {
                        error!("Error posting offer: {:?}", e);
                        self.state
                            .executor_manager
                            .cancel_reservations(reservations)
                            .await?;
                    }
                }
            }
            QueryStageSchedulerEvent::JobFinished(job_id) => {
                info!("Job {} complete", job_id);
                self.state.task_manager.complete_job(&job_id).await?;
            }
            QueryStageSchedulerEvent::JobFailed(job_id, stage_id, fail_message) => {
                error!(
                    "Job {} failed at stage {}: {}",
                    job_id, stage_id, fail_message
                );
                self.state.task_manager.complete_job(&job_id).await?;
            }
        }

        Ok(None)
    }

    // TODO
    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {:?}", error);
    }
}

fn get_job_status_from_tasks(
    tasks: &[Arc<TaskStatus>],
    executors: &HashMap<String, ExecutorMetadata>,
) -> JobStatus {
    let mut job_status = tasks
        .iter()
        .map(|task| match &task.status {
            Some(task_status::Status::Completed(CompletedTask {
                executor_id,
                partitions,
            })) => Ok((task, executor_id, partitions)),
            _ => Err(BallistaError::General("Task not completed".to_string())),
        })
        .collect::<Result<Vec<_>>>()
        .ok()
        .map(|info| {
            let mut partition_location = vec![];
            for (status, executor_id, partitions) in info {
                let input_partition_id = status.task_id.as_ref().unwrap(); // TODO unwrap
                let executor_meta = executors.get(executor_id).map(|e| e.clone().into());
                for shuffle_write_partition in partitions {
                    let shuffle_input_partition_id = Some(protobuf::PartitionId {
                        job_id: input_partition_id.job_id.clone(),
                        stage_id: input_partition_id.stage_id,
                        partition_id: input_partition_id.partition_id,
                    });
                    partition_location.push(protobuf::PartitionLocation {
                        partition_id: shuffle_input_partition_id.clone(),
                        executor_meta: executor_meta.clone(),
                        partition_stats: Some(protobuf::PartitionStats {
                            num_batches: shuffle_write_partition.num_batches as i64,
                            num_rows: shuffle_write_partition.num_rows as i64,
                            num_bytes: shuffle_write_partition.num_bytes as i64,
                            column_stats: vec![],
                        }),
                        path: shuffle_write_partition.path.clone(),
                    });
                }
            }
            job_status::Status::Completed(CompletedJob { partition_location })
        });

    if job_status.is_none() {
        // Update other statuses
        for task in tasks.iter() {
            if let Some(task_status::Status::Failed(FailedTask { error })) = &task.status
            {
                let error = error.clone();
                job_status = Some(job_status::Status::Failed(FailedJob { error }));
                break;
            }
        }
    }

    JobStatus {
        status: Some(job_status.unwrap_or(job_status::Status::Running(RunningJob {}))),
    }
}
