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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, error, info};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};

use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::serde::AsExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;
use tokio::sync::mpsc;

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::executor_manager::ExecutorReservation;
use crate::state::SchedulerState;

// TODO move to configuration file
/// Clean up job data interval
pub const CLEANUP_FINISHED_JOB_DELAY_SECS: u64 = 300;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    policy: TaskSchedulingPolicy,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        policy: TaskSchedulingPolicy,
    ) -> Self {
        Self { state, policy }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<QueryStageSchedulerEvent> for QueryStageScheduler<T, U>
{
    fn on_start(&self) {
        info!("Starting QueryStageScheduler");
    }

    fn on_stop(&self) {
        info!("Stopping QueryStageScheduler")
    }

    async fn on_receive(
        &self,
        event: QueryStageSchedulerEvent,
        tx_event: &mpsc::Sender<QueryStageSchedulerEvent>,
        _rx_event: &mpsc::Receiver<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let tx_event = EventSender::new(tx_event.clone());
        match event {
            QueryStageSchedulerEvent::JobQueued {
                job_id,
                job_name,
                session_ctx,
                plan,
            } => {
                info!("Job {} queued with name {:?}", job_id, job_name);
                let state = self.state.clone();
                tokio::spawn(async move {
                    let event = if let Err(e) = state
                        .submit_job(&job_id, &job_name, session_ctx, &plan)
                        .await
                    {
                        let msg = format!("Error planning job {}: {:?}", job_id, e);
                        error!("{}", &msg);
                        QueryStageSchedulerEvent::JobPlanningFailed(job_id, msg)
                    } else {
                        QueryStageSchedulerEvent::JobSubmitted(job_id)
                    };
                    tx_event
                        .post_event(event)
                        .await
                        .map_err(|e| error!("Fail to send event due to {}", e))
                        .unwrap();
                });
            }
            QueryStageSchedulerEvent::JobSubmitted(job_id) => {
                info!("Job {} submitted", job_id);
                if matches!(self.policy, TaskSchedulingPolicy::PushStaged) {
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

                    tx_event
                        .post_event(QueryStageSchedulerEvent::ReservationOffering(
                            reservations,
                        ))
                        .await?;
                }
            }
            QueryStageSchedulerEvent::JobPlanningFailed(job_id, failure_reason) => {
                error!("Job {} failed: {}", job_id, failure_reason);
                self.state
                    .task_manager
                    .fail_unscheduled_job(
                        &job_id,
                        failure_reason,
                        CLEANUP_FINISHED_JOB_DELAY_SECS,
                    )
                    .await?;
            }
            QueryStageSchedulerEvent::JobFinished(job_id) => {
                info!("Job {} success", job_id);
                self.state
                    .task_manager
                    .succeed_job(&job_id, CLEANUP_FINISHED_JOB_DELAY_SECS)
                    .await?;
                let executor_manager = self.state.executor_manager.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(
                        CLEANUP_FINISHED_JOB_DELAY_SECS,
                    ))
                    .await;
                    executor_manager.clean_up_executors_data(job_id).await;
                });
            }
            QueryStageSchedulerEvent::JobRunningFailed(job_id, failure_reason) => {
                error!("Job {} running failed", job_id);
                let tasks = self
                    .state
                    .task_manager
                    .abort_job(&job_id, failure_reason, CLEANUP_FINISHED_JOB_DELAY_SECS)
                    .await?;
                if !tasks.is_empty() {
                    tx_event
                        .post_event(QueryStageSchedulerEvent::CancelTasks(tasks))
                        .await?;
                }
                let executor_manager = self.state.executor_manager.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(
                        CLEANUP_FINISHED_JOB_DELAY_SECS,
                    ))
                    .await;
                    executor_manager.clean_up_executors_data(job_id).await;
                });
            }
            QueryStageSchedulerEvent::JobUpdated(job_id) => {
                info!("Job {} Updated", job_id);
                self.state.task_manager.update_job(&job_id).await?;
            }
            QueryStageSchedulerEvent::JobCancel(job_id) => {
                self.state
                    .task_manager
                    .cancel_job(&job_id, CLEANUP_FINISHED_JOB_DELAY_SECS)
                    .await?;
                let executor_manager = self.state.executor_manager.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(
                        CLEANUP_FINISHED_JOB_DELAY_SECS,
                    ))
                    .await;
                    executor_manager.clean_up_executors_data(job_id).await;
                });
            }
            QueryStageSchedulerEvent::TaskUpdating(executor_id, tasks_status) => {
                let num_status = tasks_status.len();
                match self
                    .state
                    .update_task_statuses(&executor_id, tasks_status)
                    .await
                {
                    Ok((stage_events, offers)) => {
                        if matches!(self.policy, TaskSchedulingPolicy::PushStaged) {
                            tx_event
                                .post_event(
                                    QueryStageSchedulerEvent::ReservationOffering(offers),
                                )
                                .await?;
                        }

                        for stage_event in stage_events {
                            tx_event.post_event(stage_event).await?;
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to update {} task statuses for Executor {}: {:?}",
                            num_status, executor_id, e
                        );
                        // TODO error handling
                    }
                }
            }
            QueryStageSchedulerEvent::ReservationOffering(reservations) => {
                let reservations = self.state.offer_reservation(reservations).await?;
                if !reservations.is_empty() {
                    tx_event
                        .post_event(QueryStageSchedulerEvent::ReservationOffering(
                            reservations,
                        ))
                        .await?;
                }
            }
            QueryStageSchedulerEvent::ExecutorLost(executor_id, _) => {
                match self.state.task_manager.executor_lost(&executor_id).await {
                    Ok(tasks) => {
                        if !tasks.is_empty() {
                            tx_event
                                .post_event(QueryStageSchedulerEvent::CancelTasks(tasks))
                                .await?;
                        }
                    }
                    Err(e) => {
                        let msg = format!(
                            "TaskManager error to handle Executor {} lost: {}",
                            executor_id, e
                        );
                        error!("{}", msg);
                    }
                }
            }
            QueryStageSchedulerEvent::CancelTasks(tasks) => {
                self.state
                    .executor_manager
                    .cancel_running_tasks(tasks)
                    .await?
            }
        }

        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {:?}", error);
    }
}
