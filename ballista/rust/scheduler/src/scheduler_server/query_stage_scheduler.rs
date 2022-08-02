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
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::logical_plan::LogicalPlan;
use datafusion::prelude::SessionContext;
use log::{debug, error, info};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};

use ballista_core::serde::AsExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;

use crate::scheduler_server::event::{QueryStageSchedulerEvent, SchedulerServerEvent};
use crate::scheduler_server::metrics::SchedulerMetricsCollector;

use crate::state::executor_manager::ExecutorReservation;
use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    event_sender: Option<EventSender<SchedulerServerEvent>>,
    metrics_collector: Arc<dyn SchedulerMetricsCollector>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        event_sender: Option<EventSender<SchedulerServerEvent>>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    ) -> Self {
        Self {
            state,
            event_sender,
            metrics_collector,
        }
    }

    async fn submit_job(
        &self,
        job_id: String,
        session_id: String,
        session_ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
        queued_at: u64,
    ) -> Result<()> {
        let start = Instant::now();
        let optimized_plan = session_ctx.optimize(plan)?;

        debug!("Calculated optimized plan: {:?}", optimized_plan);

        let plan = session_ctx.create_physical_plan(&optimized_plan).await?;

        self.state
            .task_manager
            .submit_job(&job_id, &session_id, plan.clone(), queued_at)
            .await?;

        let elapsed = start.elapsed();

        info!("Planned job {} in {:?}", job_id, elapsed);

        Ok(())
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
    ) -> Result<Option<QueryStageSchedulerEvent>> {
        match event {
            QueryStageSchedulerEvent::JobQueued {
                job_id,
                session_id,
                session_ctx,
                plan,
                queued_at,
            } => {
                info!("Job {} queued", job_id);
                return if let Err(e) = self
                    .submit_job(job_id.clone(), session_id, session_ctx, &plan, queued_at)
                    .await
                {
                    let msg = format!("Error planning job {}: {:?}", job_id, e);
                    error!("{}", msg);
                    Ok(Some(QueryStageSchedulerEvent::JobFailed {
                        job_id,
                        fail_message: msg,
                        queued_at,
                        failed_at: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs(),
                    }))
                } else {
                    Ok(Some(QueryStageSchedulerEvent::JobSubmitted {
                        job_id,
                        queued_at,
                        submitted_at: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs(),
                    }))
                };
            }
            QueryStageSchedulerEvent::JobSubmitted {
                job_id,
                queued_at,
                submitted_at,
            } => {
                info!("Job {} submitted", job_id);
                self.metrics_collector
                    .record_submitted(&job_id, queued_at, submitted_at);
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
            QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at,
                completed_at,
            } => {
                info!("Job {} complete", job_id);
                self.metrics_collector
                    .record_completed(&job_id, queued_at, completed_at);
                self.state.task_manager.complete_job(&job_id).await?;
            }
            QueryStageSchedulerEvent::JobFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                error!("Job {} failed: {}", job_id, fail_message);
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);
                self.state
                    .task_manager
                    .fail_job(&job_id, fail_message)
                    .await?;
            }
        }

        Ok(None)
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {:?}", error);
    }
}
