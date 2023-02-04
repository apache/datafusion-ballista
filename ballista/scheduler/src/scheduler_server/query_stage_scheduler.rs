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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, error, info};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};

use crate::metrics::SchedulerMetricsCollector;
use crate::scheduler_server::timestamp_millis;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use tokio::sync::mpsc;

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::executor_manager::ExecutorReservation;
use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    pending_tasks: AtomicUsize,
    job_resubmit_interval_ms: Option<u64>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        job_resubmit_interval_ms: Option<u64>,
    ) -> Self {
        Self {
            state,
            metrics_collector,
            pending_tasks: AtomicUsize::default(),
            job_resubmit_interval_ms,
        }
    }

    pub(crate) fn set_pending_tasks(&self, tasks: usize) {
        self.pending_tasks.store(tasks, Ordering::SeqCst);
        self.metrics_collector
            .set_pending_tasks_queue_size(tasks as u64);
    }

    pub(crate) fn pending_tasks(&self) -> usize {
        self.pending_tasks.load(Ordering::SeqCst)
    }

    pub(crate) fn metrics_collector(&self) -> &dyn SchedulerMetricsCollector {
        self.metrics_collector.as_ref()
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
                queued_at,
            } => {
                info!("Job {} queued with name {:?}", job_id, job_name);

                let state = self.state.clone();
                tokio::spawn(async move {
                    let event = if let Err(e) = state
                        .submit_job(&job_id, &job_name, session_ctx, &plan, queued_at)
                        .await
                    {
                        let fail_message = format!("Error planning job {job_id}: {e:?}");
                        error!("{}", &fail_message);
                        QueryStageSchedulerEvent::JobPlanningFailed {
                            job_id,
                            fail_message,
                            queued_at,
                            failed_at: timestamp_millis(),
                        }
                    } else {
                        QueryStageSchedulerEvent::JobSubmitted {
                            job_id,
                            queued_at,
                            submitted_at: timestamp_millis(),
                            resubmit: false,
                        }
                    };
                    tx_event
                        .post_event(event)
                        .await
                        .map_err(|e| error!("Fail to send event due to {}", e))
                        .unwrap();
                });
            }
            QueryStageSchedulerEvent::JobSubmitted {
                job_id,
                queued_at,
                submitted_at,
                resubmit,
            } => {
                if !resubmit {
                    self.metrics_collector.record_submitted(
                        &job_id,
                        queued_at,
                        submitted_at,
                    );

                    info!("Job {} submitted", job_id);
                } else {
                    debug!("Job {} resubmitted", job_id);
                }

                if self.state.config.is_push_staged_scheduling() {
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

                    if reservations.is_empty() && self.job_resubmit_interval_ms.is_some()
                    {
                        let wait_ms = self.job_resubmit_interval_ms.unwrap();

                        debug!(
                            "No task slots reserved for job {job_id}, resubmitting after {wait_ms}ms"
                        );

                        tokio::task::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(wait_ms)).await;

                            if let Err(e) = tx_event
                                .post_event(QueryStageSchedulerEvent::JobSubmitted {
                                    job_id,
                                    queued_at,
                                    submitted_at,
                                    resubmit: true,
                                })
                                .await
                            {
                                error!("error resubmitting job: {}", e);
                            }
                        });
                    } else {
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
            }
            QueryStageSchedulerEvent::JobPlanningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!("Job {} failed: {}", job_id, fail_message);
                self.state
                    .task_manager
                    .fail_unscheduled_job(&job_id, fail_message)
                    .await?;
            }
            QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at,
                completed_at,
            } => {
                self.metrics_collector
                    .record_completed(&job_id, queued_at, completed_at);

                info!("Job {} success", job_id);
                self.state.task_manager.succeed_job(&job_id).await?;
                self.state.clean_up_successful_job(job_id);
            }
            QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!("Job {} running failed", job_id);
                let (running_tasks, _pending_tasks) = self
                    .state
                    .task_manager
                    .abort_job(&job_id, fail_message)
                    .await?;

                if !running_tasks.is_empty() {
                    tx_event
                        .post_event(QueryStageSchedulerEvent::CancelTasks(running_tasks))
                        .await?;
                }
                self.state.clean_up_failed_job(job_id);
            }
            QueryStageSchedulerEvent::JobUpdated(job_id) => {
                info!("Job {} Updated", job_id);
                self.state.task_manager.update_job(&job_id).await?;
            }
            QueryStageSchedulerEvent::JobCancel(job_id) => {
                self.metrics_collector.record_cancelled(&job_id);

                info!("Job {} Cancelled", job_id);
                let (running_tasks, _pending_tasks) =
                    self.state.task_manager.cancel_job(&job_id).await?;
                self.state.clean_up_failed_job(job_id);

                tx_event
                    .post_event(QueryStageSchedulerEvent::CancelTasks(running_tasks))
                    .await?;
            }
            QueryStageSchedulerEvent::TaskUpdating(executor_id, tasks_status) => {
                let num_status = tasks_status.len();
                match self
                    .state
                    .update_task_statuses(&executor_id, tasks_status)
                    .await
                {
                    Ok((stage_events, offers)) => {
                        if self.state.config.is_push_staged_scheduling() {
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
                let (reservations, pending) =
                    self.state.offer_reservation(reservations).await?;

                self.set_pending_tasks(pending);

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
                            "TaskManager error to handle Executor {executor_id} lost: {e}"
                        );
                        error!("{}", msg);
                    }
                }
            }
            QueryStageSchedulerEvent::CancelTasks(tasks) => {
                self.state
                    .executor_manager
                    .cancel_running_tasks(tasks)
                    .await?;
            }
            QueryStageSchedulerEvent::JobDataClean(job_id) => {
                self.state.executor_manager.clean_up_job_data(job_id);
            }
        }

        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {:?}", error);
    }
}

#[cfg(test)]
mod tests {
    use crate::config::SchedulerConfig;
    use crate::scheduler_server::event::QueryStageSchedulerEvent;
    use crate::test_utils::{await_condition, SchedulerTest, TestMetricsCollector};
    use ballista_core::config::TaskSchedulingPolicy;
    use ballista_core::error::Result;
    use ballista_core::event_loop::EventAction;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, sum, LogicalPlan};
    use datafusion::test_util::scan_empty_with_partitions;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_job_resubmit() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        // Set resubmit interval of 1ms
        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_job_resubmit_interval_ms(1)
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            0,
            0,
            None,
        )
        .await?;

        test.submit("job-id", "job-name", &plan).await?;

        let query_stage_scheduler = test.query_stage_scheduler();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<QueryStageSchedulerEvent>(10);

        let event = QueryStageSchedulerEvent::JobSubmitted {
            job_id: "job-id".to_string(),
            queued_at: 0,
            submitted_at: 0,
            resubmit: false,
        };

        query_stage_scheduler.on_receive(event, &tx, &rx).await?;

        let next_event = rx.recv().await.unwrap();

        assert!(matches!(
            next_event,
            QueryStageSchedulerEvent::JobSubmitted { job_id, resubmit, .. } if job_id == "job-id" && resubmit
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_pending_task_metric() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            1,
            1,
            None,
        )
        .await?;

        test.submit("job-1", "", &plan).await?;

        // First stage has 10 tasks, one of which should be scheduled immediately
        expect_pending_tasks(&test, 9).await;

        test.tick().await?;

        // First task completes and another should be scheduler, so we should have 8
        expect_pending_tasks(&test, 8).await;

        // Complete the 8 remaining tasks in the first stage
        for _ in 0..8 {
            test.tick().await?;
        }

        // The second stage should be resolved so we should have a new pending task
        expect_pending_tasks(&test, 1).await;

        // complete the final task
        test.tick().await?;

        expect_pending_tasks(&test, 0).await;

        // complete the final task
        test.tick().await?;

        // Job should be finished now
        let _ = test.await_completion("job-1").await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_pending_task_metric_on_cancellation() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            1,
            1,
            None,
        )
        .await?;

        test.submit("job-1", "", &plan).await?;

        // First stage has 10 tasks, one of which should be scheduled immediately
        expect_pending_tasks(&test, 9).await;

        test.tick().await?;

        // First task completes and another should be scheduler, so we should have 8
        expect_pending_tasks(&test, 8).await;

        test.cancel("job-1").await?;

        // First task completes and another should be scheduler, so we should have 8
        expect_pending_tasks(&test, 0).await;

        Ok(())
    }

    async fn expect_pending_tasks(test: &SchedulerTest, expected: usize) {
        let success = await_condition(Duration::from_millis(500), 20, || {
            let pending_tasks = test.pending_tasks();

            futures::future::ready(Ok(pending_tasks == expected))
        })
        .await
        .unwrap();

        assert!(
            success,
            "Expected {} pending tasks but found {}",
            expected,
            test.pending_tasks()
        );
    }

    fn test_plan(partitions: usize) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), partitions)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }
}
