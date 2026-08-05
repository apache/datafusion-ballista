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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ballista_core::JobId;
use ballista_core::serde::protobuf::{FailedJob, JobStatus, job_status};
use log::{debug, error, info, trace, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};
use tokio::sync::mpsc::error::TrySendError;

use crate::config::SchedulerConfig;
use crate::metrics::SchedulerMetricsCollector;
use crate::scheduler_server::timestamp_millis;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    config: Arc<SchedulerConfig>,
    /// Guards against arming more than one "all executors lost" grace timer at a
    /// time. When a whole cluster dies at once the reaper posts an `ExecutorLost`
    /// per executor, and each would otherwise arm its own timer and fail every
    /// running job again. See <https://github.com/apache/datafusion-ballista/issues/2029>
    no_executor_check_pending: Arc<AtomicBool>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        config: Arc<SchedulerConfig>,
    ) -> Self {
        Self {
            state,
            metrics_collector,
            config,
            no_executor_check_pending: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn abort_job(&self, job_id: &JobId, failure_reason: String) -> Result<()> {
        let executor_manager = self.state.executor_manager.clone();
        self.state
            .task_manager
            .abort_job(job_id, failure_reason, move |running_tasks| async move {
                if running_tasks.is_empty() {
                    Ok(())
                } else {
                    executor_manager.cancel_running_tasks(running_tasks).await
                }
            })
            .await?;
        Ok(())
    }

    #[cfg(feature = "rest-api")]
    pub(crate) fn metrics_collector(&self) -> &dyn SchedulerMetricsCollector {
        self.metrics_collector.as_ref()
    }
}

#[async_trait::async_trait]
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
        let mut time_recorder = None;
        if self.config.scheduler_event_expected_processing_duration > 0 {
            time_recorder = Some((Instant::now(), event.clone()));
        };
        let event_sender = EventSender::new(tx_event.clone());
        match event {
            QueryStageSchedulerEvent::JobQueued {
                job_id,
                job_name,
                session_ctx,
                plan,
                queued_at,
                subscriber,
            } => {
                info!("Job queued: [{job_id}]");

                if let Err(e) = self
                    .state
                    .task_manager
                    .queue_job(&job_id, &job_name, queued_at)
                {
                    error!("Fail to queue job {job_id} due to {e:?}");
                    return Ok(());
                }

                let state = self.state.clone();
                tokio::spawn(async move {
                    let event = if let Err(e) = state
                        .submit_job(
                            &job_id,
                            &job_name,
                            session_ctx,
                            &plan,
                            queued_at,
                            subscriber.clone(),
                        )
                        .await
                    {
                        let error = e.to_string();
                        let fail_message = format!("Error planning job {job_id}: {e:?}");

                        // this is a corner case, as most of job status changes are handled in
                        // job state, after job is submitted to job state
                        if let Some(subscriber) = subscriber {
                            let timestamp = timestamp_millis();
                            let job_status = JobStatus {
                                job_id: job_id.clone().into(),
                                job_name,
                                status: Some(ballista_core::serde::protobuf::job_status::Status::Failed(
                                    FailedJob { error, queued_at, started_at: timestamp, ended_at: timestamp }
                                ))
                            };

                            if matches!(
                                subscriber.try_send(job_status),
                                Err(TrySendError::Full(_))
                            ) {
                                error!(
                                    "jobs notification subscriber for job {} is blocked, can't deliver status update, job notification will be missed",
                                    job_id
                                )
                            }
                        }

                        error!("{}", fail_message);
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
                        }
                    };
                    if let Err(e) = event_sender.post_event(event).await {
                        error!("Fail to send event due to {e}");
                    }
                });
            }
            QueryStageSchedulerEvent::JobSubmitted {
                job_id,
                queued_at,
                submitted_at,
            } => {
                self.metrics_collector
                    .record_submitted(&job_id, queued_at, submitted_at);

                info!("Job submitted: [{job_id}]");

                if self.state.config.is_push_staged_scheduling() {
                    event_sender
                        .post_event(QueryStageSchedulerEvent::ReviveOffers)
                        .await?;
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

                error!("Job {job_id} failed: {fail_message}");
                if let Err(e) = self
                    .state
                    .task_manager
                    .fail_unscheduled_job(&job_id, fail_message)
                    .await
                {
                    error!(
                        "Fail to invoke fail_unscheduled_job for job {job_id} due to {e:?}"
                    );
                }
            }
            QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at,
                completed_at,
            } => {
                self.metrics_collector
                    .record_completed(&job_id, queued_at, completed_at);

                info!("Job finished successfully: [{job_id}]");
                let intermediate_stage_ids =
                    match self.state.task_manager.succeed_job(&job_id).await {
                        Ok(ids) => ids,
                        Err(e) => {
                            error!(
                                "Fail to invoke succeed_job for job {job_id} due to {e:?}"
                            );
                            vec![]
                        }
                    };
                self.state
                    .clean_up_successful_job(job_id, intermediate_stage_ids);
            }
            QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!("Job failed: [{job_id}]");
                if let Err(e) = self.abort_job(&job_id, fail_message).await {
                    error!("Fail to abort job {job_id} due to {e:?}");
                }
                self.state.clean_up_failed_job(job_id);
            }
            QueryStageSchedulerEvent::JobUpdated(job_id) => {
                debug!("Job updated, job_id: [{job_id}]");
                if let Err(e) = self.state.task_manager.update_job(&job_id).await {
                    error!("Fail to invoke update_job for job {job_id} due to {e:?}");
                }
            }
            QueryStageSchedulerEvent::JobCancel(job_id) => {
                self.metrics_collector.record_cancelled(&job_id);

                info!("Job cancelled: [{job_id}]");
                if let Err(e) = self.abort_job(&job_id, "Cancelled".to_owned()).await {
                    error!("Fail to cancel job {job_id} due to {e:?}");
                }
                self.state.clean_up_failed_job(job_id);
            }
            QueryStageSchedulerEvent::TaskUpdating(executor_id, tasks_status) => {
                trace!(
                    "processing task status updates from {executor_id}: {tasks_status:?}"
                );

                let num_status = tasks_status.len();
                if self.state.config.is_push_staged_scheduling() {
                    // Refund the vcores each completing task consumed at bind
                    // time (see `bind_one` in `cluster/mod.rs`). Refunding
                    // one vcore per task would leak leftovers under the
                    // multi-partition-task model, draining executor budgets
                    // to 1 vcore over the course of a query.
                    let vcores_freed = self
                        .state
                        .task_manager
                        .sum_vcores_for_statuses(&tasks_status)
                        .await;
                    self.state
                        .executor_manager
                        .unbind_tasks(vec![(executor_id.clone(), vcores_freed)])
                        .await?;
                }
                match self
                    .state
                    .update_task_statuses(&executor_id, tasks_status)
                    .await
                {
                    Ok(stage_events) => {
                        if self.state.config.is_push_staged_scheduling() {
                            event_sender
                                .post_event(QueryStageSchedulerEvent::ReviveOffers)
                                .await?;
                        }

                        for stage_event in stage_events {
                            event_sender.post_event(stage_event).await?;
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to update {num_status} task statuses for Executor {executor_id}: {e:?}"
                        );
                    }
                }
            }
            QueryStageSchedulerEvent::ReviveOffers => {
                self.state.revive_offers(event_sender).await?;
            }
            QueryStageSchedulerEvent::ExecutorLost(executor_id, _) => {
                match self.state.task_manager.executor_lost(&executor_id).await {
                    Ok(tasks) => {
                        if !tasks.is_empty()
                            && let Err(e) = self
                                .state
                                .executor_manager
                                .cancel_running_tasks(tasks)
                                .await
                        {
                            warn!("Fail to cancel running tasks due to {e:?}");
                        }
                    }
                    Err(e) => {
                        let msg = format!(
                            "TaskManager error to handle Executor {executor_id} lost: {e}"
                        );
                        error!("{msg}");
                    }
                }

                // If that was the last executor, the running jobs whose tasks were
                // just reset can no longer make progress — there is nothing to
                // schedule them onto. Rather than hang forever, wait a bounded
                // grace period for an executor to (re)register (e.g. a rolling
                // restart) and then fail any job still running on an empty cluster.
                // Only fires for executors that were actually present, so jobs
                // merely queued waiting for their first executor (autoscaling cold
                // start) are never affected.
                // See https://github.com/apache/datafusion-ballista/issues/2029
                //
                // `no_executor_check_pending` collapses the burst of `ExecutorLost`
                // events produced when a whole cluster dies at once into a single
                // timer, so each running job is failed at most once.
                if self.state.executor_manager.get_alive_executors().is_empty()
                    && !self.no_executor_check_pending.swap(true, Ordering::SeqCst)
                {
                    let state = self.state.clone();
                    let sender = event_sender.clone();
                    let pending = self.no_executor_check_pending.clone();
                    let grace = Duration::from_secs(
                        state.config.no_executors_grace_period_seconds,
                    );
                    let lost_at = timestamp_millis();
                    tokio::spawn(async move {
                        tokio::time::sleep(grace).await;

                        // An executor may have (re)registered during the grace
                        // window; if so the reset tasks will be scheduled onto it
                        // and there is nothing to fail.
                        if state.executor_manager.get_alive_executors().is_empty() {
                            for job_id in
                                state.task_manager.get_running_job_cache().keys()
                            {
                                // Re-read the live status right before failing: a
                                // job that finished during the grace window must
                                // not be failed, and a job planned *after* the
                                // cluster went empty (started_at > lost_at) has its
                                // own window and must not inherit this one.
                                let queued_at = match state
                                    .task_manager
                                    .get_job_status(job_id)
                                    .await
                                {
                                    Ok(Some(JobStatus {
                                        status: Some(job_status::Status::Running(running)),
                                        ..
                                    })) if running.started_at <= lost_at => {
                                        running.queued_at
                                    }
                                    _ => continue,
                                };

                                let fail_message = format!(
                                    "all executors were lost and no executor re-registered within {}s; no executors remain to run the tasks for this job",
                                    grace.as_secs()
                                );
                                warn!("Failing job {job_id}: {fail_message}");
                                if let Err(e) = sender
                                    .post_event(
                                        QueryStageSchedulerEvent::JobRunningFailed {
                                            job_id: job_id.clone(),
                                            fail_message,
                                            queued_at,
                                            failed_at: timestamp_millis(),
                                        },
                                    )
                                    .await
                                {
                                    error!(
                                        "Fail to post JobRunningFailed for job {job_id}: {e:?}"
                                    );
                                }
                            }
                        }

                        // Cleared last, so the whole burst of `ExecutorLost` events
                        // that a simultaneous cluster death produces collapses into
                        // this single check — even when the grace period is 0.
                        pending.store(false, Ordering::SeqCst);
                    });
                }
            }
            QueryStageSchedulerEvent::CancelTasks(tasks) => {
                if let Err(e) = self
                    .state
                    .executor_manager
                    .cancel_running_tasks(tasks)
                    .await
                {
                    warn!("Fail to cancel running tasks due to {e:?}");
                }
            }
            QueryStageSchedulerEvent::JobDataClean(job_id) => {
                self.state.executor_manager.clean_up_job_data(job_id);
            }
        }
        if let Some((start, ec)) = time_recorder {
            let duration = start.elapsed();
            if duration.ge(&Duration::from_micros(
                self.config.scheduler_event_expected_processing_duration,
            )) {
                warn!(
                    "[METRICS] {:?} event cost {:?} us!",
                    ec,
                    duration.as_micros()
                );
            }
        }
        Ok(())
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {error:?}");
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::JobStateEvent;
    use crate::config::SchedulerConfig;
    use crate::test_utils::{SchedulerTest, TestMetricsCollector, await_condition};
    use ballista_core::config::TaskSchedulingPolicy;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::job_status;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::logical_expr::{LogicalPlan, col};
    use datafusion::test_util::scan_empty_with_partitions;
    use futures::StreamExt;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_pending_job_metric() -> Result<()> {
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

        let job_id = test.submit("", &plan).await?;

        test.tick().await?;

        let pending_jobs = test.pending_job_number();
        let expected = 0usize;
        assert_eq!(
            expected, pending_jobs,
            "Expected {expected} pending jobs but found {pending_jobs}"
        );

        let running_jobs = test.running_job_number();
        let expected = 1usize;
        assert_eq!(
            expected, running_jobs,
            "Expected {expected} running jobs but found {running_jobs}"
        );

        test.cancel(&job_id).await?;

        let expected = 0usize;
        let success = await_condition(Duration::from_millis(10), 20, || {
            let running_jobs = test.running_job_number();

            futures::future::ready(Ok(running_jobs == expected))
        })
        .await
        .unwrap();
        assert!(
            success,
            "Expected {} running jobs but found {}",
            expected,
            test.running_job_number()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_exposes_job_state_events() -> Result<()> {
        let plan = test_plan(1);
        let metrics_collector = Arc::new(TestMetricsCollector::default());
        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector,
            1,
            1,
            None,
        )
        .await?;

        let mut events = test.job_state_events().await?;
        let (_, job_id) = test.run("", &plan).await?;

        let received_success = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = events.next().await {
                if matches!(
                    event,
                    JobStateEvent::JobUpdated {
                        job_id: event_job_id,
                        status,
                    } if event_job_id == job_id
                        && matches!(
                            status.status,
                            Some(job_status::Status::Successful(_))
                        )
                ) {
                    return true;
                }
            }
            false
        })
        .await
        .expect("successful job state event should arrive");

        assert!(
            received_success,
            "job state event stream closed unexpectedly"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_running_job_fails_when_all_executors_are_lost() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        // Grace period of 0 so the job is failed as soon as the loss is observed,
        // keeping the test fast.
        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged)
                .with_no_executors_grace_period_seconds(0),
            metrics_collector.clone(),
            1,
            1,
            None,
        )
        .await?;

        let job_id = test.submit("", &plan).await?;

        // Wait until the job is actually running with tasks in flight. We
        // deliberately never `tick()`, so its tasks never complete.
        let job_id_ref = &job_id;
        let test_ref = &test;
        let running = await_condition(Duration::from_millis(50), 40, || async move {
            let status = test_ref.job_status(job_id_ref).await?;
            Ok(matches!(
                status.and_then(|s| s.status),
                Some(job_status::Status::Running(_))
            ))
        })
        .await?;
        assert!(running, "job should reach the running state");

        // The only executor is lost. With no executors left, the reset tasks can
        // never be scheduled, so the job must fail rather than hang forever
        // (#2029).
        test.lose_executor("virtual-executor-0").await?;

        let failed = await_condition(Duration::from_millis(100), 50, || async move {
            let status = test_ref.job_status(job_id_ref).await?;
            Ok(matches!(
                status.and_then(|s| s.status),
                Some(job_status::Status::Failed(_))
            ))
        })
        .await?;
        assert!(
            failed,
            "job should be failed after all executors were lost, but status was {:?}",
            test.job_status(&job_id).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_running_job_survives_partial_executor_loss() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged)
                .with_no_executors_grace_period_seconds(0),
            metrics_collector.clone(),
            2,
            1,
            None,
        )
        .await?;

        let job_id = test.submit("", &plan).await?;

        let job_id_ref = &job_id;
        let test_ref = &test;
        let running = await_condition(Duration::from_millis(50), 40, || async move {
            let status = test_ref.job_status(job_id_ref).await?;
            Ok(matches!(
                status.and_then(|s| s.status),
                Some(job_status::Status::Running(_))
            ))
        })
        .await?;
        assert!(running, "job should reach the running state");

        // Lose only one of two executors. One remains alive, so the job must not
        // be failed by the total-loss guard.
        test.lose_executor("virtual-executor-0").await?;

        // Give the (grace-0) failure path ample time to fire if it were going to.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let status = test.job_status(&job_id).await?;
        assert!(
            !matches!(
                status.as_ref().and_then(|s| s.status.clone()),
                Some(job_status::Status::Failed(_))
            ),
            "job must not be failed while an executor remains, but status was {status:?}"
        );

        Ok(())
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
