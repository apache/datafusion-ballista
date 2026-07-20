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

use ballista_core::JobId;
use ballista_core::serde::protobuf::{FailedJob, JobStatus};
use log::{debug, error, info, trace, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};
use tokio::sync::mpsc::error::TrySendError;

use crate::config::{SchedulerConfig, WorkAvailableReason};
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

                // The graph was revived before caching, so the job's tasks
                // are already visible to polling executors.
                if let Some(callback) = &self.config.on_work_available {
                    callback(WorkAvailableReason::JobSubmitted { job_id });
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
                match self.state.task_manager.update_job(&job_id).await {
                    Ok(new_tasks) => {
                        // update_job revived the graph: the new tasks are
                        // already visible to polling executors.
                        if new_tasks > 0
                            && let Some(callback) = &self.config.on_work_available
                        {
                            callback(WorkAvailableReason::NewStagesRunnable { job_id });
                        }
                    }
                    Err(e) => {
                        error!("Fail to invoke update_job for job {job_id} due to {e:?}");
                    }
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
    use crate::config::{SchedulerConfig, WorkAvailableReason};
    use crate::scheduler_server::SchedulerServer;
    use crate::test_utils::{
        SchedulerTest, TestMetricsCollector, await_condition, test_cluster_context,
    };
    use ballista_core::BALLISTA_PROTOCOL_VERSION;
    use ballista_core::config::TaskSchedulingPolicy;
    use ballista_core::error::Result;
    use ballista_core::extension::SessionConfigExt;
    use ballista_core::serde::BallistaCodec;
    use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpc;
    use ballista_core::serde::protobuf::{
        ExecutorRegistration, PollWorkParams, ShuffleWritePartition, SuccessfulTask,
        TaskStatus, task_status,
    };
    use ballista_core::serde::scheduler::{
        ExecutorOperatingSystemSpecification, ExecutorSpecification,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::logical_expr::{LogicalPlan, col};
    use datafusion::prelude::SessionConfig;
    use datafusion::test_util::scan_empty_with_partitions;
    use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tonic::Request;

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
    async fn test_on_work_available_callback() -> Result<()> {
        let reasons: Arc<Mutex<Vec<WorkAvailableReason>>> = Arc::default();
        let captured = reasons.clone();

        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                test_cluster_context(),
                BallistaCodec::default(),
                Arc::new(
                    SchedulerConfig {
                        scheduling_policy: TaskSchedulingPolicy::PullStaged,
                        ..Default::default()
                    }
                    .with_on_work_available(Arc::new(
                        move |reason| {
                            captured.lock().unwrap().push(reason);
                        },
                    )),
                ),
                Arc::new(TestMetricsCollector::default()),
            );
        scheduler.init().await?;

        let ctx = scheduler
            .state
            .session_manager
            .create_or_update_session(
                "session",
                &SessionConfig::new_with_ballista().with_target_partitions(2),
            )
            .await?;
        let job_id = scheduler.submit_job("", ctx, &test_plan(2), None).await?;

        // Job submission runs asynchronously through the event loop.
        let submitted = await_condition(Duration::from_millis(10), 100, || {
            futures::future::ready(Ok(!reasons.lock().unwrap().is_empty()))
        })
        .await?;
        assert!(submitted, "JobSubmitted callback never fired");
        assert_eq!(
            reasons.lock().unwrap().first(),
            Some(&WorkAvailableReason::JobSubmitted {
                job_id: job_id.clone()
            })
        );

        // Pull the shuffle stage's tasks; the callback promised they are
        // visible by the time it fired.
        let exec_meta = ExecutorRegistration {
            id: "executor-1".to_owned(),
            host: Some("localhost".to_owned()),
            port: 50051,
            grpc_port: 50052,
            specification: Some(ExecutorSpecification::default().with_vcores(2).into()),
            os_info: Some(ExecutorOperatingSystemSpecification::default().into()),
            ballista_protocol_version: BALLISTA_PROTOCOL_VERSION,
        };
        let polled = scheduler
            .poll_work(Request::new(PollWorkParams {
                metadata: Some(exec_meta.clone()),
                num_free_vcores: 2,
                task_status: vec![],
            }))
            .await
            .expect("poll_work failed")
            .into_inner();
        assert!(
            !polled.tasks.is_empty(),
            "expected tasks after JobSubmitted"
        );

        // Report the pulled tasks as successful; each writes the plan's two
        // shuffle output partitions.
        let task_status = polled
            .tasks
            .iter()
            .map(|task| TaskStatus {
                task_id: task.task_id,
                job_id: task.job_id.clone(),
                stage_id: task.stage_id,
                stage_attempt_num: task.stage_attempt_num,
                launch_time: 0,
                start_exec_time: 0,
                end_exec_time: 0,
                metrics: vec![],
                status: Some(task_status::Status::Successful(SuccessfulTask {
                    executor_id: exec_meta.id.clone(),
                    partitions: (0..2)
                        .map(|partition_id| ShuffleWritePartition {
                            partition_id,
                            num_batches: 1,
                            num_rows: 1,
                            num_bytes: 1,
                            file_id: None,
                            is_sort_shuffle: false,
                        })
                        .collect(),
                })),
            })
            .collect();
        scheduler
            .poll_work(Request::new(PollWorkParams {
                metadata: Some(exec_meta),
                num_free_vcores: 2,
                task_status,
            }))
            .await
            .expect("poll_work with task status failed");

        // Completing the shuffle stage resolves the final stage.
        let resolved = await_condition(Duration::from_millis(10), 100, || {
            futures::future::ready(Ok(reasons.lock().unwrap().contains(
                &WorkAvailableReason::NewStagesRunnable {
                    job_id: job_id.clone(),
                },
            )))
        })
        .await?;
        assert!(
            resolved,
            "expected NewStagesRunnable, got {:?}",
            reasons.lock().unwrap()
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
