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

use crate::cluster::{BallistaCluster, BoundTask, ExecutorSlot};
use crate::config::SchedulerConfig;
use crate::scheduler_server::event::{QueryStageSchedulerEvent, SubmitPlan};
use crate::scheduler_server::timestamp_millis;
use crate::state::execution_graph::TaskDescription;
use crate::state::executor_manager::ExecutorManager;
use crate::state::session_manager::SessionManager;
use crate::state::task_manager::{TaskLauncher, TaskManager};
use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::EventSender;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::TaskStatus;
use ballista_core::{JobId, JobStatusSubscriber};
use datafusion::execution::context::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use log::{debug, error, info, warn};
use prost::Message;
use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

mod aqe;
mod distributed_explain;
/// Execution graph representation and management.
pub mod execution_graph;
/// DOT format export for execution graphs.
pub mod execution_graph_dot;
/// Execution stage tracking and status management.
pub mod execution_stage;
/// Executor registration and management.
pub mod executor_manager;
/// Session state management.
pub mod session_manager;
/// Per-task plan rewriter (restrict scan/shuffle-reader to task's slice).
pub mod task_builder;
/// Task scheduling and lifecycle management.
pub mod task_manager;

/// Decodes a protobuf message from bytes.
pub fn decode_protobuf<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    T::decode(bytes).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not deserialize {}: {}",
            type_name::<T>(),
            e
        ))
    })
}

/// Decodes a protobuf message and converts it to another type.
pub fn decode_into<T: Message + Default + Into<U>, U>(bytes: &[u8]) -> Result<U> {
    T::decode(bytes)
        .map_err(|e| {
            BallistaError::Internal(format!(
                "Could not deserialize {}: {}",
                type_name::<T>(),
                e
            ))
        })
        .map(|t| t.into())
}

/// Encodes a protobuf message to bytes.
pub fn encode_protobuf<T: Message + Default>(msg: &T) -> Result<Vec<u8>> {
    let mut value: Vec<u8> = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut value).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not serialize {}: {}",
            type_name::<T>(),
            e
        ))
    })?;
    Ok(value)
}

/// Shared state for the Ballista scheduler.
///
/// Contains managers for executors, tasks, and sessions.
#[derive(Clone)]
pub struct SchedulerState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    /// Manager for executor registration and task slot allocation.
    pub executor_manager: ExecutorManager,
    /// Manager for job and task scheduling.
    pub task_manager: TaskManager<T, U>,
    /// Manager for DataFusion session contexts.
    pub session_manager: SessionManager,
    /// Codec for serializing logical and physical plans.
    pub codec: BallistaCodec<T, U>,
    /// Scheduler configuration.
    pub config: Arc<SchedulerConfig>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    /// Creates a new `SchedulerState` with the given cluster and configuration.
    pub fn new(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        scheduler_name: String,
        config: Arc<SchedulerConfig>,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(
                cluster.cluster_state(),
                config.clone(),
            ),
            task_manager: TaskManager::new(
                cluster.job_state(),
                codec.clone(),
                scheduler_name,
                config.clone(),
            ),
            session_manager: SessionManager::new(cluster.job_state()),
            codec,
            config,
        }
    }

    /// Creates a new `SchedulerState` with default scheduler name (for testing only).
    #[cfg(test)]
    pub fn new_with_default_scheduler_name(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        let config = Arc::new(SchedulerConfig::default());
        SchedulerState::new(cluster, codec, "localhost:50050".to_owned(), config)
    }

    #[allow(dead_code)]
    pub(crate) fn new_with_task_launcher(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        scheduler_name: String,
        config: Arc<SchedulerConfig>,
        dispatcher: Arc<dyn TaskLauncher>,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(
                cluster.cluster_state(),
                config.clone(),
            ),
            task_manager: TaskManager::with_launcher(
                cluster.job_state(),
                codec.clone(),
                scheduler_name,
                dispatcher,
                config.clone(),
            ),
            session_manager: SessionManager::new(cluster.job_state()),
            codec,
            config,
        }
    }

    /// Initializes the scheduler state.
    pub async fn init(&self) -> Result<()> {
        self.executor_manager.init().await
    }

    pub(crate) async fn revive_offers(
        &self,
        sender: EventSender<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let schedulable_tasks = self
            .executor_manager
            .bind_schedulable_tasks(self.task_manager.get_running_job_cache())
            .await?;
        if schedulable_tasks.is_empty() {
            debug!("No schedulable tasks found to be launched");
            return Ok(());
        }

        let state = self.clone();
        tokio::spawn(async move {
            let mut if_revive = false;
            match state.launch_tasks(schedulable_tasks, &sender).await {
                Ok((unassigned_executor_slots, failed_jobs)) => {
                    if !unassigned_executor_slots.is_empty() {
                        if let Err(e) = state
                            .executor_manager
                            .unbind_tasks(unassigned_executor_slots)
                            .await
                        {
                            error!("Fail to unbind tasks: {e}");
                        }
                        if_revive = true;
                    }
                    for job in failed_jobs {
                        if let Err(e) = sender
                            .post_event(QueryStageSchedulerEvent::JobRunningFailed {
                                job_id: job,
                                fail_message: "task serialization failed by executor"
                                    .to_string(),
                                queued_at: timestamp_millis(),
                                failed_at: timestamp_millis(),
                            })
                            .await
                        {
                            error!("Fail to post JobRunningFailed: {e:?}");
                        }
                    }
                }
                Err(e) => {
                    error!("Fail to launch tasks: {e}");
                    if_revive = true;
                }
            }
            if if_revive
                && let Err(e) = sender
                    .post_event(QueryStageSchedulerEvent::ReviveOffers)
                    .await
            {
                error!("Fail to send revive offers event due to {e:?}");
            }
        });

        Ok(())
    }

    /// Remove an executor.
    /// 1. The executor related info will be removed from [`ExecutorManager`]
    /// 2. A [`QueryStageSchedulerEvent::ExecutorLost`] is posted, which rolls
    ///    back the affected running execution graphs, cancels their running
    ///    tasks, and — when this was the last executor — arms the grace timer
    ///    that fails the jobs left behind on an empty cluster.
    ///
    /// Every removal path must go through here, because step 1 also drops the
    /// executor's heartbeat: once it is gone, nothing else can notice the
    /// executor is missing and post the event later.
    /// See <https://github.com/apache/datafusion-ballista/issues/2226>
    pub(crate) async fn remove_executor(
        &self,
        executor_id: &str,
        reason: Option<String>,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        if let Err(e) = self
            .executor_manager
            .remove_executor(executor_id, reason.clone())
            .await
        {
            warn!("Fail to remove executor {executor_id}: {e}");
        }

        if let Err(e) = sender
            .post_event(QueryStageSchedulerEvent::ExecutorLost(
                executor_id.to_owned(),
                reason,
            ))
            .await
        {
            error!("Fail to post ExecutorLost for executor {executor_id}: {e:?}");
        }
    }

    /// Given a vector of bound tasks,
    /// 1. Firstly reorganize according to: executor -> job stage -> tasks;
    /// 2. Then launch the task set vector to each executor one by one.
    ///
    /// If it fails to launch a task set, the related [`ExecutorSlot`] will be returned.
    ///
    /// Returns the freed executor slots and the set of job IDs the executors
    /// rejected (failed individually while the rest of the batch ran).
    async fn launch_tasks(
        &self,
        bound_tasks: Vec<BoundTask>,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) -> Result<(Vec<ExecutorSlot>, HashSet<JobId>)> {
        // Put tasks to the same executor together
        // And put tasks belonging to the same stage together for creating MultiTaskDefinition
        let mut executor_stage_assignments: HashMap<
            String,
            HashMap<(JobId, usize), Vec<TaskDescription>>,
        > = HashMap::new();
        for (executor_id, task) in bound_tasks.into_iter() {
            let stage_key = (task.key.job_id.clone(), task.key.stage_id);
            if let Some(tasks) = executor_stage_assignments.get_mut(&executor_id) {
                if let Some(executor_stage_tasks) = tasks.get_mut(&stage_key) {
                    executor_stage_tasks.push(task);
                } else {
                    tasks.insert(stage_key, vec![task]);
                }
            } else {
                let mut executor_stage_tasks: HashMap<
                    (JobId, usize),
                    Vec<TaskDescription>,
                > = HashMap::new();
                executor_stage_tasks.insert(stage_key, vec![task]);
                executor_stage_assignments.insert(executor_id, executor_stage_tasks);
            }
        }
        let mut join_handles = vec![];
        for (executor_id, tasks) in executor_stage_assignments.into_iter() {
            let tasks: Vec<Vec<TaskDescription>> = tasks.into_values().collect();
            // Total number of tasks to be launched for one executor
            let n_tasks: usize = tasks.iter().map(|stage_tasks| stage_tasks.len()).sum();
            let state = self.clone();
            let sender = sender.clone();
            let join_handle = tokio::spawn(async move {
                let job_ids: Vec<JobId> = tasks
                    .iter()
                    .flatten()
                    .map(|t| t.key.job_id.clone())
                    .collect();
                match state
                    .executor_manager
                    .get_executor_metadata(&executor_id)
                    .await
                {
                    Ok(executor) => {
                        match state
                            .task_manager
                            .launch_multi_task(&executor, tasks, &state.executor_manager)
                            .await
                        {
                            Ok(rejected) => {
                                let freed = job_ids
                                    .iter()
                                    .filter(|j| rejected.contains(*j))
                                    .count()
                                    as u32;
                                (vec![(executor_id.clone(), freed)], rejected)
                            }
                            Err(e) => {
                                let err_msg = format!("Failed to launch new task: {e}");
                                error!("{}", err_msg.clone());

                                // It's OK to remove executor aggressively,
                                // since if the executor is in healthy state, it will be registered again.
                                state
                                    .remove_executor(&executor_id, Some(err_msg), &sender)
                                    .await;

                                (
                                    vec![(executor_id.clone(), n_tasks as u32)],
                                    HashSet::new(),
                                )
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to launch new task, could not get executor metadata: {e}"
                        );
                        (vec![(executor_id.clone(), n_tasks as u32)], HashSet::new())
                    }
                }
            });
            join_handles.push(join_handle);
        }

        let results = futures::future::join_all(join_handles)
            .await
            .into_iter()
            .collect::<std::result::Result<
            Vec<(Vec<ExecutorSlot>, HashSet<JobId>)>,
            tokio::task::JoinError,
        >>()?;

        let mut unassigned_executor_slots = Vec::new();
        let mut failed_jobs = HashSet::new();
        for (slots, jobs) in results {
            unassigned_executor_slots.extend(slots);
            failed_jobs.extend(jobs);
        }

        Ok((unassigned_executor_slots, failed_jobs))
    }

    pub(crate) async fn update_task_statuses(
        &self,
        executor_id: &str,
        tasks_status: Vec<TaskStatus>,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let executor = self
            .executor_manager
            .get_executor_metadata(executor_id)
            .await?;

        self.task_manager
            .update_task_statuses(&executor, tasks_status)
            .await
    }

    pub(crate) async fn submit_job(
        &self,
        job_id: &JobId,
        job_name: &str,
        session_ctx: Arc<SessionContext>,
        plan: &SubmitPlan,
        queued_at: u64,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        let start = Instant::now();

        self.task_manager
            .submit_plan(job_id, job_name, session_ctx, plan, queued_at, subscriber)
            .await?;

        let elapsed = start.elapsed();

        info!("Job [{job_id}] planning took {elapsed:?}");

        Ok(())
    }

    /// Immediately reclaim intermediate shuffle data, then spawn a delayed
    /// future to clean up the remaining job data (final-stage output + job
    /// state) on both Scheduler and Executors.
    pub(crate) fn clean_up_successful_job(
        &self,
        job_id: JobId,
        intermediate_stage_ids: Vec<u32>,
    ) {
        self.executor_manager
            .clean_up_intermediate_job_data(job_id.clone(), intermediate_stage_ids);
        self.executor_manager.clean_up_job_data_delayed(
            job_id.clone(),
            self.config.finished_job_data_clean_up_interval_seconds,
        );
        self.task_manager.clean_up_job_delayed(
            job_id,
            self.config.finished_job_state_clean_up_interval_seconds,
        );
    }

    /// Spawn a delayed future to clean up job data on both Scheduler and Executors
    pub(crate) fn clean_up_failed_job(&self, job_id: JobId) {
        self.executor_manager.clean_up_job_data(job_id.clone());
        self.task_manager.clean_up_job_delayed(
            job_id,
            self.config.finished_job_state_clean_up_interval_seconds,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchedulerConfig;
    use crate::scheduler_server::timestamp_millis;
    use crate::state::executor_manager::ExecutorManager;
    use crate::state::task_manager::TaskLauncher;
    use crate::test_utils::test_cluster_context;
    use ballista_core::extension::SessionConfigExt;
    use ballista_core::serde::BallistaCodec;
    use ballista_core::serde::protobuf::MultiTaskDefinition;
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorMetadata, ExecutorOperatingSystemSpecification,
        ExecutorSpecification,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionConfig;
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::logical_expr::{LogicalPlan, col};
    use datafusion::test_util::scan_empty_with_partitions;
    use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

    struct RejectOne {
        reject: JobId,
    }

    #[async_trait::async_trait]
    impl TaskLauncher for RejectOne {
        async fn launch_tasks(
            &self,
            _executor: &ExecutorMetadata,
            tasks: Vec<MultiTaskDefinition>,
            _executor_manager: &ExecutorManager,
        ) -> Result<HashSet<JobId>> {
            Ok(tasks
                .iter()
                .map(|t| JobId::from(t.job_id.clone()))
                .filter(|j| j == &self.reject)
                .take(1)
                .collect())
        }
    }

    fn agg_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);
        scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), 2)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn launch_tasks_isolates_single_rejected_job() -> Result<()> {
        let bad_job = JobId::from("job-bad");
        let good_job = JobId::from("job-good");

        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new_with_task_launcher(
                test_cluster_context(),
                BallistaCodec::default(),
                "localhost:50050".to_owned(),
                Arc::new(SchedulerConfig::default()),
                Arc::new(RejectOne {
                    reject: bad_job.clone(),
                }),
            );

        let vcores = 8;
        state
            .executor_manager
            .register_executor(
                ExecutorMetadata {
                    id: "executor-1".to_string(),
                    host: String::default(),
                    port: 0,
                    grpc_port: 0,
                    specification: ExecutorSpecification::default().with_vcores(vcores),
                    os_info: ExecutorOperatingSystemSpecification::default(),
                },
                ExecutorData {
                    executor_id: "executor-1".to_string(),
                    total_vcores: vcores,
                    available_vcores: vcores,
                },
            )
            .await?;

        let ctx = state
            .session_manager
            .create_or_update_session("session", &SessionConfig::new_with_ballista())
            .await?;

        for job_id in [&good_job, &bad_job] {
            state
                .task_manager
                .queue_job(job_id, "", timestamp_millis())?;
            state
                .task_manager
                .submit_job(
                    job_id,
                    "",
                    ctx.clone(),
                    &agg_plan(),
                    timestamp_millis(),
                    None,
                )
                .await?;
        }

        let bound = state
            .executor_manager
            .bind_schedulable_tasks(state.task_manager.get_running_job_cache())
            .await?;

        let bad_task_count = bound
            .iter()
            .filter(|(_, t)| t.key.job_id == bad_job)
            .count() as u32;
        let good_task_count = bound
            .iter()
            .filter(|(_, t)| t.key.job_id == good_job)
            .count() as u32;
        assert!(bad_task_count > 0 && good_task_count > 0);

        let (tx_event, _rx_event) = tokio::sync::mpsc::channel(100);
        let sender = EventSender::new(tx_event);
        let (unassigned_slots, failed_jobs) = state.launch_tasks(bound, &sender).await?;

        assert_eq!(failed_jobs, HashSet::from([bad_job.clone()]));

        let freed: u32 = unassigned_slots.iter().map(|(_, n)| *n).sum();
        assert_eq!(freed, bad_task_count);

        assert!(
            state
                .executor_manager
                .get_executor_metadata("executor-1")
                .await
                .is_ok()
        );

        Ok(())
    }
}
