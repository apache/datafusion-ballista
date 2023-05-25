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

use datafusion::common::tree_node::{TreeNode, VisitRecursion};
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::{ListingTable, ListingTableUrl};
use datafusion::datasource::source_as_provider;
use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::circuit_breaker::controller::CircuitBreakerController;
use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};
use crate::state::session_manager::SessionManager;
use crate::state::task_manager::{TaskLauncher, TaskManager};

use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;
use crate::scheduler_server::timestamp_millis;
use crate::state::execution_graph::TaskDescription;
use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::EventSender;
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::{task_status, FailedTask, IoError, TaskStatus};
use ballista_core::serde::BallistaCodec;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use prost::Message;
use tracing::{debug, error, info, warn};

pub mod execution_graph;
pub mod execution_graph_dot;
pub mod executor_manager;
pub mod session_manager;
pub mod session_registry;
pub mod task_manager;

pub fn decode_protobuf<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    T::decode(bytes).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not deserialize {}: {}",
            type_name::<T>(),
            e
        ))
    })
}

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

#[derive(Clone)]
pub struct SchedulerState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    pub executor_manager: ExecutorManager,
    pub task_manager: TaskManager<T, U>,
    pub session_manager: SessionManager,
    pub codec: BallistaCodec<T, U>,
    pub config: SchedulerConfig,
    pub circuit_breaker: Arc<CircuitBreakerController>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    #[cfg(test)]
    pub fn new_with_default_scheduler_name(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        SchedulerState::new(
            cluster,
            codec,
            "localhost:50050".to_owned(),
            SchedulerConfig::default(),
        )
    }

    pub fn new(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        scheduler_name: String,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(
                cluster.cluster_state(),
                config.task_distribution,
            ),
            task_manager: TaskManager::new(
                cluster.job_state(),
                codec.clone(),
                scheduler_name,
            ),
            session_manager: SessionManager::new(cluster.job_state()),
            codec,
            config,
            circuit_breaker: Arc::new(CircuitBreakerController::default()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_with_task_launcher(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        scheduler_name: String,
        config: SchedulerConfig,
        dispatcher: Arc<dyn TaskLauncher<T, U>>,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(
                cluster.cluster_state(),
                config.task_distribution,
            ),
            task_manager: TaskManager::with_launcher(
                cluster.job_state(),
                scheduler_name,
                dispatcher,
            ),
            session_manager: SessionManager::new(cluster.job_state()),
            codec,
            config,
            circuit_breaker: Arc::new(CircuitBreakerController::default()),
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.executor_manager.init().await
    }

    pub(crate) async fn update_task_statuses(
        &self,
        executor_id: &str,
        tasks_status: Vec<TaskStatus>,
        tx_event: EventSender<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let executor = self
            .executor_manager
            .get_executor_metadata(executor_id)
            .await?;

        if self.config.is_push_staged_scheduling() {
            // each task can consume multiple slots, so ensure here that we count each task partition
            let total_num_tasks = tasks_status
                .iter()
                .map(|status| status.partitions.len())
                .sum::<usize>();
            let reservations = (0..total_num_tasks)
                .map(|_| ExecutorReservation::new_free(executor_id.to_owned()))
                .collect();

            tx_event
                .post_event(QueryStageSchedulerEvent::ReservationOffering(reservations));
        }

        self.task_manager
            .update_task_statuses(&executor, tasks_status, tx_event)
            .await?;

        Ok(())
    }

    pub(crate) fn reserve(
        &self,
        n: u32,
        executors: HashSet<String>,
        tx_event: EventSender<QueryStageSchedulerEvent>,
    ) {
        let executor_manager = self.executor_manager.clone();
        tokio::spawn(async move {
            match executor_manager
                .reserve_slots_on_executors(n, executors)
                .await
            {
                Ok(res) if !res.is_empty() => {
                    tx_event
                        .post_event(QueryStageSchedulerEvent::ReservationOffering(res));
                }
                Ok(_) => debug!("no tasks slots reserved, scheduling another Tick"),
                Err(e) => error!(error = %e, "error reserving task slots"),
            }
        });
    }

    fn launch_tasks_async(
        &self,
        executor_id: String,
        tasks: Vec<TaskDescription>,
        tx_event: EventSender<QueryStageSchedulerEvent>,
    ) {
        let task_manager = self.task_manager.clone();
        let executor_manager = self.executor_manager.clone();

        tokio::spawn(async move {
            let num_tasks: usize =
                tasks.iter().map(|t| t.partitions.partitions.len()).sum();

            let result = async {
                let metadata =
                    executor_manager.get_executor_metadata(&executor_id).await?;

                task_manager
                    .launch_tasks(&metadata, &tasks, &executor_manager)
                    .await
            };

            if let Err(e) = result.await {
                error!(error = %e, "failed to launch new task");

                // offer the executor reservations to be filled with new tasks
                let reservations =
                    vec![ExecutorReservation::new_free(executor_id.clone()); num_tasks];

                tx_event.post_event(QueryStageSchedulerEvent::ReservationOffering(
                    reservations,
                ));

                // send a failed status for all tasks that failed to launch so they can
                // be re-scheduled
                let status = tasks
                    .into_iter()
                    .map(|task| {
                        warn!(
                            job_id = task.partitions.job_id,
                            stage_id = task.partitions.stage_id,
                            partitions = ?task.partitions.partitions,
                            error = %e,
                            task_id = task.task_id,
                            executor_id,
                            "failed to launch task"
                        );

                        TaskStatus {
                            task_id: task.task_id as u32,
                            job_id: task.partitions.job_id,
                            stage_id: task.partitions.stage_id as u32,
                            stage_attempt_num: task.stage_attempt_num as u32,
                            partitions: task
                                .partitions
                                .partitions
                                .into_iter()
                                .map(|p| p as u32)
                                .collect(),
                            launch_time: timestamp_millis(),
                            start_exec_time: timestamp_millis(),
                            end_exec_time: timestamp_millis(),
                            metrics: vec![],
                            status: Some(task_status::Status::Failed(FailedTask {
                                retryable: true,
                                count_to_failures: false,
                                failed_reason: Some(FailedReason::IoError(IoError {
                                    message: "failed to launch task on executor"
                                        .to_string(),
                                })),
                            })),
                        }
                    })
                    .collect();

                tx_event.post_event(QueryStageSchedulerEvent::TaskUpdating(
                    executor_id,
                    status,
                ));
            }
        });
    }

    /// Process reservations which are offered. The basic process is
    /// 1. Attempt to fill the offered reservations with available tasks
    /// 2. For any reservation that filled, launch the assigned task on the executor.
    /// 3. For any reservations that could not be filled, cancel the reservation (i.e. return the
    ///    task slot back to the pool of available task slots).
    ///
    /// NOTE Error handling in this method is very important. No matter what we need to ensure
    /// that unfilled reservations are cancelled or else they could become permanently "invisible"
    /// to the scheduler.
    pub(crate) async fn offer_reservation(
        &self,
        reservations: Vec<ExecutorReservation>,
        tx_event: EventSender<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let (free_list, _) =
            match self.task_manager.fill_reservations(&reservations).await {
                Ok((assignments, unassigned_reservations, pending_tasks)) => {
                    // Put tasks to the same executor together
                    let mut executor_stage_assignments: HashMap<
                        String,
                        Vec<TaskDescription>,
                    > = HashMap::new();
                    for (executor_id, task) in assignments.into_iter() {
                        let tasks = executor_stage_assignments
                            .entry(executor_id)
                            .or_insert_with(Vec::new);
                        tasks.push(task);
                    }

                    // let mut join_handles = vec![];
                    for (executor_id, tasks) in executor_stage_assignments.into_iter() {
                        self.launch_tasks_async(executor_id, tasks, tx_event.clone());
                    }

                    (unassigned_reservations, pending_tasks)
                }
                Err(e) => {
                    error!(error = %e, "error offering reservations");
                    (reservations, 0)
                }
            };

        if !free_list.is_empty() {
            // If any reserved slots remain, return them to the pool
            self.executor_manager.cancel_reservations(free_list).await?;
        }

        Ok(())
    }

    pub(crate) async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        session_ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
        queued_at: u64,
    ) -> Result<()> {
        let start = Instant::now();

        if log::max_level() >= log::Level::Debug {
            // optimizing the plan here is redundant because the physical planner will do this again
            // but it is helpful to see what the optimized plan will be
            let optimized_plan = session_ctx.state().optimize(plan)?;
            debug!("Optimized plan: {}", optimized_plan.display_indent());
        }

        self.circuit_breaker.create(job_id);

        plan.apply(&mut |plan| {
            if let LogicalPlan::TableScan(scan) = plan {
                let provider = source_as_provider(&scan.source)?;
                if let Some(table) = provider.as_any().downcast_ref::<ListingTable>() {
                    let local_paths: Vec<&ListingTableUrl> = table
                        .table_paths()
                        .iter()
                        .filter(|url| url.as_str().starts_with("file:///"))
                        .collect();
                    if !local_paths.is_empty() {
                        // These are local files rather than remote object stores, so we
                        // need to check that they are accessible on the scheduler (the client
                        // may not be on the same host, or the data path may not be correctly
                        // mounted in the container). There could be thousands of files so we
                        // just check the first one.
                        let url = &local_paths[0].as_str();
                        // the unwraps are safe here because we checked that the url starts with file:///
                        // we need to check both versions here to support Linux & Windows
                        ListingTableUrl::parse(url.strip_prefix("file://").unwrap())
                            .or_else(|_| {
                                ListingTableUrl::parse(
                                    url.strip_prefix("file:///").unwrap(),
                                )
                            })
                            .map_err(|e| {
                                DataFusionError::External(
                                    format!(
                                        "logical plan refers to path on local file system \
                                that is not accessible in the scheduler: {url}: {e:?}"
                                    )
                                        .into(),
                                )
                            })?;
                    }
                }
            }
            Ok(VisitRecursion::Continue)
        })?;

        let plan = session_ctx.state().create_physical_plan(plan).await?;
        debug!(
            "Physical plan: {}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent()
        );

        self.task_manager
            .submit_job(job_id, job_name, &session_ctx.session_id(), plan, queued_at)
            .await?;

        let elapsed = start.elapsed();

        info!(job_id, ?elapsed, "planned job");

        Ok(())
    }

    /// Spawn a delayed future to clean up job data on both Scheduler and Executors
    pub(crate) fn clean_up_successful_job(&self, job_id: String) {
        self.circuit_breaker.delete(&job_id);
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
    pub(crate) fn clean_up_failed_job(&self, job_id: String) {
        self.circuit_breaker.delete(&job_id);
        self.executor_manager.clean_up_job_data(job_id.clone());
        self.task_manager.clean_up_job_delayed(
            job_id,
            self.config.finished_job_state_clean_up_interval_seconds,
        );
    }
}

#[cfg(test)]
mod test {

    use crate::state::SchedulerState;
    use ballista_core::config::{BallistaConfig, BALLISTA_DEFAULT_SHUFFLE_PARTITIONS};
    use ballista_core::error::Result;
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorMetadata, ExecutorSpecification,
    };
    use ballista_core::serde::BallistaCodec;
    use datafusion::config::Extensions;

    use crate::config::SchedulerConfig;

    use crate::scheduler_server::timestamp_millis;
    use crate::test_utils::{test_cluster_context, BlackholeTaskLauncher};
    use ballista_core::event_loop::EventSender;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, sum};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion::test_util::scan_empty;
    use datafusion_proto::protobuf::LogicalPlanNode;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use std::sync::Arc;

    const TEST_SCHEDULER_NAME: &str = "localhost:50050";

    // We should free any reservations which are not assigned
    #[tokio::test]
    async fn test_offer_free_reservations() -> Result<()> {
        let state: Arc<SchedulerState<LogicalPlanNode, PhysicalPlanNode>> =
            Arc::new(SchedulerState::new_with_default_scheduler_name(
                test_cluster_context(),
                BallistaCodec::default(),
            ));

        let (tx, _rx) = flume::unbounded();
        let tx_event = EventSender::new(tx);

        let executors = test_executors(1, 4);

        let (executor_metadata, executor_data) = executors[0].clone();

        let reservations = state
            .executor_manager
            .register_executor(executor_metadata, executor_data, true, false)
            .await?;

        state.offer_reservation(reservations, tx_event).await?;

        // assert_eq!(assigned, 0);
        // assert!(result.is_empty());

        // All reservations should have been cancelled so we should be able to reserve them now
        let reservations = state.executor_manager.reserve_slots(4).await?;

        assert_eq!(reservations.len(), 4);

        Ok(())
    }

    // We should fill unbound reservations to any available task
    #[tokio::test]
    async fn test_offer_fill_reservations() -> Result<()> {
        let config = BallistaConfig::builder()
            .set(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS, "4")
            .build()?;

        let state: Arc<SchedulerState<LogicalPlanNode, PhysicalPlanNode>> =
            Arc::new(SchedulerState::new_with_task_launcher(
                test_cluster_context(),
                BallistaCodec::default(),
                TEST_SCHEDULER_NAME.into(),
                SchedulerConfig::default(),
                Arc::new(BlackholeTaskLauncher::default()),
            ));

        let session_ctx = state
            .session_manager
            .create_session(&config, Extensions::default())
            .await?;

        let plan = test_graph(session_ctx.clone()).await;

        // Create 4 jobs so we have four pending tasks
        state
            .task_manager
            .queue_job("job-1", "", timestamp_millis())
            .await?;
        state
            .task_manager
            .submit_job(
                "job-1",
                "",
                session_ctx.session_id().as_str(),
                plan.clone(),
                0,
            )
            .await?;
        state
            .task_manager
            .queue_job("job-2", "", timestamp_millis())
            .await?;
        state
            .task_manager
            .submit_job(
                "job-2",
                "",
                session_ctx.session_id().as_str(),
                plan.clone(),
                0,
            )
            .await?;
        state
            .task_manager
            .queue_job("job-3", "", timestamp_millis())
            .await?;
        state
            .task_manager
            .submit_job(
                "job-3",
                "",
                session_ctx.session_id().as_str(),
                plan.clone(),
                0,
            )
            .await?;
        state
            .task_manager
            .queue_job("job-4", "", timestamp_millis())
            .await?;
        state
            .task_manager
            .submit_job(
                "job-4",
                "",
                session_ctx.session_id().as_str(),
                plan.clone(),
                0,
            )
            .await?;

        let executors = test_executors(1, 4);

        let (executor_metadata, executor_data) = executors[0].clone();

        let reservations = state
            .executor_manager
            .register_executor(executor_metadata, executor_data, true, false)
            .await?;

        let (tx, _rx) = flume::unbounded();
        let tx_event = EventSender::new(tx);

        state.offer_reservation(reservations, tx_event).await?;

        // All task slots should be assigned so we should not be able to reserve more tasks
        let reservations = state.executor_manager.reserve_slots(4).await?;

        assert_eq!(reservations.len(), 0);

        Ok(())
    }

    fn test_executors(
        total_executors: usize,
        slots_per_executor: u32,
    ) -> Vec<(ExecutorMetadata, ExecutorData)> {
        let mut result: Vec<(ExecutorMetadata, ExecutorData)> = vec![];

        for i in 0..total_executors {
            result.push((
                ExecutorMetadata {
                    id: format!("executor-{i}"),
                    host: format!("host-{i}"),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification {
                        task_slots: slots_per_executor,
                    },
                },
                ExecutorData {
                    executor_id: format!("executor-{i}"),
                    total_task_slots: slots_per_executor,
                    available_task_slots: slots_per_executor,
                },
            ));
        }

        result
    }

    async fn test_graph(ctx: Arc<SessionContext>) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        let plan = scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap();

        ctx.state().create_physical_plan(&plan).await.unwrap()
    }
}
