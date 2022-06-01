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
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tonic::transport::Channel;

use ballista_core::config::{BallistaConfig, TaskSchedulingPolicy};
use ballista_core::error::Result;
use ballista_core::event_loop::{EventAction, EventLoop};
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;
use ballista_core::serde::protobuf::TaskStatus;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use datafusion::execution::context::{default_session_builder, SessionState};
use datafusion::prelude::{SessionConfig, SessionContext};
use log::error;

use crate::scheduler_server::event::{QueryStageSchedulerEvent, SchedulerServerEvent};
use crate::scheduler_server::event_loop::SchedulerServerEventAction;
use crate::scheduler_server::query_stage_scheduler::QueryStageScheduler;
use crate::state::backend::StateBackendClient;
use crate::state::SchedulerState;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod externalscaler {
    include!(concat!(env!("OUT_DIR"), "/externalscaler.rs"));
}

pub mod event;
mod event_loop;
mod external_scaler;
mod grpc;
mod query_stage_scheduler;

type ExecutorsClient = Arc<RwLock<HashMap<String, ExecutorGrpcClient<Channel>>>>;
pub(crate) type SessionBuilder = fn(SessionConfig) -> SessionState;

#[derive(Clone)]
pub struct SchedulerServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    pub(crate) state: Arc<SchedulerState<T, U>>,
    pub start_time: u128,
    policy: TaskSchedulingPolicy,
    event_loop: Option<EventLoop<SchedulerServerEvent>>,
    pub(crate) query_stage_event_loop: EventLoop<QueryStageSchedulerEvent>,
    codec: BallistaCodec<T, U>,
    /// SessionState Builder
    session_builder: SessionBuilder,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerServer<T, U> {
    pub fn new(
        config: Arc<dyn StateBackendClient>,
        namespace: String,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        SchedulerServer::new_with_policy(
            config,
            namespace,
            TaskSchedulingPolicy::PullStaged,
            codec,
            default_session_builder,
        )
    }

    pub fn new_with_builder(
        config: Arc<dyn StateBackendClient>,
        namespace: String,
        codec: BallistaCodec<T, U>,
        session_builder: SessionBuilder,
    ) -> Self {
        SchedulerServer::new_with_policy(
            config,
            namespace,
            TaskSchedulingPolicy::PullStaged,
            codec,
            session_builder,
        )
    }

    pub fn new_with_policy(
        config: Arc<dyn StateBackendClient>,
        namespace: String,
        policy: TaskSchedulingPolicy,
        codec: BallistaCodec<T, U>,
        session_builder: SessionBuilder,
    ) -> Self {
        let state = Arc::new(SchedulerState::new(
            config,
            namespace,
            session_builder,
            codec.clone(),
        ));

        let event_loop = if matches!(policy, TaskSchedulingPolicy::PushStaged) {
            let event_action: Arc<SchedulerServerEventAction<T, U>> =
                Arc::new(SchedulerServerEventAction::new(state.clone()));
            let event_loop = EventLoop::new("scheduler".to_owned(), 10000, event_action);
            Some(event_loop)
        } else {
            None
        };
        let query_stage_scheduler =
            Arc::new(QueryStageScheduler::new(state.clone(), None));
        let query_stage_event_loop =
            EventLoop::new("query_stage".to_owned(), 10000, query_stage_scheduler);
        Self {
            state,
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            policy,
            event_loop,
            query_stage_event_loop,
            codec,
            session_builder,
        }
    }

    pub fn new_with_event_action(
        config: Arc<dyn StateBackendClient>,
        namespace: String,
        codec: BallistaCodec<T, U>,
        session_builder: SessionBuilder,
        event_action: Arc<dyn EventAction<SchedulerServerEvent>>,
    ) -> Self {
        let state = Arc::new(SchedulerState::new(
            config,
            namespace,
            session_builder,
            codec.clone(),
        ));

        let event_loop = EventLoop::new("scheduler".to_owned(), 10000, event_action);
        let query_stage_scheduler =
            Arc::new(QueryStageScheduler::new(state.clone(), None));
        let query_stage_event_loop =
            EventLoop::new("query_stage".to_owned(), 10000, query_stage_scheduler);
        Self {
            state,
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            policy: TaskSchedulingPolicy::PushStaged,
            event_loop: Some(event_loop),
            query_stage_event_loop,
            codec,
            session_builder,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        {
            // initialize state
            self.state.init().await?;
        }

        {
            if let Some(event_loop) = self.event_loop.as_mut() {
                event_loop.start()?;

                let query_stage_scheduler = Arc::new(QueryStageScheduler::new(
                    self.state.clone(),
                    Some(event_loop.get_sender()?),
                ));
                let query_stage_event_loop = EventLoop::new(
                    self.query_stage_event_loop.name.clone(),
                    self.query_stage_event_loop.buffer_size,
                    query_stage_scheduler,
                );
                self.query_stage_event_loop = query_stage_event_loop;
            }

            self.query_stage_event_loop.start()?;
        }

        Ok(())
    }

    pub(crate) async fn update_task_status(
        &self,
        executor_id: &str,
        tasks_status: Vec<TaskStatus>,
    ) -> Result<()> {
        let num_status = tasks_status.len();
        let executor = self
            .state
            .executor_manager
            .get_executor_metadata(executor_id)
            .await?;
        match self
            .state
            .task_manager
            .update_task_statuses(&executor, tasks_status)
            .await
        {
            Ok((stage_events, offers)) => {
                if let Some(event_loop) = self.event_loop.as_ref() {
                    event_loop
                        .get_sender()?
                        .post_event(SchedulerServerEvent::Offer(offers))
                        .await?;
                }

                for stage_event in stage_events {
                    self.post_stage_event(stage_event).await?;
                }
            }
            Err(e) => {
                error!(
                    "Failed to update {} task statuses for executor {}: {:?}",
                    num_status, executor_id, e
                );
                // TODO what do we do here?
            }
        }

        Ok(())
    }

    async fn post_stage_event(&self, event: QueryStageSchedulerEvent) -> Result<()> {
        self.query_stage_event_loop
            .get_sender()?
            .post_event(event)
            .await
    }
}

/// Create a DataFusion session context that is compatible with Ballista Configuration
pub fn create_datafusion_context(
    config: &BallistaConfig,
    session_builder: SessionBuilder,
) -> Arc<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(config.default_shuffle_partitions())
        .with_batch_size(config.default_batch_size())
        .with_repartition_joins(config.repartition_joins())
        .with_repartition_aggregations(config.repartition_aggregations())
        .with_repartition_windows(config.repartition_windows())
        .with_parquet_pruning(config.parquet_pruning());
    let session_state = session_builder(config);
    Arc::new(SessionContext::with_state(session_state))
}

/// Update the existing DataFusion session context with Ballista Configuration
pub fn update_datafusion_context(
    session_ctx: Arc<SessionContext>,
    config: &BallistaConfig,
) -> Arc<SessionContext> {
    {
        let mut mut_state = session_ctx.state.write();
        mut_state.config.target_partitions = config.default_shuffle_partitions();
        mut_state.config.batch_size = config.default_batch_size();
        mut_state.config.repartition_joins = config.repartition_joins();
        mut_state.config.repartition_aggregations = config.repartition_aggregations();
        mut_state.config.repartition_windows = config.repartition_windows();
        mut_state.config.parquet_pruning = config.parquet_pruning();
    }
    session_ctx
}

/// A Registry holds all the datafusion session contexts
pub struct SessionContextRegistry {
    /// A map from session_id to SessionContext
    pub running_sessions: RwLock<HashMap<String, Arc<SessionContext>>>,
}

impl Default for SessionContextRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionContextRegistry {
    /// Create the registry that session contexts can registered into.
    /// ['LocalFileSystem'] store is registered in by default to support read local files natively.
    pub fn new() -> Self {
        Self {
            running_sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a new session to this registry.
    pub async fn register_session(
        &self,
        session_ctx: Arc<SessionContext>,
    ) -> Option<Arc<SessionContext>> {
        let session_id = session_ctx.session_id();
        let mut sessions = self.running_sessions.write().await;
        sessions.insert(session_id, session_ctx)
    }

    /// Lookup the session context registered
    pub async fn lookup_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        let sessions = self.running_sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// Remove a session from this registry.
    pub async fn unregister_session(
        &self,
        session_id: &str,
    ) -> Option<Arc<SessionContext>> {
        let mut sessions = self.running_sessions.write().await;
        sessions.remove(session_id)
    }
}
#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;

    use ballista_core::config::{
        BallistaConfig, TaskSchedulingPolicy, BALLISTA_DEFAULT_SHUFFLE_PARTITIONS,
    };
    use ballista_core::error::{BallistaError, Result};
    use ballista_core::event_loop::EventAction;

    use ballista_core::serde::protobuf::{
        task_status, CompletedTask, LogicalPlanNode, PartitionId, PhysicalPlanNode,
        ShuffleWritePartition, TaskStatus,
    };
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorMetadata, ExecutorSpecification,
    };
    use ballista_core::serde::BallistaCodec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::default_session_builder;
    use datafusion::logical_plan::{col, sum, LogicalPlan};

    use datafusion::test_util::scan_empty;

    use crate::scheduler_server::event::{
        QueryStageSchedulerEvent, SchedulerServerEvent,
    };
    use crate::scheduler_server::SchedulerServer;
    use crate::state::backend::standalone::StandaloneClient;

    use crate::state::executor_manager::ExecutorReservation;
    use crate::test_utils::SchedulerEventObserver;

    #[tokio::test]
    async fn test_pull_scheduling() -> Result<()> {
        let plan = test_plan();
        let task_slots = 4;

        let scheduler = test_scheduler(TaskSchedulingPolicy::PullStaged).await?;

        let executors = test_executors(task_slots);
        for (executor_metadata, executor_data) in executors {
            scheduler
                .state
                .executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        let config = test_session(task_slots);

        let ctx = scheduler
            .state
            .session_manager
            .create_session(&config)
            .await?;

        let plan = async {
            let optimized_plan = ctx.optimize(&plan).map_err(|e| {
                BallistaError::General(format!(
                    "Could not create optimized logical plan: {}",
                    e
                ))
            })?;

            ctx.create_physical_plan(&optimized_plan)
                .await
                .map_err(|e| {
                    BallistaError::General(format!(
                        "Could not create physical plan: {}",
                        e
                    ))
                })
        }
        .await?;

        let job_id = "job";
        let session_id = ctx.session_id();

        // Submit job
        scheduler
            .state
            .task_manager
            .submit_job(job_id, &session_id, plan)
            .await
            .expect("submitting plan");

        loop {
            // Refresh the ExecutionGraph
            let mut graph = scheduler
                .state
                .task_manager
                .get_execution_graph(job_id)
                .await?;

            if let Some(task) = graph.pop_next_task("executor-1")? {
                let mut partitions: Vec<ShuffleWritePartition> = vec![];

                let num_partitions = task
                    .output_partitioning
                    .map(|p| p.partition_count())
                    .unwrap_or(1);

                for partition_id in 0..num_partitions {
                    partitions.push(ShuffleWritePartition {
                        partition_id: partition_id as u64,
                        path: "some/path".to_string(),
                        num_batches: 1,
                        num_rows: 1,
                        num_bytes: 1,
                    })
                }

                // Complete the task
                let task_status = TaskStatus {
                    status: Some(task_status::Status::Completed(CompletedTask {
                        executor_id: "executor-1".to_owned(),
                        partitions,
                    })),
                    task_id: Some(PartitionId {
                        job_id: job_id.to_owned(),
                        stage_id: task.partition.stage_id as u32,
                        partition_id: task.partition.partition_id as u32,
                    }),
                };

                scheduler
                    .update_task_status("executor-1", vec![task_status])
                    .await?;
            } else {
                break;
            }
        }

        let final_graph = scheduler
            .state
            .task_manager
            .get_execution_graph(job_id)
            .await?;

        assert!(final_graph.complete());
        assert_eq!(final_graph.output_locations().len(), 4);

        for output_location in final_graph.output_locations() {
            assert_eq!(output_location.path, "some/path".to_owned());
            assert_eq!(output_location.executor_meta.host, "localhost1".to_owned())
        }

        Ok(())
    }

    /// This test will exercise the push-based scheduling. We setup our scheduler server
    /// with `SchedulerEventObserver` to listen to `SchedulerServerEvents` and then just immediately
    /// complete the tasks.
    #[tokio::test]
    async fn test_push_scheduling() -> Result<()> {
        let plan = test_plan();
        let task_slots = 4;

        let (sender, mut event_receiver) =
            tokio::sync::mpsc::channel::<SchedulerServerEvent>(1000);
        let (error_sender, _) = tokio::sync::mpsc::channel::<BallistaError>(1000);

        let event_action = SchedulerEventObserver::new(sender, error_sender);

        let scheduler = test_scheduler_with_event_action(Arc::new(event_action)).await?;

        let executors = test_executors(task_slots);
        for (executor_metadata, executor_data) in executors {
            scheduler
                .state
                .executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        let config = test_session(task_slots);

        let ctx = scheduler
            .state
            .session_manager
            .create_session(&config)
            .await?;

        let plan = async {
            let optimized_plan = ctx.optimize(&plan).map_err(|e| {
                BallistaError::General(format!(
                    "Could not create optimized logical plan: {}",
                    e
                ))
            })?;

            ctx.create_physical_plan(&optimized_plan)
                .await
                .map_err(|e| {
                    BallistaError::General(format!(
                        "Could not create physical plan: {}",
                        e
                    ))
                })
        }
        .await?;

        let job_id = "job";
        let session_id = ctx.session_id();

        // Submit job
        scheduler
            .state
            .task_manager
            .submit_job(job_id, &session_id, plan.clone())
            .await
            .expect("submitting plan");

        // Send JobSubmitted event to kick off the event loop
        scheduler
            .query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::JobSubmitted(
                job_id.to_owned(),
                plan,
            ))
            .await?;

        // Complete tasks that are offered through scheduler events
        while let Some(SchedulerServerEvent::Offer(reservations)) =
            event_receiver.recv().await
        {
            let free_list = match scheduler
                .state
                .task_manager
                .fill_reservations(&reservations)
                .await
            {
                Ok((assignments, mut unassigned_reservations)) => {
                    // Break when we are no longer assigning tasks
                    if unassigned_reservations.len() == reservations.len() {
                        break;
                    }

                    for (executor_id, task) in assignments.into_iter() {
                        match scheduler
                            .state
                            .executor_manager
                            .get_executor_metadata(&executor_id)
                            .await
                        {
                            Ok(executor) => {
                                let mut partitions: Vec<ShuffleWritePartition> = vec![];

                                let num_partitions = task
                                    .output_partitioning
                                    .map(|p| p.partition_count())
                                    .unwrap_or(1);

                                for partition_id in 0..num_partitions {
                                    partitions.push(ShuffleWritePartition {
                                        partition_id: partition_id as u64,
                                        path: "some/path".to_string(),
                                        num_batches: 1,
                                        num_rows: 1,
                                        num_bytes: 1,
                                    })
                                }

                                // Complete the task
                                let task_status = TaskStatus {
                                    status: Some(task_status::Status::Completed(
                                        CompletedTask {
                                            executor_id: executor.id.clone(),
                                            partitions,
                                        },
                                    )),
                                    task_id: Some(PartitionId {
                                        job_id: job_id.to_owned(),
                                        stage_id: task.partition.stage_id as u32,
                                        partition_id: task.partition.partition_id as u32,
                                    }),
                                };

                                scheduler
                                    .update_task_status(&executor.id, vec![task_status])
                                    .await?;
                            }
                            Err(_e) => {
                                unassigned_reservations.push(
                                    ExecutorReservation::new_free(executor_id.clone()),
                                );
                            }
                        }
                    }
                    unassigned_reservations
                }
                Err(_e) => reservations,
            };

            // If any reserved slots remain, return them to the pool
            if free_list.len() > 0 {
                scheduler
                    .state
                    .executor_manager
                    .cancel_reservations(free_list)
                    .await?;
            }
        }

        let final_graph = scheduler
            .state
            .task_manager
            .get_execution_graph(job_id)
            .await?;

        assert!(final_graph.complete());
        assert_eq!(final_graph.output_locations().len(), 4);

        Ok(())
    }

    async fn test_scheduler(
        policy: TaskSchedulingPolicy,
    ) -> Result<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);
        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new_with_policy(
                state_storage.clone(),
                "default".to_owned(),
                policy,
                BallistaCodec::default(),
                default_session_builder,
            );
        scheduler.init().await?;

        Ok(scheduler)
    }

    async fn test_scheduler_with_event_action(
        event_action: Arc<dyn EventAction<SchedulerServerEvent>>,
    ) -> Result<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);

        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new_with_event_action(
                state_storage.clone(),
                "default".to_owned(),
                BallistaCodec::default(),
                default_session_builder,
                event_action,
            );
        scheduler.init().await?;

        Ok(scheduler)
    }

    fn test_executors(num_partitions: usize) -> Vec<(ExecutorMetadata, ExecutorData)> {
        let task_slots = (num_partitions as u32 + 1) / 2;

        vec![
            (
                ExecutorMetadata {
                    id: "executor-1".to_string(),
                    host: "localhost1".to_string(),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification { task_slots },
                },
                ExecutorData {
                    executor_id: "executor-1".to_owned(),
                    total_task_slots: task_slots,
                    available_task_slots: task_slots,
                },
            ),
            (
                ExecutorMetadata {
                    id: "executor-2".to_string(),
                    host: "localhost2".to_string(),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification {
                        task_slots: num_partitions as u32 - task_slots,
                    },
                },
                ExecutorData {
                    executor_id: "executor-2".to_owned(),
                    total_task_slots: num_partitions as u32 - task_slots,
                    available_task_slots: num_partitions as u32 - task_slots,
                },
            ),
        ]
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_session(partitions: usize) -> BallistaConfig {
        BallistaConfig::builder()
            .set(
                BALLISTA_DEFAULT_SHUFFLE_PARTITIONS,
                format!("{}", partitions).as_str(),
            )
            .build()
            .expect("creating BallistaConfig")
    }
}
