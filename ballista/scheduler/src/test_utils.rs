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

use ballista_core::error::{BallistaError, Result};
use ballista_core::extension::SessionConfigExt;
use datafusion::catalog::Session;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::config::SchedulerConfig;
use crate::metrics::SchedulerMetricsCollector;
use crate::planner::DefaultDistributedPlanner;
use crate::scheduler_server::{SchedulerServer, timestamp_millis};

use crate::state::executor_manager::ExecutorManager;
use crate::state::task_manager::TaskLauncher;

use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    FailedTask, JobStatus, MultiTaskDefinition, ShuffleWritePartition, SuccessfulTask,
    TaskId, TaskStatus, task_status,
};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorMetadata, ExecutorSpecification,
};
use ballista_core::serde::{BallistaCodec, protobuf};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::functions_aggregate::{count::count, sum::sum};
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{CsvReadOptions, JoinType, col};
use datafusion::test_util::scan_empty_with_partitions;

use crate::cluster::BallistaCluster;
use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::execution_graph::{ExecutionGraph, ExecutionStage, TaskDescription};
use ballista_core::utils::{default_config_producer, default_session_builder};
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use parking_lot::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};

/// List of TPC-H benchmark table names.
pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

const TEST_SCHEDULER_NAME: &str = "localhost:50050";

/// Sometimes we need to construct logical plans that will produce errors
/// when we try and create physical plan. A scan using `ExplodingTableProvider`
/// will do the trick
#[derive(Debug)]
pub struct ExplodingTableProvider;

#[async_trait]
impl TableProvider for ExplodingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "ExplodingTableProvider just throws an error!".to_owned(),
        ))
    }
}

/// Utility for running some async check multiple times to verify a condition. It will run the check
/// at the specified interval up to a maximum of the specified iterations.
pub async fn await_condition<Fut: Future<Output = Result<bool>>, F: Fn() -> Fut>(
    interval: Duration,
    iterations: usize,
    cond: F,
) -> Result<bool> {
    let mut iteration = 0;

    while iteration < iterations {
        let check = cond().await?;

        if check {
            return Ok(true);
        } else {
            iteration += 1;
            tokio::time::sleep(interval).await;
        }
    }

    Ok(false)
}

/// Creates a test cluster context with in-memory state.
pub fn test_cluster_context() -> BallistaCluster {
    BallistaCluster::new_memory(
        TEST_SCHEDULER_NAME,
        Arc::new(default_session_builder),
        Arc::new(default_config_producer),
    )
}

/// Creates a DataFusion session context with TPC-H tables registered from the given path.
pub async fn datafusion_test_context(path: &str) -> Result<SessionContext> {
    let default_shuffle_partitions = 2;
    let config = SessionConfig::new().with_target_partitions(default_shuffle_partitions);
    let ctx = SessionContext::new_with_config(config);
    for &table in TPCH_TABLES {
        let schema = get_tpch_schema(table);
        let options = CsvReadOptions::new()
            .schema(&schema)
            .delimiter(b'|')
            .has_header(false)
            .file_extension(".tbl");
        let dir = format!("{path}/{table}");
        ctx.register_csv(table, dir, options).await?;
    }
    Ok(ctx)
}

/// Returns the schema for a TPC-H table by name.
pub fn get_tpch_schema(table: &str) -> Schema {
    // note that the schema intentionally uses signed integers so that any generated Parquet
    // files can also be used to benchmark tools that only support signed integers, such as
    // Apache Spark

    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

/// Trait for running tasks in tests.
pub trait TaskRunner: Send + Sync + 'static {
    /// Executes tasks and returns their status.
    fn run(&self, executor_id: String, tasks: MultiTaskDefinition) -> Vec<TaskStatus>;
}

/// A task runner that wraps a function.
#[derive(Clone)]
pub struct TaskRunnerFn<F> {
    f: F,
}

impl<F> TaskRunnerFn<F>
where
    F: Fn(String, MultiTaskDefinition) -> Vec<TaskStatus> + Send + Sync + 'static,
{
    /// Creates a new task runner from a function.
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> TaskRunner for TaskRunnerFn<F>
where
    F: Fn(String, MultiTaskDefinition) -> Vec<TaskStatus> + Send + Sync + 'static,
{
    fn run(&self, executor_id: String, tasks: MultiTaskDefinition) -> Vec<TaskStatus> {
        (self.f)(executor_id, tasks)
    }
}

/// Returns a default task runner that marks all tasks as successful.
pub fn default_task_runner() -> impl TaskRunner {
    TaskRunnerFn::new(|executor_id: String, task: MultiTaskDefinition| {
        let mut statuses = vec![];

        let partitions = 1;
        let partitions: Vec<ShuffleWritePartition> = (0..partitions)
            .map(|i| ShuffleWritePartition {
                partition_id: i as u64,
                path: String::default(),
                num_batches: 1,
                num_rows: 1,
                num_bytes: 1,
            })
            .collect();

        for TaskId {
            task_id,
            partition_id,
            ..
        } in task.task_ids
        {
            let timestamp = timestamp_millis();
            statuses.push(TaskStatus {
                task_id,
                job_id: task.job_id.clone(),
                stage_id: task.stage_id,
                stage_attempt_num: task.stage_attempt_num,
                partition_id,
                launch_time: timestamp,
                start_exec_time: timestamp,
                end_exec_time: timestamp,
                metrics: vec![],
                status: Some(task_status::Status::Successful(SuccessfulTask {
                    executor_id: executor_id.clone(),
                    partitions: partitions.clone(),
                })),
            });
        }

        statuses
    })
}

#[derive(Clone)]
struct VirtualExecutor {
    executor_id: String,
    task_slots: usize,
    runner: Arc<dyn TaskRunner>,
}

impl VirtualExecutor {
    pub fn run_tasks(&self, tasks: MultiTaskDefinition) -> Vec<TaskStatus> {
        self.runner.run(self.executor_id.clone(), tasks)
    }
}

/// Launcher which consumes tasks and never sends a status update
#[derive(Default)]
pub struct BlackholeTaskLauncher {}

#[async_trait]
impl TaskLauncher for BlackholeTaskLauncher {
    async fn launch_tasks(
        &self,
        _executor: &ExecutorMetadata,
        _tasks: Vec<MultiTaskDefinition>,
        _executor_manager: &ExecutorManager,
    ) -> Result<()> {
        Ok(())
    }
}

/// A task launcher that sends tasks to virtual executors for testing.
pub struct VirtualTaskLauncher {
    sender: Sender<(String, Vec<TaskStatus>)>,
    executors: HashMap<String, VirtualExecutor>,
}

#[async_trait::async_trait]
impl TaskLauncher for VirtualTaskLauncher {
    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<MultiTaskDefinition>,
        _executor_manager: &ExecutorManager,
    ) -> Result<()> {
        let virtual_executor = self.executors.get(&executor.id).ok_or_else(|| {
            BallistaError::Internal(format!(
                "No virtual executor with ID {} found",
                executor.id
            ))
        })?;

        let status = tasks
            .into_iter()
            .flat_map(|t| virtual_executor.run_tasks(t))
            .collect();

        self.sender
            .send((executor.id.clone(), status))
            .await
            .map_err(|e| {
                BallistaError::Internal(format!("Error sending task status: {e:?}"))
            })
    }
}

/// Test harness for scheduler integration tests.
pub struct SchedulerTest {
    scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    session_config: SessionConfig,
    status_receiver: Option<Receiver<(String, Vec<TaskStatus>)>>,
}

impl SchedulerTest {
    /// Creates a new scheduler test harness with the specified configuration.
    pub async fn new(
        config: SchedulerConfig,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        num_executors: usize,
        task_slots_per_executor: usize,
        runner: Option<Arc<dyn TaskRunner>>,
    ) -> Result<Self> {
        let cluster = BallistaCluster::new_from_config(&config).await?;

        let session_config = if num_executors > 0 && task_slots_per_executor > 0 {
            SessionConfig::new_with_ballista()
                .with_target_partitions(num_executors * task_slots_per_executor)
        } else {
            SessionConfig::new_with_ballista()
        };

        let runner = runner.unwrap_or_else(|| Arc::new(default_task_runner()));

        let executors: HashMap<String, VirtualExecutor> = (0..num_executors)
            .map(|i| {
                let id = format!("virtual-executor-{i}");
                let executor = VirtualExecutor {
                    executor_id: id.clone(),
                    task_slots: task_slots_per_executor,
                    runner: runner.clone(),
                };
                (id, executor)
            })
            .collect();

        let (status_sender, status_receiver) = channel(1000);

        let launcher = VirtualTaskLauncher {
            sender: status_sender,
            executors: executors.clone(),
        };

        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new_with_task_launcher(
                "localhost:50050".to_owned(),
                cluster,
                BallistaCodec::default(),
                Arc::new(config),
                metrics_collector,
                Arc::new(launcher),
            );
        scheduler.init().await?;

        for (executor_id, VirtualExecutor { task_slots, .. }) in executors {
            let metadata = ExecutorMetadata {
                id: executor_id.clone(),
                host: String::default(),
                port: 0,
                grpc_port: 0,
                specification: ExecutorSpecification {
                    task_slots: task_slots as u32,
                },
            };

            let executor_data = ExecutorData {
                executor_id,
                total_task_slots: task_slots as u32,
                available_task_slots: task_slots as u32,
            };

            scheduler
                .state
                .executor_manager
                .register_executor(metadata, executor_data)
                .await?;
        }

        Ok(Self {
            scheduler,
            session_config,
            status_receiver: Some(status_receiver),
        })
    }

    /// Returns the number of pending jobs.
    pub fn pending_job_number(&self) -> usize {
        self.scheduler.pending_job_number()
    }

    /// Returns the number of running jobs.
    pub fn running_job_number(&self) -> usize {
        self.scheduler.running_job_number()
    }

    /// Returns the session context for tests.
    pub async fn ctx(&self) -> Result<Arc<SessionContext>> {
        self.scheduler
            .state
            .session_manager
            .create_or_update_session("session_id", &self.session_config)
            .await
    }

    /// Submits a job and returns its ID.
    pub async fn submit(&mut self, job_name: &str, plan: &LogicalPlan) -> Result<String> {
        println!("{:?}", self.session_config);
        let ctx = self
            .scheduler
            .state
            .session_manager
            .create_or_update_session("session_id", &self.session_config)
            .await?;

        let job_id = self.scheduler.submit_job(job_name, ctx, plan).await?;

        Ok(job_id)
    }

    /// Posts a scheduler event to the event loop.
    pub async fn post_scheduler_event(
        &self,
        event: QueryStageSchedulerEvent,
    ) -> Result<()> {
        self.scheduler
            .query_stage_event_loop
            .get_sender()?
            .post_event(event)
            .await
    }

    /// Processes the next task status update.
    pub async fn tick(&mut self) -> Result<()> {
        if let Some(receiver) = self.status_receiver.as_mut() {
            if let Some((executor_id, status)) = receiver.recv().await {
                self.scheduler
                    .update_task_status(&executor_id, status)
                    .await?;
            } else {
                return Err(BallistaError::Internal("Task sender dropped".to_owned()));
            }
        } else {
            return Err(BallistaError::Internal(
                "Status receiver was None".to_owned(),
            ));
        }

        Ok(())
    }

    /// Cancels a job by ID.
    pub async fn cancel(&self, job_id: &str) -> Result<()> {
        self.scheduler
            .query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::JobCancel(job_id.to_owned()))
            .await
    }

    /// Waits for job completion with a timeout in milliseconds.
    pub async fn await_completion_timeout(
        &self,
        job_id: &str,
        timeout_ms: u64,
    ) -> Result<JobStatus> {
        let mut time = 0;
        let final_status: Result<JobStatus> = loop {
            let status = self
                .scheduler
                .state
                .task_manager
                .get_job_status(job_id)
                .await?;

            if let Some(JobStatus {
                status: Some(inner),
                ..
            }) = status.as_ref()
            {
                match inner {
                    Status::Failed(_) | Status::Successful(_) => {
                        break Ok(status.unwrap());
                    }
                    _ => {
                        if time >= timeout_ms {
                            break Ok(status.unwrap());
                        } else {
                            continue;
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
            time += 100;
        };

        final_status
    }

    /// Waits for job completion indefinitely.
    pub async fn await_completion(&self, job_id: &str) -> Result<JobStatus> {
        let final_status: Result<JobStatus> = loop {
            let status = self
                .scheduler
                .state
                .task_manager
                .get_job_status(job_id)
                .await?;

            if let Some(JobStatus {
                status: Some(inner),
                ..
            }) = status.as_ref()
            {
                match inner {
                    Status::Failed(_) | Status::Successful(_) => {
                        break Ok(status.unwrap());
                    }
                    _ => continue,
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await
        };

        final_status
    }

    /// Returns job status and job_id
    pub async fn run(
        &mut self,
        job_name: &str,
        plan: &LogicalPlan,
    ) -> Result<(JobStatus, String)> {
        let ctx = self
            .scheduler
            .state
            .session_manager
            .create_or_update_session("session_id", &self.session_config)
            .await?;

        let job_id = self.scheduler.submit_job(job_name, ctx, plan).await?;

        let mut receiver = self.status_receiver.take().unwrap();

        let scheduler_clone = self.scheduler.clone();
        tokio::spawn(async move {
            while let Some((executor_id, status)) = receiver.recv().await {
                scheduler_clone
                    .update_task_status(&executor_id, status)
                    .await
                    .unwrap();
            }
        });

        let final_status: Result<JobStatus> = loop {
            let status = self
                .scheduler
                .state
                .task_manager
                .get_job_status(&job_id)
                .await?;

            if let Some(JobStatus {
                status: Some(inner),
                ..
            }) = status.as_ref()
            {
                match inner {
                    Status::Failed(_) | Status::Successful(_) => {
                        break Ok(status.unwrap());
                    }
                    _ => continue,
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await
        };

        final_status.map(|s| (s, job_id))
    }
}

/// Events recorded by the test metrics collector.
#[derive(Clone)]
pub enum MetricEvent {
    /// Job submitted event (job_id, queued_at, submitted_at).
    Submitted(String, u64, u64),
    /// Job completed event (job_id, queued_at, completed_at).
    Completed(String, u64, u64),
    /// Job cancelled event (job_id).
    Cancelled(String),
    /// Job failed event (job_id, queued_at, failed_at).
    Failed(String, u64, u64),
}

impl MetricEvent {
    /// Returns the job ID associated with this event.
    pub fn job_id(&self) -> &str {
        match self {
            MetricEvent::Submitted(job, _, _) => job.as_str(),
            MetricEvent::Completed(job, _, _) => job.as_str(),
            MetricEvent::Cancelled(job) => job.as_str(),
            MetricEvent::Failed(job, _, _) => job.as_str(),
        }
    }
}

/// A metrics collector that stores events for test assertions.
#[derive(Default, Clone)]
pub struct TestMetricsCollector {
    /// Collected metric events.
    pub events: Arc<Mutex<Vec<MetricEvent>>>,
}

impl TestMetricsCollector {
    /// Returns all events for the given job ID.
    pub fn job_events(&self, job_id: &str) -> Vec<MetricEvent> {
        let guard = self.events.lock();

        guard
            .iter()
            .filter_map(|event| {
                if event.job_id() == job_id {
                    Some(event.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl SchedulerMetricsCollector for TestMetricsCollector {
    fn record_submitted(&self, job_id: &str, queued_at: u64, submitted_at: u64) {
        let mut guard = self.events.lock();
        guard.push(MetricEvent::Submitted(
            job_id.to_owned(),
            queued_at,
            submitted_at,
        ));
    }

    fn record_completed(&self, job_id: &str, queued_at: u64, completed_at: u64) {
        let mut guard = self.events.lock();
        guard.push(MetricEvent::Completed(
            job_id.to_owned(),
            queued_at,
            completed_at,
        ));
    }

    fn record_failed(&self, job_id: &str, queued_at: u64, failed_at: u64) {
        let mut guard = self.events.lock();
        guard.push(MetricEvent::Failed(job_id.to_owned(), queued_at, failed_at));
    }

    fn record_cancelled(&self, job_id: &str) {
        let mut guard = self.events.lock();
        guard.push(MetricEvent::Cancelled(job_id.to_owned()));
    }

    fn set_pending_tasks_queue_size(&self, _value: u64) {}

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        Ok(None)
    }
}

/// Asserts that a submitted event was recorded for the job.
pub fn assert_submitted_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Submitted(_, _, _)));

    assert!(found, "Expected submitted event for job {job_id}");
}

/// Asserts that no submitted event was recorded for the job.
pub fn assert_no_submitted_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Submitted(_, _, _)));

    assert!(!found, "Expected no submitted event for job {job_id}");
}

/// Asserts that a completed event was recorded for the job.
pub fn assert_completed_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Completed(_, _, _)));

    assert!(found, "{}", "Expected completed event for job {job_id}");
}

/// Asserts that a cancelled event was recorded for the job.
pub fn assert_cancelled_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Cancelled(_)));

    assert!(found, "{}", "Expected cancelled event for job {job_id}");
}

/// Asserts that a failed event was recorded for the job.
pub fn assert_failed_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Failed(_, _, _)));

    assert!(found, "Expected failed event for job {job_id}");
}

/// Revives the execution graph and completes all tasks in the next stage.
pub fn revive_graph_and_complete_next_stage(graph: &mut ExecutionGraph) -> Result<usize> {
    let executor = mock_executor("executor-id1".to_string());
    revive_graph_and_complete_next_stage_with_executor(graph, &executor)
}

/// Revives the execution graph and completes all tasks in the next stage using the given executor.
pub fn revive_graph_and_complete_next_stage_with_executor(
    graph: &mut ExecutionGraph,
    executor: &ExecutorMetadata,
) -> Result<usize> {
    graph.revive();

    // find the num_available_tasks of the next running stage
    let num_available_tasks = graph
        .stages()
        .values()
        .map(|stage| {
            if let ExecutionStage::Running(stage) = stage {
                stage
                    .task_infos
                    .iter()
                    .filter(|info| info.is_none())
                    .count()
            } else {
                0
            }
        })
        .find(|num_available_tasks| num_available_tasks > &0)
        .unwrap();

    if num_available_tasks > 0 {
        for _ in 0..num_available_tasks {
            if let Some(task) = graph.pop_next_task(&executor.id).unwrap() {
                let task_status = mock_completed_task(task, &executor.id);
                graph.update_task_status(executor, vec![task_status], 1, 1)?;
            }
        }
    }

    Ok(num_available_tasks)
}

/// Creates a test execution graph with a simple aggregation plan.
pub async fn test_aggregation_plan(partition: usize) -> ExecutionGraph {
    test_aggregation_plan_with_job_id(partition, "job").await
}

/// Creates a test execution graph with a simple aggregation plan and custom job ID.
pub async fn test_aggregation_plan_with_job_id(
    partition: usize,
    job_id: &str,
) -> ExecutionGraph {
    let config = SessionConfig::new().with_target_partitions(partition);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let session_state = ctx.state();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("gmv", DataType::UInt64, false),
    ]);

    // we specify the input partitions to be > 1 because of https://github.com/apache/datafusion/issues/12611
    let logical_plan = scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), 2)
        .unwrap()
        .aggregate(vec![col("id")], vec![sum(col("gmv"))])
        .unwrap()
        .build()
        .unwrap();

    let optimized_plan = session_state.optimize(&logical_plan).unwrap();

    let plan = session_state
        .create_physical_plan(&optimized_plan)
        .await
        .unwrap();

    println!(
        "{}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(false)
    );
    let mut planner = DefaultDistributedPlanner::new();
    ExecutionGraph::new(
        "localhost:50050",
        job_id,
        "",
        "session",
        plan,
        0,
        Arc::new(SessionConfig::new_with_ballista()),
        &mut planner,
    )
    .unwrap()
}

/// Creates a test execution graph with two nested aggregations.
pub async fn test_two_aggregations_plan(partition: usize) -> ExecutionGraph {
    let config = SessionConfig::new().with_target_partitions(partition);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let session_state = ctx.state();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("gmv", DataType::UInt64, false),
    ]);

    // we specify the input partitions to be > 1 because of https://github.com/apache/datafusion/issues/12611
    let logical_plan = scan_empty_with_partitions(None, &schema, Some(vec![0, 1, 2]), 2)
        .unwrap()
        .aggregate(vec![col("id"), col("name")], vec![sum(col("gmv"))])
        .unwrap()
        .aggregate(vec![col("id")], vec![count(col("id"))])
        .unwrap()
        .build()
        .unwrap();

    let optimized_plan = session_state.optimize(&logical_plan).unwrap();

    let plan = session_state
        .create_physical_plan(&optimized_plan)
        .await
        .unwrap();

    println!(
        "{}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(false)
    );
    let mut planner = DefaultDistributedPlanner::new();
    ExecutionGraph::new(
        "localhost:50050",
        "job",
        "",
        "session",
        plan,
        0,
        Arc::new(SessionConfig::new_with_ballista()),
        &mut planner,
    )
    .unwrap()
}

/// Creates a test execution graph with a coalesce (limit) operation.
pub async fn test_coalesce_plan(partition: usize) -> ExecutionGraph {
    let config = SessionConfig::new().with_target_partitions(partition);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let session_state = ctx.state();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("gmv", DataType::UInt64, false),
    ]);

    // we specify the input partitions to be > 1 because of https://github.com/apache/datafusion/issues/12611
    let logical_plan = scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), 2)
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

    let optimized_plan = session_state.optimize(&logical_plan).unwrap();

    let plan = session_state
        .create_physical_plan(&optimized_plan)
        .await
        .unwrap();
    let mut planner = DefaultDistributedPlanner::new();
    ExecutionGraph::new(
        "localhost:50050",
        "job",
        "",
        "session",
        plan,
        0,
        Arc::new(SessionConfig::new_with_ballista()),
        &mut planner,
    )
    .unwrap()
}

/// Creates a test execution graph with a join operation.
pub async fn test_join_plan(partition: usize) -> ExecutionGraph {
    let mut config = SessionConfig::new().with_target_partitions(partition);
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let session_state = ctx.state();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("gmv", DataType::UInt64, false),
    ]);

    // we specify the input partitions to be > 1 because of https://github.com/apache/datafusion/issues/12611
    let left_plan = scan_empty_with_partitions(Some("left"), &schema, None, 2).unwrap();

    let right_plan = scan_empty_with_partitions(Some("right"), &schema, None, 2)
        .unwrap()
        .build()
        .unwrap();

    let sort_expr = SortExpr::new(col("id"), false, false);

    let logical_plan = left_plan
        .join(right_plan, JoinType::Inner, (vec!["id"], vec!["id"]), None)
        .unwrap()
        .aggregate(vec![col("left.id")], vec![sum(col("left.gmv"))])
        .unwrap()
        .sort(vec![sort_expr])
        .unwrap()
        .build()
        .unwrap();

    let optimized_plan = session_state.optimize(&logical_plan).unwrap();

    let plan = session_state
        .create_physical_plan(&optimized_plan)
        .await
        .unwrap();

    println!(
        "{}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(false)
    );
    let mut planner = DefaultDistributedPlanner::new();
    let graph = ExecutionGraph::new(
        "localhost:50050",
        "job",
        "",
        "session",
        plan,
        0,
        Arc::new(SessionConfig::new_with_ballista()),
        &mut planner,
    )
    .unwrap();

    println!("{graph:?}");

    graph
}

/// Creates a test execution graph with a UNION ALL operation.
pub async fn test_union_all_plan(partition: usize) -> ExecutionGraph {
    let config = SessionConfig::new().with_target_partitions(partition);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let session_state = ctx.state();

    let logical_plan = ctx
        .sql("SELECT 1 as NUMBER union all SELECT 1 as NUMBER;")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();

    let optimized_plan = session_state.optimize(&logical_plan).unwrap();

    let plan = session_state
        .create_physical_plan(&optimized_plan)
        .await
        .unwrap();

    println!(
        "{}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(false)
    );
    let mut planner = DefaultDistributedPlanner::new();
    let graph = ExecutionGraph::new(
        "localhost:50050",
        "job",
        "",
        "session",
        plan,
        0,
        Arc::new(SessionConfig::new_with_ballista()),
        &mut planner,
    )
    .unwrap();

    println!("{graph:?}");

    graph
}

/// Creates a test execution graph with a UNION (distinct) operation.
pub async fn test_union_plan(partition: usize) -> ExecutionGraph {
    let config = SessionConfig::new().with_target_partitions(partition);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let session_state = ctx.state();

    let logical_plan = ctx
        .sql("SELECT 1 as NUMBER union SELECT 1 as NUMBER;")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();

    let optimized_plan = session_state.optimize(&logical_plan).unwrap();

    let plan = session_state
        .create_physical_plan(&optimized_plan)
        .await
        .unwrap();

    println!(
        "{}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(false)
    );
    let mut planner = DefaultDistributedPlanner::new();
    let graph = ExecutionGraph::new(
        "localhost:50050",
        "job",
        "",
        "session",
        plan,
        0,
        Arc::new(SessionConfig::new_with_ballista()),
        &mut planner,
    )
    .unwrap();

    println!("{graph:?}");

    graph
}

/// Creates a mock executor metadata with the given ID.
pub fn mock_executor(executor_id: String) -> ExecutorMetadata {
    ExecutorMetadata {
        id: executor_id,
        host: "localhost2".to_string(),
        port: 8080,
        grpc_port: 9090,
        specification: ExecutorSpecification { task_slots: 1 },
    }
}

/// Creates a mock successful task status for the given task.
pub fn mock_completed_task(task: TaskDescription, executor_id: &str) -> TaskStatus {
    let mut partitions: Vec<protobuf::ShuffleWritePartition> = vec![];

    let num_partitions = task.get_output_partition_number();

    for partition_id in 0..num_partitions {
        partitions.push(protobuf::ShuffleWritePartition {
            partition_id: partition_id as u64,
            path: format!(
                "/{}/{}/{}",
                task.partition.job_id,
                task.partition.stage_id,
                task.partition.partition_id
            ),
            num_batches: 1,
            num_rows: 1,
            num_bytes: 1,
        })
    }

    // Complete the task
    protobuf::TaskStatus {
        task_id: task.task_id as u32,
        job_id: task.partition.job_id.clone(),
        stage_id: task.partition.stage_id as u32,
        stage_attempt_num: task.stage_attempt_num as u32,
        partition_id: task.partition.partition_id as u32,
        launch_time: 0,
        start_exec_time: 0,
        end_exec_time: 0,
        metrics: vec![],
        status: Some(task_status::Status::Successful(protobuf::SuccessfulTask {
            executor_id: executor_id.to_owned(),
            partitions,
        })),
    }
}

/// Creates a mock failed task status for the given task.
pub fn mock_failed_task(task: TaskDescription, failed_task: FailedTask) -> TaskStatus {
    let mut partitions: Vec<protobuf::ShuffleWritePartition> = vec![];

    let num_partitions = task.get_output_partition_number();

    for partition_id in 0..num_partitions {
        partitions.push(protobuf::ShuffleWritePartition {
            partition_id: partition_id as u64,
            path: format!(
                "/{}/{}/{}",
                task.partition.job_id,
                task.partition.stage_id,
                task.partition.partition_id
            ),
            num_batches: 1,
            num_rows: 1,
            num_bytes: 1,
        })
    }

    // Fail the task
    protobuf::TaskStatus {
        task_id: task.task_id as u32,
        job_id: task.partition.job_id.clone(),
        stage_id: task.partition.stage_id as u32,
        stage_attempt_num: task.stage_attempt_num as u32,
        partition_id: task.partition.partition_id as u32,
        launch_time: 0,
        start_exec_time: 0,
        end_exec_time: 0,
        metrics: vec![],
        status: Some(task_status::Status::Failed(failed_task)),
    }
}
