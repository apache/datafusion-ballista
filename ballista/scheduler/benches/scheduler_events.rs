use async_trait::async_trait;
use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    task_status, ExecutorHeartbeat, JobStatus, ShuffleWritePartition, SuccessfulTask,
    TaskDefinition, TaskStatus,
};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorMetadata, ExecutorSpecification,
};
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::default_session_builder;
use ballista_scheduler::cluster::memory::{InMemoryClusterState, InMemoryJobState};
use ballista_scheduler::cluster::{BallistaCluster, ClusterState};
use ballista_scheduler::config::{SchedulerConfig, TaskDistribution};
use ballista_scheduler::metrics::NoopMetricsCollector;
use ballista_scheduler::scheduler_server::SchedulerServer;
use ballista_scheduler::state::execution_graph::TaskDescription;
use ballista_scheduler::state::executor_manager::{ExecutorManager, ExecutorReservation};
use ballista_scheduler::state::task_manager::TaskLauncher;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Statistics;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    EmptyRecordBatchStream, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion::prelude::{col, count, SessionContext};
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use pprof::criterion::{Output, PProfProfiler};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::{mpsc, Mutex};
use tracing_subscriber::EnvFilter;

fn dummy_table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, true),
    ]))
}

#[derive(Debug, Clone)]
struct DummyTable(usize);

#[async_trait]
impl TableProvider for DummyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        dummy_table_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DummyTableExec(self.0)))
    }
}

#[derive(Debug)]
struct DummyTableExec(usize);

#[async_trait]
impl ExecutionPlan for DummyTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        dummy_table_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.0)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptyRecordBatchStream::new(dummy_table_schema())))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct Launcher(mpsc::Sender<(String, TaskStatus)>);

#[async_trait]
impl TaskLauncher<LogicalPlanNode, PhysicalPlanNode> for Launcher {
    fn prepare_task_definition(
        &self,
        _ctx: Arc<SessionContext>,
        _task: &TaskDescription,
    ) -> Result<TaskDefinition> {
        unimplemented!("why are you being called!")
    }

    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: &[TaskDescription],
        _executor_manager: &ExecutorManager,
    ) -> Result<()> {
        // Simulate a delay required to call the executor RPC
        tokio::time::sleep(Duration::from_millis(25)).await;

        for task in tasks {
            let shuffle_write_partitions = (0..task
                .output_partitioning
                .as_ref()
                .map(|p| p.partition_count())
                .unwrap_or(1))
                .map(|part| ShuffleWritePartition {
                    partitions: task
                        .partitions
                        .partitions
                        .iter()
                        .map(|p| *p as u32)
                        .collect(),
                    output_partition: part as u32,
                    path: "/path/to/partition".to_string(),
                    num_batches: 8,
                    num_rows: 8 * 1024,
                    num_bytes: 8 * 1024 * 1024,
                })
                .collect();

            self.0
                .send((
                    executor.id.clone(),
                    TaskStatus {
                        task_id: task.task_id as u32,
                        job_id: task.partitions.job_id.clone(),
                        stage_id: task.partitions.stage_id as u32,
                        stage_attempt_num: task.stage_attempt_num as u32,
                        partitions: task
                            .partitions
                            .partitions
                            .iter()
                            .map(|p| *p as u32)
                            .collect(),
                        launch_time: 0,
                        start_exec_time: 0,
                        end_exec_time: 0,
                        metrics: vec![],
                        status: Some(task_status::Status::Successful(SuccessfulTask {
                            executor_id: executor.id.clone(),
                            partitions: shuffle_write_partitions,
                        })),
                    },
                ))
                .await
                .map_err(|e| {
                    BallistaError::Internal(format!("Error sending status: {e:?}"))
                })?;
        }

        Ok(())
    }
}

struct RemoteClusterState {
    inner: InMemoryClusterState,
    reservation_guard: Mutex<()>,
}

impl RemoteClusterState {
    pub fn new() -> Self {
        Self {
            inner: InMemoryClusterState::default(),
            reservation_guard: Mutex::new(()),
        }
    }
}

#[async_trait]
impl ClusterState for RemoteClusterState {
    async fn reserve_slots(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<ExecutorReservation>> {
        if let Ok(_guard) = self.reservation_guard.try_lock() {
            self.inner
                .reserve_slots(num_slots, distribution, executors)
                .await
        } else {
            Ok(vec![])
        }
    }

    async fn reserve_slots_exact(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<ExecutorReservation>> {
        if let Ok(_guard) = self.reservation_guard.try_lock() {
            self.inner
                .reserve_slots_exact(num_slots, distribution, executors)
                .await
        } else {
            Ok(vec![])
        }
    }

    async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        let _guard = self.reservation_guard.lock().await;

        self.inner.cancel_reservations(reservations).await
    }

    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
        reserve: bool,
    ) -> Result<Vec<ExecutorReservation>> {
        self.inner.register_executor(metadata, spec, reserve).await
    }

    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        self.inner.save_executor_metadata(metadata).await
    }

    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata> {
        self.inner.get_executor_metadata(executor_id).await
    }

    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()> {
        self.inner.save_executor_heartbeat(heartbeat).await
    }

    async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        self.inner.remove_executor(executor_id).await
    }

    fn executor_heartbeats(&self) -> HashMap<String, ExecutorHeartbeat> {
        self.inner.executor_heartbeats()
    }

    fn get_executor_heartbeat(&self, executor_id: &str) -> Option<ExecutorHeartbeat> {
        self.inner.get_executor_heartbeat(executor_id)
    }
}

async fn setup_env(
    num_executors: usize,
) -> Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
    let scheduler_name = "bench-server".to_string();

    let cluster = BallistaCluster::new(
        Arc::new(RemoteClusterState::new()),
        Arc::new(InMemoryJobState::new(
            scheduler_name.clone(),
            default_session_builder,
        )),
    );

    let config = SchedulerConfig::default()
        .with_task_distribution(TaskDistribution::Bias)
        .with_tasks_per_tick(1024)
        .with_scheduler_tick_interval_ms(100)
        .with_scheduler_policy(TaskSchedulingPolicy::PushStaged);

    let metrics = Arc::new(NoopMetricsCollector::default());

    let codec = BallistaCodec::default();

    let (status_tx, mut status_rx) = mpsc::channel(10_000);

    let launcher = Arc::new(Launcher(status_tx));

    let mut server = SchedulerServer::new_with_task_launcher(
        scheduler_name.clone(),
        cluster,
        codec,
        config,
        metrics,
        launcher,
    );

    server.init().await.unwrap();

    let server = Arc::new(server);

    for id in 0..num_executors {
        let executor_meta = ExecutorMetadata {
            id: format!("executor-{id}"),
            host: "127.0.0.1".to_string(),
            port: 7799,
            grpc_port: 7799,
            specification: ExecutorSpecification { task_slots: 10 },
        };

        let executor_data = ExecutorData {
            executor_id: format!("executor-{id}"),
            total_task_slots: 10,
            available_task_slots: 10,
        };
        server
            .state
            .executor_manager
            .register_executor(executor_meta, executor_data, false, false)
            .await
            .unwrap();
    }

    let server_clone = server.clone();
    tokio::spawn(async move {
        while let Some((executor_id, status)) = status_rx.recv().await {
            server_clone
                .update_task_status(&executor_id, vec![status])
                .await
                .unwrap();
        }
    });

    server
}

fn criterion_benchmark(c: &mut Criterion) {
    tracing_subscriber::fmt()
        .with_ansi(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut group = c.benchmark_group("benchmarks");

    group
        .sample_size(10)
        .sampling_mode(SamplingMode::Flat)
        .measurement_time(Duration::from_secs(30));

    group.bench_function("scheduler_event", move |b| {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();

        b.to_async(rt).iter(|| async move {
            let server = setup_env(100).await;

            let ctx = Arc::new(SessionContext::default());

            let plan = ctx
                .read_table(Arc::new(DummyTable(1000)))
                .unwrap()
                .aggregate(vec![col("a")], vec![count(col("b"))])
                .unwrap()
                .logical_plan()
                .clone();

            let mut completions = vec![];

            for id in 0..50 {
                let job_id = format!("job-{id}");
                server
                    .submit_job(&job_id, "", ctx.clone(), &plan)
                    .await
                    .unwrap();

                let server = server.clone();
                completions.push(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(500)).await;

                        if let Ok(Some(JobStatus {
                            status: Some(Status::Successful(_)),
                            ..
                        })) = server.state.task_manager.get_job_status(&job_id).await
                        {
                            break;
                        }
                    }
                })
            }

            futures::future::join_all(completions).await
        });
    });

    group.finish()
}

criterion_group! {
    name = benches;
    config  = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = criterion_benchmark
}
criterion_main!(benches);
