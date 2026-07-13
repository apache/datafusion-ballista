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

//! Execution engine abstraction for query stage execution.
//!
//! This module provides traits and default implementations for executing
//! query stages in a distributed setting. The execution engine is responsible
//! for creating query stage executors from physical plans.

use ballista_core::client_pool::BallistaClientPool;
use ballista_core::execution_plans::sort_shuffle::SortShuffleWriterExec;
use ballista_core::execution_plans::{ShuffleReaderExec, ShuffleWriterExec};
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::{JobId, utils};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::prelude::SessionConfig;
use log::warn;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;

/// Extension point for customizing query stage execution.
///
/// Implement this trait to provide a custom execution engine that can
/// transform physical plans into query stage executors. This allows
/// for custom execution strategies beyond the default DataFusion-based
/// execution.
pub trait ExecutionEngine: Sync + Send {
    /// Creates a query stage executor from a physical plan.
    ///
    /// The returned executor will be responsible for executing the given
    /// plan partition and writing shuffle output to the specified work directory.
    fn create_query_stage_exec(
        &self,
        job_id: JobId,
        stage_id: usize,
        partition_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
        config: &SessionConfig,
    ) -> Result<Arc<dyn QueryStageExecutor>>;
}

/// Executor for a single query stage in a distributed query.
///
/// A query stage is a section of a query plan that has consistent partitioning
/// and can be executed as one unit with each partition running in parallel.
/// The output of each partition is re-partitioned and written to disk in
/// Arrow IPC format. Subsequent stages read these results via ShuffleReaderExec.
#[async_trait::async_trait]
pub trait QueryStageExecutor: Sync + Send + Debug + Display {
    /// Executes a single partition of this query stage.
    ///
    /// Returns metadata about the shuffle partitions written to disk,
    /// including file paths and statistics.
    async fn execute_query_stage(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>>;

    /// Collects execution metrics from all operators in the plan.
    fn collect_plan_metrics(&self) -> Vec<MetricsSet>;
}

/// Default execution engine using DataFusion's ShuffleWriterExec.
///
/// This implementation expects the input plan to be wrapped in a
/// ShuffleWriterExec and creates a DefaultQueryStageExec to execute it.
#[derive(Default)]
pub struct DefaultExecutionEngine {
    client_pool: Option<Arc<dyn BallistaClientPool>>,
}

impl DefaultExecutionEngine {
    /// Creates new Default Execution Engine without client pooling
    pub fn new() -> Self {
        Self { client_pool: None }
    }
    /// Creates new Default Execution Engine with client pooling
    pub fn with_client_pool(client_pool: Arc<dyn BallistaClientPool>) -> Self {
        Self {
            client_pool: Some(client_pool),
        }
    }
}

/// Restrict a `DataSourceExec` to the file group for `partition_id`.
///
/// DataFusion 54's `DataSourceExec` hands file groups to partition streams from
/// a shared work-queue that is only divided across partitions when all
/// partitions of one plan instance are polled concurrently. Ballista runs one
/// partition per task on its own plan instance, so a task that polls a single
/// partition in isolation would otherwise drain the whole queue and scan the
/// entire table. Keeping only this task's file group (other slots emptied,
/// partition count preserved) makes the lone `execute(partition_id)` read just
/// that group.
///
/// Returns `None` for any node that is not a file-backed `DataSourceExec`, and
/// for a `partition_id` outside the source's file groups (e.g. when an operator
/// between the scan and the stage output changed the partition count).
///
/// Only `FileScanConfig` distributes work from the shared queue. Other data
/// sources (e.g. `MemorySourceConfig`) isolate partitions in their own
/// `open(partition)` and need no restriction, so they are left unchanged. An
/// unrecognized source type is warned about, since a future source that
/// distributes work across partitions would over-read here without handling.
fn restrict_scan_to_partition(
    plan: &Arc<dyn ExecutionPlan>,
    partition_id: usize,
) -> Option<Arc<dyn ExecutionPlan>> {
    let exec = plan.downcast_ref::<DataSourceExec>()?;
    let source: &dyn Any = exec.data_source().as_ref();
    let Some(config) = source.downcast_ref::<FileScanConfig>() else {
        if source.downcast_ref::<MemorySourceConfig>().is_none() {
            warn!(
                "restrict_scan_to_partition: unrecognized DataSourceExec source type \
                 left unrestricted; if it distributes work across partitions from a \
                 shared queue, a single-partition task could over-read"
            );
        }
        return None;
    };
    if partition_id >= config.file_groups.len() {
        return None;
    }
    let file_groups: Vec<FileGroup> = config
        .file_groups
        .iter()
        .enumerate()
        .map(|(i, group)| {
            if i == partition_id {
                group.clone()
            } else {
                FileGroup::new(vec![])
            }
        })
        .collect();
    let config = FileScanConfigBuilder::from(config.clone())
        .with_file_groups(file_groups)
        .build();
    Some(DataSourceExec::from_data_source(config))
}

/// The plan the stage's shuffle writer consumes: the writer's own child, wrapped in a
/// [`MemoryGuardExec`](crate::memory_pools::MemoryGuardExec).
///
/// The guard's stream checks the executor's real allocator balance against the armed
/// limit before every batch, and fails this one task with a retriable
/// `ResourcesExhausted` rather than letting the process be OOM-killed. Placing it here
/// puts the check on the stage's entire data path.
///
/// **The insertion point is load-bearing**, for two reasons -- neither of which is that
/// the guard hides the plan beneath it. It does not: `MemoryGuardExec` implements no
/// `downcast_delegate`, so `downcast_ref` on the guard itself yields `None`, but
/// `TreeNode::transform` descends through `children()` / `with_new_children()` and
/// downcasts each node it visits individually. An opaque node conceals nothing below
/// itself from a traversal. (DataFusion says as much of `downcast_delegate`: it "should
/// not be used for plan traversal or optimizer rewrites".)
///
/// What actually matters:
///
/// 1. **The guard must not sit on the stage root.** [`build_stage_writer`] and the
///    scheduler both `downcast_ref` the root to find the shuffle writer
///    (`ShuffleWriterExec` / `SortShuffleWriterExec`). A guard *there* is
///    opaque to that downcast and the stage would not be recognised as a writer at all.
///    Hence the guard goes immediately *below* the writer, wrapping the writer's child.
///    See `the_guard_is_inserted_immediately_below_each_shuffle_writer`.
/// 2. **Exactly one guard must be inserted.** The guard adds a level to the plan, and
///    the executor's flattened metrics list must stay in step with the scheduler's view
///    of the same plan, which is zipped by position. One guard, at a known depth, keeps
///    that predictable. See `exactly_one_guard_is_inserted` and
///    `the_guard_does_not_change_the_metrics_list_length`.
///
/// Do not give `MemoryGuardExec` a `downcast_delegate`, and do not move this call onto
/// the root.
///
/// [`build_stage_writer`]: DefaultExecutionEngine::build_stage_writer
#[cfg(feature = "oom-guard")]
fn stage_input(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(crate::memory_pools::MemoryGuardExec::new(
        plan.children()[0].clone(),
    ))
}

/// The plan the stage's shuffle writer consumes: the writer's own child, unchanged.
///
/// Without the `oom-guard` feature there is no tracking allocator and nothing to check,
/// so the stage plan is rebuilt exactly as before.
#[cfg(not(feature = "oom-guard"))]
fn stage_input(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    plan.children()[0].clone()
}

impl DefaultExecutionEngine {
    /// Rebuild the scheduler's stage plan for execution on this executor and return its
    /// shuffle writer.
    ///
    /// Split out of [`ExecutionEngine::create_query_stage_exec`] so that tests can assert
    /// on the concrete rewritten plan (which the `Arc<dyn QueryStageExecutor>` it is
    /// wrapped in hides).
    fn build_stage_writer(
        &self,
        job_id: JobId,
        stage_id: usize,
        partition_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<ShuffleWriterVariant> {
        let plan = plan
            .transform(|p| {
                if let Some(reader) = p.downcast_ref::<ShuffleReaderExec>() {
                    match &self.client_pool {
                        Some(client_pool) => Ok(Transformed::yes(Arc::new(
                            reader
                                .with_work_dir(work_dir.to_string())
                                .with_client_pool(client_pool.clone()),
                        ))),
                        None => Ok(Transformed::yes(Arc::new(
                            reader.with_work_dir(work_dir.to_string()),
                        ))),
                    }
                } else if let Some(rewritten) =
                    restrict_scan_to_partition(&p, partition_id)
                {
                    Ok(Transformed::yes(rewritten))
                } else {
                    Ok(Transformed::no(p))
                }
            })?
            .data;

        // the query plan created by the scheduler always starts with a shuffle writer
        // (either ShuffleWriterExec or SortShuffleWriterExec)
        if let Some(shuffle_writer) = plan.downcast_ref::<ShuffleWriterExec>() {
            // recreate the shuffle writer with the correct working directory
            let exec = ShuffleWriterExec::try_new(
                job_id,
                stage_id,
                stage_input(&plan),
                work_dir.to_string(),
                shuffle_writer.shuffle_output_partitioning().cloned(),
            )?;
            Ok(ShuffleWriterVariant::Hash(exec))
        } else if let Some(sort_shuffle_writer) =
            plan.downcast_ref::<SortShuffleWriterExec>()
        {
            // recreate the sort shuffle writer with the correct working directory
            let exec = SortShuffleWriterExec::try_new(
                job_id,
                stage_id,
                stage_input(&plan),
                work_dir.to_string(),
                sort_shuffle_writer.shuffle_output_partitioning().clone(),
                sort_shuffle_writer.config().clone(),
            )?;
            Ok(ShuffleWriterVariant::Sort(exec))
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to new_query_stage_exec is not a ShuffleWriterExec or SortShuffleWriterExec"
                    .to_string(),
            ))
        }
    }
}

impl ExecutionEngine for DefaultExecutionEngine {
    fn create_query_stage_exec(
        &self,
        job_id: JobId,
        stage_id: usize,
        partition_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
        _config: &SessionConfig,
    ) -> Result<Arc<dyn QueryStageExecutor>> {
        let writer =
            self.build_stage_writer(job_id, stage_id, partition_id, plan, work_dir)?;
        Ok(Arc::new(DefaultQueryStageExec::new(writer)))
    }
}

/// Enum representing the different shuffle writer implementations.
#[derive(Debug, Clone)]
pub enum ShuffleWriterVariant {
    /// Hash-based shuffle writer (original implementation).
    Hash(ShuffleWriterExec),
    /// Sort-based shuffle writer.
    Sort(SortShuffleWriterExec),
}

/// Default query stage executor that wraps a shuffle writer.
///
/// This executor delegates to the underlying shuffle writer to perform the actual
/// shuffle write operation, which partitions the data and writes it to disk.
#[derive(Debug)]
pub struct DefaultQueryStageExec {
    /// The underlying shuffle writer execution plan.
    shuffle_writer: ShuffleWriterVariant,
}

impl DefaultQueryStageExec {
    /// Creates a new DefaultQueryStageExec wrapping the given shuffle writer.
    pub fn new(shuffle_writer: ShuffleWriterVariant) -> Self {
        Self { shuffle_writer }
    }
}

impl Display for DefaultQueryStageExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Hash): ({})\n{}",
                    stage_metrics.join(", "),
                    writer
                )
            }
            ShuffleWriterVariant::Sort(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Sort): ({})\n{}",
                    stage_metrics.join(", "),
                    writer
                )
            }
        }
    }
}

#[async_trait::async_trait]
impl QueryStageExecutor for DefaultQueryStageExec {
    async fn execute_query_stage(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => {
                writer
                    .clone()
                    .execute_shuffle_write(input_partition, context)
                    .await
            }
            ShuffleWriterVariant::Sort(writer) => {
                writer
                    .clone()
                    .execute_shuffle_write(input_partition, context)
                    .await
            }
        }
    }

    fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => utils::collect_plan_metrics(writer),
            ShuffleWriterVariant::Sort(writer) => utils::collect_plan_metrics(writer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::client_pool::{BallistaClientPool, PooledClient};
    #[cfg(feature = "oom-guard")]
    use ballista_core::execution_plans::sort_shuffle::SortShuffleConfig;
    use ballista_core::extension::BallistaConfigGrpcEndpoint;
    use ballista_core::utils::GrpcClientConfig;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    #[cfg(feature = "oom-guard")]
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::union::UnionExec;

    /// The schema every plan in these tests is built over.
    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    /// The work dir the rewritten stage plan must be given.
    const WORK_DIR: &str = "/work/dir";

    /// A stage plan as the scheduler hands it over: rooted at a `ShuffleWriterExec`.
    fn hash_writer(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            ShuffleWriterExec::try_new(
                "job".into(),
                1,
                input,
                "/scheduler/dir".to_string(),
                None,
            )
            .unwrap(),
        )
    }

    /// The same, rooted at a `SortShuffleWriterExec` -- the branch that is easy to forget.
    /// Only the guard-placement tests build this variant, and they only exist when the
    /// guard is compiled in.
    #[cfg(feature = "oom-guard")]
    fn sort_writer(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let partitioning = Partitioning::Hash(vec![col("a", &test_schema()).unwrap()], 2);
        Arc::new(
            SortShuffleWriterExec::try_new(
                "job".into(),
                1,
                input,
                "/scheduler/dir".to_string(),
                partitioning,
                SortShuffleConfig::default(),
            )
            .unwrap(),
        )
    }

    /// The rebuilt shuffle writer, as an `ExecutionPlan`, whichever variant it is.
    fn writer_root(writer: &ShuffleWriterVariant) -> Arc<dyn ExecutionPlan> {
        match writer {
            ShuffleWriterVariant::Hash(w) => Arc::new(w.clone()),
            ShuffleWriterVariant::Sort(w) => Arc::new(w.clone()),
        }
    }

    /// The first node of type `T` anywhere in the tree, in pre-order.
    fn find_first<T: ExecutionPlan + 'static>(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.downcast_ref::<T>().is_some() {
            return Some(Arc::clone(plan));
        }
        plan.children().into_iter().find_map(find_first::<T>)
    }

    /// A client pool test double with a recognizable `Debug` rendering: the reader's
    /// `client_pool` field is private, so `Debug` is how a test observes that the
    /// `transform` pass installed it.
    #[derive(Debug)]
    struct TestClientPool;

    #[async_trait::async_trait]
    impl BallistaClientPool for TestClientPool {
        async fn acquire(
            &self,
            _host: &str,
            _port: u16,
            _config: &GrpcClientConfig,
            _customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
        ) -> ballista_core::error::Result<PooledClient> {
            unimplemented!("no client is ever acquired in these tests")
        }

        async fn evict_idle(&self) {}
    }

    /// Build a `DataSourceExec` over `n` file groups, one file each.
    fn scan_with_file_groups(n: usize) -> Arc<dyn ExecutionPlan> {
        let schema = test_schema();
        let source = Arc::new(ParquetSource::new(schema));
        let mut builder =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source);
        for i in 0..n {
            builder =
                builder.with_file_group(FileGroup::new(vec![PartitionedFile::new(
                    format!("file{i}.parquet"),
                    100,
                )]));
        }
        DataSourceExec::from_data_source(builder.build())
    }

    /// Number of files in each file group of a `DataSourceExec`.
    fn group_file_counts(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
        let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
        let source: &dyn Any = exec.data_source().as_ref();
        let config = source.downcast_ref::<FileScanConfig>().unwrap();
        config.file_groups.iter().map(|g| g.len()).collect()
    }

    #[test]
    fn restrict_scan_keeps_only_its_own_group() {
        let plan = scan_with_file_groups(4);
        // partition 2 keeps only group 2; the partition count is preserved.
        let restricted = restrict_scan_to_partition(&plan, 2).expect("scan rewritten");
        assert_eq!(group_file_counts(&restricted), vec![0, 0, 1, 0]);
    }

    #[test]
    fn restrict_scan_partition_out_of_range_is_left_untouched() {
        let plan = scan_with_file_groups(3);
        // an operator between the scan and the stage output may change the
        // partition count; in that case we must not rewrite the scan.
        assert!(restrict_scan_to_partition(&plan, 3).is_none());
    }

    #[test]
    fn restrict_scan_ignores_non_file_scans() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        assert!(restrict_scan_to_partition(&plan, 0).is_none());
    }

    /// Inserting the guard must not disturb the `transform` pass: the
    /// `ShuffleReaderExec` still receives its `work_dir` / `client_pool` (without which
    /// the stage cannot read its shuffle input) and the `DataSourceExec` is still
    /// restricted to this task's partition (without which the task over-reads the shared
    /// file group -- silently wrong results, not an error). Runs in both feature
    /// configurations.
    ///
    /// Note what this does *not* prove. A guard hoisted above the `transform` pass would
    /// still leave both rewrites intact, because `TreeNode::transform` walks the plan via
    /// `children()` / `with_new_children()` -- which `MemoryGuardExec` implements -- and
    /// downcasts each node individually, so an opaque node in the path hides nothing
    /// beneath it. The guard's opacity to `downcast_ref` only breaks a downcast aimed at a
    /// node the guard sits *directly on top of* -- in this function, the shuffle writer on
    /// the stage root. That is what `the_guard_is_inserted_immediately_below_each_shuffle_writer`
    /// and `exactly_one_guard_is_inserted` pin down; both fail if the guard is inserted
    /// anywhere but immediately below the writer.
    #[test]
    fn the_transform_pass_still_reaches_the_reader_and_the_scan() {
        let engine = DefaultExecutionEngine::with_client_pool(Arc::new(TestClientPool));

        let reader: Arc<dyn ExecutionPlan> = Arc::new(
            ShuffleReaderExec::try_new(
                1,
                vec![vec![]],
                test_schema(),
                Partitioning::UnknownPartitioning(1),
            )
            .unwrap(),
        );
        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![reader, scan_with_file_groups(4)]).unwrap();

        let writer = engine
            .build_stage_writer("job".into(), 2, 2, hash_writer(union), WORK_DIR)
            .unwrap();
        let root = writer_root(&writer);

        let reader = find_first::<ShuffleReaderExec>(&root)
            .expect("the rebuilt plan must still contain the shuffle reader");
        // `work_dir` and `client_pool` are private to `ShuffleReaderExec`; its derived
        // `Debug` is the only view a test outside `ballista-core` has of them.
        let rendered = format!("{reader:?}");
        assert!(
            rendered.contains(&format!("work_dir: Some({WORK_DIR:?})")),
            "the reader must have been given its work_dir, got: {rendered}"
        );
        assert!(
            rendered.contains("client_pool: Some(TestClientPool)"),
            "the reader must have been given the client pool, got: {rendered}"
        );

        let scan = find_first::<DataSourceExec>(&root)
            .expect("the rebuilt plan must still contain the scan");
        assert_eq!(
            group_file_counts(&scan),
            vec![0, 0, 1, 0],
            "the scan must be restricted to this task's partition (2 of 4); an \
             unrestricted scan over-reads the shared file group -- wrong results"
        );
    }

    /// `collect_plan_metrics` pushes a node's metrics *and* recurses into its children,
    /// and the scheduler's `merge_stage_metrics` positionally zips the executor's list
    /// against its own guard-free plan -- silently dropping the metrics of every stage of
    /// every job on a length mismatch. `MemoryGuardExec` therefore reports no metrics of
    /// its own, and the two lists must stay the same length. This fails the instant
    /// someone adds a `metrics()` delegation to the guard.
    #[test]
    fn the_guard_does_not_change_the_metrics_list_length() {
        let engine = DefaultExecutionEngine::new();
        // The plan as the scheduler holds it: no guard, ever.
        let scheduler_plan = hash_writer(scan_with_file_groups(2));
        let writer = engine
            .build_stage_writer("job".into(), 1, 0, Arc::clone(&scheduler_plan), WORK_DIR)
            .unwrap();
        let executor_plan = writer_root(&writer);

        assert_eq!(
            utils::collect_plan_metrics(executor_plan.as_ref()).len(),
            utils::collect_plan_metrics(scheduler_plan.as_ref()).len(),
            "the executor's flattened metrics list must stay the same length as the \
             scheduler's guard-free view of the same plan"
        );
    }

    /// Number of `MemoryGuardExec` nodes anywhere in the tree.
    #[cfg(feature = "oom-guard")]
    fn count_guards(plan: &Arc<dyn ExecutionPlan>) -> usize {
        use crate::memory_pools::MemoryGuardExec;
        usize::from(plan.downcast_ref::<MemoryGuardExec>().is_some())
            + plan.children().into_iter().map(count_guards).sum::<usize>()
    }

    /// The guard sits immediately below the shuffle writer -- and nowhere else. The root
    /// must stay an un-wrapped writer (the scheduler's own downcast, and this function's,
    /// depend on it), and the guard's child must be the original input.
    ///
    /// Asserted for *both* writer variants: the `SortShuffleWriterExec` branch is easy to
    /// forget.
    #[cfg(feature = "oom-guard")]
    #[test]
    fn the_guard_is_inserted_immediately_below_each_shuffle_writer() {
        use crate::memory_pools::MemoryGuardExec;

        let engine = DefaultExecutionEngine::new();
        let input = Arc::new(EmptyExec::new(test_schema())) as Arc<dyn ExecutionPlan>;

        for (label, plan) in [
            ("ShuffleWriterExec", hash_writer(Arc::clone(&input))),
            ("SortShuffleWriterExec", sort_writer(Arc::clone(&input))),
        ] {
            let writer = engine
                .build_stage_writer("job".into(), 1, 0, plan, WORK_DIR)
                .unwrap();
            let root = writer_root(&writer);

            assert!(
                root.downcast_ref::<MemoryGuardExec>().is_none(),
                "{label}: the stage root must remain the writer, not a guard -- \
                 `create_query_stage_exec` downcasts the root to find the writer"
            );

            let children = root.children();
            assert_eq!(children.len(), 1, "{label}: the writer has one child");
            let guard = children[0]
                .downcast_ref::<MemoryGuardExec>()
                .unwrap_or_else(|| {
                    panic!("{label}: the writer's child must be a MemoryGuardExec")
                });
            assert!(
                Arc::ptr_eq(guard.input(), &input),
                "{label}: the guard must wrap the writer's original child, unchanged"
            );
        }
    }

    /// Exactly one guard, ever: no guard-over-guard, and none deeper in the tree. Every
    /// downcast site assumes the guard lives at exactly one known depth.
    #[cfg(feature = "oom-guard")]
    #[test]
    fn exactly_one_guard_is_inserted() {
        let engine = DefaultExecutionEngine::new();
        let reader: Arc<dyn ExecutionPlan> = Arc::new(
            ShuffleReaderExec::try_new(
                1,
                vec![vec![]],
                test_schema(),
                Partitioning::UnknownPartitioning(1),
            )
            .unwrap(),
        );
        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![reader, scan_with_file_groups(4)]).unwrap();

        for plan in [
            hash_writer(Arc::clone(&union)),
            sort_writer(Arc::clone(&union)),
        ] {
            let writer = engine
                .build_stage_writer("job".into(), 1, 0, plan, WORK_DIR)
                .unwrap();
            let root = writer_root(&writer);
            assert_eq!(
                count_guards(&root),
                1,
                "exactly one guard must be inserted, immediately below the writer"
            );
        }
    }
}
