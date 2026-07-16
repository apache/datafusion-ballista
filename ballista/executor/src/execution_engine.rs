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
use datafusion::physical_plan::union::UnionExec;
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

/// Restrict every file scan in `plan` to the file group the task running
/// `partition` will actually poll.
///
/// A task executes exactly one partition of its stage, so each scan beneath it
/// must be pinned to the one file group that partition reads (see
/// [`restrict_scan_to_partition`]). Which group that is depends on the path from
/// the stage root: most operators pass a partition straight through to their
/// children, but a `UnionExec` concatenates its children's partitions, so its
/// child `i` sees `partition` minus the partition counts of the children before
/// it. Applying the stage's partition number directly to a scan under a union
/// therefore addresses the wrong group, and for partitions past the scan's group
/// count it addresses none at all — leaving that scan unrestricted and free to
/// read the whole table.
///
/// Children a partition never reaches are left untouched: this task will not
/// execute them.
fn restrict_scans_for_partition(
    plan: &Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<UnionExec>().is_some() {
        // `UnionExec::execute(p)` walks its children subtracting each one's
        // partition count until `p` lands inside a child, then executes that
        // child alone. Mirror that walk so the scan under the serving child is
        // pinned to the group that child's local partition reads.
        let mut local = Some(partition);
        let mut children = Vec::with_capacity(plan.children().len());
        for child in plan.children() {
            let count = child.properties().output_partitioning().partition_count();
            match local {
                Some(p) if p < count => {
                    children.push(restrict_scans_for_partition(child, p)?);
                    local = None;
                }
                Some(p) => {
                    local = Some(p - count);
                    children.push(Arc::clone(child));
                }
                None => children.push(Arc::clone(child)),
            }
        }
        return Arc::clone(plan).with_new_children(children);
    }

    if let Some(rewritten) = restrict_scan_to_partition(plan, partition) {
        return Ok(rewritten);
    }

    let children = plan
        .children()
        .into_iter()
        .map(|child| restrict_scans_for_partition(child, partition))
        .collect::<Result<Vec<_>>>()?;
    if children.is_empty() {
        return Ok(Arc::clone(plan));
    }
    Arc::clone(plan).with_new_children(children)
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
                } else {
                    Ok(Transformed::no(p))
                }
            })?
            .data;
        let plan = restrict_scans_for_partition(&plan, partition_id)?;

        // the query plan created by the scheduler always starts with a shuffle writer
        // (either ShuffleWriterExec or SortShuffleWriterExec)
        if let Some(shuffle_writer) = plan.downcast_ref::<ShuffleWriterExec>() {
            // recreate the shuffle writer with the correct working directory
            let exec = ShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                work_dir.to_string(),
                shuffle_writer.shuffle_output_partitioning().cloned(),
            )?;
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Hash(exec),
            )))
        } else if let Some(sort_shuffle_writer) =
            plan.downcast_ref::<SortShuffleWriterExec>()
        {
            // recreate the sort shuffle writer with the correct working directory
            let exec = SortShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                work_dir.to_string(),
                sort_shuffle_writer.shuffle_output_partitioning().clone(),
                sort_shuffle_writer.config().clone(),
            )?;
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Sort(exec),
            )))
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to new_query_stage_exec is not a ShuffleWriterExec or SortShuffleWriterExec"
                    .to_string(),
            ))
        }
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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::empty::EmptyExec;

    /// Build a `DataSourceExec` over `n` file groups, one file each.
    fn scan_with_file_groups(n: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
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

    /// Find the file-group layout of each scan under a union, left child first.
    fn union_group_counts(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<usize>> {
        plan.children().into_iter().map(group_file_counts).collect()
    }

    /// A `UnionExec` concatenates its children's partitions, so stage partition
    /// `left_count + k` is the right child's local partition `k`. The scan under
    /// the serving child must be pinned to that local group.
    #[test]
    fn union_partition_maps_to_the_right_childs_local_group() {
        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![scan_with_file_groups(4), scan_with_file_groups(4)])
                .unwrap();
        assert_eq!(
            union.properties().output_partitioning().partition_count(),
            8
        );

        // Partition 1 is served by the left child's local partition 1.
        let restricted = restrict_scans_for_partition(&union, 1).unwrap();
        assert_eq!(
            union_group_counts(&restricted),
            vec![vec![0, 1, 0, 0], vec![1, 1, 1, 1]],
            "left scan pinned to group 1; right scan is never polled for this partition"
        );

        // Partition 6 is served by the right child's local partition 2 (6 - 4).
        // Before the mapping existed this addressed no group at all and left the
        // scan free to read the whole table.
        let restricted = restrict_scans_for_partition(&union, 6).unwrap();
        assert_eq!(
            union_group_counts(&restricted),
            vec![vec![1, 1, 1, 1], vec![0, 0, 1, 0]],
            "right scan pinned to local group 2"
        );
    }

    /// Every partition of a union must pin exactly one group in exactly one
    /// child, so across the whole stage each file group is read exactly once.
    #[test]
    fn every_union_partition_pins_exactly_one_group() {
        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![scan_with_file_groups(3), scan_with_file_groups(3)])
                .unwrap();
        for partition in 0..6 {
            let restricted = restrict_scans_for_partition(&union, partition).unwrap();
            let counts = union_group_counts(&restricted);
            let (serving, idle) = if partition < 3 { (0, 1) } else { (1, 0) };
            let expected_local = partition % 3;
            assert_eq!(
                counts[serving].iter().sum::<usize>(),
                1,
                "partition {partition}: serving child must keep exactly one group"
            );
            assert_eq!(
                counts[serving][expected_local], 1,
                "partition {partition}: expected local group {expected_local}, got {:?}",
                counts[serving]
            );
            assert_eq!(
                counts[idle],
                vec![1, 1, 1],
                "partition {partition}: the child it never polls is left untouched"
            );
        }
    }

    /// Without a union a partition passes straight through, so the behaviour is
    /// unchanged from restricting the scan directly.
    #[test]
    fn scan_without_union_is_pinned_to_its_own_partition() {
        let plan = scan_with_file_groups(4);
        let restricted = restrict_scans_for_partition(&plan, 2).unwrap();
        assert_eq!(group_file_counts(&restricted), vec![0, 0, 1, 0]);
    }
}
