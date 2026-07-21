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
use ballista_core::serde::scheduler::PartitionStats;
use ballista_core::{JobId, utils};
use datafusion::arrow::array::{
    Array, StringArray, StructArray, UInt32Array, UInt64Array,
};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::prelude::SessionConfig;
use futures::stream::TryStreamExt;
use log::debug;
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
    #[allow(clippy::too_many_arguments)]
    fn create_query_stage_exec(
        &self,
        job_id: JobId,
        stage_id: usize,
        task_id: usize,
        global_output_partition_ids: Vec<usize>,
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
    /// Executes this query stage's assigned partition slice.
    ///
    /// Returns metadata about the shuffle partitions written to disk,
    /// including file paths and statistics.
    async fn execute_query_stage(
        &self,
        task_id: usize,
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

impl ExecutionEngine for DefaultExecutionEngine {
    fn create_query_stage_exec(
        &self,
        job_id: JobId,
        stage_id: usize,
        task_id: usize,
        global_output_partition_ids: Vec<usize>,
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
                    // Scan restriction is scheduler-side (see
                    // ballista/scheduler/src/state/task_builder.rs). The plan
                    // arriving here is shrink-restricted to slice.len()
                    // partitions; the writer walks its child plan to attach
                    // global identity, using `global_output_partition_ids` for the
                    // pass-through case and detecting plan-level partitioning
                    // resets (SPM, RepartitionExec::Hash) for the rest.
                    Ok(Transformed::no(p))
                }
            })?
            .data;

        // the query plan created by the scheduler always starts with a shuffle writer
        // (either ShuffleWriterExec or SortShuffleWriterExec)
        if plan.downcast_ref::<ShuffleWriterExec>().is_some() {
            let exec = ShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                work_dir.to_string(),
            )?
            .with_task_id(task_id)
            .with_global_output_partition_ids(global_output_partition_ids);
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Passthrough(exec),
            )))
        } else if let Some(sort_shuffle_writer) =
            plan.downcast_ref::<SortShuffleWriterExec>()
        {
            let exec = SortShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                work_dir.to_string(),
                sort_shuffle_writer.shuffle_output_partitioning().clone(),
                sort_shuffle_writer.config().clone(),
            )?
            .with_task_id(task_id)
            .with_global_output_partition_ids(global_output_partition_ids);
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
    /// Passthrough shuffle writer: preserves its input partitioning,
    /// one file per output partition.
    Passthrough(ShuffleWriterExec),
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
            ShuffleWriterVariant::Passthrough(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Passthrough): ({})\n{}",
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
        task_id: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        let (plan_arc, is_sort_shuffle): (Arc<dyn ExecutionPlan>, bool) =
            match &self.shuffle_writer {
                ShuffleWriterVariant::Passthrough(writer) => {
                    (Arc::new(writer.clone()), false)
                }
                ShuffleWriterVariant::Sort(writer) => (Arc::new(writer.clone()), true),
            };
        debug!(
            "executor plan pre-run (task_id={task_id}):\n{}",
            DisplayableExecutionPlan::new(plan_arc.as_ref()).indent(true)
        );

        // Both variants share the same coordinator+oneshot handoff shape via
        // `execute(N)` — drive K parallel calls so every oneshot receiver is
        // taken concurrently and each output partition's summaries flow out
        // as soon as its files are closed.
        let result =
            drive_shuffle_writer_stage(plan_arc.clone(), context, is_sort_shuffle).await;

        debug!(
            "executor plan post-run (task_id={task_id}, ok={}):\n{}",
            result.is_ok(),
            DisplayableExecutionPlan::with_metrics(plan_arc.as_ref()).indent(true)
        );
        result
    }

    fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Passthrough(writer) => {
                utils::collect_plan_metrics(writer)
            }
            ShuffleWriterVariant::Sort(writer) => utils::collect_plan_metrics(writer),
        }
    }
}

/// Spawn K parallel `plan.execute(N, ctx)` calls against a shuffle writer,
/// collect metadata batches from each, and turn them back into
/// `Vec<ShuffleWritePartition>`. All K streams must be driven concurrently
/// so the writer's internal coordinator sees every oneshot receiver taken.
///
/// `is_sort_shuffle` is stamped onto every summary produced from the batches
/// — the metadata schema doesn't carry the flag (it's a handoff-only shape),
/// but the reader side needs it in `PartitionLocation` to pick the right
/// on-disk layout. The caller knows the variant from `ShuffleWriterVariant`.
async fn drive_shuffle_writer_stage(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    is_sort_shuffle: bool,
) -> Result<Vec<ShuffleWritePartition>> {
    let k = plan.properties().output_partitioning().partition_count();

    let mut stream_futures = Vec::with_capacity(k);
    for n in 0..k {
        let plan = plan.clone();
        let ctx = context.clone();
        stream_futures.push(tokio::spawn(async move {
            let mut stream = plan.execute(n, ctx)?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.try_next().await? {
                batches.push(batch);
            }
            metadata_batches_to_summaries(batches, is_sort_shuffle)
        }));
    }

    let mut summaries = Vec::with_capacity(k);
    for handle in stream_futures {
        let per_partition = handle.await.map_err(|e| {
            DataFusionError::Execution(format!("shuffle writer drain panicked: {e}"))
        })??;
        summaries.extend(per_partition);
    }
    // Drop summaries for output slots that produced no data. The coordinator
    // uses zero-content entries as sentinels so `execute(N)` streams don't
    // stall on an unfilled oneshot; those must not become PartitionLocations
    // the scheduler tries to fetch.
    summaries.retain(|s| s.num_bytes > 0);
    Ok(summaries)
}

/// Convert the writer's metadata batches (one per output partition, each
/// with a single row) back into `ShuffleWritePartition` summaries.
fn metadata_batches_to_summaries(
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
    is_sort_shuffle: bool,
) -> Result<Vec<ShuffleWritePartition>> {
    let stats_fields = PartitionStats::default().arrow_struct_fields();
    let num_rows_idx = stats_fields
        .iter()
        .position(|f| f.name() == "num_rows")
        .expect("num_rows field present in PartitionStats");
    let num_batches_idx = stats_fields
        .iter()
        .position(|f| f.name() == "num_batches")
        .expect("num_batches field present in PartitionStats");
    let num_bytes_idx = stats_fields
        .iter()
        .position(|f| f.name() == "num_bytes")
        .expect("num_bytes field present in PartitionStats");

    let mut out = Vec::new();
    for batch in batches {
        let partition_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata batch: partition column not UInt32".into(),
                )
            })?;
        let _path_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata batch: path column not Utf8".into(),
                )
            })?;
        let file_id_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata batch: file_id column not UInt64".into(),
                )
            })?;
        let stats_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata batch: stats column not Struct".into(),
                )
            })?;
        let num_rows_arr = stats_col
            .column(num_rows_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata stats.num_rows not UInt64".into(),
                )
            })?;
        let num_batches_arr = stats_col
            .column(num_batches_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata stats.num_batches not UInt64".into(),
                )
            })?;
        let num_bytes_arr = stats_col
            .column(num_bytes_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle metadata stats.num_bytes not UInt64".into(),
                )
            })?;

        for row in 0..batch.num_rows() {
            let file_id = if file_id_col.is_null(row) {
                None
            } else {
                Some(file_id_col.value(row))
            };
            out.push(ShuffleWritePartition {
                partition_id: partition_col.value(row) as u64,
                num_batches: num_batches_arr.value(row),
                num_rows: num_rows_arr.value(row),
                num_bytes: num_bytes_arr.value(row),
                file_id,
                is_sort_shuffle,
            });
        }
    }
    Ok(out)
}

// TODO: port these tests to scheduler/src/state/task_builder.rs (they used
// to cover the executor-side restrict function that has moved scheduler-side).
