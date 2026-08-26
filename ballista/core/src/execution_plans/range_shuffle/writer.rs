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

//! Passthrough shuffle writer whose output can be seeked into.

use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream, Statistics, StatisticsArgs,
    statistics::ChildStats,
};
use futures::TryStreamExt;
use log::debug;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use crate::JobId;
use crate::error::BallistaError;
use crate::execution_plans::ObservedWindowState;
use crate::execution_plans::create_shuffle_path;
use crate::execution_plans::shuffle_writer::{
    ShuffleWriteMetrics, WriterState, collect_window_state_against_slice, result_schema,
    run_coordinator, summaries_to_batch, walk_child_partition_mapping,
};
use crate::execution_plans::shuffle_writer_trait::ShuffleWriter;
use crate::extension::SessionConfigExt;
use crate::serde::protobuf::ShuffleWritePartition;

use super::index::{KeyCollector, index_path, index_schema, write_index_file};
use super::ipc_file::write_stream_to_ipc_file;

/// A passthrough shuffle writer that emits Arrow IPC **file** format.
///
/// Identical to [`ShuffleWriterExec`] in shape — one file per output
/// partition, K concurrent drains, the same K-summary contract to the
/// framework — and differs only in the framing of what it writes. The stream
/// format that [`ShuffleWriterExec`] emits has no index, so a consumer wanting
/// one value range out of a producer file has to read from the head; the file
/// format's footer lists every record batch's byte range, which is what makes
/// a seek possible.
///
/// # Why a separate writer rather than a mode on the existing one
///
/// The two formats are not interchangeable on the read side: an IPC stream
/// decoder rejects a file (the leading magic is not a continuation marker) and
/// vice versa. A flag on [`ShuffleWriterExec`] would put every existing reader
/// — local, Flight `do_get`, and the raw block transport — one config change
/// away from failing to decode what it is handed. As a distinct operator, the
/// existing shuffle keeps its format untouched and range shuffle is a new file
/// type that only the readers taught to recognise it will open.
///
/// This is planted only where the consumer is a
/// [`RangeShuffleReaderExec`](crate::execution_plans::RangeShuffleReaderExec),
/// which is the same condition the reader is chosen under: the stage's child
/// declares an output ordering.
///
/// # Message layout
///
/// Each drain reads back its file's footer, so it knows the byte range of
/// every record batch and every dictionary. That layout is what the sidecar
/// index is written from: each block is paired with the first key the
/// `KeyCollector` saw in it, giving a consumer the two halves it needs to turn
/// a value range into byte ranges without opening the data file.
///
/// [`ShuffleWriterExec`]: crate::execution_plans::ShuffleWriterExec
#[derive(Debug)]
pub struct RangeShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: JobId,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Ordering the child declares, which the index is keyed on. Captured at
    /// construction so the write path and the index agree on the sort key by
    /// construction rather than by both consulting the plan separately.
    ordering: LexOrdering,
    /// Path to write output streams to
    work_dir: String,
    /// Task id used as `file_id` in shuffle paths so files from different
    /// tasks (including retries) don't collide. Stamped by the executor's
    /// `create_query_stage_exec`.
    task_id: usize,
    /// Global partition ids this task's restricted plan covers, in slice
    /// order. Position `i` in the child plan corresponds to
    /// `global_output_partition_ids[i]` globally.
    global_output_partition_ids: Vec<usize>,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
    /// Shared coordinator handoff state. Clones share the same Arc, so a
    /// clone produced by `with_new_children` participates in the same
    /// coordinator.
    state: Arc<Mutex<WriterState>>,
}

impl Clone for RangeShuffleWriterExec {
    fn clone(&self) -> Self {
        Self {
            job_id: self.job_id.clone(),
            stage_id: self.stage_id,
            plan: self.plan.clone(),
            ordering: self.ordering.clone(),
            work_dir: self.work_dir.clone(),
            task_id: self.task_id,
            global_output_partition_ids: self.global_output_partition_ids.clone(),
            metrics: self.metrics.clone(),
            properties: self.properties.clone(),
            state: self.state.clone(),
        }
    }
}

impl std::fmt::Display for RangeShuffleWriterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let printable_plan = DisplayableExecutionPlan::with_metrics(self.plan.as_ref())
            .set_show_statistics(true)
            .indent(false);
        write!(
            f,
            "RangeShuffleWriterExec: job={} stage={} work_dir={} plan: \n {}",
            self.job_id, self.stage_id, self.work_dir, printable_plan
        )
    }
}

impl RangeShuffleWriterExec {
    /// Create a new range shuffle writer. `task_id` defaults to 0; the
    /// executor stamps the real value at `create_query_stage_exec` time.
    pub fn try_new(
        job_id: JobId,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
    ) -> Result<Self> {
        // The index is keyed on the child's declared ordering, so a child that
        // declares none has nothing to index and this writer is the wrong one.
        // The adapter only plants it where an ordering exists, so reaching here
        // without one is a planting bug rather than a user error.
        let ordering = plan.output_ordering().cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "RangeShuffleWriterExec requires an ordered input, but `{}` \
                 declares none",
                plan.name()
            ))
        })?;

        // This writer never repartitions, so its output partitioning is
        // exactly its input plan's.
        let partitioning = plan.properties().output_partitioning().clone();
        let output_partition_count = partitioning.partition_count();
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(plan.schema()),
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Ok(Self {
            job_id,
            stage_id,
            plan,
            ordering,
            work_dir,
            task_id: 0,
            global_output_partition_ids: (0..output_partition_count).collect(),
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            state: Arc::new(Mutex::new(WriterState {
                initialized: false,
                handoffs: (0..output_partition_count).map(|_| None).collect(),
            })),
        })
    }

    /// Bind this writer to a specific task_id, so shuffle files from
    /// different tasks in the same stage don't collide on file_id.
    pub fn with_task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    /// Task id (append-order slot within the stage) this writer is bound to.
    pub fn task_id(&self) -> usize {
        self.task_id
    }

    /// Bind this writer to the task's assigned global partition slice.
    pub fn with_global_output_partition_ids(
        mut self,
        global_output_partition_ids: Vec<usize>,
    ) -> Self {
        self.global_output_partition_ids = global_output_partition_ids;
        self
    }

    /// Global partition slice this writer instance is bound to.
    pub fn global_output_partition_ids(&self) -> &[usize] {
        &self.global_output_partition_ids
    }

    /// Drain every window-state collector in this stage, translating each
    /// capture's task-local partition index to its global one.
    ///
    /// Shares `ShuffleWriterExec::collect_window_state`'s body: this writer
    /// preserves its input partitioning too, so a window can sit under it and
    /// its captures must reach the downstream prefix merge. Silently returning
    /// none would make that merge arithmetically wrong with nothing later to
    /// catch it.
    pub fn collect_window_state(&self) -> Result<Vec<(usize, ObservedWindowState)>> {
        collect_window_state_against_slice(
            &self.plan,
            &self.global_output_partition_ids,
            "RangeShuffleWriterExec",
        )
    }

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Work directory shuffle files are written under.
    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }

    /// Executes the shuffle write for this task, draining all K output
    /// partitions concurrently.
    ///
    /// Returns `(handoff_idx, summary)` pairs, where `summary.partition_id` is
    /// the **global** output partition id downstream will address.
    ///
    /// All K must drain concurrently rather than one at a time:
    /// `OrderedRangeRepartitionExec` below pushes to all K senders from shared
    /// scatter tasks, so draining one to EOF first fills the undrained
    /// channels and deadlocks the scatter side.
    pub fn execute_shuffle_write(
        self,
        context: Arc<TaskContext>,
    ) -> impl Future<Output = Result<Vec<(usize, ShuffleWritePartition)>>> {
        let task_id = self.task_id;
        let plan = self.plan.clone();
        let partition_map =
            walk_child_partition_mapping(&plan, &self.global_output_partition_ids);
        let metrics = self.metrics.clone();

        async move {
            let now = Instant::now();
            let config = context.session_config().ballista_config();
            let compression_type = config.shuffle_compression_codec()?;
            let channel_capacity = config.shuffle_writer_channel_capacity();

            let num_partitions =
                plan.properties().output_partitioning().partition_count();
            // One schema for every partition this task writes: same ordering,
            // same data schema, so building it per drain would just repeat the
            // same type resolution.
            let index_schema = index_schema(&self.ordering, plan.schema().as_ref())
                .map_err(BallistaError::into_datafusion)?;
            let mut handles = JoinSet::new();
            for local_input_partition in 0..num_partitions {
                let write_metrics =
                    ShuffleWriteMetrics::new(local_input_partition, &metrics);
                let global_partition =
                    partition_map.resolve(local_input_partition) as usize;
                let path = create_shuffle_path(
                    &self.work_dir,
                    &self.job_id,
                    self.stage_id,
                    global_partition,
                    Some(task_id as u64),
                    false,
                )?;

                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                debug!("Writing range shuffle results to {path:?}");

                let stream = plan.execute(local_input_partition, context.clone())?;
                let index_schema = index_schema.clone();
                let ordering = self.ordering.clone();
                handles.spawn(async move {
                    // The keys are collected from the batches on their way to
                    // the writer, so a key and the block it describes come from
                    // the same batch rather than from two passes that could
                    // disagree.
                    let mut keyed = KeyCollector::new(stream, ordering);
                    let (stats, layout) = write_stream_to_ipc_file(
                        &mut keyed,
                        path.as_path(),
                        &write_metrics.write_time,
                        channel_capacity,
                        compression_type,
                    )
                    .await
                    .map_err(BallistaError::into_datafusion)?;
                    let keys = keyed.into_keys();

                    write_index_file(
                        index_path(path.as_path()).as_path(),
                        index_schema,
                        &layout,
                        &keys,
                    )
                    .map_err(BallistaError::into_datafusion)?;

                    let rows = stats.num_rows.unwrap_or(0) as usize;
                    write_metrics.input_rows.add(rows);
                    write_metrics.output_rows.add(rows);
                    debug!(
                        "range shuffle partition {global_partition} indexed {} record \
                         blocks and {} dictionary blocks",
                        layout.record_batches.len(),
                        layout.dictionaries.len(),
                    );
                    Ok::<_, DataFusionError>((local_input_partition, stats))
                });
            }

            let mut results = Vec::with_capacity(num_partitions);
            while let Some(joined) = handles.join_next().await {
                let (local_input_partition, stats) = joined.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "range shuffle-write drain task panicked: {e}"
                    ))
                })??;
                results.push((
                    local_input_partition,
                    ShuffleWritePartition {
                        partition_id: partition_map.resolve(local_input_partition),
                        num_batches: stats.num_batches.unwrap_or(0),
                        num_rows: stats.num_rows.unwrap_or(0),
                        num_bytes: stats.num_bytes.unwrap_or(0),
                        file_id: Some(task_id as u64),
                        is_sort_shuffle: false,
                    },
                ));
            }
            debug!(
                "range shuffle task_id {} drained {} partitions in {}s",
                task_id,
                num_partitions,
                now.elapsed().as_secs()
            );
            Ok(results)
        }
    }
}

impl DisplayAs for RangeShuffleWriterExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let partitioning = self.properties().output_partitioning();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RangeShuffleWriterExec: partitioning: {partitioning}, format: arrow ipc file"
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={partitioning}")
            }
        }
    }
}

impl ExecutionPlan for RangeShuffleWriterExec {
    fn name(&self) -> &str {
        "RangeShuffleWriterExec"
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    /// Owns no expressions — this writer preserves its input partitioning, so
    /// any partitioning expressions belong to the child plan.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [_] = children.as_slice() else {
            return Err(DataFusionError::Plan(
                "Ballista RangeShuffleWriterExec expects single child".to_owned(),
            ));
        };
        let input = children.pop().expect("single child checked above");
        Ok(Arc::new(
            RangeShuffleWriterExec::try_new(
                self.job_id.clone(),
                self.stage_id,
                input,
                self.work_dir.clone(),
            )?
            .with_task_id(self.task_id)
            .with_global_output_partition_ids(self.global_output_partition_ids.clone()),
        ))
    }

    /// Return the stream for output partition `partition`.
    ///
    /// The first call initializes K oneshot channels and spawns one
    /// coordinator that drives every output partition's write, sending each
    /// summary to its matching partition's oneshot. Callers should spawn all K
    /// `execute(N, ctx)` calls concurrently so the drains don't serialize.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = result_schema();

        let mut state = self.state.lock().map_err(|_| {
            DataFusionError::Internal(
                "RangeShuffleWriterExec state mutex poisoned".to_owned(),
            )
        })?;

        if !state.initialized {
            state.initialized = true;
            let k = state.handoffs.len();
            let mut senders: Vec<oneshot::Sender<Result<Vec<ShuffleWritePartition>>>> =
                Vec::with_capacity(k);
            for slot in state.handoffs.iter_mut() {
                let (tx, rx) = oneshot::channel();
                senders.push(tx);
                *slot = Some(rx);
            }
            let writer = self.clone();
            let ctx = context.clone();
            tokio::spawn(async move {
                run_coordinator(writer.execute_shuffle_write(ctx), senders).await;
            });
        }

        let rx = state
            .handoffs
            .get_mut(partition)
            .and_then(Option::take)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "RangeShuffleWriterExec: execute({partition}) called twice or out of range (have {} partitions)",
                    state.handoffs.len()
                ))
            })?;
        drop(state);

        let work_dir = self.work_dir.clone();
        let job_id = self.job_id.clone();
        let stage_id = self.stage_id;
        let schema_captured = schema.clone();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                let summaries = rx.await.map_err(|_| {
                    DataFusionError::Internal(
                        "RangeShuffleWriterExec coordinator dropped without sending"
                            .to_owned(),
                    )
                })??;
                summaries_to_batch(
                    summaries,
                    schema_captured,
                    &work_dir,
                    &job_id,
                    stage_id,
                )
            })
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
}

impl ShuffleWriter for RangeShuffleWriterExec {
    fn job_id(&self) -> &JobId {
        &self.job_id
    }

    fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Always `None`: this writer preserves its input partitioning.
    fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        None
    }

    fn input_partition_count(&self) -> usize {
        self.plan
            .properties()
            .output_partitioning()
            .partition_count()
    }

    fn clone_box(&self) -> Arc<dyn ShuffleWriter> {
        Arc::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::super::index::{
        SORT_OPTIONS_METADATA, byte_len_column, byte_offset_column, is_dict_column,
        num_rows_column, read_index_file,
    };
    use super::super::ipc_file::read_file_layout;
    use super::*;
    use crate::execution_plans::shuffle_reader::fetch_partition_local;
    use crate::serde::scheduler::{
        ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
        PartitionId, PartitionLocation, PartitionStats,
    };
    use crate::utils::collect_stream;
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    fn test_batch(keys: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let payload: Vec<String> = keys.iter().map(|k| format!("row-{k}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys.to_vec())),
                Arc::new(StringArray::from(payload)),
            ],
        )
        .unwrap()
    }

    fn key_ordering() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
            "k", 0,
        )))])
        .unwrap()
    }

    /// A two-partition input, each partition holding two batches, sorted on
    /// the key — this writer indexes on its child's declared ordering, so an
    /// unordered child is refused at construction.
    fn input_plan() -> Arc<dyn ExecutionPlan> {
        let partitions = vec![
            vec![test_batch(&[1, 2]), test_batch(&[3, 4])],
            vec![test_batch(&[5, 6]), test_batch(&[7, 8])],
        ];
        let schema = partitions[0][0].schema();
        let source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, schema, None).unwrap(),
        )));
        Arc::new(SortExec::new(key_ordering(), source).with_preserve_partitioning(true))
    }

    fn location(job: &str, stage_id: usize, partition: usize) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: partition,
            partition_id: PartitionId {
                job_id: job.into(),
                stage_id,
                partition_id: partition,
            },
            executor_meta: ExecutorMetadata {
                id: "test-executor".to_string(),
                host: "127.0.0.1".to_string(),
                port: 0,
                grpc_port: 0,
                specification: ExecutorSpecification::default(),
                os_info: ExecutorOperatingSystemSpecification::default(),
            },
            partition_stats: PartitionStats::default(),
            file_id: Some(0),
            is_sort_shuffle: false,
        }
    }

    /// Drive every output partition concurrently, the way the executor does —
    /// the coordinator only runs once every oneshot receiver has been taken.
    async fn drive(writer: Arc<RangeShuffleWriterExec>, ctx: Arc<TaskContext>) {
        let k = writer.properties().output_partitioning().partition_count();
        let mut handles = Vec::with_capacity(k);
        for n in 0..k {
            let writer = writer.clone();
            let ctx = ctx.clone();
            handles.push(tokio::spawn(async move {
                let mut stream = writer.execute(n, ctx).unwrap();
                collect_stream(&mut stream).await.unwrap();
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
    }

    /// The writer's output has to come back through the reader that will
    /// actually open it in production, not through a decoder chosen by the
    /// test. This is what catches a writer and reader disagreeing on format.
    #[tokio::test]
    async fn round_trips_through_the_local_reader() {
        let work_dir = TempDir::new().unwrap();
        let work_dir_path = work_dir.path().to_str().unwrap().to_owned();
        let job = "job-range-round-trip";
        let stage_id = 1;

        let writer = Arc::new(
            RangeShuffleWriterExec::try_new(
                job.into(),
                stage_id,
                input_plan(),
                work_dir_path.clone(),
            )
            .unwrap(),
        );
        drive(writer, SessionContext::new().task_ctx()).await;

        // Partition k's file holds exactly the batches of input partition k,
        // in order — this writer passes its input partitioning through.
        for (partition, keys) in [(0, vec![1, 2, 3, 4]), (1, vec![5, 6, 7, 8])] {
            let loc = location(job, stage_id, partition);
            let mut stream = fetch_partition_local(&work_dir_path, &loc).unwrap();
            let batches = collect_stream(&mut stream).await.unwrap();

            let read_keys: Vec<i32> = batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(read_keys, keys, "partition {partition} round trip");
        }
    }

    /// The index is what a consumer reads instead of the data file, so it has
    /// to describe that file: a row per message, keys that match the batches'
    /// first rows, and byte ranges that decode.
    #[tokio::test]
    async fn writes_an_index_describing_the_data_file() {
        let work_dir = TempDir::new().unwrap();
        let work_dir_path = work_dir.path().to_str().unwrap().to_owned();
        let job = "job-range-index";
        let stage_id = 5;

        let writer = Arc::new(
            RangeShuffleWriterExec::try_new(
                job.into(),
                stage_id,
                input_plan(),
                work_dir_path.clone(),
            )
            .unwrap(),
        );
        drive(writer, SessionContext::new().task_ctx()).await;

        // Input partition 0 holds keys 1..4, which the sort coalesces into a
        // single output batch, so the index has one row keyed on its first row.
        let data =
            create_shuffle_path(&work_dir_path, &job.into(), stage_id, 0, Some(0), false)
                .unwrap();
        let index = index_path(&data);
        assert!(index.exists(), "index must sit beside the data file");

        let batch = read_index_file(&index).unwrap();
        assert_eq!(batch.num_rows(), 1, "one row per record batch written");

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("key column carries the sort expression's own type");
        assert_eq!(keys.values().to_vec(), vec![1], "each batch's first key");

        let is_dict = is_dict_column(&batch).unwrap();
        assert!(
            (0..is_dict.len()).all(|row| !is_dict.value(row)),
            "no dictionary columns in this data",
        );

        let num_rows = num_rows_column(&batch).unwrap();
        assert_eq!(num_rows.values().to_vec(), vec![4]);

        // The offsets have to address the data file, not merely be plausible.
        let offsets = byte_offset_column(&batch).unwrap();
        let lens = byte_len_column(&batch).unwrap();
        let layout = read_file_layout(&data).unwrap();
        for (row, block) in layout.record_batches.iter().enumerate() {
            assert_eq!(offsets.value(row), block.offset, "offset row {row}");
            assert_eq!(lens.value(row), block.len, "len row {row}");
        }
    }

    /// The index names what it is indexed on, so a reader can check the file
    /// agrees with the ordering it plans to search by rather than assume it.
    #[tokio::test]
    async fn index_schema_records_the_sort_key_and_options() {
        let work_dir = TempDir::new().unwrap();
        let work_dir_path = work_dir.path().to_str().unwrap().to_owned();
        let job = "job-range-index-schema";
        let stage_id = 6;

        let writer = Arc::new(
            RangeShuffleWriterExec::try_new(
                job.into(),
                stage_id,
                input_plan(),
                work_dir_path.clone(),
            )
            .unwrap(),
        );
        drive(writer, SessionContext::new().task_ctx()).await;

        let data =
            create_shuffle_path(&work_dir_path, &job.into(), stage_id, 0, Some(0), false)
                .unwrap();
        let batch = read_index_file(&index_path(&data)).unwrap();
        let schema = batch.schema();

        assert_eq!(
            schema.field(0).name(),
            "k@0",
            "the key column is named for the expression it carries",
        );
        assert!(schema.field(0).is_nullable(), "keys are nullable");
        assert_eq!(
            schema
                .metadata()
                .get(SORT_OPTIONS_METADATA)
                .map(String::as_str),
            Some("asc nulls_first"),
        );
    }

    /// The point of this writer is the seekable format, so the file it leaves
    /// behind must actually be one — a regression here is silent until a
    /// consumer tries to seek.
    #[tokio::test]
    async fn writes_the_seekable_ipc_file_format() {
        let work_dir = TempDir::new().unwrap();
        let work_dir_path = work_dir.path().to_str().unwrap().to_owned();
        let job = "job-range-format";
        let stage_id = 3;

        let writer = Arc::new(
            RangeShuffleWriterExec::try_new(
                job.into(),
                stage_id,
                input_plan(),
                work_dir_path.clone(),
            )
            .unwrap(),
        );
        drive(writer, SessionContext::new().task_ctx()).await;

        for partition in 0..2 {
            let path = create_shuffle_path(
                &work_dir_path,
                &job.into(),
                stage_id,
                partition,
                Some(0),
                false,
            )
            .unwrap();
            assert!(
                super::super::is_ipc_file(&path),
                "partition {partition} must be an Arrow IPC file, not a stream",
            );
        }
    }
}
