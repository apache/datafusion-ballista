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

//! Sort-based shuffle writer execution plan.
//!
//! This execution plan writes shuffle output as a single consolidated file
//! per *task*, along with an index file mapping partition IDs to byte
//! offsets. A task owning several local input partitions buckets them
//! concurrently and then emits one file holding every bucket, laid out
//! partition-major so each output partition stays one contiguous range.

use datafusion::arrow::array::Array;
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use super::super::shuffle_writer::{handoff_stream, take_handoff};
use super::super::shuffle_writer_trait::ShuffleWriter;
use super::buffer::BufferedBatches;
use super::config::SortShuffleConfig;
use super::index::ShuffleIndex;
use super::partition_id::{
    compute_partition_indices_from_ids, drop_partition_id, reset_buckets,
    schema_without_partition_id,
};
use super::spill::SpillManager;
use crate::JobId;
use crate::extension::SessionConfigExt;
use crate::serde::protobuf::ShuffleWritePartition;

use crate::utils::create_write_options;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics, StatisticsArgs, apply_expression_roots,
    displayable, statistics::ChildStats,
};
use futures::StreamExt;
use log::{debug, warn};

/// Result of finalizing shuffle output: (data_path, index_path, partition_write_stats)
/// where partition_write_stats is (partition_id, num_batches, num_rows, num_bytes)
type FinalizeResult = (PathBuf, PathBuf, Vec<(usize, u64, u64, u64)>);

/// One local input partition's finished contribution to its task's shuffle
/// file: its in-memory rows already encoded to IPC bytes, one buffer per
/// output partition, plus the spill files it wrote along the way.
///
/// Encoding happens on the per-input task, so the expensive part — the
/// interleave, IPC framing and compression — stays parallel across inputs.
/// The coordinator only concatenates the finished buffers, which keeps the
/// serial tail down to byte copies.
struct InputPartitionOutput {
    /// IPC bytes for each output partition's in-memory rows. Empty for a
    /// partition this input contributed no buffered rows to.
    encoded: Vec<Vec<u8>>,
    /// Counts for the rows in `encoded`, one entry per output partition.
    in_memory_stats: Vec<EncodedStats>,
    spill_manager: SpillManager,
    schema: SchemaRef,
    /// Kept alive so the pool keeps accounting for this input until the
    /// consolidated write has consumed its buffers.
    _reservation: MemoryReservation,
    // collect null counts for better planning
    null_counts: Vec<u64>,
}

/// What one output partition's encoded rows amount to.
#[derive(Clone, Copy, Default)]
struct EncodedStats {
    num_batches: u64,
    num_rows: u64,
    num_bytes: u64,
}

/// One input partition's rows, encoded into one IPC buffer per output
/// partition, with the counts for each.
struct EncodedPartitions {
    encoded: Vec<Vec<u8>>,
    stats: Vec<EncodedStats>,
}

/// Encode one input partition's buffered rows into IPC bytes, one buffer per
/// output partition, and report per-partition row/batch/byte counts.
///
/// This is the work that used to happen inside each input's own file write.
/// Doing it here keeps it on the per-input task — parallel across inputs —
/// and leaves the coordinator with nothing but concatenation. The raw
/// batches are dropped as soon as the buffers are built, so peak memory
/// trades uncompressed Arrow rows for smaller encoded bytes rather than
/// holding both.
fn encode_buffered_partitions(
    buffered: &mut BufferedBatches,
    schema: &SchemaRef,
    opts: &datafusion::arrow::ipc::writer::IpcWriteOptions,
) -> Result<EncodedPartitions> {
    let num_partitions = buffered.num_partitions();
    let mut encoded = vec![Vec::new(); num_partitions];
    let mut stats = vec![EncodedStats::default(); num_partitions];

    // One writer open at a time: each holds its own compression context.
    buffered.drain(|partition, batches| {
        let mut writer =
            StreamWriter::try_new_with_options(Vec::new(), schema, opts.clone())?;
        let stat = &mut stats[partition];
        for batch in batches {
            let batch = batch?;
            stat.num_rows += batch.num_rows() as u64;
            stat.num_bytes += batch.get_array_memory_size() as u64;
            stat.num_batches += 1;
            writer.write(&batch)?;
        }
        encoded[partition] = writer.into_inner()?;
        Ok(())
    })?;

    Ok(EncodedPartitions { encoded, stats })
}

/// Coordinator handoff state — same shape as `ShuffleWriterExec`. Each output
/// hash partition (0..K) gets one `oneshot::Receiver`; the first `execute(N)`
/// call spawns the coordinator, subsequent calls take their receiver and
/// await the summaries for their output partition.
struct WriterState {
    initialized: bool,
    handoffs: Vec<Option<oneshot::Receiver<Result<Vec<ShuffleWritePartition>>>>>,
    column_null_counts: Vec<u64>,
}

impl Debug for WriterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterState")
            .field("initialized", &self.initialized)
            .field("handoffs", &self.handoffs.len())
            .finish()
    }
}

/// How a [`SortShuffleWriterExec`] assigns each row to an output partition.
///
/// The partition count is part of the routing, so anything that rebuilds a
/// writer around a new input should carry the whole value over.
#[derive(Debug, Clone, PartialEq)]
pub enum ShuffleRouting {
    /// Hash the key expressions with DataFusion's repartition hash, as
    /// `RepartitionExec` does, and take the remainder modulo `partition_count`.
    Hash {
        /// Expressions evaluated against the input to produce the hash keys.
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        /// Number of output partitions.
        partition_count: usize,
    },
    /// Send each row to the partition named by an integer input column, which
    /// the writer drops. A null, negative or out-of-range id fails the task.
    PartitionId {
        /// Index of the integer partition-id column in the input schema.
        column: usize,
        /// Number of output partitions.
        partition_count: usize,
    },
}

impl TryFrom<Partitioning> for ShuffleRouting {
    type Error = DataFusionError;

    /// Only hash partitioning is a routing the writer can apply on its own.
    fn try_from(partitioning: Partitioning) -> Result<Self> {
        match partitioning {
            Partitioning::Hash(exprs, partition_count) => Ok(Self::Hash {
                exprs,
                partition_count,
            }),
            other => Err(DataFusionError::Plan(format!(
                "SortShuffleWriterExec only supports Hash partitioning, got: {other:?}"
            ))),
        }
    }
}

impl ShuffleRouting {
    /// Number of output partitions rows are routed across.
    pub fn partition_count(&self) -> usize {
        match self {
            Self::Hash {
                partition_count, ..
            }
            | Self::PartitionId {
                partition_count, ..
            } => *partition_count,
        }
    }

    /// The schema of what a writer with this routing writes for `input`: `input`
    /// itself, less the id column of a partition-id shuffle.
    pub fn output_schema(&self, input: &SchemaRef) -> Result<SchemaRef> {
        match self {
            Self::Hash { .. } => Ok(Arc::clone(input)),
            Self::PartitionId { column, .. } => {
                schema_without_partition_id(input, *column)
            }
        }
    }

    /// The partitioning of the data this routing produces. Whatever function
    /// produced the ids, a partition-id shuffle is not known to be
    /// hash-partitioned, only to have `partition_count` partitions.
    fn output_partitioning(&self) -> Partitioning {
        match self {
            Self::Hash {
                exprs,
                partition_count,
            } => Partitioning::Hash(exprs.clone(), *partition_count),
            Self::PartitionId {
                partition_count, ..
            } => Partitioning::UnknownPartitioning(*partition_count),
        }
    }
}

/// Sort-based shuffle writer that produces a single consolidated output file
/// per input partition with an index file for partition offsets.
///
/// Under the multi-partition-task model each task's restricted plan carries
/// input partitions 0..N_local, and different tasks in the same stage share
/// those local indices. To keep shuffle files unique across tasks the writer
/// stamps output paths with the task's `global_output_partition_ids` (bound
/// via [`Self::with_global_output_partition_ids`]) — position `i` of the
/// local plan maps to global id `global_output_partition_ids[i]`, which is
/// what gets embedded in the file path.
///
/// # Threading
///
/// One Ballista task holds one `SortShuffleWriterExec`. The first
/// `execute(k)` call from the executor initializes K oneshot handoffs (one
/// per output partition) and spawns `run_coordinator`, which spawns M
/// concurrent tokio tasks — one per local input partition in the task's
/// slice. Each input writer pulls `child.execute(i)`, buckets rows into K
/// sub-slices as its [`ShuffleRouting`] says (by hash, or by partition id),
/// sorts and spills, and produces one indexed data file.
/// The coordinator collects M×K per-bucket summaries and re-buckets them
/// by output partition so each of the K oneshots receives the M summaries
/// for its output. Each `execute(k)` returns a stream that awaits its
/// oneshot and emits a single metadata batch pointing into the M files.
///
/// M concurrent per-input producers is the same fan-out shape as
/// DataFusion's `RepartitionExec` (see `pull_from_input` in
/// `datafusion/physical-plan/src/repartition/mod.rs`): N producers, each
/// running `plan.execute(i)`, fanning to K sinks. Here the K sinks are
/// bucketed sub-slices in one indexed file per input instead of K mpsc
/// senders per producer.
///
/// Data flows bottom → top (child produces, executor consumes), matching
/// DataFusion's diagram convention for `RepartitionExec`. Arrows point up
/// with the data. **M and K are independent**: M = the task's slice of the
/// child's partitions, K = the hash-partition target from the planner.
/// Example: M=2, K=3.
///
/// ```text
///                     K=3 output partitions (pulled by executor)
///
///        writer.execute(0)  writer.execute(1)  writer.execute(2)
///                ▲                 ▲                 ▲
///                │                 │                 │
///           ┌──────────┐      ┌──────────┐      ┌──────────┐
///           │ oneshot  │      │ oneshot  │      │ oneshot  │
///           │ (out=0)  │      │ (out=1)  │      │ (out=2)  │
///           └──────────┘      └──────────┘      └──────────┘
///                ▲                 ▲                 ▲
///                │       each oneshot carries one summary
///                │       naming bucket k's range in the
///                │       task's single data file
///                └─────────────────┼─────────────────┘
///                                  │  write ONE data file
///                                  │  + ONE index, then emit
///                                  │  K summaries
///                         ┌─────────────────┐
///                         │ run_coordinator │
///                         └─────────────────┘
///                                  ▲
///                    ┌─────────────┴─────────────┐
///                    │ buffered rows + spills    │ buffered rows + spills
///                    │                           │
///           ┌────────────────┐           ┌────────────────┐
///           │  input bucketer│           │  input bucketer│
///           │      (0)       │           │      (1)       │
///           │ sort by hash   │           │ sort by hash   │
///           │ bucket, spill  │           │ bucket, spill  │
///           │ if needed      │           │ if needed      │
///           └────────────────┘           └────────────────┘
///                    ▲                           ▲
///                    │                           │
///            child.execute(0)            child.execute(1)
///
///                     M=2 input partitions
///                  (task's slice of child plan)
/// ```
///
/// File layout — one pair per *task*, K buckets concatenated in one file,
/// each bucket holding that partition's rows from every input in turn:
///
/// ```text
///           data.arrow:        [ bucket 0 ][ bucket 1 ][ bucket 2 ]
///                               \_ in0,in1 _/
///           data.arrow.index:  { 0 → off0, 1 → off1, 2 → off2 }
/// ```
///
/// Laying the file out partition-major keeps bucket k contiguous even though
/// M inputs contributed to it, so a reader for output partition k opens one
/// file per task, seeks to `index[k]`, and reads a single range.
#[derive(Debug)]
pub struct SortShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: JobId,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    work_dir: String,
    /// How rows are assigned to output partitions
    routing: ShuffleRouting,
    /// Sort shuffle configuration
    config: SortShuffleConfig,
    /// Task id (the task's append-order slot in
    /// `RunningStage.task_infos`). Stamped by the executor's
    /// `create_query_stage_exec`; defaults to 0 for plans that arrive from
    /// `try_new` directly (proto decode before executor stamping, or
    /// tests). Kept for symmetry with `ShuffleWriterExec`.
    task_id: usize,
    /// Global input-partition ids this task's restricted plan covers, in
    /// slice order. Position `i` of the local plan maps to
    /// `global_output_partition_ids[i]` globally — used in file paths and
    /// reported summaries so files from different tasks in the same stage
    /// don't collide (task slices are disjoint by construction).
    global_output_partition_ids: Vec<usize>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: Arc<PlanProperties>,
    /// Shared coordinator handoff state. Clones share the same Arc so a
    /// clone produced by `with_new_children` (or by the coordinator when
    /// it spawns per-input-partition write tasks) participates in the
    /// same coordinator.
    state: Arc<Mutex<WriterState>>,
}

impl Clone for SortShuffleWriterExec {
    fn clone(&self) -> Self {
        Self {
            job_id: self.job_id.clone(),
            stage_id: self.stage_id,
            plan: self.plan.clone(),
            work_dir: self.work_dir.clone(),
            routing: self.routing.clone(),
            config: self.config.clone(),
            task_id: self.task_id,
            global_output_partition_ids: self.global_output_partition_ids.clone(),
            metrics: self.metrics.clone(),
            properties: self.properties.clone(),
            state: self.state.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct SortShuffleWriteMetrics {
    /// Time spent writing batches to the output file
    write_time: metrics::Time,
    /// Time spent partitioning input batches
    repart_time: metrics::Time,
    /// Time spent spilling to disk
    spill_time: metrics::Time,
    /// Number of input rows
    input_rows: metrics::Count,
    /// Number of output rows
    output_rows: metrics::Count,
    /// Number of times the writer flushed buffered partitions to disk, whether
    /// because the runtime `MemoryPool` rejected a reservation grow or because
    /// the per-task buffer budget was reached. One event typically writes one
    /// batch per non-empty output partition, so this is strictly less than or
    /// equal to the number of batches written into spill files.
    spill_count: metrics::Count,
    /// Bytes spilled to disk
    spill_bytes: metrics::Count,
}

/// What made the writer flush its buffered partitions to disk.
///
/// The writer has two independent spill triggers and they can fire on the same
/// input batch, so the cause is recorded per event rather than assumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpillTrigger {
    /// The runtime `MemoryPool` refused to grow this writer's reservation:
    /// the executor as a whole is out of budget.
    MemoryPool,
    /// The per-task buffered-bytes budget
    /// (`ballista.shuffle.sort_based.memory_limit_per_task_bytes`) was reached.
    /// The pool still had room; this writer hit its own configured cap.
    TaskBudget,
    /// Both fired on the same batch.
    Both,
}

impl SpillTrigger {
    /// Classify a spill decision, or `None` if neither trigger fired.
    fn classify(pool_rejected: bool, budget_reached: bool) -> Option<Self> {
        match (pool_rejected, budget_reached) {
            (true, true) => Some(Self::Both),
            (true, false) => Some(Self::MemoryPool),
            (false, true) => Some(Self::TaskBudget),
            (false, false) => None,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::MemoryPool => "memory pool rejected the reservation grow",
            Self::TaskBudget => "per-task buffer budget reached",
            Self::Both => {
                "memory pool rejected the reservation grow and per-task buffer budget reached"
            }
        }
    }
}

/// Per-task tally of spill events by trigger, for the end-of-task summary.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SpillTriggerCounts {
    memory_pool: u64,
    task_budget: u64,
    both: u64,
}

impl SpillTriggerCounts {
    fn record(&mut self, trigger: SpillTrigger) {
        match trigger {
            SpillTrigger::MemoryPool => self.memory_pool += 1,
            SpillTrigger::TaskBudget => self.task_budget += 1,
            SpillTrigger::Both => self.both += 1,
        }
    }

    /// Human-readable breakdown listing only the triggers that actually fired,
    /// so the summary names the real cause instead of a generic one.
    fn summary(&self) -> String {
        let mut parts = Vec::new();
        if self.memory_pool > 0 {
            parts.push(format!("memory pool rejection x{}", self.memory_pool));
        }
        if self.task_budget > 0 {
            parts.push(format!("per-task buffer budget x{}", self.task_budget));
        }
        if self.both > 0 {
            parts.push(format!("both x{}", self.both));
        }
        if parts.is_empty() {
            "none".to_string()
        } else {
            parts.join(", ")
        }
    }
}

impl SortShuffleWriteMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            write_time: MetricBuilder::new(metrics).subset_time("write_time", partition),
            repart_time: MetricBuilder::new(metrics)
                .subset_time("repart_time", partition),
            spill_time: MetricBuilder::new(metrics).subset_time("spill_time", partition),
            input_rows: MetricBuilder::new(metrics).counter("input_rows", partition),
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            spill_count: MetricBuilder::new(metrics).counter("spill_count", partition),
            spill_bytes: MetricBuilder::new(metrics).counter("spill_bytes", partition),
        }
    }
}

impl SortShuffleWriterExec {
    /// Create a new sort-based shuffle writer that hashes rows into
    /// `shuffle_output_partitioning`, which must be [`Partitioning::Hash`]. See
    /// [`Self::try_new_with_routing`] for other routings.
    pub fn try_new(
        job_id: JobId,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        shuffle_output_partitioning: Partitioning,
        config: SortShuffleConfig,
    ) -> Result<Self> {
        Self::try_new_with_routing(
            job_id,
            stage_id,
            plan,
            work_dir,
            shuffle_output_partitioning.try_into()?,
            config,
        )
    }

    /// Create a new sort-based shuffle writer that places rows as `routing` says.
    pub fn try_new_with_routing(
        job_id: JobId,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        routing: ShuffleRouting,
        config: SortShuffleConfig,
    ) -> Result<Self> {
        // Zero would leave the partition reducer with a zero divisor and
        // `handoffs` empty.
        if routing.partition_count() == 0 {
            return Err(DataFusionError::Plan(
                "SortShuffleWriterExec requires at least one output partition"
                    .to_string(),
            ));
        }

        let schema = routing.output_schema(&plan.schema())?;

        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema),
            routing.output_partitioning(),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));

        // Default identity slice — one global id per local input partition.
        // The executor's `create_query_stage_exec` stamps the real slice via
        // `with_global_output_partition_ids` before dispatch.
        let child_partition_count =
            plan.properties().output_partitioning().partition_count();
        let default_partition_slice: Vec<usize> = (0..child_partition_count).collect();
        let output_partition_count = routing.partition_count();

        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
            routing,
            config,
            task_id: 0,
            global_output_partition_ids: default_partition_slice,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            state: Arc::new(Mutex::new(WriterState {
                initialized: false,
                handoffs: (0..output_partition_count).map(|_| None).collect(),
                column_null_counts: Vec::new(),
            })),
        })
    }

    /// Bind this writer to a specific task_id. Called by the executor
    /// after decoding the plan so writer instances from different tasks in
    /// the same stage carry their identity for logging / debugging.
    pub fn with_task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    /// Compute null counts for each column. This will be enhanced in future to add further
    /// col stats for better query planning
    pub fn column_null_counts(&self) -> Vec<u64> {
        self.state
            .lock()
            .map(|s| s.column_null_counts.clone())
            .unwrap_or_default()
    }

    /// Bind this writer to the directory it writes under. Called by the
    /// executor after decoding the plan.
    pub fn with_work_dir(mut self, work_dir: String) -> Self {
        self.work_dir = work_dir;
        self
    }

    /// Task id (append-order slot within the stage) this writer instance
    /// is bound to.
    pub fn task_id(&self) -> usize {
        self.task_id
    }

    /// Bind this writer to the task's assigned global partition slice.
    /// Position `i` of the local plan corresponds to `slice[i]` globally,
    /// which is used to name shuffle files so different tasks in the same
    /// stage don't collide on file paths.
    pub fn with_global_output_partition_ids(
        mut self,
        global_output_partition_ids: Vec<usize>,
    ) -> Self {
        self.global_output_partition_ids = global_output_partition_ids;
        self
    }

    /// Global partition ids this task's restricted plan covers.
    pub fn global_output_partition_ids(&self) -> &[usize] {
        &self.global_output_partition_ids
    }

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Get the shuffle output partitioning: `Hash` for a hash shuffle, otherwise
    /// only `UnknownPartitioning` of the partition count.
    pub fn shuffle_output_partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    /// How this writer assigns rows to output partitions.
    pub fn routing(&self) -> &ShuffleRouting {
        &self.routing
    }

    /// The id column of a partition-id shuffle, for display.
    fn partition_id_column(&self) -> Option<Column> {
        match &self.routing {
            ShuffleRouting::Hash { .. } => None,
            ShuffleRouting::PartitionId { column, .. } => Some(Column::new(
                self.plan.schema().field(*column).name(),
                *column,
            )),
        }
    }

    /// Get the sort shuffle configuration
    pub fn config(&self) -> &SortShuffleConfig {
        &self.config
    }

    /// Get the input partition count
    pub fn input_partition_count(&self) -> usize {
        self.plan
            .properties()
            .output_partitioning()
            .partition_count()
    }

    /// Bucket one input partition's rows by output partition, spilling under
    /// memory pressure, and hand the result back to the task's coordinator.
    ///
    /// This does not write the shuffle data file — the coordinator does that
    /// once for the whole task, from every input partition's output. Spill
    /// files are still per input partition, so their paths bit-pack
    /// `(task_id, input_partition)` into one u64 and never collide.
    ///
    /// `input_partition` is a **local** index into this task's restricted
    /// plan (0..N_local).
    fn execute_shuffle_write(
        self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> impl Future<Output = Result<InputPartitionOutput>> {
        let metrics = SortShuffleWriteMetrics::new(input_partition, &self.metrics);
        let config = self.config.clone();
        let plan = self.plan.clone();
        let work_dir = self.work_dir.clone();
        let job_id = self.job_id.clone();
        let stage_id = self.stage_id;
        let routing = self.routing.clone();
        let task_id = self.task_id;

        async move {
            let mut stream = plan.execute(input_partition, context.clone())?;
            let schema = routing.output_schema(&stream.schema())?;
            let ballista_config = Arc::new(context.session_config().ballista_config());
            let compression_type = ballista_config.shuffle_compression_codec()?;

            let num_output_partitions = routing.partition_count();

            let mut spill_manager = SpillManager::new(
                &work_dir,
                &job_id,
                stage_id,
                task_id,
                input_partition,
                schema.clone(),
                compression_type,
            )
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            let mut buffered = BufferedBatches::new(
                num_output_partitions,
                schema.clone(),
                config.batch_size,
            );

            let mut reservation =
                MemoryConsumer::new(format!("SortShuffleWriter[{input_partition}]"))
                    .with_can_spill(true)
                    .register(&context.runtime_env().memory_pool);

            let mut hash_buffer: Vec<u64> = Vec::new();
            let mut per_partition_rows: Vec<Vec<u32>> = Vec::new();
            // Fixed for the whole task, so the reducer is built once here
            // rather than re-derived per batch.
            let partition_reducer = StrengthReducedU64::new(num_output_partitions as u64);
            let mut spill_events: u64 = 0;
            let mut spill_triggers = SpillTriggerCounts::default();
            // `memory_limit` is a second spill trigger, independent of the
            // `MemoryPool`; 0 disables it.
            let memory_limit = config.memory_limit_per_task_bytes;
            let per_task_budget_enabled = memory_limit > 0;
            let mut null_counts = vec![0u64; schema.fields().len()];

            while let Some(result) = stream.next().await {
                let input_batch = result?;
                metrics.input_rows.add(input_batch.num_rows());

                // Compute partition assignment for every row.
                let timer = metrics.repart_time.timer();
                let input_batch = match &routing {
                    ShuffleRouting::Hash { exprs, .. } => {
                        compute_partition_indices(
                            &input_batch,
                            exprs,
                            partition_reducer,
                            &mut hash_buffer,
                            &mut per_partition_rows,
                        )?;
                        input_batch
                    }
                    ShuffleRouting::PartitionId { column, .. } => {
                        compute_partition_indices_from_ids(
                            &input_batch,
                            *column,
                            num_output_partitions,
                            &mut per_partition_rows,
                        )?;
                        drop_partition_id(input_batch, *column, &schema)?
                    }
                };
                timer.done();

                // Counted on what is written, so they line up with a reader's columns.
                for (i, col) in input_batch.columns().iter().enumerate() {
                    null_counts[i] += col.logical_null_count() as u64;
                }

                buffered.push_batch(input_batch, &per_partition_rows);
                let buffered_bytes = buffered.memory_size();

                // Mirror the buffered bytes in the pool. A refused resize
                // means the pool has not accounted for the rows just
                // buffered, so spill them and free the reservation.
                let pool_rejected_growth =
                    reservation.try_resize(buffered_bytes).is_err();
                let budget_reached =
                    per_task_budget_enabled && buffered_bytes >= memory_limit;

                if let Some(trigger) =
                    SpillTrigger::classify(pool_rejected_growth, budget_reached)
                {
                    let spill_timer = metrics.spill_time.timer();
                    let (event_batches, event_bytes) = spill_all_partitions(
                        &mut buffered,
                        &mut spill_manager,
                        &mut reservation,
                    )?;
                    spill_timer.done();

                    if event_batches > 0 {
                        spill_events += 1;
                        spill_triggers.record(trigger);
                        debug!(
                            "Sort shuffle writer for input partition {} spilled \
                             event #{} ({}): {} batches, {} bytes \
                             (cumulative: {} events, {} batches, {} bytes)",
                            input_partition,
                            spill_events,
                            trigger.label(),
                            event_batches,
                            event_bytes,
                            spill_events,
                            spill_manager.total_spilled_batches(),
                            spill_manager.total_bytes_spilled(),
                        );
                    }
                }
            }

            // Finish spill writers before the coordinator reads them back while
            // building the task's consolidated file.
            spill_manager
                .finish_writers()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            metrics.spill_count.add(spill_events as usize);
            metrics
                .spill_bytes
                .add(spill_manager.total_bytes_spilled() as usize);

            let total_spilled_batches = spill_manager.total_spilled_batches();
            let total_bytes_spilled = spill_manager.total_bytes_spilled();

            let repart_time = Duration::from_nanos(metrics.repart_time.value() as u64);
            let spill_time = Duration::from_nanos(metrics.spill_time.value() as u64);
            if total_bytes_spilled > 0 {
                warn!(
                    "Sort shuffle spill: job={} stage={} input_partition={} \
                     spilled {} bytes in {} batches ({} events); \
                     triggered by: {} (per-task buffer budget: {}); \
                     repart_time={:?} spill_time={:?}",
                    job_id,
                    stage_id,
                    input_partition,
                    total_bytes_spilled,
                    total_spilled_batches,
                    spill_events,
                    spill_triggers.summary(),
                    if per_task_budget_enabled {
                        format!("{memory_limit} bytes")
                    } else {
                        "disabled".to_string()
                    },
                    repart_time,
                    spill_time,
                );
            } else {
                debug!(
                    "Sort shuffle bucketing for input partition {} completed. \
                     repart_time={:?} spill_time={:?}, \
                     Spill events: {}, Spill batches: {}, Spill bytes: {}",
                    input_partition,
                    repart_time,
                    spill_time,
                    spill_events,
                    total_spilled_batches,
                    total_bytes_spilled
                );
            }

            // Encode here, on this input's own task, so the interleave/IPC/
            // compression cost stays parallel across the task's inputs.
            let opts = create_write_options(compression_type)?;
            let EncodedPartitions { encoded, stats } =
                encode_buffered_partitions(&mut buffered, &schema, &opts)?;

            Ok(InputPartitionOutput {
                encoded,
                in_memory_stats: stats,
                spill_manager,
                schema,
                _reservation: reservation,
                null_counts,
            })
        }
    }
}

/// Spills *all* buffered partitions: appends each partition's buffered rows
/// to that partition's spill file. After this call, `buffered.is_empty()` is
/// true and `reservation.size() == 0`.
///
/// Returns `(batches_written, bytes_written)` for this single spill event so
/// the caller can log per-event diagnostics. A return of `(0, 0)` means there
/// was nothing buffered and no event occurred.
fn spill_all_partitions(
    buffered: &mut BufferedBatches,
    spill_manager: &mut SpillManager,
    reservation: &mut MemoryReservation,
) -> Result<(u64, u64)> {
    if buffered.is_empty() {
        return Ok((0, 0));
    }
    let mut batches_written: u64 = 0;
    let mut bytes_written: u64 = 0;
    buffered.drain(|partition_id, batches| {
        for batch in batches {
            let written = spill_manager
                .spill(partition_id, &batch?)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            batches_written += 1;
            bytes_written += written;
        }
        Ok(())
    })?;
    reservation.free();
    Ok((batches_written, bytes_written))
}

/// Write the task's entire shuffle output as one data file plus one index.
///
/// The file is laid out **partition-major**: a leading schema-header stream,
/// then output partition 0's bytes contributed by every local input
/// partition, then partition 1's, and so on. Keeping each output partition a
/// single contiguous range is what lets the index stay one offset per
/// partition and lets a reader fetch a partition with one ranged read —
/// against one file per task rather than one file per input partition.
///
/// Within a partition's range the bytes are concatenated Arrow IPC streams
/// (spilled bytes copied verbatim, then in-memory rows re-encoded), which the
/// reader already crosses transparently.
///
/// Returns (data_path, index_path, partition_stats) where partition_stats is
/// a vector of (partition_id, num_batches, num_rows, num_bytes) tuples.
#[allow(clippy::too_many_arguments)]
fn write_task_consolidated(
    work_dir: &str,
    job_id: &JobId,
    stage_id: usize,
    file_id: usize,
    outputs: &mut [InputPartitionOutput],
    schema: &SchemaRef,
    compression_type: Option<CompressionType>,
) -> Result<FinalizeResult> {
    let num_partitions = outputs.first().map(|o| o.encoded.len()).unwrap_or(0);
    let mut index = ShuffleIndex::new(num_partitions);
    let mut partition_stats = Vec::with_capacity(num_partitions);

    let mut output_dir = PathBuf::from(work_dir);
    output_dir.push(job_id.as_str());
    output_dir.push(format!("{stage_id}"));
    output_dir.push(format!("{file_id}"));
    std::fs::create_dir_all(&output_dir)?;

    let data_path = output_dir.join("data.arrow");
    let index_path = output_dir.join("data.arrow.index");

    debug!("Writing consolidated shuffle output to {:?}", data_path);

    let file = File::create(&data_path)?;
    let mut output = BufWriter::new(file);

    let opts = create_write_options(compression_type)?;

    // Leading schema-header stream (schema message + EOS, no batches) so the
    // reader can recover the schema even when the requested partition is empty.
    {
        let mut header =
            StreamWriter::try_new_with_options(&mut output, schema, opts.clone())?;
        header.finish()?;
    }

    for partition_id in 0..num_partitions {
        // Flush before reading the kernel position so the BufWriter buffer
        // is empty; std::io::copy below also writes directly to the inner
        // File and would land after any pending buffered bytes otherwise.
        output.flush()?;
        let partition_start = output.get_mut().stream_position()? as i64;
        index.set_offset(partition_id, partition_start);

        let mut num_batches: u64 = 0;
        let mut num_rows: u64 = 0;
        let mut num_bytes: u64 = 0;

        for out in outputs.iter() {
            let (spill_batches, spill_rows, spill_bytes) =
                out.spill_manager.partition_stats(partition_id);

            if let Some(spill_path) = out.spill_manager.spill_path(partition_id) {
                // Same flush-then-bypass discipline as above: anything already
                // buffered has to land before the raw copy appends to the file.
                output.flush()?;
                let mut spill_file = File::open(spill_path)?;
                std::io::copy(&mut spill_file, output.get_mut())?;
            }
            num_batches += spill_batches;
            num_rows += spill_rows;
            num_bytes += spill_bytes;

            let buf = &out.encoded[partition_id];
            if !buf.is_empty() {
                output.write_all(buf)?;
            }
            let mem = out.in_memory_stats[partition_id];
            num_batches += mem.num_batches;
            num_rows += mem.num_rows;
            num_bytes += mem.num_bytes;
        }

        partition_stats.push((partition_id, num_batches, num_rows, num_bytes));
    }

    output.flush()?;
    let total = output.get_mut().stream_position()? as i64;
    index.set_total_length(total);
    index
        .write_to_file(&index_path)
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    Ok((data_path, index_path, partition_stats))
}
impl DisplayAs for SortShuffleWriterExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let partitioning = self.shuffle_output_partitioning();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SortShuffleWriterExec: partitioning={partitioning}")?;
                if let Some(column) = self.partition_id_column() {
                    write!(f, ", partition_id_column={column}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={partitioning}")?;
                if let Some(column) = self.partition_id_column() {
                    write!(f, "\npartition_id_column={column}")?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for SortShuffleWriterExec {
    fn name(&self) -> &str {
        "SortShuffleWriterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    /// A hash shuffle hashes rows itself, so the key expressions are its own. A
    /// partition-id shuffle has none: the plan below computed the column.
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        match &self.routing {
            ShuffleRouting::Hash { exprs, .. } => apply_expression_roots(exprs, f),
            ShuffleRouting::PartitionId { .. } => Ok(TreeNodeRecursion::Continue),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            let input = children.pop().ok_or_else(|| {
                DataFusionError::Plan(
                    "SortShuffleWriterExec expects single child".to_owned(),
                )
            })?;

            Ok(Arc::new(SortShuffleWriterExec::try_new_with_routing(
                self.job_id.clone(),
                self.stage_id,
                input,
                self.work_dir.clone(),
                self.routing.clone(),
                self.config.clone(),
            )?))
        } else {
            Err(DataFusionError::Plan(
                "SortShuffleWriterExec expects single child".to_owned(),
            ))
        }
    }

    /// Return the stream for output partition `partition`.
    ///
    /// The first call locks the shared state, initializes K oneshot channels
    /// (one per output partition), and spawns a coordinator that drives the
    /// per-input-partition writes for all M local input partitions of the
    /// task's restricted plan. Each input write emits K per-bucket
    /// summaries pointing at that input's on-disk file; the coordinator
    /// re-buckets them by output partition and sends each bucket's
    /// summaries to the matching oneshot. Subsequent calls take their
    /// receiver and return a stream that awaits it.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut guard = self.state.lock().map_err(|_| {
            DataFusionError::Internal(
                "SortShuffleWriterExec state mutex poisoned".to_owned(),
            )
        })?;
        let state = &mut *guard;
        let rx = take_handoff(
            "SortShuffleWriterExec",
            &mut state.initialized,
            &mut state.handoffs,
            partition,
            |senders| {
                let writer = self.clone();
                tokio::spawn(async move {
                    run_coordinator(writer, context, senders).await;
                });
            },
        )?;
        drop(guard);

        Ok(handoff_stream(
            "SortShuffleWriterExec",
            rx,
            self.work_dir.clone(),
            self.job_id.clone(),
            self.stage_id,
        ))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let mut stats = Arc::clone(&input_stats[0]);
        match &self.routing {
            ShuffleRouting::Hash { .. } => {}
            // The id column is not part of the output, so its statistics aren't.
            ShuffleRouting::PartitionId { column, .. } => {
                let columns = &mut Arc::make_mut(&mut stats).column_statistics;
                if *column < columns.len() {
                    columns.remove(*column);
                }
            }
        }
        Ok(stats)
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
}

impl ShuffleWriter for SortShuffleWriterExec {
    fn job_id(&self) -> &JobId {
        &self.job_id
    }

    fn stage_id(&self) -> usize {
        self.stage_id
    }

    fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        Some(SortShuffleWriterExec::shuffle_output_partitioning(self))
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

/// Drives per-input-partition writes for the task's restricted plan, then
/// re-buckets summaries by output partition and sends each bucket's
/// summaries to the matching oneshot. Runs the M input writes concurrently
/// (one tokio task each) so shuffle-write parallelism matches what main's
/// design achieved by having DataFusion drive `execute(0..M)` in parallel.
///
/// On failure, sends the error to every sender so no waiting stream hangs.
async fn run_coordinator(
    writer: SortShuffleWriterExec,
    ctx: Arc<TaskContext>,
    senders: Vec<oneshot::Sender<Result<Vec<ShuffleWritePartition>>>>,
) {
    let k = senders.len();
    let mut senders: Vec<Option<oneshot::Sender<Result<Vec<ShuffleWritePartition>>>>> =
        senders.into_iter().map(Some).collect();

    let num_input_partitions = writer
        .plan
        .properties()
        .output_partitioning()
        .partition_count();

    // Spawn per-input-partition write tasks concurrently — this matches the
    // parallelism main's design achieved by having DataFusion drive
    // `execute(0..M)` concurrently.
    let mut writes = JoinSet::new();
    for input_partition in 0..num_input_partitions {
        let w = writer.clone();
        let c = ctx.clone();
        writes.spawn(async move { w.execute_shuffle_write(input_partition, c).await });
    }

    let mut grouped: Vec<Vec<ShuffleWritePartition>> =
        (0..k).map(|_| Vec::new()).collect();
    let mut outputs: Vec<InputPartitionOutput> = Vec::with_capacity(num_input_partitions);
    let mut first_error: Option<DataFusionError> = None;
    while let Some(joined) = writes.join_next().await {
        match joined {
            Ok(Ok(output)) => outputs.push(output),
            Ok(Err(e)) => {
                first_error = Some(e);
                break;
            }
            Err(join_err) => {
                first_error = Some(DataFusionError::Execution(format!(
                    "write task panicked: {join_err}"
                )));
                break;
            }
        }
    }
    writes.abort_all();

    // Fold each input partition's per-column null counts into the task-level
    // total and publish it on the shared state for `execute_query_stage`.
    let mut column_null_counts: Vec<u64> = Vec::new();
    for output in outputs.iter() {
        if column_null_counts.len() < output.null_counts.len() {
            column_null_counts.resize(output.null_counts.len(), 0);
        }
        for (slot, n) in column_null_counts.iter_mut().zip(&output.null_counts) {
            *slot += n;
        }
    }
    if let Ok(mut state) = writer.state.lock() {
        state.column_null_counts = column_null_counts;
    }

    // Every input partition has finished bucketing; emit the task's single
    // data file from all of them, then describe each output partition once.
    // The file is keyed by `task_id`, which is unique within (job, stage).
    if first_error.is_none() && !outputs.is_empty() {
        let file_id = writer.task_id as u64;
        let schema = outputs[0].schema.clone();
        let compression = ctx
            .session_config()
            .ballista_config()
            .shuffle_compression_codec();

        let write_metrics = SortShuffleWriteMetrics::new(0, &writer.metrics);
        let timer = write_metrics.write_time.timer();
        let result = compression.and_then(|compression_type| {
            write_task_consolidated(
                &writer.work_dir,
                &writer.job_id,
                writer.stage_id,
                file_id as usize,
                &mut outputs,
                &schema,
                compression_type,
            )
        });
        timer.done();

        match result {
            Ok((data_path, index_path, partition_stats)) => {
                let total_rows: u64 = partition_stats.iter().map(|(_, _, r, _)| *r).sum();
                write_metrics.output_rows.add(total_rows as usize);
                debug!(
                    "Sort shuffle write for task {} completed. Output: {:?}, \
                     Index: {:?}, Inputs: {}, Rows: {}",
                    writer.task_id,
                    data_path,
                    index_path,
                    outputs.len(),
                    total_rows,
                );
                for (part_id, num_batches, num_rows, num_bytes) in partition_stats {
                    if num_rows > 0 && part_id < k {
                        grouped[part_id].push(ShuffleWritePartition {
                            partition_id: part_id as u64,
                            num_batches,
                            num_rows,
                            num_bytes,
                            file_id: Some(file_id),
                            is_sort_shuffle: true,
                        });
                    }
                }
            }
            Err(e) => {
                first_error.get_or_insert(e);
            }
        }
    }

    for output in outputs.iter_mut() {
        if let Err(e) = output.spill_manager.cleanup() {
            warn!("Sort shuffle spill cleanup failed: {e:?}");
        }
    }

    if let Some(e) = first_error {
        // Share the original error with every output handoff so classification
        // preserves FetchFailed/IO details regardless of stream completion order.
        let shared = Arc::new(e);
        for slot in senders.iter_mut() {
            if let Some(sender) = slot.take() {
                let err = DataFusionError::from(&shared);
                let _ = sender.send(Err(err));
            }
        }
        return;
    }

    for (slot, bucket) in senders.iter_mut().zip(grouped) {
        if let Some(sender) = slot.take() {
            let _ = sender.send(Ok(bucket));
        }
    }
}

impl std::fmt::Display for SortShuffleWriterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let printable_plan = displayable(self.plan.as_ref())
            .set_show_statistics(true)
            .indent(false);
        write!(
            f,
            "SortShuffleWriterExec: job={} stage={} work_dir={} partitioning={:?} plan: \n {}",
            self.job_id,
            self.stage_id,
            self.work_dir,
            self.shuffle_output_partitioning(),
            printable_plan
        )
    }
}

/// Computes `value % divisor` without a division in the hot loop, for a divisor
/// that is fixed across many values.
///
/// The output partition count is only known at runtime, so the compiler must
/// emit a real 64-bit `div` for every row. Precomputing the divisor's reduced
/// form turns that into a mask (powers of two) or a reciprocal multiply.
///
/// Ported from DataFusion's `BatchPartitioner`, where the type is private; see
/// apache/datafusion#21900 (up to 1.16x on TPC-H sf10).
#[derive(Debug, Clone, Copy)]
enum StrengthReducedU64 {
    PowerOfTwo { mask: u64 },
    Reciprocal { divisor: u64, reciprocal: u128 },
}

impl StrengthReducedU64 {
    fn new(divisor: u64) -> Self {
        debug_assert!(divisor > 0);

        if divisor.is_power_of_two() {
            Self::PowerOfTwo { mask: divisor - 1 }
        } else {
            Self::Reciprocal {
                divisor,
                // ceil(2^128 / divisor), computed without representing 2^128
                reciprocal: u128::MAX / u128::from(divisor) + 1,
            }
        }
    }

    fn divisor(self) -> u64 {
        match self {
            Self::PowerOfTwo { mask } => mask + 1,
            Self::Reciprocal { divisor, .. } => divisor,
        }
    }

    /// Pushes each row index onto the `out` entry its hash maps to.
    ///
    /// The divisor form is matched once, outside the per-row loop, so the loop
    /// body stays a mask or a multiply rather than a branch plus a `div`.
    fn partition_indices(self, hash_buffer: &[u64], out: &mut [Vec<u32>]) {
        match self {
            Self::PowerOfTwo { mask } => {
                for (row, &h) in hash_buffer.iter().enumerate() {
                    out[(h & mask) as usize].push(row as u32);
                }
            }
            Self::Reciprocal {
                divisor,
                reciprocal,
            } => {
                for (row, &h) in hash_buffer.iter().enumerate() {
                    let quotient = Self::quotient(h, reciprocal);
                    out[(h - quotient * divisor) as usize].push(row as u32);
                }
            }
        }
    }

    #[cfg(test)]
    fn remainder(self, value: u64) -> u64 {
        match self {
            Self::PowerOfTwo { mask } => value & mask,
            Self::Reciprocal {
                divisor,
                reciprocal,
            } => value - Self::quotient(value, reciprocal) * divisor,
        }
    }

    /// `value / divisor`, recovered from the precomputed reciprocal as the high
    /// 128 bits of a 192-bit product.
    #[inline]
    fn quotient(value: u64, reciprocal: u128) -> u64 {
        let reciprocal_low = reciprocal as u64;
        let reciprocal_high = (reciprocal >> 64) as u64;
        let low_product = u128::from(value) * u128::from(reciprocal_low);
        let high_product = u128::from(value) * u128::from(reciprocal_high);
        let carry = ((high_product & u128::from(u64::MAX)) + (low_product >> 64)) >> 64;

        ((high_product >> 64) + carry) as u64
    }
}

/// Computes per-row output partition assignments for hash partitioning.
///
/// Fills `out` with `reducer.divisor()` entries, where entry `p` lists the
/// row indices in `batch` that hash to partition `p`. Hash semantics are
/// byte-identical to `datafusion::physical_plan::repartition::BatchPartitioner::Hash`.
///
/// `hash_buffer` and `out` are reused across calls to amortize allocations;
/// both are cleared and resized internally. `out`'s inner `Vec`s are cleared
/// rather than dropped, so their capacity survives into the next batch and
/// the steady state allocates nothing.
fn compute_partition_indices(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    reducer: StrengthReducedU64,
    hash_buffer: &mut Vec<u64>,
    out: &mut Vec<Vec<u32>>,
) -> Result<()> {
    let arrays = evaluate_expressions_to_arrays(exprs, batch)?;
    hash_buffer.clear();
    hash_buffer.resize(batch.num_rows(), 0);
    create_hashes(
        &arrays,
        REPARTITION_RANDOM_STATE.random_state(),
        hash_buffer,
    )?;

    reset_buckets(out, reducer.divisor() as usize);
    reducer.partition_indices(hash_buffer, out);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BallistaError;
    use crate::execution_plans::ChaosExec;
    use datafusion::arrow::array::{NullArray, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use tempfile::TempDir;

    fn create_test_input() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(3)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];

        let memory_data_source =
            Arc::new(MemorySourceConfig::try_new(&partitions, schema, None)?);

        Ok(Arc::new(DataSourceExec::new(memory_data_source)))
    }

    async fn drive_partition_results(
        plan: Arc<SortShuffleWriterExec>,
        task_ctx: Arc<TaskContext>,
    ) -> Vec<crate::error::Result<Vec<RecordBatch>>> {
        let k = plan.properties().output_partitioning().partition_count();
        let mut handles = Vec::with_capacity(k);
        for n in 0..k {
            let plan = plan.clone();
            let ctx = task_ctx.clone();
            handles.push(tokio::spawn(async move {
                let mut stream = plan.execute(n, ctx).map_err(BallistaError::from)?;
                crate::utils::collect_stream(&mut stream).await
            }));
        }
        let mut results = Vec::new();
        for h in handles {
            results.push(
                h.await
                    .expect("drive_partition_results task should not panic"),
            );
        }
        results
    }

    fn assert_shared_io_error(err: &BallistaError) {
        let BallistaError::DataFusionError(e) = err else {
            panic!("expected DataFusionError, got {err:?}");
        };
        assert!(
            matches!(e.as_ref(), DataFusionError::Shared(_)),
            "expected shared DataFusionError, got {e:?}"
        );
        assert!(
            matches!(e.find_root(), DataFusionError::IoError(_)),
            "expected IO root, got {:?}",
            e.find_root()
        );
    }

    #[test]
    fn sort_shuffle_writer_requires_nonzero_partitions() -> Result<()> {
        let work_dir = TempDir::new()?;

        let err = SortShuffleWriterExec::try_new(
            "job1".into(),
            1,
            create_test_input()?,
            work_dir.path().to_str().unwrap().to_string(),
            Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 0),
            SortShuffleConfig::default(),
        )
        .expect_err("zero output partitions should fail")
        .to_string();

        assert!(
            err.contains("requires at least one output partition"),
            "actual: {err}"
        );

        Ok(())
    }

    /// The reciprocal-multiply path must agree with `%` exactly, including at
    /// the u64 boundaries where a mis-derived reciprocal would drift by one.
    #[test]
    fn strength_reduced_u64_remainder_matches_modulo() {
        // Powers of two take the mask path; the rest take the reciprocal path.
        let divisors = [
            1u64,
            2,
            3,
            4,
            5,
            7,
            8,
            10,
            16,
            31,
            32,
            63,
            64,
            65,
            97,
            // Realistic Ballista output partition counts.
            200,
            400,
            800,
            (1 << 32) - 1,
            1 << 32,
            (1 << 32) + 1,
            (1u64 << 63) - 1,
            1u64 << 63,
            u64::MAX - 1,
            u64::MAX,
        ];
        let values = [
            0u64,
            1,
            2,
            3,
            4,
            5,
            31,
            32,
            33,
            63,
            64,
            65,
            (1 << 32) - 2,
            (1 << 32) - 1,
            1 << 32,
            (1 << 32) + 1,
            (1u64 << 63) - 1,
            1u64 << 63,
            (1u64 << 63) + 1,
            u64::MAX - 1,
            u64::MAX,
        ];

        for divisor in divisors {
            let reducer = StrengthReducedU64::new(divisor);
            assert_eq!(reducer.divisor(), divisor);

            for value in values {
                assert_eq!(
                    reducer.remainder(value),
                    value % divisor,
                    "value={value} divisor={divisor}"
                );
            }

            // Also sweep pseudo-random values, not just hand-picked boundaries.
            let mut value = 0x1234_5678_9abc_def0_u64 ^ divisor;
            for _ in 0..10_000 {
                value = value
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                assert_eq!(
                    reducer.remainder(value),
                    value % divisor,
                    "value={value} divisor={divisor}"
                );
            }
        }
    }

    /// `partition_indices` must place rows exactly where `%` would, on both the
    /// mask and reciprocal paths.
    #[test]
    fn strength_reduced_u64_partition_indices_matches_modulo() {
        let hashes: Vec<u64> = (0..500u64)
            .map(|i| {
                i.wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407)
            })
            .collect();

        for num_partitions in [1u64, 2, 8, 16, 64, 97, 200, 400] {
            let reducer = StrengthReducedU64::new(num_partitions);

            let mut actual: Vec<Vec<u32>> = vec![Vec::new(); num_partitions as usize];
            reducer.partition_indices(&hashes, &mut actual);

            let mut expected: Vec<Vec<u32>> = vec![Vec::new(); num_partitions as usize];
            for (row, &h) in hashes.iter().enumerate() {
                expected[(h % num_partitions) as usize].push(row as u32);
            }

            assert_eq!(actual, expected, "num_partitions={num_partitions}");
        }
    }

    #[test]
    fn compute_partition_indices_distributes_rows_by_hash() {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..1000_i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..1000).map(|i| format!("v{i}")).collect::<Vec<String>>(),
                )),
            ],
        )
        .unwrap();

        let exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
            vec![Arc::new(
                datafusion::physical_expr::expressions::Column::new("k", 0),
            )];

        let mut hash_buffer: Vec<u64> = Vec::new();
        let mut result: Vec<Vec<u32>> = Vec::new();
        compute_partition_indices(
            &batch,
            &exprs,
            StrengthReducedU64::new(8),
            &mut hash_buffer,
            &mut result,
        )
        .unwrap();

        // 8 partition slots
        assert_eq!(result.len(), 8);
        // Total row count is preserved
        let total: usize = result.iter().map(|v| v.len()).sum();
        assert_eq!(total, 1000);
        // No row index appears in two partitions
        let mut seen = vec![false; 1000];
        for indices in &result {
            for &row in indices {
                assert!(!seen[row as usize], "row {row} appeared twice");
                seen[row as usize] = true;
            }
        }
        // Distribution is non-trivial — at least 2 distinct partitions used
        let used = result.iter().filter(|v| !v.is_empty()).count();
        assert!(used >= 2, "expected hash to use multiple partitions");
    }

    #[test]
    fn spill_trigger_classifies_each_cause_distinctly() {
        assert_eq!(SpillTrigger::classify(false, false), None);
        assert_eq!(
            SpillTrigger::classify(true, false),
            Some(SpillTrigger::MemoryPool)
        );
        assert_eq!(
            SpillTrigger::classify(false, true),
            Some(SpillTrigger::TaskBudget)
        );
        assert_eq!(SpillTrigger::classify(true, true), Some(SpillTrigger::Both));

        // The two causes must not be described with the same wording, otherwise
        // the log cannot tell an operator which knob to turn.
        assert_ne!(
            SpillTrigger::MemoryPool.label(),
            SpillTrigger::TaskBudget.label()
        );
    }

    #[test]
    fn spill_trigger_counts_summarize_only_triggers_that_fired() {
        let mut counts = SpillTriggerCounts::default();
        assert_eq!(counts.summary(), "none");

        counts.record(SpillTrigger::TaskBudget);
        counts.record(SpillTrigger::TaskBudget);
        assert_eq!(counts.summary(), "per-task buffer budget x2");

        counts.record(SpillTrigger::MemoryPool);
        counts.record(SpillTrigger::Both);
        assert_eq!(
            counts.summary(),
            "memory pool rejection x1, per-task buffer budget x2, both x1"
        );
    }

    #[tokio::test]
    async fn test_sort_shuffle_writer() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_test_input()?));
        let work_dir = TempDir::new()?;

        let config = SortShuffleConfig::default();

        let writer = SortShuffleWriterExec::try_new(
            "job1".into(),
            1,
            input_plan,
            work_dir.path().to_str().unwrap().to_string(),
            Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2),
            config,
        )?;

        let mut stream = writer.execute(0, task_ctx)?;
        let batches: Vec<RecordBatch> = stream
            .by_ref()
            .try_collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        // partition | path | file_id | stats — shared with `ShuffleWriterExec`
        // so the reader path can consume both writers uniformly.
        assert_eq!(batch.num_columns(), 4);

        // Verify output files exist
        let output_dir = work_dir.path().join("job1").join("1").join("0");
        assert!(output_dir.join("data.arrow").exists());
        assert!(output_dir.join("data.arrow.index").exists());

        Ok(())
    }

    #[tokio::test]
    async fn write_failure_is_shared_to_all_output_partitions() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan: Arc<dyn ExecutionPlan> = Arc::new(ChaosExec::new(
            create_test_input()?,
            1.0,
            "transient",
            Some(42),
        )?);
        let work_dir = TempDir::new()?;

        let writer = Arc::new(SortShuffleWriterExec::try_new(
            "job1".into(),
            1,
            input_plan,
            work_dir.path().to_str().unwrap().to_string(),
            Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2),
            SortShuffleConfig::default(),
        )?);

        let results = drive_partition_results(writer, task_ctx).await;
        assert_eq!(2, results.len());
        for result in results {
            let err = result.expect_err("expected injected write failure");
            assert_shared_io_error(&err);
        }

        Ok(())
    }

    /// Shared helper for round-trip tests. Builds `num_batches` batches of
    /// `rows_per_batch` rows each with schema `(k: Int64, v: Int64)`, writes
    /// them through `SortShuffleWriterExec` into `num_partitions` output
    /// partitions with a per-task buffered-bytes budget of
    /// `sort_shuffle_memory_limit_bytes` against a `FairSpillPool` of
    /// `pool_bytes`, reads every partition back via
    /// `stream_sort_shuffle_partition`, and asserts:
    ///   - Every input key `0..total_rows` is present exactly once in the union
    ///     of partition outputs.
    ///   - `spill_count > 0` iff `expect_spills` is `true`.
    ///   - The memory pool has `reserved == 0` after completion.
    async fn run_round_trip(
        num_batches: usize,
        rows_per_batch: usize,
        num_partitions: usize,
        sort_shuffle_memory_limit_bytes: usize,
        pool_bytes: usize,
        expect_spills: bool,
    ) -> Result<()> {
        use super::super::reader::stream_sort_shuffle_partition;
        use datafusion::arrow::array::Int64Array;
        use datafusion::execution::memory_pool::FairSpillPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use std::collections::HashSet;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));

        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|b| {
                let start = (b * rows_per_batch) as i64;
                let keys: Vec<i64> = (start..start + rows_per_batch as i64).collect();
                let values: Vec<i64> = keys.iter().map(|k| k * 2).collect();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(keys)),
                        Arc::new(Int64Array::from(values)),
                    ],
                )
                .unwrap()
            })
            .collect();
        let partitions = vec![batches];
        let memory_data_source = Arc::new(MemorySourceConfig::try_new(
            &partitions,
            schema.clone(),
            None,
        )?);
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(memory_data_source));

        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(pool_bytes)))
                .build()?,
        );
        let session_ctx = SessionContext::new_with_config_rt(
            datafusion::execution::config::SessionConfig::new(),
            runtime_env,
        );
        let task_ctx = session_ctx.task_ctx();

        let work_dir = TempDir::new()?;

        let writer = SortShuffleWriterExec::try_new(
            "round_trip_job".into(),
            1,
            input,
            work_dir.path().to_str().unwrap().to_string(),
            Partitioning::Hash(vec![Arc::new(Column::new("k", 0))], num_partitions),
            SortShuffleConfig::default()
                .with_memory_limit_per_task_bytes(sort_shuffle_memory_limit_bytes),
        )?;

        let mut stream = writer.execute(0, task_ctx)?;
        let summary_batches: Vec<RecordBatch> = stream
            .by_ref()
            .try_collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
        assert_eq!(summary_batches.len(), 1, "expected one summary batch");

        let metrics = writer.metrics().expect("metrics after execute");
        let spill_count = metrics
            .iter()
            .find(|m| m.value().name() == "spill_count")
            .map(|m| m.value().as_usize())
            .unwrap_or(0);
        if expect_spills {
            assert!(
                spill_count > 0,
                "expected spilling under tight per-task buffer budget, got spill_count={spill_count}"
            );
        } else {
            assert_eq!(
                spill_count, 0,
                "expected no spills with generous per-task buffer budget, got spill_count={spill_count}"
            );
        }

        let data_path = work_dir
            .path()
            .join("round_trip_job")
            .join("1")
            .join("0")
            .join("data.arrow");
        let index_path = data_path.with_extension("arrow.index");
        let mut seen: HashSet<i64> = HashSet::new();
        let total_rows = num_batches * rows_per_batch;
        for partition_id in 0..num_partitions {
            let mut s =
                stream_sort_shuffle_partition(&data_path, &index_path, partition_id)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            while let Some(batch_result) = s.next().await {
                let batch = batch_result?;
                let arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for v in arr.values() {
                    seen.insert(*v);
                }
            }
        }
        assert_eq!(seen.len(), total_rows);
        for k in 0..total_rows as i64 {
            assert!(seen.contains(&k), "key {k} missing from round-trip");
        }

        let pool_reserved = session_ctx.runtime_env().memory_pool.reserved();
        assert_eq!(
            pool_reserved, 0,
            "expected MemoryReservation freed after finalization, but pool reports {pool_reserved} bytes still reserved"
        );

        Ok(())
    }

    // The writer has two independent spill triggers, and these tests arm exactly
    // one at a time so a failure names its cause:
    //
    //   1. the shared `MemoryPool` rejecting a `try_grow` — sized by `pool_bytes`
    //   2. the private per-task budget `memory_limit_per_task_bytes` being
    //      reached — sized by `sort_shuffle_memory_limit_bytes`
    //
    // The `*_DISARMED` constants below are the "this trigger is not what is
    // under test" settings. A test payload batch is 8192 rows x 2 Int64 columns,
    // i.e. ~128 KiB.

    /// Pool far larger than any test payload, so `try_grow` always succeeds and
    /// trigger 1 never fires. Still bounded, so the `pool.reserved() == 0`
    /// assertion after the run stays meaningful.
    const POOL_NEVER_REJECTS: usize = 64 * 1024 * 1024;

    /// Per-task budget far above any test payload, so trigger 2 never fires.
    const BUDGET_NEVER_REACHED: usize = 1024 * 1024 * 1024;

    #[tokio::test]
    async fn spills_when_per_task_budget_reached_and_round_trips() -> Result<()> {
        // Trigger 2 only: 10 batches (~1.28 MiB) against a 512 KiB per-task
        // budget forces spilling, while the pool stays out of the picture.
        run_round_trip(
            /* num_batches */ 10,
            /* rows_per_batch */ 8192,
            /* num_partitions */ 4,
            /* sort_shuffle_memory_limit_bytes */ 512 * 1024,
            /* pool_bytes */ POOL_NEVER_REJECTS,
            /* expect_spills */ true,
        )
        .await
    }

    /// A task owning several input partitions must emit exactly ONE data file
    /// holding every bucket, not one file per input. The rows still have to
    /// round-trip: each output partition's byte range now concatenates that
    /// partition's rows from every input in turn, so this also covers the
    /// reader crossing input boundaries inside a single range.
    #[tokio::test]
    async fn multiple_input_partitions_write_one_file() -> Result<()> {
        use super::super::reader::stream_sort_shuffle_partition;
        use datafusion::arrow::array::Int64Array;
        use datafusion::physical_plan::ExecutionPlanProperties;
        use std::collections::HashSet;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));

        let num_inputs = 3;
        let rows_per_input = 500;
        let num_partitions = 4;

        // One input partition per outer Vec, so the writer sees M = 3.
        let partitions: Vec<Vec<RecordBatch>> = (0..num_inputs)
            .map(|p| {
                let start = (p * rows_per_input) as i64;
                let keys: Vec<i64> = (start..start + rows_per_input as i64).collect();
                let values: Vec<i64> = keys.iter().map(|k| k * 2).collect();
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(keys)),
                            Arc::new(Int64Array::from(values)),
                        ],
                    )
                    .unwrap(),
                ]
            })
            .collect();

        let memory_data_source = Arc::new(MemorySourceConfig::try_new(
            &partitions,
            schema.clone(),
            None,
        )?);
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(memory_data_source));
        assert_eq!(
            input.output_partitioning().partition_count(),
            num_inputs,
            "test needs a multi-partition input to exercise consolidation"
        );

        let work_dir = TempDir::new()?;
        let writer = SortShuffleWriterExec::try_new(
            "multi_input_job".into(),
            1,
            input,
            work_dir.path().to_str().unwrap().to_string(),
            Partitioning::Hash(vec![Arc::new(Column::new("k", 0))], num_partitions),
            SortShuffleConfig::default(),
        )?
        .with_task_id(7);

        let ctx = SessionContext::new();
        // Drain every output partition so the coordinator runs to completion.
        for output_partition in 0..num_partitions {
            let mut stream = writer.execute(output_partition, ctx.task_ctx())?;
            while stream.next().await.is_some() {}
        }

        // One directory named after the task id, holding one data file.
        let stage_dir = work_dir.path().join("multi_input_job").join("1");
        let file_dirs: Vec<_> = std::fs::read_dir(&stage_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            file_dirs,
            vec!["7".to_string()],
            "a task must write exactly one shuffle file, keyed by task id"
        );

        let data_path = stage_dir.join("7").join("data.arrow");
        let index_path = data_path.with_extension("arrow.index");

        let mut seen: HashSet<i64> = HashSet::new();
        for partition_id in 0..num_partitions {
            let mut s =
                stream_sort_shuffle_partition(&data_path, &index_path, partition_id)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            while let Some(batch_result) = s.next().await {
                let batch = batch_result?;
                let arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for v in arr.values() {
                    assert!(seen.insert(*v), "key {v} appeared twice across partitions");
                }
            }
        }

        let total_rows = num_inputs * rows_per_input;
        assert_eq!(seen.len(), total_rows, "every input row must round-trip");
        for k in 0..total_rows as i64 {
            assert!(seen.contains(&k), "key {k} missing from round-trip");
        }

        Ok(())
    }

    /// Build a sort-shuffle writer over `partitions`, drive every output
    /// partition to completion (which is what publishes the task-level column
    /// stats onto the shared state), and return the per-column null counts.
    async fn collect_null_counts(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        hash_col: &str,
        num_output_partitions: usize,
    ) -> Result<Vec<u64>> {
        let col_idx = schema.index_of(hash_col)?;
        let routing = ShuffleRouting::Hash {
            exprs: vec![Arc::new(Column::new(hash_col, col_idx))],
            partition_count: num_output_partitions,
        };
        collect_null_counts_routed(schema, partitions, routing).await
    }

    /// [`collect_null_counts`] for any routing.
    async fn collect_null_counts_routed(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        routing: ShuffleRouting,
    ) -> Result<Vec<u64>> {
        let source = Arc::new(MemorySourceConfig::try_new(&partitions, schema, None)?);
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(source));
        let task_ctx = SessionContext::new().task_ctx();
        let work_dir = TempDir::new()?;

        let writer = Arc::new(SortShuffleWriterExec::try_new_with_routing(
            "null_count_job".into(),
            1,
            input,
            work_dir.path().to_str().unwrap().to_string(),
            routing,
            SortShuffleConfig::default(),
        )?);

        for r in drive_partition_results(writer.clone(), task_ctx).await {
            r.expect("partition drive should succeed");
        }
        Ok(writer.column_null_counts())
    }

    #[tokio::test]
    async fn column_null_counts_fold_across_inputs() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        // Per batch: "a" has 1 null, "b" has 2.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![None, None, Some("x")])),
            ],
        )?;
        // Two input partitions of the same batch -> the coordinator must SUM
        // them: "a" = 1x2 = 2, "b" = 2x2 = 4. Also proves the shared-state
        // read-back (we assert on the original Arc, not the driven clone) and
        // that nulls are counted over the whole batch despite hashing on "a".
        let partitions = vec![vec![batch.clone()], vec![batch]];
        let counts = collect_null_counts(schema, partitions, "a", 2).await?;
        assert_eq!(counts, vec![2, 4]);
        Ok(())
    }

    #[tokio::test]
    async fn column_null_counts_zero_for_fully_populated_column() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![Some("p"), None, Some("q")])),
            ],
        )?;
        let counts = collect_null_counts(schema, vec![vec![batch]], "a", 2).await?;
        assert_eq!(counts, vec![0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn column_null_counts_empty_input_is_schema_width_zeros() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        // A zero-row batch: the accumulator is still schema-width and the
        // coordinator's fold still runs, yielding zeros rather than an empty vec.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(Vec::<Option<u32>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )?;
        let counts = collect_null_counts(schema, vec![vec![batch]], "a", 2).await?;
        assert_eq!(counts, vec![0, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn column_null_counts_counts_logical_nulls_for_null_type() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("n", DataType::Null, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(NullArray::new(3)),
            ],
        )?;
        let counts = collect_null_counts(schema, vec![vec![batch]], "a", 2).await?;
        assert_eq!(counts, vec![0, 3]);
        Ok(())
    }

    #[tokio::test]
    async fn multi_spill_round_trips() -> Result<()> {
        // Trigger 2 only, with a larger total payload and a tighter budget than
        // the baseline test, so we expect multiple spill events per partition.
        // The byte-copy finalize path should produce a partition section that is
        // `spill_stream || in_memory_stream` (two concatenated IPC streams), and
        // the reader must yield every input row regardless.
        run_round_trip(
            /* num_batches */ 20,
            /* rows_per_batch */ 8192,
            /* num_partitions */ 2,
            /* sort_shuffle_memory_limit_bytes */ 256 * 1024,
            /* pool_bytes */ POOL_NEVER_REJECTS,
            /* expect_spills */ true,
        )
        .await
    }

    #[tokio::test]
    async fn in_memory_only_round_trips() -> Result<()> {
        // Neither trigger armed, so nothing spills. Validates the path where
        // each partition section is exactly one in-memory IPC stream.
        run_round_trip(
            /* num_batches */ 4,
            /* rows_per_batch */ 1024,
            /* num_partitions */ 4,
            /* sort_shuffle_memory_limit_bytes */ BUDGET_NEVER_REACHED,
            /* pool_bytes */ POOL_NEVER_REJECTS,
            /* expect_spills */ false,
        )
        .await
    }

    #[tokio::test]
    async fn zero_budget_disables_per_task_spill_trigger() -> Result<()> {
        // A per-task budget of 0 disables trigger 2 entirely. With a pool that
        // never rejects, neither trigger can fire, so the whole payload stays in
        // memory and round-trips without spilling — even though the same payload
        // would spill under any small non-zero budget.
        run_round_trip(
            /* num_batches */ 10,
            /* rows_per_batch */ 8192,
            /* num_partitions */ 4,
            /* sort_shuffle_memory_limit_bytes */ 0,
            /* pool_bytes */ POOL_NEVER_REJECTS,
            /* expect_spills */ false,
        )
        .await
    }

    #[tokio::test]
    async fn zero_budget_still_spills_under_memory_pool_pressure() -> Result<()> {
        // With the per-task budget disabled (0), the runtime memory pool is the
        // sole spill trigger. A 64 KiB pool cannot admit a ~128 KiB batch, so
        // pool rejection still forces spilling.
        run_round_trip(
            /* num_batches */ 8,
            /* rows_per_batch */ 8192,
            /* num_partitions */ 2,
            /* sort_shuffle_memory_limit_bytes */ 0,
            /* pool_bytes */ 64 * 1024,
            /* expect_spills */ true,
        )
        .await
    }

    #[tokio::test]
    async fn spills_when_memory_pool_rejects_growth() -> Result<()> {
        // Trigger 1 only: a 64 KiB pool cannot admit even one ~128 KiB batch, so
        // every `try_grow` is rejected, while the per-task budget can never fire.
        // Pool rejection is therefore the only signal that can produce a spill.
        //
        // A rejected grow means the pool has not accounted for the batch just
        // buffered. The writer must spill in response rather than keep buffering
        // memory the pool believes is free.
        run_round_trip(
            /* num_batches */ 8,
            /* rows_per_batch */ 8192,
            /* num_partitions */ 2,
            /* sort_shuffle_memory_limit_bytes */ BUDGET_NEVER_REACHED,
            /* pool_bytes */ 64 * 1024,
            /* expect_spills */ true,
        )
        .await
    }

    #[tokio::test]
    async fn empty_partitions_round_trip() -> Result<()> {
        use super::super::reader::stream_sort_shuffle_partition;
        use datafusion::arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let total_rows = 256;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![0_i64; total_rows]))],
        )
        .unwrap();
        let partitions = vec![vec![batch]];
        let memory_data_source = Arc::new(MemorySourceConfig::try_new(
            &partitions,
            schema.clone(),
            None,
        )?);
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(memory_data_source));

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let work_dir = TempDir::new()?;

        let num_partitions = 8;
        let writer = SortShuffleWriterExec::try_new(
            "empty_partitions_job".into(),
            1,
            input,
            work_dir.path().to_str().unwrap().to_string(),
            Partitioning::Hash(vec![Arc::new(Column::new("k", 0))], num_partitions),
            SortShuffleConfig::default(),
        )?;

        let mut stream = writer.execute(0, task_ctx)?;
        let _summary: Vec<RecordBatch> = stream
            .by_ref()
            .try_collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

        let data_path = work_dir
            .path()
            .join("empty_partitions_job")
            .join("1")
            .join("0")
            .join("data.arrow");
        let index_path = data_path.with_extension("arrow.index");

        let mut row_counts = Vec::with_capacity(num_partitions);
        for partition_id in 0..num_partitions {
            let mut s =
                stream_sort_shuffle_partition(&data_path, &index_path, partition_id)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            let mut count = 0_usize;
            while let Some(batch_result) = s.next().await {
                let batch = batch_result?;
                count += batch.num_rows();
            }
            row_counts.push(count);
        }

        let total_seen: usize = row_counts.iter().sum();
        assert_eq!(total_seen, total_rows, "round-trip lost rows");
        let non_empty = row_counts.iter().filter(|c| **c > 0).count();
        assert!(non_empty >= 1, "expected at least one non-empty partition");
        assert!(
            non_empty < num_partitions,
            "expected at least one empty partition"
        );

        Ok(())
    }

    /// Drift-detection test: verifies that `compute_partition_indices` produces
    /// exactly the same per-row partition assignment as DataFusion's own
    /// `BatchPartitioner::new_hash_partitioner`. If a future DataFusion release
    /// changes the hash seed or algorithm this test will fail, signalling that
    /// the sort-shuffle path has diverged.
    #[test]
    fn compute_partition_indices_matches_batch_partitioner() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::metrics::Time;
        use datafusion::physical_plan::repartition::BatchPartitioner;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let values: Vec<i64> = (0..10).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.clone()))],
        )
        .unwrap();

        let exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
            vec![Arc::new(
                datafusion::physical_expr::expressions::Column::new("k", 0),
            )];

        // Our implementation
        let mut hash_buffer: Vec<u64> = Vec::new();
        let mut ours: Vec<Vec<u32>> = Vec::new();
        compute_partition_indices(
            &batch,
            &exprs,
            StrengthReducedU64::new(4),
            &mut hash_buffer,
            &mut ours,
        )
        .unwrap();

        // Reference: DataFusion's BatchPartitioner::new_hash_partitioner
        let mut ref_partitioner =
            BatchPartitioner::new_hash_partitioner(exprs.clone(), 4, Time::default())
                .unwrap();
        let mut ref_assignments = [usize::MAX; 10];
        ref_partitioner
            .partition(batch.clone(), |partition, sub_batch| {
                // BatchPartitioner returns the rows for `partition` in the order
                // they appear in the original batch. Recover the row indices via
                // value lookup (unique values in 0..10 make this unambiguous).
                let arr = sub_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for v in arr.values() {
                    let original_row = values.iter().position(|x| x == v).unwrap();
                    ref_assignments[original_row] = partition;
                }
                Ok(())
            })
            .unwrap();

        // Cross-check: every row's partition assignment from our helper must
        // match BatchPartitioner's. Guards against hash-seed or algorithm drift.
        for (row, &ref_partition) in ref_assignments.iter().enumerate() {
            let our_partition = ours
                .iter()
                .position(|v| v.contains(&(row as u32)))
                .expect("row not assigned to any partition");
            assert_eq!(
                our_partition, ref_partition,
                "partition assignment for row {row} differs from BatchPartitioner"
            );
        }
    }

    // Partition-id routing

    /// The tests' client placement: by range, which no hash produces.
    fn placed_by_range(value: i64, rows_per_partition: i64) -> i32 {
        (value / rows_per_partition) as i32
    }

    /// `(v: Int64, pid: Int32, w: Utf8)` input: one entry per input partition,
    /// each a list of batches of `v` values, with `pid` computed by `pid_of`.
    fn partition_id_input(
        partitions: Vec<Vec<Vec<i64>>>,
        pid_of: impl Fn(i64) -> i32,
    ) -> Arc<dyn ExecutionPlan> {
        use datafusion::arrow::array::{Int32Array, Int64Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("pid", DataType::Int32, false),
            Field::new("w", DataType::Utf8, true),
        ]));
        let data: Vec<Vec<RecordBatch>> = partitions
            .iter()
            .map(|batches| {
                batches
                    .iter()
                    .map(|values| {
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                Arc::new(Int64Array::from(values.clone())),
                                Arc::new(Int32Array::from(
                                    values.iter().map(|v| pid_of(*v)).collect::<Vec<_>>(),
                                )),
                                Arc::new(StringArray::from(
                                    values
                                        .iter()
                                        .map(|v| format!("w{v}"))
                                        .collect::<Vec<_>>(),
                                )),
                            ],
                        )
                        .unwrap()
                    })
                    .collect()
            })
            .collect();
        MemorySourceConfig::try_new_exec(&data, schema, None).unwrap()
    }

    fn partition_id_writer(
        input: Arc<dyn ExecutionPlan>,
        work_dir: &TempDir,
        config: SortShuffleConfig,
        partition_count: usize,
    ) -> Result<SortShuffleWriterExec> {
        SortShuffleWriterExec::try_new_with_routing(
            "pid_job".into(),
            1,
            input,
            work_dir.path().to_str().unwrap().to_string(),
            ShuffleRouting::PartitionId {
                column: 1,
                partition_count,
            },
            config,
        )
    }

    /// Drives every output partition of `writer` to completion.
    async fn drive_to_completion(writer: &Arc<SortShuffleWriterExec>) {
        let results =
            drive_partition_results(writer.clone(), SessionContext::new().task_ctx());
        for result in results.await {
            result.expect("partition drive should succeed");
        }
    }

    /// What the task-0 writer of `pid_job` stage 1 wrote, per output partition.
    async fn read_partition_id_output(
        work_dir: &TempDir,
        partition_count: usize,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        use super::super::reader::{get_index_path, stream_sort_shuffle_partition};

        let data_path = work_dir
            .path()
            .join("pid_job")
            .join("1")
            .join("0")
            .join("data.arrow");
        let index_path = get_index_path(&data_path);
        let mut partitions = Vec::with_capacity(partition_count);
        for partition_id in 0..partition_count {
            let stream =
                stream_sort_shuffle_partition(&data_path, &index_path, partition_id)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            partitions.push(stream.try_collect().await?);
        }
        Ok(partitions)
    }

    /// The `v` values of `batches`, checked to be `(v, w)` without the id column.
    fn values_without_id_column(batches: &[RecordBatch]) -> Vec<i64> {
        use datafusion::arrow::array::{AsArray, Int64Array};
        use datafusion::arrow::datatypes::Int64Type;

        let mut values = Vec::new();
        for batch in batches {
            let names: Vec<_> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(names, ["v", "w"], "the id column must not be written");
            let v: &Int64Array = batch.column(0).as_primitive::<Int64Type>();
            values.extend(v.values().iter().copied());
        }
        values
    }

    /// Asserts that output partition `p` holds exactly the `v` values
    /// `p * n..(p + 1) * n` for `n = rows_per_partition`.
    async fn assert_range_placement(
        work_dir: &TempDir,
        partition_count: usize,
        rows_per_partition: i64,
    ) -> Result<()> {
        let partitions = read_partition_id_output(work_dir, partition_count).await?;
        for (partition, batches) in partitions.iter().enumerate() {
            let mut values = values_without_id_column(batches);
            values.sort();
            let first = partition as i64 * rows_per_partition;
            let expected: Vec<i64> = (first..first + rows_per_partition).collect();
            assert_eq!(values, expected, "partition {partition}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn partition_id_routing_places_each_row_in_the_partition_it_names() -> Result<()>
    {
        let rows_per_partition = 100;
        let num_partitions = 5;
        // Two input partitions of 250 rows, with ids 0..=4 by range of `v`.
        let input = partition_id_input(
            vec![vec![(0..250).collect()], vec![(250..500).collect()]],
            |v| placed_by_range(v, rows_per_partition),
        );
        let work_dir = TempDir::new()?;
        let writer = Arc::new(partition_id_writer(
            input,
            &work_dir,
            SortShuffleConfig::default(),
            num_partitions,
        )?);

        drive_to_completion(&writer).await;

        assert_range_placement(&work_dir, num_partitions, rows_per_partition).await
    }

    /// A one-byte budget spills every batch, covering spill files without the id
    /// column.
    #[tokio::test]
    async fn partition_id_routing_survives_spilling() -> Result<()> {
        let rows_per_partition = 500;
        let num_partitions = 4;
        let chunks: Vec<Vec<i64>> = (0..8)
            .map(|b| (b * 250..(b + 1) * 250).collect::<Vec<i64>>())
            .collect();
        let input =
            partition_id_input(vec![chunks], |v| placed_by_range(v, rows_per_partition));
        let work_dir = TempDir::new()?;
        let writer = Arc::new(partition_id_writer(
            input,
            &work_dir,
            SortShuffleConfig::default().with_memory_limit_per_task_bytes(1),
            num_partitions,
        )?);

        drive_to_completion(&writer).await;

        let spill_count = writer
            .metrics()
            .expect("metrics after execute")
            .iter()
            .find(|m| m.value().name() == "spill_count")
            .map(|m| m.value().as_usize())
            .unwrap_or(0);
        assert!(spill_count > 0, "expected the tiny budget to force spills");
        assert_range_placement(&work_dir, num_partitions, rows_per_partition).await
    }

    /// Partitions that no row names still exist, and are empty.
    #[tokio::test]
    async fn partition_id_routing_leaves_unnamed_partitions_empty() -> Result<()> {
        let num_partitions = 4;
        // Everything goes to partition 2.
        let input = partition_id_input(vec![vec![(0..50).collect()]], |_| 2);
        let work_dir = TempDir::new()?;
        let writer = Arc::new(partition_id_writer(
            input,
            &work_dir,
            SortShuffleConfig::default(),
            num_partitions,
        )?);

        drive_to_completion(&writer).await;

        let partitions = read_partition_id_output(&work_dir, num_partitions).await?;
        let row_counts: Vec<usize> = partitions
            .iter()
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum())
            .collect();
        assert_eq!(row_counts, [0, 0, 50, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn partition_id_routing_fails_on_an_id_past_the_last_partition() -> Result<()> {
        let input = partition_id_input(vec![vec![vec![0, 1, 2]]], |v| v as i32);
        let work_dir = TempDir::new()?;
        let writer = Arc::new(partition_id_writer(
            input,
            &work_dir,
            SortShuffleConfig::default(),
            2,
        )?);

        let results =
            drive_partition_results(writer, SessionContext::new().task_ctx()).await;

        // The failure is shared with every output partition so none of them hangs.
        assert_eq!(results.len(), 2);
        for result in results {
            let err = result.expect_err("an out-of-range id must fail the write");
            assert!(err.to_string().contains("outside 0..2"), "actual: {err}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn partition_id_routing_counts_nulls_in_the_written_columns() -> Result<()> {
        use datafusion::arrow::array::Int32Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("pid", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), None, None, Some(4)])),
                Arc::new(Int32Array::from(vec![0, 1, 0, 1])),
                Arc::new(StringArray::from(vec![None, Some("x"), None, None])),
            ],
        )?;
        let routing = ShuffleRouting::PartitionId {
            column: 1,
            partition_count: 2,
        };

        let counts =
            collect_null_counts_routed(schema, vec![vec![batch]], routing).await?;

        // One count per written column (`a`, `b`), never one for the id column.
        assert_eq!(counts, vec![2, 3]);
        Ok(())
    }

    #[test]
    fn partition_id_writer_reports_the_input_without_the_id_column() -> Result<()> {
        let work_dir = TempDir::new()?;
        let input = partition_id_input(vec![vec![vec![0]]], |v| v as i32);

        let writer =
            partition_id_writer(input, &work_dir, SortShuffleConfig::default(), 3)?;

        let schema = writer.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["v", "w"]);
        // Not Hash: the placement is the client's, not DataFusion's. (`Partitioning`
        // never compares `UnknownPartitioning` equal, even to itself.)
        assert!(matches!(
            writer.shuffle_output_partitioning(),
            Partitioning::UnknownPartitioning(3)
        ));
        Ok(())
    }

    /// A rebuilt writer (`with_new_children`, the executor's stage construction)
    /// must keep its routing, or it would silently become a different shuffle.
    #[test]
    fn partition_id_writer_keeps_its_routing_when_rebuilt() -> Result<()> {
        use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};

        let work_dir = TempDir::new()?;
        let writer = Arc::new(partition_id_writer(
            partition_id_input(vec![vec![vec![0]]], |v| v as i32),
            &work_dir,
            SortShuffleConfig::default(),
            3,
        )?);

        let rebuilt = writer.replace_children(
            vec![partition_id_input(vec![vec![vec![0]]], |v| v as i32)],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;

        let rebuilt = rebuilt
            .downcast_ref::<SortShuffleWriterExec>()
            .expect("a SortShuffleWriterExec");
        assert_eq!(
            rebuilt.routing(),
            &ShuffleRouting::PartitionId {
                column: 1,
                partition_count: 3
            }
        );
        assert_eq!(rebuilt.schema().fields().len(), 2);
        Ok(())
    }

    /// The id column's own checks are tested in `partition_id.rs`; this shows
    /// they reach the writer.
    #[test]
    fn partition_id_writer_rejects_zero_partitions_and_a_bad_id_column() {
        let work_dir = TempDir::new().unwrap();
        let build = |column, partition_count| {
            SortShuffleWriterExec::try_new_with_routing(
                "pid_job".into(),
                1,
                partition_id_input(vec![vec![vec![0]]], |v| v as i32),
                work_dir.path().to_str().unwrap().to_string(),
                ShuffleRouting::PartitionId {
                    column,
                    partition_count,
                },
                SortShuffleConfig::default(),
            )
            .unwrap_err()
            .to_string()
        };

        assert!(build(1, 0).contains("requires at least one output partition"));
        // Column 2 is a string.
        assert!(build(2, 4).contains("must be an integer"));
    }

    /// A sort-shuffle writer's output partition ids are `0..K` however it routes.
    #[test]
    fn partition_id_writer_outputs_every_partition_of_its_k_space() -> Result<()> {
        let work_dir = TempDir::new()?;
        let writer: Arc<dyn ExecutionPlan> = Arc::new(partition_id_writer(
            partition_id_input(vec![vec![vec![0]], vec![vec![1]]], |v| v as i32),
            &work_dir,
            SortShuffleConfig::default(),
            6,
        )?);

        let ids =
            crate::execution_plans::compute_global_output_partition_ids(&writer, &[3, 4]);

        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5]);
        Ok(())
    }

    /// The input's statistics describe the id column too, but the writer's
    /// output has no such column, so its statistics must drop that entry.
    #[test]
    fn partition_id_writer_statistics_have_no_entry_for_the_id_column() -> Result<()> {
        use datafusion::common::ColumnStatistics;
        use datafusion::common::stats::Precision;
        use datafusion::physical_plan::statistics::StatisticsContext;
        use datafusion::physical_plan::test::exec::StatisticsExec;

        let schema = Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("pid", DataType::Int32, false),
            Field::new("w", DataType::Utf8, true),
        ]);
        let column = |nulls| ColumnStatistics {
            null_count: Precision::Exact(nulls),
            ..ColumnStatistics::new_unknown()
        };
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(10),
                total_byte_size: Precision::Absent,
                column_statistics: vec![column(1), column(2), column(3)],
            },
            schema,
        ));
        let work_dir = TempDir::new()?;
        let writer =
            partition_id_writer(input, &work_dir, SortShuffleConfig::default(), 4)?;

        let stats = StatisticsContext::new().compute(&writer, &StatisticsArgs::new())?;

        // `v` and `w` remain, in order, without `pid`'s count of 2.
        let nulls: Vec<_> = stats
            .column_statistics
            .iter()
            .map(|c| c.null_count)
            .collect();
        assert_eq!(nulls, [Precision::Exact(1), Precision::Exact(3)]);
        assert_eq!(stats.num_rows, Precision::Exact(10));
        Ok(())
    }

    #[test]
    fn partition_id_writer_shows_its_id_column_in_the_plan() -> Result<()> {
        let work_dir = TempDir::new()?;
        let writer = partition_id_writer(
            partition_id_input(vec![vec![vec![0]]], |v| v as i32),
            &work_dir,
            SortShuffleConfig::default(),
            3,
        )?;

        let shown = displayable(&writer).indent(false).to_string();
        assert_eq!(
            shown.lines().next().unwrap(),
            "SortShuffleWriterExec: partitioning=UnknownPartitioning(3), partition_id_column=pid@1"
        );

        let tree = displayable(&writer).tree_render().to_string();
        assert!(tree.contains("partition_id_column:"), "actual: {tree}");
        assert!(tree.contains("pid@1"), "actual: {tree}");
        Ok(())
    }
}
