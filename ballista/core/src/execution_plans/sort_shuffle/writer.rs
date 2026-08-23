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

use std::fmt::Debug;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use super::super::shuffle_writer::{result_schema, summaries_to_batch};
use super::super::shuffle_writer_trait::ShuffleWriter;
use super::buffer::BufferedBatches;
use super::config::SortShuffleConfig;
use super::index::ShuffleIndex;
use super::partitioned_batch_iterator::PartitionedBatchIterator;
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
use datafusion::common::internal_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics, StatisticsArgs, apply_expression_roots,
    displayable, statistics::ChildStats,
};
use futures::{StreamExt, TryStreamExt};
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
    config: &SortShuffleConfig,
    opts: &datafusion::arrow::ipc::writer::IpcWriteOptions,
) -> Result<EncodedPartitions> {
    let num_partitions = buffered.num_partitions();
    let (batches, indices) = buffered.take();

    let mut encoded = Vec::with_capacity(num_partitions);
    let mut stats = Vec::with_capacity(num_partitions);

    for partition_indices in indices.iter() {
        let mut buf: Vec<u8> = Vec::new();
        let (mut num_batches, mut num_rows, mut num_bytes) = (0u64, 0u64, 0u64);

        if !partition_indices.is_empty() {
            let iter = PartitionedBatchIterator::new(
                &batches,
                partition_indices,
                config.batch_size,
            );
            let mut writer =
                StreamWriter::try_new_with_options(&mut buf, schema, opts.clone())?;
            for result in iter {
                let batch = result?;
                num_rows += batch.num_rows() as u64;
                num_bytes += batch.get_array_memory_size() as u64;
                num_batches += 1;
                writer.write(&batch)?;
            }
            writer.finish()?;
        }

        encoded.push(buf);
        stats.push(EncodedStats {
            num_batches,
            num_rows,
            num_bytes,
        });
    }

    Ok(EncodedPartitions { encoded, stats })
}

/// Coordinator handoff state — same shape as `ShuffleWriterExec`. Each output
/// hash partition (0..K) gets one `oneshot::Receiver`; the first `execute(N)`
/// call spawns the coordinator, subsequent calls take their receiver and
/// await the summaries for their output partition.
struct WriterState {
    initialized: bool,
    handoffs: Vec<Option<oneshot::Receiver<Result<Vec<ShuffleWritePartition>>>>>,
}

impl Debug for WriterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterState")
            .field("initialized", &self.initialized)
            .field("handoffs", &self.handoffs.len())
            .finish()
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
/// slice. Each input writer pulls `child.execute(i)`, hash-buckets rows
/// into K sub-slices, sorts and spills, and produces one indexed data file.
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
    /// Shuffle output partitioning (must be Hash partitioning)
    shuffle_output_partitioning: Partitioning,
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
            shuffle_output_partitioning: self.shuffle_output_partitioning.clone(),
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
    /// Create a new sort-based shuffle writer.
    pub fn try_new(
        job_id: JobId,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        shuffle_output_partitioning: Partitioning,
        config: SortShuffleConfig,
    ) -> Result<Self> {
        // Sort shuffle only supports hash partitioning
        match &shuffle_output_partitioning {
            Partitioning::Hash(_, _) => {}
            other => {
                return Err(DataFusionError::Plan(format!(
                    "SortShuffleWriterExec only supports Hash partitioning, got: {other:?}"
                )));
            }
        }

        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(plan.schema()),
            shuffle_output_partitioning.clone(),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));

        // Default identity slice — one global id per local input partition.
        // The executor's `create_query_stage_exec` stamps the real slice via
        // `with_global_output_partition_ids` before dispatch.
        let child_partition_count =
            plan.properties().output_partitioning().partition_count();
        let default_partition_slice: Vec<usize> = (0..child_partition_count).collect();
        let output_partition_count = properties.output_partitioning().partition_count();

        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
            shuffle_output_partitioning,
            config,
            task_id: 0,
            global_output_partition_ids: default_partition_slice,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            state: Arc::new(Mutex::new(WriterState {
                initialized: false,
                handoffs: (0..output_partition_count).map(|_| None).collect(),
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

    /// Get the shuffle output partitioning
    pub fn shuffle_output_partitioning(&self) -> &Partitioning {
        &self.shuffle_output_partitioning
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
        let partitioning = self.shuffle_output_partitioning.clone();
        let task_id = self.task_id;

        async move {
            let mut stream = plan.execute(input_partition, context.clone())?;
            let schema = stream.schema();
            let ballista_config = Arc::new(context.session_config().ballista_config());
            let compression_type = ballista_config.shuffle_compression_codec()?;

            let Partitioning::Hash(exprs, num_output_partitions) = partitioning else {
                return Err(DataFusionError::Internal(
                    "Expected hash partitioning".to_string(),
                ));
            };

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

            let mut buffered =
                BufferedBatches::new(num_output_partitions, schema.clone());

            let mut reservation =
                MemoryConsumer::new(format!("SortShuffleWriter[{input_partition}]"))
                    .with_can_spill(true)
                    .register(&context.runtime_env().memory_pool);

            let mut hash_buffer: Vec<u64> = Vec::new();
            let mut per_partition_rows: Vec<Vec<u32>> = Vec::new();
            let mut spill_events: u64 = 0;
            let mut spill_triggers = SpillTriggerCounts::default();
            // Absolute buffered-bytes counter, independent of the runtime
            // `MemoryPool`. When `memory_limit` is non-zero it caps this counter
            // as a second spill trigger; a `memory_limit` of 0 disables the cap
            // so spilling is driven solely by memory-pool pressure.
            let mut buffered_bytes: usize = 0;
            // A limit of 0 disables the per-task budget, leaving the runtime
            // `MemoryPool` as the sole spill trigger.
            let memory_limit = config.memory_limit_per_task_bytes;
            let per_task_budget_enabled = memory_limit > 0;

            while let Some(result) = stream.next().await {
                let input_batch = result?;
                metrics.input_rows.add(input_batch.num_rows());

                // Compute partition assignment for every row.
                let timer = metrics.repart_time.timer();
                compute_partition_indices(
                    &input_batch,
                    &exprs,
                    num_output_partitions,
                    &mut hash_buffer,
                    &mut per_partition_rows,
                )?;
                timer.done();

                // Estimate memory growth: input batch + index Vec growth.
                let mut growth = input_batch.get_array_memory_size();
                let before = buffered.indices_allocated_size();
                buffered.push_batch(input_batch, &per_partition_rows);
                let after = buffered.indices_allocated_size();
                growth += after.saturating_sub(before);

                // Mirror the growth in the runtime pool reservation so the pool
                // sees this writer's memory usage. A rejected grow means the
                // pool is under pressure and has *not* accounted for the batch
                // just buffered, so spill instead of holding memory the pool
                // believes is free. Spilling flushes that batch to disk and
                // frees the reservation, which is the response a spillable
                // consumer owes the pool.
                let pool_rejected_growth = reservation.try_grow(growth).is_err();
                buffered_bytes = buffered_bytes.saturating_add(growth);
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
                        config.batch_size,
                    )?;
                    spill_timer.done();
                    buffered_bytes = 0;

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
                encode_buffered_partitions(&mut buffered, &schema, &config, &opts)?;

            Ok(InputPartitionOutput {
                encoded,
                in_memory_stats: stats,
                spill_manager,
                schema,
                _reservation: reservation,
            })
        }
    }
}

/// Spills *all* buffered partitions: for each partition, materializes its
/// indices through `PartitionedBatchIterator` and appends each yielded batch
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
    batch_size: usize,
) -> Result<(u64, u64)> {
    if buffered.is_empty() {
        return Ok((0, 0));
    }
    let mut batches_written: u64 = 0;
    let mut bytes_written: u64 = 0;
    let (batches, indices) = buffered.take();
    for (partition_id, partition_indices) in indices.iter().enumerate() {
        if partition_indices.is_empty() {
            continue;
        }
        let iter = PartitionedBatchIterator::new(&batches, partition_indices, batch_size);
        for result in iter {
            let batch = result?;
            let written = spill_manager
                .spill(partition_id, &batch)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            batches_written += 1;
            bytes_written += written;
        }
    }
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
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SortShuffleWriterExec: partitioning={}",
                    self.shuffle_output_partitioning
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={}", self.shuffle_output_partitioning)
            }
        }
    }
}

impl ExecutionPlan for SortShuffleWriterExec {
    fn name(&self) -> &str {
        "SortShuffleWriterExec"
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

    /// This writer hashes rows itself rather than relying on an upstream
    /// `RepartitionExec`, so the partitioning expressions are its own.
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // `try_new` rejects every other scheme, so a non-`Hash` partitioning
        // here means the operator was built around that check.
        let Partitioning::Hash(exprs, _) = &self.shuffle_output_partitioning else {
            return internal_err!(
                "SortShuffleWriterExec only supports Hash partitioning, got: {:?}",
                self.shuffle_output_partitioning
            );
        };
        apply_expression_roots(exprs, f)
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

            Ok(Arc::new(SortShuffleWriterExec::try_new(
                self.job_id.clone(),
                self.stage_id,
                input,
                self.work_dir.clone(),
                self.shuffle_output_partitioning.clone(),
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
        let schema = result_schema();

        let mut state = self.state.lock().map_err(|_| {
            DataFusionError::Internal(
                "SortShuffleWriterExec state mutex poisoned".to_owned(),
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
                run_coordinator(writer, ctx, senders).await;
            });
        }

        let rx = state
            .handoffs
            .get_mut(partition)
            .and_then(Option::take)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "SortShuffleWriterExec: execute({partition}) called twice or out of range (have {} partitions)",
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
                        "SortShuffleWriterExec coordinator dropped without sending"
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

impl ShuffleWriter for SortShuffleWriterExec {
    fn job_id(&self) -> &JobId {
        &self.job_id
    }

    fn stage_id(&self) -> usize {
        self.stage_id
    }

    fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        Some(&self.shuffle_output_partitioning)
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
            self.shuffle_output_partitioning,
            printable_plan
        )
    }
}

/// Computes per-row output partition assignments for hash partitioning.
///
/// Fills `out` with `num_partitions` entries, where entry `p` lists the
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
    num_partitions: usize,
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

    out.resize_with(num_partitions, Vec::new);
    for rows in out.iter_mut() {
        rows.clear();
    }
    for (row, &h) in hash_buffer.iter().enumerate() {
        out[(h % num_partitions as u64) as usize].push(row as u32);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BallistaError;
    use crate::execution_plans::ChaosExec;
    use datafusion::arrow::array::{StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::prelude::SessionContext;
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
        compute_partition_indices(&batch, &exprs, 8, &mut hash_buffer, &mut result)
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
        compute_partition_indices(&batch, &exprs, 4, &mut hash_buffer, &mut ours)
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
}
