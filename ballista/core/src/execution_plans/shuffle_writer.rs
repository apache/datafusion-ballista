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

//! ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
//! can be executed as one unit with each partition being executed in parallel. The output of each
//! partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
//! will use the ShuffleReaderExec to read these results.

use std::fmt::Debug;
use std::future::Future;
use std::iter::Iterator;
use std::sync::Arc;
use std::time::Instant;

use crate::JobId;
use crate::execution_plans::SortShuffleWriterExec;
use crate::execution_plans::create_shuffle_path;
use crate::extension::SessionConfigExt;
use crate::utils;

use crate::serde::protobuf::ShuffleWritePartition;
use crate::serde::scheduler::PartitionStats;
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};

use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures::TryStreamExt;

use datafusion::arrow::error::ArrowError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use log::debug;
use std::sync::Mutex;
use tokio::sync::oneshot;

use super::shuffle_writer_trait::ShuffleWriter;

/// Rule for translating a passthrough writer's local output-partition index
/// (`0..plan.output_partitioning().partition_count()`) to a global partition
/// id — the thing that ends up in `ShuffleWritePartition.partition_id` and
/// keys `PartitionLocation`s downstream.
#[derive(Debug, Clone)]
enum GlobalPartitionMap {
    /// The plan collapses to a single output partition (e.g.
    /// `SortPreservingMergeExec`). Every local index → global partition 0.
    Collapsed,
    /// The plan re-establishes a hash-partition K-space (e.g.
    /// `RepartitionExec::Hash(_, K)`). Local index == global (0..K).
    HashSpace,
    /// Nothing between the writer and the leaves rewrites partitioning —
    /// local index `i` is `slice[i]` globally. Empty when no slice was
    /// stamped, in which case local is used as-is (identity).
    PassThrough(Vec<usize>),
}

impl GlobalPartitionMap {
    fn resolve(&self, local: usize) -> u64 {
        match self {
            GlobalPartitionMap::Collapsed => 0,
            GlobalPartitionMap::HashSpace => local as u64,
            GlobalPartitionMap::PassThrough(slice) => {
                slice.get(local).copied().unwrap_or(local) as u64
            }
        }
    }
}

/// Walk from `plan` toward the leaves, stopping at the first operator that
/// determines the output partitioning shape:
///
/// - `SortPreservingMergeExec` → `Collapsed` (fan-in to 1).
/// - `RepartitionExec` producing a hash / round-robin K-space → `HashSpace`.
/// - Otherwise recurse into the sole child (Filter/Sort/Projection/… are
///   partitioning-preserving passthroughs).
/// - If we hit a leaf or a fan-in without recognising it, treat it as
///   passthrough (the caller passes `global_output_partition_ids`).
fn walk_child_partition_mapping(
    plan: &Arc<dyn ExecutionPlan>,
    global_output_partition_ids: &[usize],
) -> GlobalPartitionMap {
    if plan.downcast_ref::<SortPreservingMergeExec>().is_some() {
        return GlobalPartitionMap::Collapsed;
    }
    if let Some(repart) = plan.downcast_ref::<RepartitionExec>() {
        match repart.partitioning() {
            Partitioning::Hash(_, _) | Partitioning::RoundRobinBatch(_) => {
                return GlobalPartitionMap::HashSpace;
            }
            Partitioning::UnknownPartitioning(_) => {
                // RepartitionExec still exchanges rows and freshly numbers
                // its K outputs, so global_output_partition_ids[local] would be a
                // meaningless mapping. DataFusion's BatchPartitioner also
                // rejects this scheme (`not_impl_err!`), so reaching this
                // arm means an upstream invariant has broken — fail loudly.
                panic!(
                    "unexpected RepartitionExec::UnknownPartitioning \
                     in shuffle-writer child plan"
                );
            }
        }
    }
    let children = plan.children();
    if children.len() == 1 {
        return walk_child_partition_mapping(children[0], global_output_partition_ids);
    }
    GlobalPartitionMap::PassThrough(global_output_partition_ids.to_vec())
}

/// Compute the set of global output partition ids a task's writer will
/// produce, given the global *input* partition ids the task consumes.
///
/// This is called on the scheduler when preparing a `TaskDefinition` for
/// the wire. The executor-side writer then uses these ids directly rather
/// than re-walking the plan.
///
/// Two cases:
///
/// - `SortShuffleWriter(Hash(K))` — HashSpace by construction; the K-space
///   `[0..K-1]` is intrinsic. Input ids are irrelevant.
/// - `ShuffleWriter` — always passthrough; the child's plan shape decides:
///   - `SortPreservingMergeExec` in the child chain → `[0]` (collapse).
///   - `RepartitionExec(Hash|RoundRobin)` in the child chain → `[0..K-1]`.
///   - Otherwise (leaf-preserved partitioning) → the input ids as-is.
///
/// Any other plan root is treated as a passthrough of the input ids —
/// this is the compat crutch for tests and codepaths that call the
/// helper without a real writer at the root.
pub fn compute_global_output_partition_ids(
    stage_plan: &Arc<dyn ExecutionPlan>,
    global_input_partition_ids: &[usize],
) -> Vec<usize> {
    if let Some(w) = stage_plan.downcast_ref::<SortShuffleWriterExec>() {
        let Partitioning::Hash(_, k) = w.shuffle_output_partitioning() else {
            unreachable!("SortShuffleWriterExec is Hash-partitioned by construction");
        };
        return (0..*k).collect();
    }
    if stage_plan.downcast_ref::<ShuffleWriterExec>().is_some() {
        let children = stage_plan.children();
        let [child] = children.as_slice() else {
            unreachable!("ShuffleWriterExec always has exactly one child");
        };
        return match walk_child_partition_mapping(child, global_input_partition_ids) {
            GlobalPartitionMap::Collapsed => vec![0],
            GlobalPartitionMap::HashSpace => {
                let k = child.properties().output_partitioning().partition_count();
                (0..k).collect()
            }
            GlobalPartitionMap::PassThrough(ids) => ids,
        };
    }
    global_input_partition_ids.to_vec()
}

/// Default bounded-channel capacity for the async-to-blocking I/O bridge.
pub const DEFAULT_SHUFFLE_CHANNEL_CAPACITY: usize = 8;

/// Shared state for the coordinator+handoff pattern (see range_repartition.rs
/// upstream for the reference design). The first `execute(N)` call spawns a
/// single coordinator task that owns all K writes; every `execute(N)` call
/// takes its own `oneshot::Receiver` and awaits the summaries for partition N.
///
/// The receiver payload is a `Vec` so writers that produce multiple files per
/// output partition (e.g. `SortShuffleWriterExec` under a multi-partition
/// task, where each slice member yields one file each containing K logical
/// partitions) can hand the full set to a single `execute(N)` stream.
/// Passthrough / hash-repart writers produce at most one summary per slot.
struct WriterState {
    initialized: bool,
    /// One receiver per output partition. `execute(N)` takes `handoffs[N]`;
    /// the coordinator holds the matching sender and pushes the summaries
    /// once partition N's files are closed.
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

/// ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of each
/// partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
///
/// # Threading
///
/// One Ballista task holds one `ShuffleWriterExec`. The first `execute(k)`
/// call from the executor initializes K oneshot handoffs and spawns
/// `run_coordinator`, which invokes `execute_shuffle_write`. The writer only
/// ever passes its child's partitioning through (the child already has the
/// target K partitions), so `execute_shuffle_write`
/// spawns K concurrent tokio tasks — one per output partition — each
/// pulling `child.execute(k)` and streaming directly to
/// `data-{task_id}.arrow` for partition k. Each drain emits one summary;
/// the coordinator routes it to the oneshot whose slot matches the output
/// partition. `execute(k)` returns a stream that awaits its oneshot and
/// emits one metadata batch pointing at that single file.
///
/// K concurrent per-output drainers is the same fan-out shape as
/// DataFusion's `RepartitionExec`, applied at the writer boundary: the
/// writer never coordinates across output partitions in-process, and any
/// upstream operator (e.g. `DynamicRangeRepartitionExec`) that pushes to
/// all K senders must see all K drainers running concurrently — draining
/// one to EOF before the next would fill the undrained channels and
/// deadlock the scatter side.
///
/// Data flows bottom → top (child produces, executor consumes), matching
/// DataFusion's convention. M = K (the child preserves partition count).
/// Example: K=3.
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
///                │       each oneshot: 1 summary
///                │       pointing at ONE data file
///                └─────────────────┼─────────────────┘
///                                  │  route each summary
///                                  │  to its output slot
///                         ┌─────────────────┐
///                         │ run_coordinator │
///                         └─────────────────┘
///                                  ▲
///              ┌───────────────────┼───────────────────┐
///              │                   │                   │
///     ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
///     │  drain task    │  │  drain task    │  │  drain task    │
///     │   (out=0)      │  │   (out=1)      │  │   (out=2)      │
///     │ stream → disk  │  │ stream → disk  │  │ stream → disk  │
///     └────────────────┘  └────────────────┘  └────────────────┘
///              ▲                   ▲                   ▲
///              │                   │                   │
///      child.execute(0)    child.execute(1)    child.execute(2)
///
///                     K=3 child partitions
///          (passthrough: child preserves K, no repartitioning)
/// ```
///
/// File layout — K files, one per output partition:
///
/// ```text
///           .../{stage_id}/{partition_k}/data-{task_id}.arrow
/// ```
///
/// Contrast with [`SortShuffleWriterExec`]:
/// its M concurrent per-input writers produce M files, each holding K
/// buckets internally. Here K concurrent per-output drainers produce K
/// files, each holding exactly one output partition. Both writers expose
/// the same K-summary contract to the framework — the on-disk shape is
/// what differs, and downstream `ShuffleReaderExec` uses the summaries to
/// open whichever set of files is right.
///
/// This writer never repartitions: hash-repartition stages use
/// [`SortShuffleWriterExec`] instead, so the only scheme left here is
/// passthrough and the writer carries no output-partitioning of its own.
///
/// The coordinator + oneshot plumbing is a shared idiom with
/// [`SortShuffleWriterExec`]; passthrough alone doesn't structurally need
/// it — only the K concurrent drains, which are the real deadlock guard.
/// This could collapse to per-`execute(k)` eager K-spawn with `JoinHandle`
/// handoff, keeping the coordinator idiom only where it does real work
/// (SortShuffle's M×K summary re-bucketing).
#[derive(Debug)]
pub struct ShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: JobId,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    work_dir: String,
    /// Task id (the task's append-order slot in `RunningStage.task_infos`)
    /// used as `file_id` in shuffle paths so files from different tasks
    /// (including retries) don't collide. Seeded by the executor's
    /// `create_query_stage_exec`; defaults to 0 for plans that arrive from
    /// `try_new` directly (proto decode before executor stamping, or tests).
    task_id: usize,
    /// Global partition ids this task's restricted plan covers, in slice
    /// order. Position `i` in the child plan corresponds to
    /// `global_output_partition_ids[i]` globally.
    ///
    /// Consumed by the passthrough (`None`) branch when the plan is a straight
    /// pass-through of the slice. If the child plan contains a
    /// partitioning-resetting operator (SPM → 1 output, RepartitionExec::Hash
    /// → 0..K) the writer detects that at path-build time and uses `local`
    /// directly instead of `global_output_partition_ids[local]`.
    global_output_partition_ids: Vec<usize>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: Arc<PlanProperties>,
    /// Shared coordinator handoff state. Clones share the same Arc, so a
    /// clone of the writer produced by `with_new_children` participates in
    /// the same coordinator.
    state: Arc<Mutex<WriterState>>,
}

impl Clone for ShuffleWriterExec {
    fn clone(&self) -> Self {
        Self {
            job_id: self.job_id.clone(),
            stage_id: self.stage_id,
            plan: self.plan.clone(),
            work_dir: self.work_dir.clone(),
            task_id: self.task_id,
            global_output_partition_ids: self.global_output_partition_ids.clone(),
            metrics: self.metrics.clone(),
            properties: self.properties.clone(),
            state: self.state.clone(),
        }
    }
}

impl std::fmt::Display for ShuffleWriterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let printable_plan = DisplayableExecutionPlan::with_metrics(self.plan.as_ref())
            .set_show_statistics(true)
            .indent(false);
        write!(
            f,
            "ShuffleWriterExec: job={} stage={} work_dir={} plan: \n {}",
            self.job_id, self.stage_id, self.work_dir, printable_plan
        )
    }
}

#[derive(Debug, Clone)]
struct ShuffleWriteMetrics {
    /// Time spend writing batches to shuffle files
    write_time: metrics::Time,
    input_rows: metrics::Count,
    output_rows: metrics::Count,
}

impl ShuffleWriteMetrics {
    /// `input_partition` is the operator-local input partition index this
    /// bucket tracks (0..slice_len). Under multi-partition tasks, a single
    /// task builds K such buckets — one per operator-local input partition
    /// it drains — so the stage still sees K per-partition buckets total,
    /// exactly as it did before K-drain (K tasks × 1 bucket = 1 task × K
    /// buckets). The scheduler maps `input_partition` to a stage-global
    /// input partition id via `TaskDescription.global_input_partition_ids`.
    fn new(input_partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let write_time =
            MetricBuilder::new(metrics).subset_time("write_time", input_partition);

        let input_rows =
            MetricBuilder::new(metrics).counter("input_rows", input_partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(input_partition);

        Self {
            write_time,
            input_rows,
            output_rows,
        }
    }
}

impl ShuffleWriterExec {
    /// Create a new shuffle writer. `task_id` defaults to 0; the executor
    /// stamps the real value via [`Self::with_task_id`] at
    /// `create_query_stage_exec` time.
    pub fn try_new(
        job_id: JobId,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
    ) -> Result<Self> {
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
        // TODO: kill the default-identity slice. It's a compat crutch for
        // plans decoded straight from proto (before executor stamping) and
        // unit tests that don't set a slice. Once every construction path
        // calls `with_global_output_partition_ids`, drop the default and fold it into
        // `try_new`'s signature.
        let default_partition_slice: Vec<usize> = (0..output_partition_count).collect();
        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
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
    /// after decoding the plan so shuffle files from different tasks in
    /// the same stage don't collide on file_id.
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

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Get the input partition count
    pub fn input_partition_count(&self) -> usize {
        self.plan
            .properties()
            .output_partitioning()
            .partition_count()
    }

    /// Executes the shuffle write operation for this task.
    ///
    /// Returns `(handoff_idx, summary)` pairs. `handoff_idx` indexes into the
    /// coordinator's per-output-partition oneshot slots (0..K in this
    /// operator's output_partitioning). `summary.partition_id` is the
    /// **global** output partition id downstream will address, computed via
    /// `walk_child_partition_mapping` over the child plan — either
    /// `global_output_partition_ids[local]`, `local` (hash K-space), or `0`
    /// (collapsed / SPM).
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

            // Passthrough shuffle: drain each of the child's output
            // partitions into its own file. All K must drain
            // CONCURRENTLY, not sequentially — coordinating operators
            // below (DynamicRangeRepartitionExec) push to all K
            // senders from shared scatter tasks; draining one to EOF
            // before the next starts fills up the undrained channel
            // and deadlocks the scatter side.
            let num_partitions =
                plan.properties().output_partitioning().partition_count();
            let mut handles = Vec::with_capacity(num_partitions);
            for local_input_partition in 0..num_partitions {
                // Each drain owns its own metric bucket, keyed by the
                // operator-local input partition it drains. Passthrough
                // is 1:1 so local input == local output here.
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

                debug!("Writing results to {path:?}");

                let mut stream = plan.execute(local_input_partition, context.clone())?;
                handles.push(tokio::spawn(async move {
                    let stats = utils::write_stream_to_disk(
                        &mut stream,
                        path.as_path(),
                        &write_metrics.write_time,
                        channel_capacity,
                        compression_type,
                    )
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
                    let rows = stats.num_rows.unwrap_or(0) as usize;
                    write_metrics.input_rows.add(rows);
                    write_metrics.output_rows.add(rows);
                    Ok::<_, DataFusionError>((local_input_partition, stats))
                }));
            }

            let mut results = Vec::with_capacity(num_partitions);
            for handle in handles {
                let (local_input_partition, stats) = handle.await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "shuffle-write drain task panicked: {e}"
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
                "task_id {} drained {} partitions in {}s",
                task_id,
                num_partitions,
                now.elapsed().as_secs()
            );
            Ok(results)
        }
    }
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            // "None" is retained for plan-shape stability: this writer never
            // repartitions, so the value can only ever be None.
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ShuffleWriterExec: partitioning: None")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning=None")
            }
        }
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn name(&self) -> &str {
        "ShuffleWriterExec"
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

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            let input = children.pop().ok_or_else(|| {
                DataFusionError::Plan(
                    "Ballista ShuffleWriterExec expects single child".to_owned(),
                )
            })?;

            Ok(Arc::new(
                ShuffleWriterExec::try_new(
                    self.job_id.clone(),
                    self.stage_id,
                    input,
                    self.work_dir.clone(),
                )?
                .with_task_id(self.task_id)
                .with_global_output_partition_ids(
                    self.global_output_partition_ids.clone(),
                ),
            ))
        } else {
            Err(DataFusionError::Plan(
                "Ballista ShuffleWriterExec expects single child".to_owned(),
            ))
        }
    }

    /// Return the stream for output partition `partition`.
    ///
    /// The first call locks the shared state, initializes K oneshot
    /// channels, and spawns a single coordinator task that drives the
    /// write work for all K output partitions and sends each summary to
    /// its matching partition's oneshot sender. Subsequent calls take
    /// their receiver and return a stream that awaits it.
    ///
    /// Caller (typically `DefaultQueryStageExec::execute_query_stage`)
    /// should spawn all K `execute(N, ctx)` calls concurrently so the
    /// stream drains don't serialize.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = result_schema();

        let mut state = self.state.lock().map_err(|_| {
            DataFusionError::Internal("ShuffleWriterExec state mutex poisoned".to_owned())
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
                    "ShuffleWriterExec: execute({partition}) called twice or out of range (have {} partitions)",
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
                let summaries = rx
                    .await
                    .map_err(|_| {
                        DataFusionError::Internal(
                            "ShuffleWriterExec coordinator dropped without sending"
                                .to_owned(),
                        )
                    })?
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
                summaries_to_batch(
                    summaries,
                    schema_captured,
                    &work_dir,
                    &job_id,
                    stage_id,
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
            })
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.plan.partition_statistics(partition)
    }
}

impl ShuffleWriter for ShuffleWriterExec {
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

/// Internal metadata schema handed from a writer's coordinator up to
/// `drive_shuffle_writer_stage`. Purely a handoff shape — not persisted, not
/// sent to the scheduler. Shared between `ShuffleWriterExec` and
/// `SortShuffleWriterExec` so the executor can drive both via the same
/// `execute(N)` contract.
pub(crate) fn result_schema() -> SchemaRef {
    let stats = PartitionStats::default();
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt32, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("file_id", DataType::UInt64, true),
        stats.arrow_struct_repr(),
    ]))
}

/// Drives the shared write work for all K output partitions. Runs
/// `execute_shuffle_write` once, then routes each `(handoff_idx, summary)`
/// pair to the matching sender. Slots that never receive a summary (partition
/// produced no rows) are filled with an empty sentinel so their `execute(N)`
/// stream terminates cleanly.
///
/// On failure, sends the error to every sender so no waiting stream hangs.
async fn run_coordinator(
    writer: ShuffleWriterExec,
    ctx: Arc<TaskContext>,
    senders: Vec<oneshot::Sender<Result<Vec<ShuffleWritePartition>>>>,
) {
    let k = senders.len();
    let mut senders: Vec<Option<oneshot::Sender<Result<Vec<ShuffleWritePartition>>>>> =
        senders.into_iter().map(Some).collect();

    match writer.execute_shuffle_write(ctx).await {
        Ok(summaries) => {
            // Bucket per-file summaries by their handoff slot. Passthrough and
            // hash writers put at most one summary per slot; sort-based
            // writers put up to `slice.len()` (one per input file in the
            // slice). Slots with no summaries stay empty and the receiver's
            // execute(N) stream ends up emitting an empty metadata batch.
            let mut grouped: Vec<Vec<ShuffleWritePartition>> =
                (0..k).map(|_| Vec::new()).collect();
            for (handoff_idx, summary) in summaries {
                if handoff_idx < k {
                    grouped[handoff_idx].push(summary);
                }
            }
            for (slot, bucket) in senders.iter_mut().zip(grouped) {
                if let Some(sender) = slot.take() {
                    let _ = sender.send(Ok(bucket));
                }
            }
        }
        Err(e) => {
            let msg = format!("{e:?}");
            for (i, slot) in senders.iter_mut().enumerate() {
                if let Some(sender) = slot.take() {
                    let err_msg = if i == 0 {
                        msg.clone()
                    } else {
                        format!("shuffle writer failed: {msg}")
                    };
                    let _ = sender.send(Err(DataFusionError::Execution(err_msg)));
                }
            }
        }
    }
}

/// Build a metadata `MemoryStream` describing the files a single output
/// partition was written to (produced by the coordinator via oneshot). One
/// row per summary — sort-based writers put multiple summaries in a single
/// slot (one per input file the task drained), passthrough / hash writers put
/// at most one.
pub(crate) fn summaries_to_batch(
    summaries: Vec<ShuffleWritePartition>,
    schema: SchemaRef,
    work_dir: &str,
    job_id: &JobId,
    stage_id: usize,
) -> Result<MemoryStream> {
    let cap = summaries.len().max(1);
    let mut partition_builder = UInt32Builder::with_capacity(cap);
    let mut path_builder = StringBuilder::with_capacity(cap, cap * 64);
    let mut file_id_builder = UInt64Builder::with_capacity(cap);
    let mut num_rows_builder = UInt64Builder::with_capacity(cap);
    let mut num_batches_builder = UInt64Builder::with_capacity(cap);
    let mut num_bytes_builder = UInt64Builder::with_capacity(cap);

    for summary in &summaries {
        let path = create_shuffle_path(
            work_dir,
            job_id,
            stage_id,
            summary.partition_id as usize,
            summary.file_id,
            summary.is_sort_shuffle,
        )?
        .to_string_lossy()
        .to_string();

        partition_builder.append_value(summary.partition_id as u32);
        path_builder.append_value(path);
        match summary.file_id {
            Some(fid) => file_id_builder.append_value(fid),
            None => file_id_builder.append_null(),
        }
        num_rows_builder.append_value(summary.num_rows);
        num_batches_builder.append_value(summary.num_batches);
        num_bytes_builder.append_value(summary.num_bytes);
    }

    let partition_num: ArrayRef = Arc::new(partition_builder.finish());
    let path_arr: ArrayRef = Arc::new(path_builder.finish());
    let file_id_arr: ArrayRef = Arc::new(file_id_builder.finish());
    let field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
        Box::new(num_rows_builder),
        Box::new(num_batches_builder),
        Box::new(num_bytes_builder),
    ];
    let mut stats_builder = StructBuilder::new(
        PartitionStats::default().arrow_struct_fields(),
        field_builders,
    );
    for _ in &summaries {
        stats_builder.append(true);
    }
    let stats = Arc::new(stats_builder.finish());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![partition_num, path_arr, file_id_arr, stats],
    )?;

    debug!("SHUFFLE PARTITION METADATA:\n{batch:?}");

    MemoryStream::try_new(vec![batch], schema, None)
}

#[cfg(test)]
#[allow(dead_code, unused_imports)] // clippy false positive with local imports
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, StructArray, UInt32Array, UInt64Array};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    /// Spawn K parallel `plan.execute(N, ctx)` calls, collect each stream's
    /// batches, and return them concatenated in partition order. Mirrors
    /// what `DefaultQueryStageExec::execute_query_stage` does in production:
    /// the writer's coordinator only runs once every oneshot receiver has
    /// been taken.
    async fn drive_all_partitions(
        plan: Arc<ShuffleWriterExec>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let k = plan.properties().output_partitioning().partition_count();
        let mut handles = Vec::with_capacity(k);
        for n in 0..k {
            let plan = plan.clone();
            let ctx = task_ctx.clone();
            handles.push(tokio::spawn(async move {
                let mut stream = plan.execute(n, ctx)?;
                utils::collect_stream(&mut stream)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
            }));
        }
        let mut all = Vec::new();
        for h in handles {
            let batches = h.await.map_err(|e| {
                DataFusionError::Execution(format!("drive_all_partitions panic: {e}"))
            })??;
            all.extend(batches);
        }
        Ok(all)
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // No output partitioning: passthrough writer, one file per one of
        // the input plan's 2 partitions.
        let input_plan = create_input_plan()?;
        let work_dir = TempDir::new()?;
        let query_stage = Arc::new(ShuffleWriterExec::try_new(
            JobId::new("jobOne"),
            1,
            input_plan,
            work_dir.path().to_str().unwrap().to_owned(),
        )?);
        let batches = drive_all_partitions(query_stage, task_ctx).await?;
        // K=2 output partitions -> one metadata batch per execute(N) call
        // (drive_all_partitions collects one per K).
        assert_eq!(2, batches.len());
        for batch in &batches {
            assert_eq!(4, batch.num_columns());
            let path = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let f = path.value(row);
                assert!(
                    (0..2).any(|p| f.ends_with(&format!("/jobOne/1/{p}/data-0.arrow"))
                        || f.ends_with(&format!("\\jobOne\\1\\{p}\\data-0.arrow"))),
                    "unexpected shuffle file path: {f}"
                );
            }
        }

        let total: u64 = batches
            .iter()
            .flat_map(|b| {
                let stats = b.column(3).as_any().downcast_ref::<StructArray>().unwrap();
                let num_rows = stats
                    .column_by_name("num_rows")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .clone();
                (0..b.num_rows()).map(move |i| num_rows.value(i))
            })
            .sum();
        // Row conservation: 2 input partitions × 2 batches × 2 rows = 8.
        assert_eq!(8, total);

        Ok(())
    }

    #[tokio::test]
    async fn display_renders_child_operator_metrics() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            JobId::new("jobDisplay"),
            1,
            input_plan,
            work_dir.path().to_str().unwrap().to_owned(),
        )?;
        let mut stream = query_stage.execute(0, task_ctx)?;
        let _ = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

        let rendered = format!("{query_stage}");
        assert!(
            rendered.contains("metrics="),
            "expected child-operator metrics in rendered plan:\n{rendered}"
        );
        assert!(
            rendered.contains("elapsed_compute"),
            "expected populated elapsed_compute metric in rendered plan:\n{rendered}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_no_repart_write_failure_propagates() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // Place a directory at the data-{file_id}.arrow path so File::create
        // fails inside write_stream_to_disk, not at create_dir_all.
        // Path: work_dir / job_id / stage_id / partition / data-{file_id}.arrow
        // task_slot defaults to 0 in try_new, so file_id is 0.
        let tmp = tempfile::TempDir::new().unwrap();
        let data_arrow_dir = tmp
            .path()
            .join("jobOne")
            .join("1")
            .join("0")
            .join("data-0.arrow");
        std::fs::create_dir_all(&data_arrow_dir).unwrap();
        let work_dir = tmp.path().to_str().unwrap().to_owned();

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let query_stage = Arc::new(ShuffleWriterExec::try_new(
            "jobOne".into(),
            1,
            input_plan,
            work_dir,
        )?);
        let result = drive_all_partitions(query_stage, task_ctx).await;
        assert!(
            result.is_err(),
            "expected File::create failure in write_stream_to_disk to propagate"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_create_dir_failure_propagates() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // Place a regular file at the stage_id path component so
        // create_dir_all fails when the writer tries to create the
        // output partition subdirectory underneath it.
        // Path structure: work_dir / job_id / stage_id / ...
        let tmp = tempfile::TempDir::new().unwrap();
        let job_dir = tmp.path().join("jobOne");
        std::fs::create_dir_all(&job_dir).unwrap();
        let stage_id_as_file = job_dir.join("1");
        std::fs::File::create(&stage_id_as_file).unwrap();
        let work_dir = tmp.path().to_str().unwrap().to_owned();

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let query_stage = Arc::new(ShuffleWriterExec::try_new(
            "jobOne".into(),
            1,
            input_plan,
            work_dir,
        )?);
        let result = drive_all_partitions(query_stage, task_ctx).await;
        assert!(
            result.is_err(),
            "expected create_dir_all failure in writer to propagate"
        );
        Ok(())
    }

    fn create_input_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // define data.
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
}
