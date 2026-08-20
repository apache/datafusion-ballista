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

//! Cross-partition state merge for windowed aggregates in an AQE range-shuffle
//! pipeline.
//!
//! Range-shuffle produces `N` ordered disjoint partitions of a stream sorted
//! by the window's ORDER BY key. Each executor task then runs a windowed
//! aggregate over its slice, producing correct per-row values *within* the
//! slice but not across slices — task `k`'s running SUM starts at zero, not
//! at the sum of everything in partitions `[0..k)`.
//!
//! # Division of labor
//!
//! The prefix-merge splits cleanly across the scheduler/executor boundary:
//!
//! - **Scheduler (global step):** collects each upstream task's finalized
//!   [`Accumulator::state`] via task-status transport, then computes the
//!   prefix-merge — for each partition `k`, combining the individual states
//!   of partitions `[0..k)` into a single already-merged state per window
//!   expression. This is the step that requires global visibility across
//!   tasks; only the scheduler has it.
//! - **Executor (local step, this operator):** receives the *already-merged*
//!   state for its partition in its constructor and folds it row-wise into
//!   the window-aggregate columns. Aggregate-agnostic by construction — the
//!   fold is the same [`Accumulator::merge_batch`]-shaped composition the
//!   group-by `Partial → Final` protocol uses, so SUM adds, MIN/MAX take the
//!   extreme, sketches (KLL, TDigest, HLL) merge as sketches, without this
//!   operator knowing which aggregate is which.
//!
//! # Apply descriptors
//!
//! `try_new` takes a `Vec<`[`WindowApply`]`>` — one entry per output column
//! that needs cross-partition correction, telling the operator *how* to
//! rewrite that column. Two shapes:
//!
//! - [`WindowApply::Scalar`] — fast path. Combines each row's existing value
//!   with a scheduler-provided scalar via [`ScalarOp`] (`Add`/`Min`/`Max` for
//!   SUM/COUNT/MIN/MAX and `row_number`; `Overwrite` for `first_value` /
//!   `last_value`). No `Accumulator` constructed.
//! - [`WindowApply::Aggregate`] — fallback. Constructs a fresh `Accumulator`
//!   seeded from the pre-merged state, feeds `args` per row, overwrites the
//!   column with `evaluate()`. Fits AVG (without decomposition), sketch-backed
//!   windows (APPROX_DISTINCT, APPROX_QUANTILE), and statistical aggregates.
//!
//! Non-corrected window functions don't appear in `applies` at all. `lead` /
//! `lag` / `nth_value` are solved by halo rows in the shuffle layer.
//! `rank` / `dense_rank` / `percent_rank` / `cume_dist` / `ntile` need a
//! separate segment-tree-plus-broadcast design and are out of scope here.
//!
//! An empty `applies` list makes this operator a passthrough. A non-empty one
//! always rewrites its output columns, including when a state slot is `None` —
//! that seeds the accumulator with nothing rather than skipping the column, so
//! the result is the partition-local aggregate, not the input value.
//!
//! [`Accumulator::state`]: datafusion::logical_expr::Accumulator::state
//! [`Accumulator::merge_batch`]: datafusion::logical_expr::Accumulator::merge_batch

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use log::debug;
use parking_lot::Mutex;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, ScalarValue, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Accumulator, AggregateUDF};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::{
    ColumnarValue, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, StatisticsArgs,
    apply_expression_roots, statistics::ChildStats,
};
use futures::{Stream, StreamExt, ready};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::execution_plans::plan_algebra::{
    PartitionSliceable, slice_by_global_partition,
};

/// A single already-prefix-merged window-aggregate state. Indexed by window
/// expression (same order the upstream `BoundedWindowAggExec` reports in
/// `window_expr()`); `None` at a slot indicates a non-aggregate window
/// function (`row_number`, `rank`, `lead`/`lag`, ...) that contributes no
/// state. The inner `Vec<ScalarValue>` is whatever `Accumulator::state()`
/// returned for that aggregate (1 for SUM/COUNT/MIN/MAX, 2 for AVG's
/// `(sum, count)`, N for sketch-backed / higher-moment aggregates).
///
/// The scheduler produces one of these per input partition, having already
/// combined the individual states from every prior partition into one merged
/// value per window expression — see the `prefix_merge` module docs for the
/// division of labor. This operator applies it; it does not compute it.
///
/// A newtype rather than an alias for `Vec<Option<Vec<ScalarValue>>>`. That
/// type appears in this operator's public signatures, where it is neither
/// readable nor searchable, and it spells "no state for this window
/// expression" two ways — a missing index and a `None` — which every caller
/// then has to handle. [`Self::slot`] collapses both into one answer. Any
/// later change to the representation also stays internal rather than
/// breaking whoever wrote the concrete type.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FinalizedPartitionState {
    /// Indexed by position in the upstream operator's `window_expr()` list.
    per_window_expr: Vec<Option<Vec<ScalarValue>>>,
}

impl FinalizedPartitionState {
    /// Build from one slot per window expression, in `window_expr()` order.
    pub fn new(per_window_expr: Vec<Option<Vec<ScalarValue>>>) -> Self {
        Self { per_window_expr }
    }

    /// Merged state for `window_expr_index`, or `None` when that expression
    /// published none or the index is past the end.
    pub fn slot(&self, window_expr_index: usize) -> Option<&Vec<ScalarValue>> {
        self.per_window_expr
            .get(window_expr_index)
            .and_then(|slot| slot.as_ref())
    }

    /// Every slot in window-expression order, for wire encoding.
    pub fn slots(&self) -> &[Option<Vec<ScalarValue>>] {
        &self.per_window_expr
    }

    /// Number of window expressions this state covers.
    pub fn len(&self) -> usize {
        self.per_window_expr.len()
    }

    /// True when no window expression published state.
    pub fn is_empty(&self) -> bool {
        self.per_window_expr.is_empty()
    }
}

/// How to combine each row's existing value in an output column with a
/// scheduler-provided scalar offset. The result overwrites the column.
///
/// [`Overwrite`] ignores the row's existing value and just writes the offset;
/// it's the shape needed for `first_value` / `last_value`, where the scheduler
/// picks the correct global value once and every row gets a copy.
///
/// [`Overwrite`]: ScalarOp::Overwrite
///
/// `#[non_exhaustive]`: more ops will land (the ranking family, and whatever
/// a segment-tree correction needs), and each would otherwise be a breaking
/// change for anyone matching on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScalarOp {
    /// `output := row_value + offset`. Fits SUM, COUNT, and ranking functions
    /// like `row_number` (which are effectively `COUNT(*)`).
    Add,
    /// `output := min(row_value, offset)`. Fits MIN.
    Min,
    /// `output := max(row_value, offset)`. Fits MAX.
    Max,
    /// `output := offset`. Ignores `row_value`. Fits `first_value` over an
    /// ever-expanding frame: every row's answer is the same global first
    /// value, so the scheduler picks it once and every row gets a copy.
    ///
    /// Not `last_value`. Over an ever-expanding frame that is the current
    /// row's own value and needs no correction at all, so overwriting every
    /// row with one scalar would be wrong.
    Overwrite,
}

/// How to correct one window-function output column at row-apply time. Each
/// entry describes exactly one column PrefixMergeExec should rewrite.
///
/// Two shapes are covered by construction; anything else is out of scope
/// (`lead`/`lag`/`nth_value` are solved by halo rows in the shuffle layer, and
/// ranking-family functions like `rank`/`percent_rank`/`ntile` want a separate
/// segment-tree-plus-broadcast infrastructure).
/// `#[non_exhaustive]`: the two shapes here cover what the prefix rewrite
/// plants today, and a third (a segment-tree broadcast for the ranking
/// family) is anticipated.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WindowApply {
    /// Fast path: monoidal op between each row's existing value and a
    /// scheduler-provided scalar. No `Accumulator` is constructed.
    ///
    /// Fits every aggregate whose per-row output is itself a valid partial
    /// state that composes via a single scalar op — SUM, COUNT, MIN, MAX —
    /// plus ranking functions like `row_number` (offset = prior row count)
    /// and value-selection functions like `first_value` / `last_value`
    /// (offset = scheduler-picked global value; op = [`ScalarOp::Overwrite`]).
    Scalar {
        /// Combining op between `row_value` and `offset`.
        op: ScalarOp,
        /// One scalar per input partition. `offset[k]` combines with every
        /// row passing through partition `k`. Length must match the input's
        /// output partition count.
        offset: Vec<ScalarValue>,
        /// Column overwritten with `op(row_value, offset[partition])`.
        output_column: usize,
    },
    /// Fallback path: fresh `Accumulator` per partition, seeded with the
    /// merged offset state via [`Accumulator::merge_batch`], updated per row
    /// with `args` evaluated against that row, then [`Accumulator::evaluate`]
    /// overwrites `output_column`.
    ///
    /// Fits aggregates whose per-row output isn't a valid partial state:
    /// AVG (without decomposition), sketch-backed windows like
    /// APPROX_DISTINCT and APPROX_QUANTILE, and statistical aggregates like
    /// STDDEV / VAR / correlation whose state is a tuple of running moments.
    ///
    /// [`Accumulator::merge_batch`]: datafusion::logical_expr::Accumulator::merge_batch
    /// [`Accumulator::evaluate`]: datafusion::logical_expr::Accumulator::evaluate
    Aggregate {
        /// UDF used to construct a fresh `Accumulator` per partition.
        udf: Arc<AggregateUDF>,
        /// Aggregate's argument expressions, evaluated against each input row
        /// and fed to `Accumulator::update_batch`. For SUM/COUNT/MIN/MAX where
        /// re-running the accumulator is redundant, prefer the [`Scalar`]
        /// variant; this path is for cases where re-running is required.
        ///
        /// [`Scalar`]: WindowApply::Scalar
        args: Vec<Arc<dyn PhysicalExpr>>,
        /// Column overwritten with the accumulator's `evaluate()` result.
        output_column: usize,
        /// Position in the upstream `BoundedWindowAggExec`'s `window_expr()`
        /// list — the index into the inner `Vec` inside
        /// [`FinalizedPartitionState`] where this aggregate's merged offset
        /// state lives.
        window_expr_index: usize,
    },
}

impl WindowApply {
    fn output_column(&self) -> usize {
        match self {
            WindowApply::Scalar { output_column, .. }
            | WindowApply::Aggregate { output_column, .. } => *output_column,
        }
    }
}

/// Apply pre-merged window-aggregate state (computed by the scheduler) to
/// each row of the current partition's output. See the `prefix_merge` module
/// docs for the division of labor between scheduler and executor and the AQE
/// pipeline this fits into.
///
/// [`WindowApply::Aggregate`] entries are applied per row via a seeded
/// `Accumulator`; [`WindowApply::Scalar`] entries are applied per batch via
/// arrow kernels.
pub struct PrefixMergeExec {
    input: Arc<dyn ExecutionPlan>,
    /// One entry per window-function output column that needs cross-partition
    /// correction. Non-corrected columns (e.g. `lead`/`lag` handled by halos,
    /// or ranking functions left to segment-tree infrastructure) don't appear
    /// here.
    applies: Vec<WindowApply>,
    /// `per_partition_state[k]` is the *already-merged* state summarising
    /// every input partition in `[0..k)`. Only consumed by
    /// [`WindowApply::Aggregate`] entries — [`WindowApply::Scalar`] carries
    /// its own offsets. Length equals
    /// `input.output_partitioning().partition_count()`.
    ///
    /// Late-bound: `None` until [`PrefixMergeExec::resolve_state`]. The rule
    /// plants this operator at plan time, but the state only exists once
    /// every upstream task has closed its window and published accumulator
    /// state. `execute` refuses while unresolved rather than treating an
    /// absent carry-in as zero, which would silently emit partition-local
    /// aggregates.
    per_partition_state: Arc<Mutex<Option<Vec<FinalizedPartitionState>>>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl PrefixMergeExec {
    /// Wrap `input` with per-column apply descriptors, state pending.
    ///
    /// The rewrite rule's path: the operator is planted at plan time and the
    /// scheduler calls [`Self::resolve_state`] once the upstream stage's
    /// tasks have reported.
    pub fn try_new_pending(
        input: Arc<dyn ExecutionPlan>,
        applies: Vec<WindowApply>,
    ) -> Result<Self> {
        Self::try_new_inner(input, applies, None)
    }

    /// Wrap `input` with state already known — wire decode, and
    /// task-restriction, which slices state parallel to the input.
    pub fn try_new_resolved(
        input: Arc<dyn ExecutionPlan>,
        applies: Vec<WindowApply>,
        per_partition_state: Vec<FinalizedPartitionState>,
    ) -> Result<Self> {
        Self::try_new_inner(input, applies, Some(per_partition_state))
    }

    /// Errors on any of:
    /// - `per_partition_state.len()` != input's partition count, when given.
    /// - Any [`WindowApply::Scalar`]'s `offset.len()` != input's partition
    ///   count.
    /// - Any entry's `output_column` outside the input schema's field range.
    fn try_new_inner(
        input: Arc<dyn ExecutionPlan>,
        applies: Vec<WindowApply>,
        per_partition_state: Option<Vec<FinalizedPartitionState>>,
    ) -> Result<Self> {
        let partition_count = input.output_partitioning().partition_count();
        if let Some(state) = per_partition_state.as_ref()
            && state.len() != partition_count
        {
            return internal_err!(
                "PrefixMergeExec: per_partition_state.len() {} does not match \
                 input partition count {}",
                state.len(),
                partition_count
            );
        }
        let field_count = input.schema().fields().len();
        for (i, apply) in applies.iter().enumerate() {
            let col = apply.output_column();
            if col >= field_count {
                return internal_err!(
                    "PrefixMergeExec: applies[{i}] output_column {col} out of \
                     range (schema has {field_count} fields)"
                );
            }
            if let WindowApply::Scalar { offset, .. } = apply
                && offset.len() != partition_count
            {
                return internal_err!(
                    "PrefixMergeExec: applies[{i}] Scalar offset.len() {} does \
                     not match input partition count {}",
                    offset.len(),
                    partition_count
                );
            }
        }
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            applies,
            per_partition_state: Arc::new(Mutex::new(per_partition_state)),
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Bind the prefix state. Called by the scheduler once the upstream
    /// stage's tasks have all reported their finalized accumulator state and
    /// it has been prefix-merged into one carry-in per partition.
    ///
    /// Idempotent overwrite, matching `RangeFilterExec::resolve_bounds`: AQE
    /// re-plans, and a later pass may resolve the same operator again with
    /// the same values.
    pub fn resolve_state(&self, state: Vec<FinalizedPartitionState>) -> Result<()> {
        let partition_count = self.input.output_partitioning().partition_count();
        if state.len() != partition_count {
            return internal_err!(
                "PrefixMergeExec: resolve_state got {} entries for {} input \
                 partitions",
                state.len(),
                partition_count
            );
        }
        debug!(
            "PrefixMergeExec: resolved prefix state for {} partitions: {:?}",
            state.len(),
            state
        );
        self.per_partition_state.lock().replace(state);
        Ok(())
    }

    /// Per-column apply descriptors. `applies()[i]` corresponds to one
    /// output column that will be rewritten by the prefix-merge.
    pub fn applies(&self) -> &[WindowApply] {
        &self.applies
    }

    /// This operator's input. The scheduler descends from here to find the
    /// state-sync boundary and the window operator whose expressions the
    /// reported state is indexed against.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The prefix state carried per input partition, or `None` before
    /// [`Self::resolve_state`]. Only consumed by [`WindowApply::Aggregate`]
    /// entries.
    pub fn per_partition_state(&self) -> Option<Vec<FinalizedPartitionState>> {
        self.per_partition_state.lock().clone()
    }
}

impl Debug for PrefixMergeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefixMergeExec")
            .field(
                "partition_count",
                &self.per_partition_state.lock().as_ref().map(|s| s.len()),
            )
            .field("applies", &self.applies.len())
            .finish()
    }
}

impl DisplayAs for PrefixMergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PrefixMergeExec: partitions={}, applies={}",
                    self.per_partition_state
                        .lock()
                        .as_ref()
                        .map_or(-1_i64, |s| s.len() as i64),
                    self.applies.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "PrefixMergeExec")
            }
        }
    }
}

impl PartitionSliceable for PrefixMergeExec {
    /// Both the per-partition state and every [`WindowApply::Scalar`]'s
    /// offsets are indexed by global input partition, so both slice parallel
    /// to the input. [`WindowApply::Aggregate`] carries `window_expr_index`,
    /// which indexes window expressions rather than partitions, so it rides
    /// over unchanged.
    fn slice_to_partitions(
        &self,
        child: Arc<dyn ExecutionPlan>,
        partitions: &[usize],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let applies = self
            .applies
            .iter()
            .map(|apply| match apply {
                WindowApply::Scalar {
                    op,
                    offset,
                    output_column,
                } => Ok(WindowApply::Scalar {
                    op: *op,
                    offset: slice_by_global_partition(
                        offset,
                        partitions,
                        "PrefixMergeExec",
                        "scalar offsets",
                    )?,
                    output_column: *output_column,
                }),
                aggregate => Ok(aggregate.clone()),
            })
            .collect::<Result<Vec<_>>>()?;
        let state = self.per_partition_state.lock().clone().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "PrefixMergeExec: task-restriction before resolve_state()".into(),
            )
        })?;
        Ok(Arc::new(Self::try_new_resolved(
            child,
            applies,
            slice_by_global_partition(
                &state,
                partitions,
                "PrefixMergeExec",
                "state slots",
            )?,
        )?))
    }
}

impl ExecutionPlan for PrefixMergeExec {
    fn name(&self) -> &str {
        "PrefixMergeExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// Only [`WindowApply::Aggregate`] holds expressions. The scalar path's
    /// offsets are already-evaluated [`ScalarValue`]s baked in by the
    /// scheduler, so there is nothing there for a rewriter to visit.
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(
            self.applies.iter().flat_map(|apply| match apply {
                WindowApply::Aggregate { args, .. } => args.as_slice(),
                WindowApply::Scalar { .. } => [].as_slice(),
            }),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!(
                "PrefixMergeExec expects exactly one child, got {}",
                children.len()
            );
        };
        let rebuilt = match self.per_partition_state.lock().clone() {
            Some(state) => PrefixMergeExec::try_new_resolved(
                input.clone(),
                self.applies.clone(),
                state,
            )?,
            None => {
                PrefixMergeExec::try_new_pending(input.clone(), self.applies.clone())?
            }
        };
        Ok(Arc::new(rebuilt))
    }

    /// Passthrough: no distribution requirement on the child.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    /// Passthrough: no ordering requirement on the child. In practice the
    /// upstream range-shuffle already delivers sorted-by-ORDER-BY input; the
    /// merge doesn't reorder rows within a partition.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    /// Each output row corresponds 1:1 to an input row; the merge only
    /// rewrites the window-aggregate columns.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    /// Row count and per-column stats pass through unchanged: the merge
    /// rewrites values in the window-aggregate columns but adds no rows.
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Every input row is emitted exactly once.
    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let resolved = self.per_partition_state.lock().clone();
        // Refuse rather than treat an absent carry-in as zero: that would
        // emit partition-local aggregates that look plausible and are wrong.
        let Some(resolved) = resolved else {
            return internal_err!(
                "PrefixMergeExec: execute() called before resolve_state()"
            );
        };
        if partition >= resolved.len() {
            return internal_err!(
                "PrefixMergeExec: partition {} out of bounds ({} slots)",
                partition,
                resolved.len()
            );
        }
        let input_schema = self.input.schema();
        let output_schema = self.schema();

        // Indexed by window expression only. The scheduler rejects any
        // report carrying a PARTITION BY key, so at most one group per
        // partition reaches here and there is no key dimension to project
        // away.
        let key_state = &resolved[partition];

        let mut appliers: Vec<PreparedApply> = Vec::with_capacity(self.applies.len());
        for (i, apply) in self.applies.iter().enumerate() {
            match apply {
                WindowApply::Aggregate {
                    udf,
                    args,
                    output_column,
                    window_expr_index,
                } => {
                    let offset_state = key_state.slot(*window_expr_index);
                    appliers.push(PreparedApply::Aggregate(AggregateApply::new(
                        i,
                        udf,
                        args,
                        *output_column,
                        &input_schema,
                        offset_state,
                    )?));
                }
                WindowApply::Scalar {
                    op,
                    offset,
                    output_column,
                } => {
                    appliers.push(PreparedApply::Scalar(ScalarApply {
                        apply_index: i,
                        op: *op,
                        offset: offset[partition].clone(),
                        output_column: *output_column,
                    }));
                }
            }
        }

        let input = self.input.execute(partition, ctx)?;
        let stream = ApplyStream {
            input,
            appliers,
            schema: Arc::clone(&output_schema),
            baseline: BaselineMetrics::new(&self.metrics, partition),
            path_metrics: PathMetrics {
                scalar_time: MetricBuilder::new(&self.metrics)
                    .subset_time("scalar_apply_time", partition),
                aggregate_time: MetricBuilder::new(&self.metrics)
                    .subset_time("aggregate_apply_time", partition),
                rows_corrected: MetricBuilder::new(&self.metrics)
                    .counter("rows_corrected", partition),
            },
        };
        Ok(Box::pin(stream))
    }
}

/// One [`WindowApply`] entry prepared for a specific input partition: the
/// scheduler's offset has been resolved down to a single value (or a seeded
/// `Accumulator`) that can be applied per batch.
enum PreparedApply {
    Scalar(ScalarApply),
    Aggregate(AggregateApply),
}

impl PreparedApply {
    fn apply(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        match self {
            PreparedApply::Scalar(s) => s.apply(batch),
            PreparedApply::Aggregate(a) => a.apply(batch),
        }
    }
}

/// A single [`WindowApply::Scalar`] prepared for one input partition: the
/// scheduler's per-partition offset has been narrowed down to a single
/// [`ScalarValue`] and the combining `op` is applied batch-at-a-time via
/// arrow kernels.
struct ScalarApply {
    apply_index: usize,
    op: ScalarOp,
    offset: ScalarValue,
    output_column: usize,
}

impl ScalarApply {
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        use datafusion::arrow::compute::kernels::{cmp, numeric, zip};

        if self.output_column >= batch.num_columns() {
            return internal_err!(
                "PrefixMergeExec: applies[{}] output_column {} out of range \
                 at execute time (batch has {} columns)",
                self.apply_index,
                self.output_column,
                batch.num_columns()
            );
        }
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch);
        }
        let col = batch.column(self.output_column);
        let offset_arr: ArrayRef = self.offset.to_array_of_size(num_rows)?;
        let new_col: ArrayRef = match self.op {
            ScalarOp::Add => numeric::add(col, &offset_arr)?,
            ScalarOp::Min => {
                // element-wise min: keep `col` where col ≤ offset, else offset
                let mask = cmp::lt_eq(col, &offset_arr)?;
                zip::zip(&mask, col, &offset_arr)?
            }
            ScalarOp::Max => {
                // element-wise max: keep `col` where col ≥ offset, else offset
                let mask = cmp::gt_eq(col, &offset_arr)?;
                zip::zip(&mask, col, &offset_arr)?
            }
            ScalarOp::Overwrite => offset_arr,
        };
        let mut columns = batch.columns().to_vec();
        columns[self.output_column] = new_col;
        rebuild_batch(
            batch.schema(),
            columns,
            self.apply_index,
            self.output_column,
        )
    }
}

/// A single [`WindowApply::Aggregate`] prepared for a specific input
/// partition: the Accumulator has already been seeded from the offset state.
struct AggregateApply {
    /// Position of the source `WindowApply` in the exec's `applies` list —
    /// carried through so error messages can point at the offender.
    apply_index: usize,
    accumulator: Box<dyn Accumulator>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    output_column: usize,
}

impl AggregateApply {
    fn new(
        apply_index: usize,
        udf: &Arc<AggregateUDF>,
        args: &[Arc<dyn PhysicalExpr>],
        output_column: usize,
        input_schema: &SchemaRef,
        offset_state: Option<&Vec<ScalarValue>>,
    ) -> Result<Self> {
        let agg_expr = AggregateExprBuilder::new(Arc::clone(udf), args.to_vec())
            .schema(Arc::clone(input_schema))
            .alias(format!("prefix_merge_apply_{apply_index}"))
            .build()?;
        let mut accumulator = agg_expr.create_accumulator()?;
        if let Some(state_scalars) = offset_state {
            let offset_arrays: Vec<ArrayRef> = state_scalars
                .iter()
                .map(|s| s.to_array_of_size(1))
                .collect::<Result<Vec<_>>>()?;
            accumulator.merge_batch(&offset_arrays)?;
        }
        Ok(Self {
            apply_index,
            accumulator,
            args: args.to_vec(),
            output_column,
        })
    }

    /// Evaluate `args` against `batch`, replay them through `accumulator`
    /// row by row, and overwrite `output_column` with the accumulator's
    /// per-row `evaluate()` result.
    fn apply(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch);
        }
        let arg_arrays: Vec<ArrayRef> = self
            .args
            .iter()
            .map(|expr| match expr.evaluate(&batch)? {
                ColumnarValue::Array(a) => Ok(a),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows),
            })
            .collect::<Result<Vec<_>>>()?;

        let mut new_values: Vec<ScalarValue> = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let row_args: Vec<ArrayRef> =
                arg_arrays.iter().map(|a| a.slice(i, 1)).collect();
            self.accumulator.update_batch(&row_args)?;
            new_values.push(self.accumulator.evaluate()?);
        }
        let new_column = ScalarValue::iter_to_array(new_values)?;

        if self.output_column >= batch.num_columns() {
            return internal_err!(
                "PrefixMergeExec: applies[{}] output_column {} out of range \
                 at execute time (batch has {} columns)",
                self.apply_index,
                self.output_column,
                batch.num_columns()
            );
        }
        let mut columns = batch.columns().to_vec();
        columns[self.output_column] = new_column;
        rebuild_batch(
            batch.schema(),
            columns,
            self.apply_index,
            self.output_column,
        )
    }
}

/// Time split by apply path, so the accumulator replay's cost is a number the
/// operator reports rather than something only a benchmark can see.
///
/// The two paths differ by more than a constant: [`WindowApply::Scalar`] is an
/// arrow kernel over a whole batch, while [`WindowApply::Aggregate`] seeds an
/// accumulator and replays every row through it. A sketch-heavy query pays the
/// second and a SUM-heavy one need not, which a single total would hide.
struct PathMetrics {
    scalar_time: Time,
    aggregate_time: Time,
    rows_corrected: Count,
}

struct ApplyStream {
    input: SendableRecordBatchStream,
    appliers: Vec<PreparedApply>,
    schema: SchemaRef,
    baseline: BaselineMetrics,
    path_metrics: PathMetrics,
}

impl Stream for ApplyStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Split the borrow once: the timer holds `baseline` while the loop
        // mutates `appliers` and reads `path_metrics`.
        let this = self.get_mut();
        let poll = match ready!(this.input.poll_next_unpin(cx)) {
            Some(Ok(mut batch)) => {
                let rows = batch.num_rows();
                let _timer = this.baseline.elapsed_compute().timer();
                for applier in &mut this.appliers {
                    // Charge each batch to the path that handled it, so the
                    // two stay separable when one query mixes them.
                    let path = match applier {
                        PreparedApply::Scalar(_) => &this.path_metrics.scalar_time,
                        PreparedApply::Aggregate(_) => &this.path_metrics.aggregate_time,
                    };
                    let timer = path.timer();
                    let applied = applier.apply(batch);
                    timer.done();
                    batch = match applied {
                        Ok(b) => b,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                }
                if !this.appliers.is_empty() {
                    this.path_metrics.rows_corrected.add(rows);
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        };
        this.baseline.record_poll(poll)
    }
}

impl RecordBatchStream for ApplyStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Rebuild a batch with one column replaced, naming the apply responsible if
/// the new column's type no longer matches the schema.
///
/// Arrow's own error reports a type mismatch at a column index and nothing
/// about which correction produced it. An apply can drift from its column by
/// promotion in a scalar kernel, or by an accumulator's `evaluate()` widening
/// relative to the column it overwrites — both of which are about the apply,
/// not the batch.
///
/// # Arguments
///
/// * `schema` - the batch's schema, unchanged by the rewrite
/// * `columns` - columns with `output_column` already replaced
/// * `apply_index` - position in the exec's `applies` list, for the message
/// * `output_column` - the column that was replaced, for the message
fn rebuild_batch(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    apply_index: usize,
    output_column: usize,
) -> Result<RecordBatch> {
    let replaced = columns[output_column].data_type().clone();
    RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|e| {
        datafusion::common::DataFusionError::Internal(format!(
            "PrefixMergeExec: applies[{apply_index}] produced {replaced} for column \
             {output_column}, which the schema declares as {}: {e}",
            schema.field(output_column).data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    /// Metrics must separate the two apply paths. A single total would hide
    /// that a sketch-heavy query pays the accumulator replay while a
    /// SUM-heavy one need not, which is the thing worth knowing.
    #[tokio::test]
    async fn metrics_split_time_by_apply_path() -> Result<()> {
        use datafusion::physical_plan::common::collect;

        let exec = Arc::new(PrefixMergeExec::try_new_resolved(
            partitioned_source(1, 3),
            vec![WindowApply::Scalar {
                op: ScalarOp::Add,
                offset: vec![ScalarValue::Int64(Some(10))],
                output_column: 0,
            }],
            empty_state(1),
        )?);

        let ctx = datafusion::prelude::SessionContext::new().task_ctx();
        let _ = collect(exec.execute(0, ctx)?).await?;

        let metrics = exec.metrics().expect("operator must report metrics");
        let named = |name: &str| {
            metrics
                .iter()
                .find(|m| m.value().name() == name)
                .map(|m| m.value().as_usize())
        };
        assert_eq!(
            named("rows_corrected"),
            Some(3),
            "every row passing a non-empty applies list is corrected"
        );
        assert!(
            named("scalar_apply_time").is_some(),
            "the scalar path ran and must be timed separately: {metrics:?}"
        );
        assert!(
            named("aggregate_apply_time").is_some_and(|t| t == 0),
            "the aggregate path did not run and must read zero, not be absent: \
             {metrics:?}"
        );
        Ok(())
    }

    /// A drifted apply must name itself. Arrow reports a type mismatch at a
    /// column index and nothing about which correction caused it, which is
    /// the wrong half of the story when several applies rewrite one batch.
    #[test]
    fn rebuild_batch_names_the_drifted_apply() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("keep", DataType::Int64, false),
            Field::new("corrected", DataType::Float64, false),
        ]));
        // applies[3] wrote an Int64 over a Float64 column.
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
        ];
        let err = rebuild_batch(schema, columns, 3, 1)
            .expect_err("type drift must be rejected");
        let message = err.to_string();
        for expected in ["applies[3]", "column 1", "Int64", "Float64"] {
            assert!(
                message.contains(expected),
                "error must mention {expected}: {message}"
            );
        }
    }

    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;

    fn one_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// Two-partition memory source; partition `k` carries the single batch
    /// `[k * rows_per .. (k + 1) * rows_per)`.
    fn partitioned_source(partitions: usize, rows_per: usize) -> Arc<dyn ExecutionPlan> {
        let schema = one_col_schema();
        let mut per_partition: Vec<Vec<RecordBatch>> = Vec::with_capacity(partitions);
        for k in 0..partitions {
            let start = (k * rows_per) as i64;
            let arr = Int64Array::from_iter_values(start..start + rows_per as i64);
            per_partition.push(vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap(),
            ]);
        }
        let source = MemorySourceConfig::try_new(&per_partition, schema, None).unwrap();
        Arc::new(DataSourceExec::new(Arc::new(source)))
    }

    fn empty_state(partitions: usize) -> Vec<FinalizedPartitionState> {
        (0..partitions)
            .map(|_| FinalizedPartitionState::default())
            .collect()
    }

    /// Mismatched state length surfaces as an error rather than a panic at
    /// runtime.
    #[test]
    fn try_new_rejects_state_length_mismatch() {
        let input = partitioned_source(2, 3);
        let err = PrefixMergeExec::try_new_resolved(
            input,
            vec![],
            vec![FinalizedPartitionState::default()],
        )
        .expect_err("length mismatch must surface as an error");
        assert!(
            err.to_string()
                .contains("does not match input partition count"),
            "unexpected error: {err}"
        );
    }

    /// A [`WindowApply::Scalar`] whose `offset.len()` doesn't match the
    /// input partition count is caught at construction, before any rows flow.
    #[test]
    fn try_new_rejects_scalar_offset_length_mismatch() {
        let input = partitioned_source(2, 3);
        let apply = WindowApply::Scalar {
            op: ScalarOp::Add,
            offset: vec![ScalarValue::Int64(Some(0))], // only 1, need 2
            output_column: 0,
        };
        let err = PrefixMergeExec::try_new_resolved(input, vec![apply], empty_state(2))
            .expect_err("scalar offset length mismatch must surface as an error");
        assert!(
            err.to_string().contains("Scalar offset.len()"),
            "unexpected error: {err}"
        );
    }

    /// An `output_column` past the input schema's field count errors.
    #[test]
    fn try_new_rejects_output_column_out_of_range() {
        let input = partitioned_source(2, 3);
        let apply = WindowApply::Scalar {
            op: ScalarOp::Add,
            offset: vec![ScalarValue::Int64(Some(0)); 2],
            output_column: 5, // schema has 1 field
        };
        let err = PrefixMergeExec::try_new_resolved(input, vec![apply], empty_state(2))
            .expect_err("out-of-range output_column must surface as an error");
        assert!(
            err.to_string().contains("output_column 5 out of range"),
            "unexpected error: {err}"
        );
    }

    /// Runs the accumulator to completion over `values`, then returns the
    /// state — the shape a real upstream `BoundedWindowAggExec` reports for
    /// this window aggregate at partition close.
    fn approx_distinct_state(
        udf: &Arc<AggregateUDF>,
        input_schema: &SchemaRef,
        values: &[i64],
    ) -> Result<Vec<ScalarValue>> {
        let column: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("v", 0));
        let agg_expr = AggregateExprBuilder::new(Arc::clone(udf), vec![column])
            .schema(Arc::clone(input_schema))
            .alias("state_helper")
            .build()?;
        let mut acc = agg_expr.create_accumulator()?;
        let arr: ArrayRef =
            Arc::new(Int64Array::from_iter_values(values.iter().copied()));
        acc.update_batch(&[arr])?;
        acc.state()
    }

    /// End-to-end demonstration on the sketch case that motivates this whole
    /// design: cumulative APPROX_DISTINCT across a range-shuffled ordered
    /// stream, corrected per row by re-running the HLL accumulator seeded
    /// with the pre-merged state of every prior partition.
    ///
    /// Setup:
    /// - Partition 0's mock upstream output: `[(1,1), (2,2), (3,3)]` — v and
    ///   the local running distinct count.
    /// - Partition 1's mock upstream output: `[(4,1), (5,2), (6,3)]` — again
    ///   local, so wrong globally: the running distinct at the last row
    ///   should be 6, not 3.
    /// - The scheduler collects partition 0's terminal Accumulator state
    ///   (HLL registers seeded with {1,2,3}) and hands it to partition 1 as
    ///   its offset. Partition 0 gets an empty offset (nothing before it).
    ///
    /// Expected corrected output: `[(1,1),(2,2),(3,3)]` on partition 0 and
    /// `[(4,4),(5,5),(6,6)]` on partition 1 — matches what a single BWAG on
    /// the concatenated `[1..6]` would produce.
    ///
    /// This is the case that a two-pass halo scheme can't handle without
    /// emitting HLL registers on every output row: recovering state from
    /// a running distinct-count scalar is not tractable. The side-channel
    /// design routes state around the data stream and applies it per row
    /// here.
    #[tokio::test]
    async fn approx_distinct_corrects_running_distinct_across_partitions() -> Result<()> {
        use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
        use datafusion::physical_expr::expressions::Column;

        // BWAG's output schema: original argument column + the running
        // approx-distinct column that the aggregate produced.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("running_distinct", DataType::UInt64, false),
        ]));
        let batch = |vs: &[i64], rs: &[u64]| -> RecordBatch {
            let v_col = Arc::new(Int64Array::from_iter_values(vs.iter().copied()));
            let r_col =
                Arc::new(datafusion::arrow::array::UInt64Array::from_iter_values(
                    rs.iter().copied(),
                ));
            RecordBatch::try_new(schema.clone(), vec![v_col, r_col]).unwrap()
        };
        // Local (wrong-globally) BWAG output per partition.
        let p0 = batch(&[1, 2, 3], &[1, 2, 3]);
        let p1 = batch(&[4, 5, 6], &[1, 2, 3]);
        let source =
            MemorySourceConfig::try_new(&[vec![p0], vec![p1]], schema.clone(), None)?;
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(source)));

        let udf = approx_distinct_udaf();
        // Partition 1's offset = the HLL state after ingesting {1,2,3}.
        let p0_state = approx_distinct_state(
            &udf,
            &Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            &[1, 2, 3],
        )?;
        // Partition 1 gets partition 0's HLL state at the one aggregate slot
        // (window_expr_index 0). Partition 0 gets an empty state — nothing
        // to merge in.
        let per_partition_state: Vec<FinalizedPartitionState> = vec![
            FinalizedPartitionState::default(),
            FinalizedPartitionState::new(vec![Some(p0_state)]),
        ];

        let apply = WindowApply::Aggregate {
            udf: Arc::clone(&udf),
            // Feed the accumulator with the original argument column `v`
            // from the batch (not the already-computed running_distinct).
            args: vec![Arc::new(Column::new("v", 0))],
            output_column: 1,
            window_expr_index: 0,
        };
        let exec = Arc::new(PrefixMergeExec::try_new_resolved(
            input,
            vec![apply],
            per_partition_state,
        )?);

        let ctx = SessionContext::new().task_ctx();

        // Partition 0: offset is empty, running_distinct output unchanged.
        let out_p0: Vec<RecordBatch> =
            exec.execute(0, ctx.clone())?.try_collect().await?;
        let p0_running = out_p0[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            p0_running,
            vec![1, 2, 3],
            "partition 0 with empty offset should preserve local running distinct"
        );

        // Partition 1: seeded with partition 0's HLL, running_distinct
        // corrected to the global cumulative count.
        let out_p1: Vec<RecordBatch> =
            exec.execute(1, ctx.clone())?.try_collect().await?;
        let p1_running = out_p1[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            p1_running,
            vec![4, 5, 6],
            "partition 1 seeded with partition-0 state should show \
             global-cumulative running distinct"
        );
        Ok(())
    }

    /// AVG's per-row output (`sum / count`) also isn't a valid partial state:
    /// you can't recover `(sum, count)` from a single mean, so a naive
    /// `ProjectionExec` adding a scalar offset would give the wrong answer.
    /// The seeded-`Accumulator` re-run handles it uniformly — same code path
    /// that worked for APPROX_DISTINCT.
    ///
    /// Setup mirrors the APPROX_DISTINCT test:
    /// - Partition 0's mock upstream output: `[(10, 10.0), (20, 15.0),
    ///   (30, 20.0)]` — v and the local running mean.
    /// - Partition 1's mock upstream output: `[(40, 40.0), (50, 45.0),
    ///   (60, 50.0)]` — again local, so wrong globally.
    /// - Partition 1's offset = AVG's terminal state after ingesting
    ///   `{10, 20, 30}` = `(sum=60, count=3)`.
    ///
    /// Expected corrected partition-1 running mean:
    /// - `(60+40)/(3+1) = 25.0`
    /// - `(100+50)/(4+1) = 30.0`
    /// - `(150+60)/(5+1) = 35.0`
    ///
    /// Which is what a single BWAG on the concatenated `[10..60 step 10]`
    /// would produce.
    #[tokio::test]
    async fn avg_corrects_running_mean_across_partitions() -> Result<()> {
        use datafusion::arrow::array::Float64Array;
        use datafusion::functions_aggregate::average::avg_udaf;
        use datafusion::physical_expr::expressions::Column;

        // DataFusion's `AvgAccumulator` is only wired up for Float64 (and
        // decimal / duration) inputs — the planner casts Int64 to Float64
        // before AVG in a real query. Use Float64 directly so the test
        // hits the standard accumulator without stitching a cast in.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Float64, false),
            Field::new("running_avg", DataType::Float64, false),
        ]));
        let batch = |vs: &[f64], means: &[f64]| -> RecordBatch {
            let v_col = Arc::new(Float64Array::from_iter_values(vs.iter().copied()));
            let m_col = Arc::new(Float64Array::from_iter_values(means.iter().copied()));
            RecordBatch::try_new(schema.clone(), vec![v_col, m_col]).unwrap()
        };
        // Local (wrong-globally) BWAG output per partition.
        let p0 = batch(&[10.0, 20.0, 30.0], &[10.0, 15.0, 20.0]);
        let p1 = batch(&[40.0, 50.0, 60.0], &[40.0, 45.0, 50.0]);
        let source =
            MemorySourceConfig::try_new(&[vec![p0], vec![p1]], schema.clone(), None)?;
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(source)));

        let udf = avg_udaf();
        // Partition 1's offset = AVG's terminal state after ingesting
        // partition 0's values. Constructed inline (not through the
        // Int64-only helper the APPROX_DISTINCT test uses) because AVG needs
        // Float64 args.
        let single_col_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let p0_state = {
            let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("v", 0));
            let agg_expr = AggregateExprBuilder::new(Arc::clone(&udf), vec![column])
                .schema(single_col_schema)
                .alias("avg_state_helper")
                .build()?;
            let mut acc = agg_expr.create_accumulator()?;
            let arr: ArrayRef =
                Arc::new(Float64Array::from_iter_values([10.0, 20.0, 30.0]));
            acc.update_batch(&[arr])?;
            acc.state()?
        };
        // Partition 1 gets partition 0's (sum, count) state at the one
        // aggregate slot; partition 0 has nothing to merge in.
        let per_partition_state: Vec<FinalizedPartitionState> = vec![
            FinalizedPartitionState::default(),
            FinalizedPartitionState::new(vec![Some(p0_state)]),
        ];

        let apply = WindowApply::Aggregate {
            udf: Arc::clone(&udf),
            args: vec![Arc::new(Column::new("v", 0))],
            output_column: 1,
            window_expr_index: 0,
        };
        let exec = Arc::new(PrefixMergeExec::try_new_resolved(
            input,
            vec![apply],
            per_partition_state,
        )?);

        let ctx = SessionContext::new().task_ctx();

        // Partition 0: no offset, output preserved.
        let out_p0: Vec<RecordBatch> =
            exec.execute(0, ctx.clone())?.try_collect().await?;
        let p0_avg = out_p0[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            p0_avg,
            vec![10.0, 15.0, 20.0],
            "partition 0 with empty offset should preserve local running mean"
        );

        // Partition 1: seeded with (sum=60, count=3), corrected per row.
        let out_p1: Vec<RecordBatch> =
            exec.execute(1, ctx.clone())?.try_collect().await?;
        let p1_avg = out_p1[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            p1_avg,
            vec![25.0, 30.0, 35.0],
            "partition 1 seeded with partition-0 AVG state should show \
             global-cumulative running mean"
        );
        Ok(())
    }

    /// The scalar case — SUM, applied via the fast path (`WindowApply::Scalar`
    /// with `ScalarOp::Add`, no `Accumulator` reconstructed). Rounds out the
    /// demo trio (SUM/AVG/APPROX_DISTINCT), covers the operator's arrow-kernel
    /// batch-arithmetic path, and shows the same shape of test works whether
    /// the aggregate needs a seeded accumulator or a plain add.
    ///
    /// - Partition 0's mock upstream output: `[(1,1),(2,3),(3,6)]` — v and
    ///   the local running sum.
    /// - Partition 1's mock upstream output: `[(4,4),(5,9),(6,15)]` — again
    ///   local, so wrong globally.
    /// - Partition offsets: `[0, 6]` — the scheduler prefix-summed the
    ///   per-task terminal sums (partition 0 had none prior; partition 1
    ///   inherits partition 0's total).
    ///
    /// Expected corrected running sums: partition 0 unchanged `[1,3,6]`,
    /// partition 1 corrected `[10,15,21]`.
    #[tokio::test]
    async fn sum_corrects_running_sum_across_partitions() -> Result<()> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("running_sum", DataType::Int64, false),
        ]));
        let batch = |vs: &[i64], sums: &[i64]| -> RecordBatch {
            let v_col = Arc::new(Int64Array::from_iter_values(vs.iter().copied()));
            let s_col = Arc::new(Int64Array::from_iter_values(sums.iter().copied()));
            RecordBatch::try_new(schema.clone(), vec![v_col, s_col]).unwrap()
        };
        let p0 = batch(&[1, 2, 3], &[1, 3, 6]);
        let p1 = batch(&[4, 5, 6], &[4, 9, 15]);
        let source =
            MemorySourceConfig::try_new(&[vec![p0], vec![p1]], schema.clone(), None)?;
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(source)));

        let apply = WindowApply::Scalar {
            op: ScalarOp::Add,
            offset: vec![ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(6))],
            output_column: 1,
        };
        let exec = Arc::new(PrefixMergeExec::try_new_resolved(
            input,
            vec![apply],
            empty_state(2),
        )?);

        let ctx = SessionContext::new().task_ctx();

        let out_p0: Vec<RecordBatch> =
            exec.execute(0, ctx.clone())?.try_collect().await?;
        let p0_sum = out_p0[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            p0_sum,
            vec![1, 3, 6],
            "partition 0 with offset 0 should preserve local running sum"
        );

        let out_p1: Vec<RecordBatch> =
            exec.execute(1, ctx.clone())?.try_collect().await?;
        let p1_sum = out_p1[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            p1_sum,
            vec![10, 15, 21],
            "partition 1 with offset 6 should show global-cumulative running sum"
        );
        Ok(())
    }
}
