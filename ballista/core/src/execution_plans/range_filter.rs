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

//! Filter inputs with per-input-partition half-open bounds.
//!
//! `execute(k)` applies the predicate
//!
//! ```text
//!   raw_bounds[k].0 - halo_lo <= routing_expr < raw_bounds[k].1 + halo_hi
//! ```
//!
//! `None` on either bound means unbounded on that side (virtual ±∞). Zero halo
//! (`halo_lo == halo_hi == 0`) recovers the exact range-repartition trim used
//! above `ShuffleReaderExec` for hash-agg correctness; non-zero halo widens
//! each partition's read range to include a boundary "context" band
//! (`WindowFrame` PRECEDING/FOLLOWING for bounded RANGE frames
//!
//! # Separation of concerns
//!
//! RFE is a pure per-partition filter: the scheduler decides which
//! (input-partition → half-open cut range) mapping applies (see
//! `resolve_range_filter_bounds` in the AQE adapter) and hands the
//! **unwidened** ranges here. Halos live on RFE — the parallel-window
//! rewrite rule plants them at plan time — and RFE widens the incoming
//! ranges by its own halos at [`RangeFilterExec::resolve_bounds`] time. This means the
//! scheduler stays halo-blind at the RFE boundary.
//!
//! # Late-binding bounds
//!
//! `raw_bounds` is `Arc<Mutex<Option<Vec<...>>>>` — the ParallelWindow rewrite
//! rule plants a `RangeFilterExec` at plan time, well before the runtime cuts
//! are known. The scheduler calls [`RangeFilterExec::resolve_bounds`] after
//! stage 0's `RuntimeStatsExec` reports have been merged. `execute` refuses
//! while bounds are unresolved; serialization refuses too — over-the-wire
//! plans always ship with bounds bound.
//!
//! # Type generality
//!
//! `ScalarValue` throughout, including the fast path: bounds compare with
//! `PartialOrd` and the binary search addresses the array by index, so any
//! ordered type a `ScalarValue` holds works. Halo widening is typed
//! arithmetic, so a halo of a type the key cannot be widened by is refused
//! rather than coerced — a `Float64` halo against a `Timestamp` key is a
//! planner bug, not something to round into a grid. A zero halo widens by
//! nothing and so needs no arithmetic at all.

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::{
    EmptyRecordBatchStream, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, apply_expression_roots,
};
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt, ready};
use parking_lot::Mutex;

/// Half-open `[lo, hi)` bound for one input partition. `None` on either side
/// means unbounded (virtual ±∞).
pub type RangeBound = (Option<ScalarValue>, Option<ScalarValue>);

/// Bounds after halo widening. Same shape as [`RangeBound`]; the halo has
/// been folded in.
pub type WidenedBound = (Option<ScalarValue>, Option<ScalarValue>);

/// Both raw and widened bounds. `raw` is preserved for serialization; the
/// executor consumes `widened`.
struct BoundsState {
    raw: Vec<RangeBound>,
    widened: Vec<WidenedBound>,
}

/// Filter over an ordered input with a per-input-partition half-open range
/// predicate, widened by the operator's halo. Range logic (cuts → per-partition
/// half-open ranges → task-slice) lives scheduler-side; RFE is the runtime
/// filter that applies the resolved bounds.
pub struct RangeFilterExec {
    input: Arc<dyn ExecutionPlan>,
    routing_expr: Arc<dyn PhysicalExpr>,
    /// Lower halo — subtracted from each partition's `lo` at widen time.
    halo_lo: ScalarValue,
    /// Upper halo — added from each partition's `hi` at widen time.
    halo_hi: ScalarValue,
    /// Late-bound: `None` until [`RangeFilterExec::resolve_bounds`]; `execute` and serde
    /// refuse while unresolved.
    bounds: Arc<Mutex<Option<BoundsState>>>,
    /// True when `input.output_ordering()` leads with `routing_expr` in
    /// ascending order. Enables the min/max fast path + binary-search slice
    /// in [`RangeFilterStream`] — with a sorted input, per-batch first/last
    /// values bound the entire batch, so most batches never touch
    /// `filter_record_batch`.
    sorted_on_key: bool,
    /// Whether the input declares NULLs at the low end of the order. With
    /// [`Self::sorted_on_key`] it decides which single partition claims the
    /// whole NULL run — see `takes_nulls` in `execute`.
    nulls_first: bool,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl RangeFilterExec {
    /// Construct with bounds pending (rule path — the ParallelWindow rewrite
    /// plants the operator at plan time; the scheduler resolves bounds after
    /// stage 0's stats reports merge).
    ///
    /// # Arguments
    ///
    /// * `input` - upstream operator; its partition count fixes the eventual
    ///   `raw_bounds.len()`.
    /// * `routing_expr` - numeric physical expression each row is bucketed by.
    /// * `halo_lo`, `halo_hi` - non-negative widening amounts applied by
    ///   [`RangeFilterExec::resolve_bounds`]. Both must be finite Float64 today.
    pub fn try_new_pending(
        input: Arc<dyn ExecutionPlan>,
        routing_expr: Arc<dyn PhysicalExpr>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
    ) -> Result<Self> {
        Self::try_new_inner(input, routing_expr, halo_lo, halo_hi, None)
    }

    /// Construct with bounds already known. Used by wire decode and by
    /// task-restriction (task builder slices raw bounds parallel to the input
    /// restriction, then hands them here as a fresh operator).
    ///
    /// # Arguments
    ///
    /// * `input`, `routing_expr`, `halo_lo`, `halo_hi` - same as
    ///   [`Self::try_new_pending`].
    /// * `raw_bounds` - one half-open cut range per input partition. Widening
    ///   by halos happens internally; caller passes unwidened.
    pub fn try_new_resolved(
        input: Arc<dyn ExecutionPlan>,
        routing_expr: Arc<dyn PhysicalExpr>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
        raw_bounds: Vec<RangeBound>,
    ) -> Result<Self> {
        Self::try_new_inner(input, routing_expr, halo_lo, halo_hi, Some(raw_bounds))
    }

    fn try_new_inner(
        input: Arc<dyn ExecutionPlan>,
        routing_expr: Arc<dyn PhysicalExpr>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
        raw_bounds: Option<Vec<RangeBound>>,
    ) -> Result<Self> {
        let schema = input.schema();
        let expr_type = routing_expr.data_type(&schema)?;
        if !expr_type.is_numeric() && !expr_type.is_temporal() {
            return internal_err!(
                "RangeFilterExec: routing_expr must be numeric or temporal, got {expr_type}"
            );
        }
        validate_halo("halo_lo", &halo_lo)?;
        validate_halo("halo_hi", &halo_hi)?;
        let bounds_state = raw_bounds
            .map(|raw| build_bounds_state(&input, raw, &halo_lo, &halo_hi))
            .transpose()?;
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        let leading_sort = input
            .output_ordering()
            .map(|ord| ord.first().clone())
            .filter(|first| first.expr.as_ref() == routing_expr.as_ref());
        let sorted_on_key = leading_sort
            .as_ref()
            .is_some_and(|first| !first.options.descending);
        let nulls_first = match &leading_sort {
            Some(first) => first.options.nulls_first,
            // Where the run sits is a fact about the declared order, and there
            // isn't one — either no ordering at all, or an ordering on some
            // other expression. Defaulting would hand the run to whichever end
            // the default names, by accident rather than by decision.
            None if routing_expr.nullable(&schema)? => {
                return internal_err!(
                    "RangeFilterExec: routing_expr is nullable but the input declares no \
                     ordering on it, so which partition holds the NULL run is unknown"
                );
            }
            // Non-nullable: no run to place, so the value is never consulted.
            None => false,
        };
        Ok(Self {
            input,
            routing_expr,
            halo_lo,
            halo_hi,
            bounds: Arc::new(Mutex::new(bounds_state)),
            sorted_on_key,
            nulls_first,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Idempotent overwrite. Called by the scheduler once stage-0 sketches
    /// merge into cuts and the adapter has projected those cuts onto per-input
    /// partition half-open ranges. Widens by RFE's halos before caching.
    pub fn resolve_bounds(&self, raw_bounds: Vec<RangeBound>) -> Result<()> {
        let state =
            build_bounds_state(&self.input, raw_bounds, &self.halo_lo, &self.halo_hi)?;
        self.bounds.lock().replace(state);
        Ok(())
    }

    /// Snapshot the unwidened bounds. `None` before [`RangeFilterExec::resolve_bounds`];
    /// `Some` after. Callers that need the widened form should either call
    /// [`Self::widened_bounds`] or expand `raw_bounds[k] ± (halo_lo, halo_hi)`
    /// themselves.
    pub fn raw_bounds(&self) -> Option<Vec<RangeBound>> {
        self.bounds.lock().as_ref().map(|s| s.raw.clone())
    }

    /// Snapshot the halo-widened bounds — what the runtime filter actually
    /// applies. Convenience for callers that already have the halos and just
    /// want the resolved form.
    pub fn widened_bounds(&self) -> Option<Vec<WidenedBound>> {
        self.bounds.lock().as_ref().map(|s| s.widened.clone())
    }

    /// The physical expression whose value each row is bucketed by.
    pub fn routing_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.routing_expr
    }

    /// Halo-widening amount applied to each partition's lower bound.
    pub fn halo_lo(&self) -> &ScalarValue {
        &self.halo_lo
    }

    /// Halo-widening amount applied to each partition's upper bound.
    pub fn halo_hi(&self) -> &ScalarValue {
        &self.halo_hi
    }
}

impl Debug for RangeFilterExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeFilterExec")
            .field("routing_expr", &self.routing_expr.to_string())
            .field("halo_lo", &self.halo_lo)
            .field("halo_hi", &self.halo_hi)
            .field(
                "bounds",
                &self.bounds.lock().as_ref().map(|s| s.raw.clone()),
            )
            .finish()
    }
}

impl DisplayAs for RangeFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let bounds = self.bounds.lock();
                let bounds_str = match bounds.as_ref() {
                    Some(state) => format!("{:?}", state.raw),
                    None => "pending".to_string(),
                };
                write!(
                    f,
                    "RangeFilterExec: routing={}, halo=[{}, {}], bounds={}",
                    self.routing_expr, self.halo_lo, self.halo_hi, bounds_str
                )
            }
            DisplayFormatType::TreeRender => write!(f, "RangeFilterExec"),
        }
    }
}

impl ExecutionPlan for RangeFilterExec {
    fn name(&self) -> &str {
        "RangeFilterExec"
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

    /// The routing expression is evaluated per batch to test each row against
    /// the partition's cut range. The bounds themselves are `ScalarValue`s,
    /// not expressions.
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots([&self.routing_expr], f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!(
                "RangeFilterExec expects exactly one child, got {}",
                children.len()
            );
        };
        // Preserve any resolved bounds across the rewrite — a rewriter that
        // just re-parents shouldn't lose the eventual scheduler resolution.
        // Partition restriction is a separate concern (task_builder rebuilds
        // via try_new_resolved with a sliced raw_bounds).
        let raw_bounds = self.bounds.lock().as_ref().map(|s| s.raw.clone());
        Ok(Arc::new(Self::try_new_inner(
            input.clone(),
            self.routing_expr.clone(),
            self.halo_lo.clone(),
            self.halo_hi.clone(),
            raw_bounds,
        )?))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let widened = {
            let guard = self.bounds.lock();
            let state = guard.as_ref().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "RangeFilterExec: execute() called before resolve_bounds()".into(),
                )
            })?;
            state.widened.get(partition).cloned().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(format!(
                    "RangeFilterExec: partition {partition} out of bounds ({} bounds)",
                    state.widened.len()
                ))
            })?
        };
        let (lo, hi) = widened;
        // The NULL run belongs to whichever partition is unbounded at the end
        // the run occupies. An unbounded end only ever belongs to a global end
        // partition and survives the scheduler slicing bounds down to a task,
        // so this needs no global partition identity.
        let takes_nulls = if self.nulls_first {
            lo.is_none()
        } else {
            hi.is_none()
        };
        let predicate = build_predicate_from_bounds(
            self.routing_expr.clone(),
            lo.clone(),
            hi.clone(),
        );
        let schema = self.schema();
        let input = self.input.execute(partition, ctx)?;
        let fast_path = self.sorted_on_key.then(|| FastPathState {
            routing_expr: self.routing_expr.clone(),
            lo,
            hi,
        });
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let path_metrics = PathMetrics {
            fast_skip: MetricBuilder::new(&self.metrics)
                .counter("fast_skip_batches", partition),
            fast_pass: MetricBuilder::new(&self.metrics)
                .counter("fast_pass_batches", partition),
            fast_slice: MetricBuilder::new(&self.metrics)
                .counter("fast_slice_batches", partition),
            slow: MetricBuilder::new(&self.metrics).counter("slow_batches", partition),
            input_rows: MetricBuilder::new(&self.metrics)
                .counter("input_rows", partition),
        };
        let stream = RangeFilterStream {
            schema: schema.clone(),
            predicate,
            takes_nulls,
            input,
            fast_path,
            baseline,
            path_metrics,
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// A halo must be a non-negative, non-NULL width. Float halos are also checked
/// for finiteness: an infinite one widens every bound to cover everything and a
/// NaN one compares false against all of it.
fn validate_halo(name: &str, halo: &ScalarValue) -> Result<()> {
    if halo.is_null() {
        return internal_err!("RangeFilterExec: {name} must not be NULL");
    }
    // Only the float families have values that are neither a width nor
    // comparable: an infinity widens every bound to cover everything, a NaN
    // compares false against all of it.
    let finite = match halo {
        ScalarValue::Float64(Some(v)) => v.is_finite(),
        ScalarValue::Float32(Some(v)) => v.is_finite(),
        _ => true,
    };
    if !finite {
        return internal_err!("RangeFilterExec: {name} must be finite, got {halo}");
    }
    if halo < &ScalarValue::new_zero(&halo.data_type())? {
        return internal_err!("RangeFilterExec: {name} must be non-negative, got {halo}");
    }
    Ok(())
}

/// Whether widening by `halo` is a no-op. Worth asking before the arithmetic,
/// because a zero halo of one type must not refuse a key of another: the
/// scheduler passes `Float64(0.0)` for every consumer with no halo at all.
pub(crate) fn is_zero_halo(halo: &ScalarValue) -> Result<bool> {
    Ok(halo == &ScalarValue::new_zero(&halo.data_type())?)
}

/// `value` moved down by `halo`, or unchanged when the halo is zero. The
/// scheduler widens the same way when it routes whole files, so both live here
/// with the operator that defines what a halo means.
pub(crate) fn widen_below(
    value: &ScalarValue,
    halo: &ScalarValue,
) -> Result<ScalarValue> {
    if is_zero_halo(halo)? {
        return Ok(value.clone());
    }
    value.sub(halo)
}

/// `value` moved up by `halo`. Counterpart to [`widen_below`].
pub(crate) fn widen_above(
    value: &ScalarValue,
    halo: &ScalarValue,
) -> Result<ScalarValue> {
    if is_zero_halo(halo)? {
        return Ok(value.clone());
    }
    value.add(halo)
}

/// Validate + widen raw bounds. Emits a `BoundsState` with the raw preserved
/// for serialization and the widened ready for `execute`.
fn build_bounds_state(
    input: &Arc<dyn ExecutionPlan>,
    raw: Vec<RangeBound>,
    halo_lo: &ScalarValue,
    halo_hi: &ScalarValue,
) -> Result<BoundsState> {
    let partition_count = input.output_partitioning().partition_count();
    if raw.len() != partition_count {
        return internal_err!(
            "RangeFilterExec: raw_bounds.len() ({}) does not match input partition count ({partition_count})",
            raw.len()
        );
    }
    let widened = raw
        .iter()
        .map(|(lo, hi)| {
            let lo_w = lo
                .as_ref()
                .map(|lo| widen_below(lo, halo_lo))
                .transpose()?;
            let hi_w = hi
                .as_ref()
                .map(|hi| widen_above(hi, halo_hi))
                .transpose()?;
            if let (Some(l), Some(h)) = (&lo_w, &hi_w)
                && l > h
            {
                return internal_err!(
                    "RangeFilterExec: widened bound produced inverted [{l}, {h}) — check cuts + halo"
                );
            }
            Ok((lo_w, hi_w))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundsState { raw, widened })
}

/// Assemble the boolean `PhysicalExpr` predicate from a widened `[lo, hi)`.
/// Used both by [`RangeFilterStream`]'s slow path and by the tests that inspect
/// the generated expression tree.
fn build_predicate_from_bounds(
    routing_expr: Arc<dyn PhysicalExpr>,
    lo: Option<ScalarValue>,
    hi: Option<ScalarValue>,
) -> Arc<dyn PhysicalExpr> {
    let ge = |lo: ScalarValue| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            routing_expr.clone(),
            Operator::GtEq,
            Arc::new(Literal::new(lo)),
        ))
    };
    let lt = |hi: ScalarValue| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            routing_expr.clone(),
            Operator::Lt,
            Arc::new(Literal::new(hi)),
        ))
    };
    match (lo, hi) {
        (None, None) => Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        (None, Some(hi)) => lt(hi),
        (Some(lo), None) => ge(lo),
        (Some(lo), Some(hi)) => Arc::new(BinaryExpr::new(ge(lo), Operator::And, lt(hi))),
    }
}

/// Fast-path state. Present only when the input is sorted ascending on
/// `routing_expr` — then a batch's first and last routing values bound the
/// whole batch's value range, unlocking three shortcuts:
///
/// - `last < lo` or `first >= hi` — batch is entirely outside the partition's
///   window. Drop it.
/// - `first >= lo && last < hi` — batch is entirely inside. Pass it through
///   unchanged (Arc-clone).
/// - Otherwise — the window covers a prefix, suffix, or interior slice.
///   Binary-search the routing column for the slice bounds and
///   `RecordBatch::slice` (zero-copy).
///
/// Falls back to the general `filter_record_batch` path when the batch's
/// routing column contains nulls (Float64Array binary search would treat null
/// slots as garbage values).
struct FastPathState {
    routing_expr: Arc<dyn PhysicalExpr>,
    lo: Option<ScalarValue>,
    hi: Option<ScalarValue>,
}

/// Per-execute counters that split "which fast-path branch fired" so we can
/// tell — under a real workload — whether the sorted-input shortcuts are
/// actually firing or whether we've silently fallen through to
/// `filter_record_batch`. `input_rows` combined with `BaselineMetrics::output_rows`
/// gives the filter's drop ratio.
struct PathMetrics {
    fast_skip: Count,
    fast_pass: Count,
    fast_slice: Count,
    slow: Count,
    input_rows: Count,
}

struct RangeFilterStream {
    schema: SchemaRef,
    predicate: Arc<dyn PhysicalExpr>,
    /// Whether this partition claims the NULL run. See `execute`.
    takes_nulls: bool,
    input: SendableRecordBatchStream,
    fast_path: Option<FastPathState>,
    baseline: BaselineMetrics,
    path_metrics: PathMetrics,
}

impl RangeFilterStream {
    /// Apply the general predicate to `batch` — used both by the non-sorted
    /// fallback and by the sorted path when the routing column has nulls.
    ///
    /// A NULL key is absent from the comparison rather than failing it: the
    /// kernels leave its mask entry NULL and `filter_record_batch` reads that as
    /// exclude. Its place comes from the ordering instead, which is what
    /// `takes_nulls` carries.
    fn slow_filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.path_metrics.slow.add(1);
        let mask = self
            .predicate
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))?;
        let mask = as_boolean_array(&mask)?;
        let positioned: BooleanArray = if mask.null_count() == 0 {
            mask.clone()
        } else {
            mask.iter()
                .map(|inside| Some(inside.unwrap_or(self.takes_nulls)))
                .collect()
        };
        Ok(filter_record_batch(batch, &positioned)?)
    }

    /// Try the sorted-input shortcuts. Returns `None` iff the batch is
    /// entirely outside the window (drop). Returns `Some(batch)` with the
    /// selected rows otherwise.
    fn fast_filter(
        &self,
        state: &FastPathState,
        batch: RecordBatch,
    ) -> Result<Option<RecordBatch>> {
        let n = batch.num_rows();
        let arr = state
            .routing_expr
            .evaluate(&batch)
            .and_then(|v| v.into_array(n))?;
        // A NULL has a position in the ordering, not a comparison result, so
        // the shortcuts below cannot place it. The bail is load-bearing, not an
        // optimization: everything past it reads values by index and assumes
        // every index holds one.
        //
        // TODO: sorted input puts the run at one end, so the values occupy
        // `[null_count, n)` or `[0, n - null_count)`, and searching that span
        // leaves the run contiguous with the selection — the slice would stay
        // zero-copy where this copies. It makes plan-declared null placement
        // load-bearing for slicing, which this bail does not, so it wants the
        // counters above as its evidence.
        if arr.null_count() > 0 {
            let filtered = self.slow_filter(&batch)?;
            return Ok((filtered.num_rows() > 0).then_some(filtered));
        }
        let first = ScalarValue::try_from_array(&arr, 0)?;
        let last = ScalarValue::try_from_array(&arr, n - 1)?;
        // Skip: entire batch is outside the window.
        if state.hi.as_ref().is_some_and(|hi| &first >= hi)
            || state.lo.as_ref().is_some_and(|lo| &last < lo)
        {
            self.path_metrics.fast_skip.add(1);
            return Ok(None);
        }
        // Pass-through: entire batch is inside the window.
        let above_lo = state.lo.as_ref().is_none_or(|lo| &first >= lo);
        let below_hi = state.hi.as_ref().is_none_or(|hi| &last < hi);
        if above_lo && below_hi {
            self.path_metrics.fast_pass.add(1);
            return Ok(Some(batch));
        }
        // Mixed: partition the sorted column and slice.
        let start = match &state.lo {
            None => 0,
            Some(lo) => partition_point(&arr, n, lo)?,
        };
        let end = match &state.hi {
            None => n,
            Some(hi) => partition_point(&arr, n, hi)?,
        };
        if start >= end {
            self.path_metrics.fast_skip.add(1);
            return Ok(None);
        }
        self.path_metrics.fast_slice.add(1);
        Ok(Some(batch.slice(start, end - start)))
    }
}

/// Index of the first row of `arr` at or above `bound`, over an ascending
/// non-null column of `len` rows.
///
/// `slice::partition_point` over the array by index rather than over a typed
/// values buffer, so the shortcut works for any ordered type a `ScalarValue`
/// holds. Costs one `ScalarValue` per probe — 13 for an 8192-row batch, against
/// a linear pass over the whole thing.
fn partition_point(arr: &ArrayRef, len: usize, bound: &ScalarValue) -> Result<usize> {
    let mut below = 0;
    let mut above = len;
    while below < above {
        let probe = below + (above - below) / 2;
        if &ScalarValue::try_from_array(arr, probe)? < bound {
            below = probe + 1;
        } else {
            above = probe;
        }
    }
    Ok(below)
}

impl Stream for RangeFilterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = loop {
            // Poll child *before* starting the timer — otherwise time spent
            // waiting for upstream (shuffle IO, Parquet read) is billed to
            // this operator's elapsed_compute, hiding whether the filter
            // itself is fast or slow.
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.path_metrics.input_rows.add(batch.num_rows());
                    let timer = self.baseline.elapsed_compute().timer();
                    let filtered = match &self.fast_path {
                        Some(state) => match self.fast_filter(state, batch)? {
                            Some(b) => b,
                            None => {
                                timer.done();
                                continue;
                            }
                        },
                        None => {
                            let out = self.slow_filter(&batch)?;
                            if out.num_rows() == 0 {
                                timer.done();
                                continue;
                            }
                            out
                        }
                    };
                    timer.done();
                    break Poll::Ready(Some(Ok(filtered)));
                }
                Some(Err(e)) => break Poll::Ready(Some(Err(e))),
                None => {
                    let input_schema = self.input.schema();
                    self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                    break Poll::Ready(None);
                }
            }
        };
        self.baseline.record_poll(poll)
    }
}

impl RecordBatchStream for RangeFilterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::LexOrdering;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};
    use datafusion::prelude::SessionContext;

    fn v_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]))
    }

    fn v_source(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let schema = v_schema();
        let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&vec![vec![]; partitions], schema.clone(), None)
                .unwrap(),
        )));
        Arc::new(
            RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(partitions))
                .unwrap(),
        )
    }

    fn v_col() -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new_with_schema("v", v_schema().as_ref()).unwrap())
    }

    fn sorted_v_source(
        batches: Vec<RecordBatch>,
        options: SortOptions,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = v_schema();
        let sort_expr = PhysicalSortExpr::new(v_col(), options);
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let cfg = MemorySourceConfig::try_new(&[batches], schema.clone(), None).unwrap();
        let cfg = cfg.try_with_sort_information(vec![ordering]).unwrap();
        Arc::new(DataSourceExec::new(Arc::new(cfg)))
    }

    fn asc() -> SortOptions {
        SortOptions {
            descending: false,
            nulls_first: true,
        }
    }

    fn batch(values: &[f64]) -> RecordBatch {
        let arr = Float64Array::from(values.to_vec());
        RecordBatch::try_new(v_schema(), vec![Arc::new(arr)]).unwrap()
    }

    fn sv(v: f64) -> ScalarValue {
        ScalarValue::Float64(Some(v))
    }

    /// Build the K half-open ranges implied by K-1 cuts, ±∞ at ends.
    fn ranges_from_cuts(cuts: &[f64]) -> Vec<RangeBound> {
        let k = cuts.len() + 1;
        (0..k)
            .map(|i| {
                let lo = i.checked_sub(1).and_then(|j| cuts.get(j).copied()).map(sv);
                let hi = cuts.get(i).copied().map(sv);
                (lo, hi)
            })
            .collect()
    }

    /// Wrap `build_predicate_from_bounds` in a cuts-oriented helper so the
    /// predicate-shape tests below can stay expressed in "K partitions with
    /// these cuts" terms.
    fn predicate_for(
        cuts: &[f64],
        partition: usize,
        halo_lo: f64,
        halo_hi: f64,
    ) -> Arc<dyn PhysicalExpr> {
        let ranges = ranges_from_cuts(cuts);
        let (lo, hi) = &ranges[partition];
        let widen = |bound: &Option<ScalarValue>, halo: f64| {
            bound.as_ref().map(|s| match s {
                ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(*v + halo)),
                other => panic!("float64 only in these shape tests, got {other:?}"),
            })
        };
        build_predicate_from_bounds(v_col(), widen(lo, -halo_lo), widen(hi, halo_hi))
    }

    #[test]
    fn raw_bounds_len_must_match_input_partitions() {
        let src = v_source(3);
        let bounds = ranges_from_cuts(&[10.0]); // 2 partitions
        let err =
            RangeFilterExec::try_new_resolved(src, v_col(), sv(0.0), sv(0.0), bounds)
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("does not match input partition count"),
            "got: {err}"
        );
    }

    #[test]
    fn halo_must_be_non_negative() {
        let src = v_source(2);
        let bounds = ranges_from_cuts(&[5.0]);
        let err = RangeFilterExec::try_new_resolved(
            src.clone(),
            v_col(),
            sv(-1.0),
            sv(0.0),
            bounds.clone(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("halo_lo"));
        let err = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            sv(0.0),
            sv(f64::NAN),
            bounds,
        )
        .unwrap_err();
        assert!(err.to_string().contains("halo_hi"));
    }

    /// Bounds of any ordered type now resolve, and both paths select on them.
    /// A zero halo widens by nothing, so a `Float64(0.0)` halo does not refuse
    /// an `Int64` key — the scheduler passes that pair for every consumer with
    /// no halo at all.
    #[tokio::test]
    async fn non_float64_bounds_filter_on_both_paths() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let column: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("v", schema.as_ref()).unwrap());
        let rows: Vec<i64> = (0..20).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(datafusion::arrow::array::Int64Array::from(rows))],
        )
        .unwrap();
        let sorted = PhysicalSortExpr::new(
            column.clone(),
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        );

        for declare_sorted in [false, true] {
            let source =
                MemorySourceConfig::try_new(&[vec![batch.clone()]], schema.clone(), None)
                    .unwrap();
            let source = if declare_sorted {
                source
                    .try_with_sort_information(vec![[sorted.clone()].into()])
                    .unwrap()
            } else {
                source
            };
            let input: Arc<dyn ExecutionPlan> =
                Arc::new(DataSourceExec::new(Arc::new(source)));
            let rf = Arc::new(
                RangeFilterExec::try_new_resolved(
                    input,
                    column.clone(),
                    sv(0.0),
                    sv(0.0),
                    vec![(
                        Some(ScalarValue::Int64(Some(5))),
                        Some(ScalarValue::Int64(Some(12))),
                    )],
                )
                .unwrap(),
            );
            // Sorted input takes the binary-search slice, unsorted the mask.
            assert_eq!(rf.sorted_on_key, declare_sorted);

            let ctx = SessionContext::new().task_ctx();
            let mut stream = rf.execute(0, ctx).unwrap();
            let mut selected: Vec<i64> = Vec::new();
            while let Some(res) = stream.next().await {
                let b = res.unwrap();
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap();
                selected.extend(col.values());
            }
            assert_eq!(
                selected,
                (5..12).collect::<Vec<i64>>(),
                "sorted={declare_sorted}"
            );
        }
    }

    /// A nullable key whose input declares no ordering on it has no answer for
    /// where the run belongs, so construction refuses rather than defaulting to
    /// an end and quietly handing the run to whichever partition that names.
    #[test]
    fn a_nullable_key_without_a_declared_order_is_refused() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let column: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("v", schema.as_ref()).unwrap());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![Some(1.0), None]))],
        )
        .unwrap();
        let source =
            MemorySourceConfig::try_new(&[vec![batch]], schema.clone(), None).unwrap();
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(source)));

        let err = RangeFilterExec::try_new_resolved(
            input,
            column,
            sv(0.0),
            sv(0.0),
            vec![(None, None)],
        )
        .unwrap_err();
        assert!(err.to_string().contains("NULL run"), "got: {err}");
    }

    /// The NULL run belongs to exactly one partition: the one unbounded at the
    /// end the run occupies. Every other partition drops it, and no partition
    /// ever compares a value against a NULL bound.
    ///
    /// Both placements, over K=3 partitions with cuts at 10 and 20, so the run's
    /// partition is index 0 under `nulls_first` and index 2 under `nulls_last`.
    #[tokio::test]
    async fn the_null_run_lands_in_exactly_one_partition() {
        for nulls_first in [true, false] {
            let schema =
                Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
            let column: Arc<dyn PhysicalExpr> =
                Arc::new(Column::new_with_schema("v", schema.as_ref()).unwrap());
            // Three NULLs beside one value per partition-to-be.
            let mut values: Vec<Option<f64>> = vec![Some(5.0), Some(15.0), Some(25.0)];
            let nulls = vec![None; 3];
            if nulls_first {
                values.splice(0..0, nulls);
            } else {
                values.extend(nulls);
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Float64Array::from(values))],
            )
            .unwrap();

            let bounds = vec![
                (None, Some(sv(10.0))),
                (Some(sv(10.0)), Some(sv(20.0))),
                (Some(sv(20.0)), None),
            ];
            // Declared sorted so `nulls_first` is readable; the routing column
            // has NULLs, so every batch takes the mask path regardless.
            let sorted = PhysicalSortExpr::new(
                column.clone(),
                SortOptions {
                    descending: false,
                    nulls_first,
                },
            );
            let source = MemorySourceConfig::try_new(
                &[vec![batch.clone()], vec![batch.clone()], vec![batch]],
                schema.clone(),
                None,
            )
            .unwrap()
            .try_with_sort_information(vec![[sorted].into()])
            .unwrap();
            let input: Arc<dyn ExecutionPlan> =
                Arc::new(DataSourceExec::new(Arc::new(source)));
            let rf = Arc::new(
                RangeFilterExec::try_new_resolved(
                    input,
                    column.clone(),
                    sv(0.0),
                    sv(0.0),
                    bounds,
                )
                .unwrap(),
            );
            assert_eq!(rf.nulls_first, nulls_first);

            let mut null_rows_per_partition = Vec::new();
            let mut value_rows_per_partition = Vec::new();
            for partition in 0..3 {
                let ctx = SessionContext::new().task_ctx();
                let mut stream = rf.execute(partition, ctx).unwrap();
                let (mut nulls, mut vals) = (0usize, Vec::new());
                while let Some(res) = stream.next().await {
                    let b = res.unwrap();
                    let col =
                        b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
                    for row in 0..col.len() {
                        if col.is_null(row) {
                            nulls += 1;
                        } else {
                            vals.push(col.value(row));
                        }
                    }
                }
                null_rows_per_partition.push(nulls);
                value_rows_per_partition.push(vals);
            }

            let claimant = if nulls_first { 0 } else { 2 };
            let expected: Vec<usize> =
                (0..3).map(|p| if p == claimant { 3 } else { 0 }).collect();
            assert_eq!(
                null_rows_per_partition, expected,
                "nulls_first={nulls_first}: the run belongs to one partition"
            );
            // And the values are unaffected by the run riding along.
            assert_eq!(
                value_rows_per_partition,
                vec![vec![5.0], vec![15.0], vec![25.0]],
                "nulls_first={nulls_first}"
            );
        }
    }

    #[test]
    fn pending_construction_defers_check() {
        let src = v_source(3);
        let rf =
            RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0)).unwrap();
        assert!(rf.raw_bounds().is_none());
    }

    #[tokio::test]
    async fn execute_before_resolve_errors() {
        let src = v_source(2);
        let rf =
            RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0)).unwrap();
        let ctx = SessionContext::new().task_ctx();
        let Err(err) = rf.execute(0, ctx) else {
            panic!("execute() should error before resolve_bounds")
        };
        assert!(err.to_string().contains("before resolve_bounds"));
    }

    #[test]
    fn resolve_bounds_validates() {
        let src = v_source(3);
        let rf =
            RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0)).unwrap();
        // 2 bounds don't fit 3-partition input.
        let bounds_too_short = ranges_from_cuts(&[1.0]);
        let err = rf.resolve_bounds(bounds_too_short).unwrap_err();
        assert!(
            err.to_string()
                .contains("does not match input partition count"),
            "got: {err}"
        );
        // Well-formed bounds succeed and round-trip.
        let good = ranges_from_cuts(&[1.0, 5.0]);
        rf.resolve_bounds(good.clone()).unwrap();
        assert_eq!(rf.raw_bounds().unwrap(), good);
    }

    #[test]
    fn build_predicate_half_open_boundaries() {
        // K = 3, cuts = [10, 20], no halo.
        let cuts = [10.0, 20.0];
        let p0 = predicate_for(&cuts, 0, 0.0, 0.0).to_string();
        let p1 = predicate_for(&cuts, 1, 0.0, 0.0).to_string();
        let p2 = predicate_for(&cuts, 2, 0.0, 0.0).to_string();
        assert!(p0.contains("<") && p0.contains("10"));
        assert!(p1.contains("10") && p1.contains("20") && p1.contains("AND"));
        assert!(p2.contains(">=") && p2.contains("20"));
    }

    #[test]
    fn build_predicate_halo_widens_boundaries() {
        // K = 3, cuts = [10, 20], halo_lo=3, halo_hi=0:
        //   part 1: v >= (10 - 3) AND v < (20 - 0)  =>  v >= 7 AND v < 20
        let cuts = [10.0, 20.0];
        let p1 = predicate_for(&cuts, 1, 3.0, 0.0).to_string();
        assert!(
            p1.contains("7") && p1.contains("20"),
            "expected halo-widened lo bound, got: {p1}"
        );
    }

    #[test]
    fn build_predicate_k1_true_when_no_cuts() {
        let expr = predicate_for(&[], 0, 0.0, 0.0).to_string();
        assert!(expr.contains("true"), "expected lit(true), got: {expr}");
    }

    #[test]
    fn sorted_on_key_detected_when_input_ascending_on_routing_expr() {
        // sorted_v_source is a single-partition DataSourceExec — one raw bound.
        let src = sorted_v_source(vec![batch(&[1.0, 2.0, 3.0])], asc());
        let rf = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            sv(0.0),
            sv(0.0),
            vec![(None, None)],
        )
        .unwrap();
        assert!(rf.sorted_on_key);
    }

    #[test]
    fn sorted_on_key_false_when_input_descending_on_routing_expr() {
        let desc = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let src = sorted_v_source(vec![batch(&[3.0, 2.0, 1.0])], desc);
        let rf = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            sv(0.0),
            sv(0.0),
            vec![(None, None)],
        )
        .unwrap();
        // Fast path assumes ascending — reverse order would flip min/max.
        assert!(!rf.sorted_on_key);
    }

    #[test]
    fn sorted_on_key_false_when_input_advertises_no_ordering() {
        // RepartitionExec on a single partition drops ordering information.
        let src = v_source(2);
        let bounds = ranges_from_cuts(&[2.0]);
        let rf =
            RangeFilterExec::try_new_resolved(src, v_col(), sv(0.0), sv(0.0), bounds)
                .unwrap();
        assert!(!rf.sorted_on_key);
    }

    /// Helper to drain one partition of a resolved RangeFilterExec into a
    /// concatenated `Vec<f64>` of the output routing values.
    async fn drain(rf: Arc<RangeFilterExec>, partition: usize) -> Vec<f64> {
        let ctx = SessionContext::new().task_ctx();
        let mut stream = rf.execute(partition, ctx).unwrap();
        let mut out = Vec::new();
        while let Some(res) = stream.next().await {
            let b = res.unwrap();
            let col = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..col.len() {
                out.push(col.value(i));
            }
        }
        out
    }

    /// Build a resolved 1-input-partition RangeFilterExec whose sole bound
    /// is `ranges_from_cuts(cuts)[global_k]` — the equivalent of picking one
    /// K-shape partition for the whole test batch.
    fn one_partition_rf(
        input: Arc<dyn ExecutionPlan>,
        cuts: &[f64],
        global_k: usize,
    ) -> Arc<RangeFilterExec> {
        let ranges = ranges_from_cuts(cuts);
        let bounds = vec![ranges[global_k].clone()];
        Arc::new(
            RangeFilterExec::try_new_resolved(input, v_col(), sv(0.0), sv(0.0), bounds)
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn fast_path_skips_batch_entirely_below_partition_lo() {
        let src = sorted_v_source(vec![batch(&[1.0, 2.0, 3.0])], asc());
        let rf = one_partition_rf(src, &[10.0, 20.0], 1);
        assert!(rf.sorted_on_key);
        let rows = drain(rf, 0).await;
        assert!(
            rows.is_empty(),
            "expected zero surviving rows, got {rows:?}"
        );
    }

    #[tokio::test]
    async fn fast_path_skips_batch_entirely_above_partition_hi() {
        let src = sorted_v_source(vec![batch(&[10.0, 20.0, 30.0])], asc());
        let rf = one_partition_rf(src, &[10.0, 20.0], 0);
        let rows = drain(rf, 0).await;
        assert!(
            rows.is_empty(),
            "expected zero surviving rows, got {rows:?}"
        );
    }

    #[tokio::test]
    async fn fast_path_passes_batch_entirely_inside_window() {
        let input_rows = vec![11.0, 15.0, 19.0];
        let src = sorted_v_source(vec![batch(&input_rows)], asc());
        let rf = one_partition_rf(src, &[10.0, 20.0], 1);
        let rows = drain(rf, 0).await;
        assert_eq!(rows, input_rows);
    }

    #[tokio::test]
    async fn fast_path_slices_mixed_batch() {
        let src =
            sorted_v_source(vec![batch(&[5.0, 10.0, 15.0, 19.0, 20.0, 25.0])], asc());
        let rf = one_partition_rf(src, &[10.0, 20.0], 1);
        let rows = drain(rf, 0).await;
        assert_eq!(rows, vec![10.0, 15.0, 19.0]);
    }

    #[tokio::test]
    async fn fast_path_slices_open_upper_bound() {
        let src = sorted_v_source(vec![batch(&[5.0, 15.0, 25.0, 35.0])], asc());
        let rf = one_partition_rf(src, &[10.0, 20.0], 2);
        let rows = drain(rf, 0).await;
        assert_eq!(rows, vec![25.0, 35.0]);
    }

    #[tokio::test]
    async fn fast_path_falls_back_when_routing_column_has_nulls() {
        let nullable_schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let arr = Float64Array::from(vec![Some(5.0), None, Some(15.0), None, Some(25.0)]);
        let b =
            RecordBatch::try_new(nullable_schema.clone(), vec![Arc::new(arr)]).unwrap();
        let v_expr: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("v", nullable_schema.as_ref()).unwrap());
        let sort_expr = PhysicalSortExpr::new(v_expr.clone(), asc());
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let cfg = MemorySourceConfig::try_new(&[vec![b]], nullable_schema.clone(), None)
            .unwrap();
        let cfg = cfg.try_with_sort_information(vec![ordering]).unwrap();
        let src: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(cfg)));
        let ranges = ranges_from_cuts(&[10.0, 20.0]);
        let rf = Arc::new(
            RangeFilterExec::try_new_resolved(
                src,
                v_expr,
                sv(0.0),
                sv(0.0),
                vec![ranges[1].clone()],
            )
            .unwrap(),
        );
        assert!(rf.sorted_on_key, "input still advertises ordering");
        // Global partition 1 = [10, 20). Only 15.0 qualifies; nulls compare false.
        let rows = drain(rf, 0).await;
        assert_eq!(rows, vec![15.0]);
    }

    #[tokio::test]
    async fn slow_path_matches_fast_path_when_input_unsorted() {
        let cfg = MemorySourceConfig::try_new(
            &[vec![batch(&[5.0, 10.0, 15.0, 19.0, 20.0, 25.0])]],
            v_schema(),
            None,
        )
        .unwrap();
        let mem_src: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(cfg)));
        // RepartitionExec to 1 partition drops the ordering claim without
        // fanning the data out (RoundRobinBatch to 1 = identity).
        let repart: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(mem_src, Partitioning::RoundRobinBatch(1)).unwrap(),
        );
        assert!(repart.output_ordering().is_none());
        let rf = one_partition_rf(repart, &[10.0, 20.0], 1);
        assert!(!rf.sorted_on_key);
        let rows = drain(rf, 0).await;
        assert_eq!(rows, vec![10.0, 15.0, 19.0]);
    }

    #[tokio::test]
    async fn execute_filters_by_partition_range() {
        // Build an input with 3 sorted partitions containing a batch each,
        // resolve bounds equivalent to cuts [10, 20], verify each partition
        // emits only its slice.
        let schema = v_schema();
        let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(
                &[
                    vec![batch(&[5.0, 15.0, 25.0])],
                    vec![batch(&[5.0, 15.0, 25.0])],
                    vec![batch(&[5.0, 15.0, 25.0])],
                ],
                schema.clone(),
                None,
            )
            .unwrap(),
        )));
        let rf = Arc::new(
            RangeFilterExec::try_new_resolved(
                source,
                v_col(),
                sv(0.0),
                sv(0.0),
                ranges_from_cuts(&[10.0, 20.0]),
            )
            .unwrap(),
        );
        let ctx = SessionContext::new().task_ctx();
        for (partition, expected) in [(0, 5.0), (1, 15.0), (2, 25.0)] {
            let mut stream = rf.execute(partition, ctx.clone()).unwrap();
            let batch = stream.next().await.unwrap().unwrap();
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(col.len(), 1, "partition {partition}");
            assert_eq!(col.value(0), expected, "partition {partition}");
        }
    }
}
