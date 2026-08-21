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
//!   raw_bounds[k].0 - halo_lo <= filter_expr < raw_bounds[k].1 + halo_hi
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

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, IsNullExpr, Literal};
use datafusion::physical_expr::{
    Distribution, LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
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

use crate::execution_plans::plan_algebra::{
    PartitionSliceable, slice_by_global_partition,
};

/// Half-open `[lo, hi)` bound for one input partition. `None` on either side
/// means unbounded (virtual ±∞).
pub type RangeBound = (Option<ScalarValue>, Option<ScalarValue>);

/// Bounds after halo widening
pub type WidenedBound = (Option<ScalarValue>, Option<ScalarValue>);

/// Both raw and widened bounds. `raw` is preserved for serialization; the
/// executor consumes `widened`.
struct BoundsState {
    raw: Vec<RangeBound>,
    widened: Vec<WidenedBound>,
}

/// The order in which rows will arrive
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputOrder {
    /// Rows arrive in no particular order and the NULL run is at this end.
    /// Advertises no input ordering requirement, so an unordered
    /// range-repartition upstream stays legal.
    Unordered {
        /// Which end of the order the NULL run occupies.
        nulls_first: bool,
    },
    /// Rows arrive in this order. Required of the input, so nothing planted
    /// between can reorder them, and the run's end is `nulls_first`.
    Ordered(SortOptions),
}

impl InputOrder {
    /// Which end the NULL run occupies.
    fn nulls_first(&self) -> bool {
        match self {
            Self::Unordered { nulls_first } => *nulls_first,
            Self::Ordered(options) => options.nulls_first,
        }
    }
}

/// Filter over an ordered input with a per-input-partition half-open range
/// predicate, widened by the operator's halo. Range logic (cuts → per-partition
/// half-open ranges → task-slice) lives scheduler-side; RFE is the runtime
/// filter that applies the resolved bounds.
pub struct RangeFilterExec {
    input: Arc<dyn ExecutionPlan>,
    filter_expr: Arc<dyn PhysicalExpr>,
    /// Lower halo — subtracted from each partition's `lo` at widen time.
    halo_lo: ScalarValue,
    /// Upper halo — added from each partition's `hi` at widen time.
    halo_hi: ScalarValue,
    /// Late-bound: `None` until [`RangeFilterExec::resolve_bounds`]; `execute` and serde
    /// refuse while unresolved.
    bounds: Arc<Mutex<Option<BoundsState>>>,
    /// The order in which rows will arrive
    input_order: Option<InputOrder>,
    /// True when `input.output_ordering()` leads with `filter_expr` in ascending order.
    /// Enables the min/max fast path with a sorted input
    sorted_on_key: bool,
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
    /// * `filter_expr` - numeric physical expression each row is compared by.
    /// * `halo_lo`, `halo_hi` - non-negative widening amounts applied by
    ///   [`RangeFilterExec::resolve_bounds`], in `filter_expr`'s own type. A
    ///   float halo must also be finite.
    /// * `input_order` - the order in which rows will arrive
    pub fn try_new_pending(
        input: Arc<dyn ExecutionPlan>,
        filter_expr: Arc<dyn PhysicalExpr>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
        input_order: Option<InputOrder>,
    ) -> Result<Self> {
        Self::try_new_inner(input, filter_expr, halo_lo, halo_hi, input_order, None)
    }

    /// Construct with bounds already known. Used by wire decode and by
    /// task-restriction (task builder slices raw bounds parallel to the input
    /// restriction, then hands them here as a fresh operator).
    ///
    /// # Arguments
    ///
    /// * `input`, `filter_expr`, `halo_lo`, `halo_hi`, `input_order` - same as
    ///   [`Self::try_new_pending`].
    /// * `raw_bounds` - one half-open cut range per input partition. Widening
    ///   by halos happens internally; caller passes unwidened.
    pub fn try_new_resolved(
        input: Arc<dyn ExecutionPlan>,
        filter_expr: Arc<dyn PhysicalExpr>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
        input_order: Option<InputOrder>,
        raw_bounds: Vec<RangeBound>,
    ) -> Result<Self> {
        Self::try_new_inner(
            input,
            filter_expr,
            halo_lo,
            halo_hi,
            input_order,
            Some(raw_bounds),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_inner(
        input: Arc<dyn ExecutionPlan>,
        filter_expr: Arc<dyn PhysicalExpr>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
        input_order: Option<InputOrder>,
        raw_bounds: Option<Vec<RangeBound>>,
    ) -> Result<Self> {
        let schema = input.schema();
        let expr_type = filter_expr.data_type(&schema)?;
        // TODO: as long as halos are 0, we should be able to support things like strings
        if !expr_type.is_numeric() && !expr_type.is_temporal() {
            return internal_err!(
                "RangeFilterExec: filter_expr must be numeric or temporal, got {expr_type}"
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
        if input_order.is_none() && filter_expr.nullable(&schema)? {
            return internal_err!(
                "RangeFilterExec: filter_expr is nullable but no input order was given, \
                 so which partition holds the NULL run is unknown"
            );
        }
        let sorted_on_key = input
            .output_ordering()
            .map(|ord| ord.first().clone())
            .is_some_and(|first| {
                first.expr.as_ref() == filter_expr.as_ref() && !first.options.descending
            });
        Ok(Self {
            input,
            filter_expr,
            halo_lo,
            halo_hi,
            bounds: Arc::new(Mutex::new(bounds_state)),
            input_order,
            sorted_on_key,
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
    pub fn filter_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.filter_expr
    }

    /// What the cuts' producer said about the rows arriving.
    pub fn input_order(&self) -> Option<InputOrder> {
        self.input_order
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
            .field("filter_expr", &self.filter_expr.to_string())
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
                    self.filter_expr, self.halo_lo, self.halo_hi, bounds_str
                )
            }
            DisplayFormatType::TreeRender => write!(f, "RangeFilterExec"),
        }
    }
}

impl PartitionSliceable for RangeFilterExec {
    /// `raw_bounds` is indexed by input partition, so it slices parallel to
    /// the input. Halos and routing carry over verbatim — the fresh operator
    /// re-widens the unwidened bounds itself.
    fn slice_to_partitions(
        &self,
        child: Arc<dyn ExecutionPlan>,
        partitions: &[usize],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let raw_bounds = self.raw_bounds().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "RangeFilterExec: task-restriction before resolve_bounds()".into(),
            )
        })?;
        Ok(Arc::new(Self::try_new_resolved(
            child,
            self.filter_expr().clone(),
            self.halo_lo().clone(),
            self.halo_hi().clone(),
            self.input_order(),
            slice_by_global_partition(
                &raw_bounds,
                partitions,
                "RangeFilterExec",
                "raw bounds",
            )?,
        )?))
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
        apply_expression_roots([&self.filter_expr], f)
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
            self.filter_expr.clone(),
            self.halo_lo.clone(),
            self.halo_hi.clone(),
            self.input_order,
            raw_bounds,
        )?))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    /// Only an [`InputOrder::Ordered`] producer demands one. Requiring it is
    /// what stops `EnsureRequirements` sinking an unrelated sort beneath this
    /// operator, which would replace the ordering the NULL run's placement and
    /// the fast path both read. An unordered producer keeps `None` so no
    /// `SortExec` is planted over rows that were never meant to be sorted.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let requirement = match self.input_order {
            Some(InputOrder::Ordered(options)) => {
                LexOrdering::new(vec![PhysicalSortExpr {
                    expr: self.filter_expr.clone(),
                    options,
                }])
                .map(|lex| OrderingRequirements::new(lex.into()))
            }
            Some(InputOrder::Unordered { .. }) | None => None,
        };
        vec![requirement]
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
        // if the lower or upper side of the range is unbounded, it should include the NULLs
        let nulls_first = self.input_order.is_some_and(|order| order.nulls_first());
        let takes_nulls = if nulls_first {
            lo.is_none()
        } else {
            hi.is_none()
        };
        let mut predicate =
            build_predicate_from_bounds(self.filter_expr.clone(), lo.clone(), hi.clone());
        if takes_nulls {
            predicate = Arc::new(BinaryExpr::new(
                predicate,
                Operator::Or,
                Arc::new(IsNullExpr::new(self.filter_expr.clone())),
            ));
        }
        let schema = self.schema();
        let input = self.input.execute(partition, ctx)?;
        let fast_path = self.sorted_on_key.then(|| FastPathState {
            filter_expr: self.filter_expr.clone(),
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

pub(crate) fn is_zero_halo(halo: &ScalarValue) -> Result<bool> {
    Ok(halo == &ScalarValue::new_zero(&halo.data_type())?)
}

pub(crate) fn widen_below(
    value: &ScalarValue,
    halo: &ScalarValue,
) -> Result<ScalarValue> {
    if is_zero_halo(halo)? {
        return Ok(value.clone());
    }
    value.sub(halo)
}

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
            if let (Some(l), Some(h)) = (&lo_w, &hi_w) && l > h {
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
    filter_expr: Arc<dyn PhysicalExpr>,
    lo: Option<ScalarValue>,
    hi: Option<ScalarValue>,
) -> Arc<dyn PhysicalExpr> {
    let ge = |lo: ScalarValue| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            filter_expr.clone(),
            Operator::GtEq,
            Arc::new(Literal::new(lo)),
        ))
    };
    let lt = |hi: ScalarValue| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            filter_expr.clone(),
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
/// `filter_expr` — then a batch's first and last values bound the
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
    filter_expr: Arc<dyn PhysicalExpr>,
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
    input: SendableRecordBatchStream,
    fast_path: Option<FastPathState>,
    baseline: BaselineMetrics,
    path_metrics: PathMetrics,
}

impl RangeFilterStream {
    /// Apply the general predicate to `batch` — used both by the non-sorted
    /// fallback and by the sorted path when the routing column has nulls.
    fn slow_filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.path_metrics.slow.add(1);
        let mask = self
            .predicate
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))?;
        Ok(filter_record_batch(batch, as_boolean_array(&mask)?)?)
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
            .filter_expr
            .evaluate(&batch)
            .and_then(|v| v.into_array(n))?;
        // TODO: include NULLs in the fast path
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

/// Binary search - same as the rust version, but works on arrow primitives
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
        let err = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            sv(0.0),
            sv(0.0),
            None,
            bounds,
        )
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
            None,
            bounds.clone(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("halo_lo"));
        let err = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            sv(0.0),
            sv(f64::NAN),
            None,
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
                    None,
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
            None,
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
            // The filter column has NULLs, so every batch takes the mask path;
            // the declared sort only decides which partition claims the run.
            let options = SortOptions {
                descending: false,
                nulls_first,
            };
            let sorted = PhysicalSortExpr::new(column.clone(), options);
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
                    Some(InputOrder::Ordered(options)),
                    bounds,
                )
                .unwrap(),
            );
            assert_eq!(rf.input_order, Some(InputOrder::Ordered(options)));

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
        let rf = RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0), None)
            .unwrap();
        assert!(rf.raw_bounds().is_none());
    }

    #[tokio::test]
    async fn execute_before_resolve_errors() {
        let src = v_source(2);
        let rf = RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0), None)
            .unwrap();
        let ctx = SessionContext::new().task_ctx();
        let Err(err) = rf.execute(0, ctx) else {
            panic!("execute() should error before resolve_bounds")
        };
        assert!(err.to_string().contains("before resolve_bounds"));
    }

    #[test]
    fn resolve_bounds_validates() {
        let src = v_source(3);
        let rf = RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0), None)
            .unwrap();
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
    fn sorted_on_key_detected_when_input_ascending_on_filter_expr() {
        // sorted_v_source is a single-partition DataSourceExec — one raw bound.
        let src = sorted_v_source(vec![batch(&[1.0, 2.0, 3.0])], asc());
        let rf = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            sv(0.0),
            sv(0.0),
            None,
            vec![(None, None)],
        )
        .unwrap();
        assert!(rf.sorted_on_key);
    }

    #[test]
    fn sorted_on_key_false_when_input_descending_on_filter_expr() {
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
            None,
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
        let rf = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            sv(0.0),
            sv(0.0),
            None,
            bounds,
        )
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
            RangeFilterExec::try_new_resolved(
                input,
                v_col(),
                sv(0.0),
                sv(0.0),
                None,
                bounds,
            )
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
                Some(InputOrder::Ordered(asc())),
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
                None,
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
