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
//! `ScalarValue` at the API + serde surface. The internal fast path is
//! Float64-only today (matches URRE/ORRE T-Digest); widening to other
//! numeric primitives is a KLL-migration follow-up that
//! will land without breaking callers.

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::cast::{as_boolean_array, as_float64_array};
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
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt, ready};
use parking_lot::Mutex;

/// Half-open `[lo, hi)` bound for one input partition. `None` on either side
/// means unbounded (virtual ±∞).
pub type RangeBound = (Option<ScalarValue>, Option<ScalarValue>);

/// Bounds after halo widening. Float64-only internally today — see the
/// "Type generality" section in the module doc.
pub type WidenedBound = (Option<f64>, Option<f64>);

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
        if !expr_type.is_numeric() {
            return internal_err!(
                "RangeFilterExec: routing_expr must be numeric, got {expr_type}"
            );
        }
        let halo_lo_f64 = as_f64(&halo_lo)?;
        let halo_hi_f64 = as_f64(&halo_hi)?;
        if !halo_lo_f64.is_finite() || halo_lo_f64 < 0.0 {
            return internal_err!(
                "RangeFilterExec: halo_lo must be finite and non-negative, got {halo_lo_f64}"
            );
        }
        if !halo_hi_f64.is_finite() || halo_hi_f64 < 0.0 {
            return internal_err!(
                "RangeFilterExec: halo_hi must be finite and non-negative, got {halo_hi_f64}"
            );
        }
        let bounds_state = raw_bounds
            .map(|raw| build_bounds_state(&input, raw, halo_lo_f64, halo_hi_f64))
            .transpose()?;
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        let sorted_on_key = input
            .output_ordering()
            .map(|ord| {
                let first = ord.first();
                first.expr.as_ref() == routing_expr.as_ref() && !first.options.descending
            })
            .unwrap_or(false);
        Ok(Self {
            input,
            routing_expr,
            halo_lo,
            halo_hi,
            bounds: Arc::new(Mutex::new(bounds_state)),
            sorted_on_key,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Idempotent overwrite. Called by the scheduler once stage-0 sketches
    /// merge into cuts and the adapter has projected those cuts onto per-input
    /// partition half-open ranges. Widens by RFE's halos before caching.
    pub fn resolve_bounds(&self, raw_bounds: Vec<RangeBound>) -> Result<()> {
        let halo_lo = as_f64(&self.halo_lo)?;
        let halo_hi = as_f64(&self.halo_hi)?;
        let state = build_bounds_state(&self.input, raw_bounds, halo_lo, halo_hi)?;
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
            state.widened.get(partition).copied().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(format!(
                    "RangeFilterExec: partition {partition} out of bounds ({} bounds)",
                    state.widened.len()
                ))
            })?
        };
        let (lo, hi) = widened;
        let predicate = build_predicate_from_bounds(self.routing_expr.clone(), lo, hi);
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
            input,
            fast_path,
            baseline,
            path_metrics,
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Extract an `f64` from a `ScalarValue`. Restricted to `Float64` today
/// because the surrounding operators (URRE/ORRE, T-Digest) only understand
/// Float64. TODO widen with the KLL migration to accept any
/// `arrow::datatypes::ArrowPrimitiveType`.
fn as_f64(sv: &ScalarValue) -> Result<f64> {
    match sv {
        ScalarValue::Float64(Some(v)) => Ok(*v),
        ScalarValue::Float64(None) => {
            internal_err!("RangeFilterExec: null ScalarValue is not permitted")
        }
        other => internal_err!(
            "RangeFilterExec: only Float64 ScalarValue supported today, got {other:?}"
        ),
    }
}

/// Validate + widen raw bounds. Emits a `BoundsState` with the raw preserved
/// for serialization and the widened ready for `execute`.
fn build_bounds_state(
    input: &Arc<dyn ExecutionPlan>,
    raw: Vec<RangeBound>,
    halo_lo: f64,
    halo_hi: f64,
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
            let lo_f = lo.as_ref().map(as_f64).transpose()?.map(|v| v - halo_lo);
            let hi_f = hi.as_ref().map(as_f64).transpose()?.map(|v| v + halo_hi);
            if let (Some(l), Some(h)) = (lo_f, hi_f)
                && l > h
            {
                return internal_err!(
                    "RangeFilterExec: widened bound produced inverted [{l}, {h}) — check cuts + halo"
                );
            }
            Ok((lo_f, hi_f))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundsState { raw, widened })
}

/// Assemble the boolean `PhysicalExpr` predicate from a widened `[lo, hi)`.
/// Used both by [`RangeFilterStream`]'s slow path and by the tests that inspect
/// the generated expression tree.
fn build_predicate_from_bounds(
    routing_expr: Arc<dyn PhysicalExpr>,
    lo: Option<f64>,
    hi: Option<f64>,
) -> Arc<dyn PhysicalExpr> {
    let lit = |v: f64| -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Float64(Some(v))))
    };
    let ge = |lo: f64| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            routing_expr.clone(),
            Operator::GtEq,
            lit(lo),
        ))
    };
    let lt = |hi: f64| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(routing_expr.clone(), Operator::Lt, lit(hi)))
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
    lo: Option<f64>,
    hi: Option<f64>,
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
        let mask = as_boolean_array(&mask)?;
        Ok(filter_record_batch(batch, mask)?)
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
        let col = as_float64_array(&arr)?;
        // Nulls in routing column: `values()` returns garbage for null slots,
        // and NULL vs bound comparisons must be false. Slow path handles both.
        if col.null_count() > 0 {
            let filtered = self.slow_filter(&batch)?;
            return Ok((filtered.num_rows() > 0).then_some(filtered));
        }
        let first = col.value(0);
        let last = col.value(n - 1);
        // Skip: entire batch is outside the window.
        if state.hi.is_some_and(|hi| first >= hi) || state.lo.is_some_and(|lo| last < lo)
        {
            self.path_metrics.fast_skip.add(1);
            return Ok(None);
        }
        // Pass-through: entire batch is inside the window.
        let above_lo = state.lo.is_none_or(|lo| first >= lo);
        let below_hi = state.hi.is_none_or(|hi| last < hi);
        if above_lo && below_hi {
            self.path_metrics.fast_pass.add(1);
            return Ok(Some(batch));
        }
        // Mixed: partition the sorted column and slice.
        let values = col.values();
        let start = state.lo.map_or(0, |lo| values.partition_point(|v| *v < lo));
        let end = state.hi.map_or(n, |hi| values.partition_point(|v| *v < hi));
        if start >= end {
            self.path_metrics.fast_skip.add(1);
            return Ok(None);
        }
        self.path_metrics.fast_slice.add(1);
        Ok(Some(batch.slice(start, end - start)))
    }
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
        let lo_f = lo.as_ref().map(|s| match s {
            ScalarValue::Float64(Some(v)) => *v - halo_lo,
            _ => panic!("float64 only in tests"),
        });
        let hi_f = hi.as_ref().map(|s| match s {
            ScalarValue::Float64(Some(v)) => *v + halo_hi,
            _ => panic!("float64 only in tests"),
        });
        build_predicate_from_bounds(v_col(), lo_f, hi_f)
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

    #[test]
    fn non_float64_bounds_are_rejected() {
        let src = v_source(2);
        let bounds = vec![
            (None, Some(ScalarValue::Int64(Some(5)))),
            (Some(ScalarValue::Int64(Some(5))), None),
        ];
        let err =
            RangeFilterExec::try_new_resolved(src, v_col(), sv(0.0), sv(0.0), bounds)
                .unwrap_err();
        assert!(err.to_string().contains("only Float64"), "got: {err}");
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
