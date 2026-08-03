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

//! Filter over ordered inputs with per-partition half-open range predicates.
//!
//! `execute(k)` applies the predicate
//!
//! ```text
//!   cuts[k-1] - halo_lo <= routing_expr < cuts[k] + halo_hi
//! ```
//!
//! to partition `k`, with virtual `-∞` / `+∞` sentinels on the ends. Zero
//! halo (`halo_lo == halo_hi == 0.0`) recovers the exact range-repartition
//! trim used above `ShuffleReaderExec` for hash-agg correctness; non-zero
//! halo widens each partition's read range to include a boundary "context"
//! band (`WindowFrame` PRECEDING/FOLLOWING for bounded RANGE frames — see
//! [[parallel-range-window]]).
//!
//! Ordering knowledge on the input opens the door to a future value-index
//! binary-search path (aligned with the [`ValueIndexReader`] direction from
//! PR #2204) that a generic `FilterExec` couldn't do — the predicate here is
//! monotone over `routing_expr` and the input is sorted on it, so the
//! partition's slice is a contiguous run in the input.
//!
//! # Late-binding cuts
//!
//! `cuts` is `Arc<Mutex<Option<Vec<f64>>>>` — the ParallelWindow rewrite rule
//! plants a `RangeFilterExec` at plan time, well before the runtime cuts are
//! known. The scheduler calls [`RangeFilterExec::resolve_cuts`] after stage 0's
//! `RuntimeStatsExec` reports have been merged (mirrors
//! `ExchangeExec::resolve_range_repartition_routing`). `execute` refuses
//! while cuts are unresolved; serialization refuses too — over-the-wire
//! plans always ship with cuts bound.
//!
//! # Type generality
//!
//! Cuts and halo widths are `f64` today. This matches URRE/ORRE's Float64
//! hardcode (T-Digest is Float64-only). Widening to other `Ord`
//! `ScalarValue` types is a KLL-migration follow-up ([[kll-sketch]]) —
//! see the type-generality note in [[parallel-range-window]].
//!
//! [`ValueIndexReader`]: crate::execution_plans::ShuffleReaderExec

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
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

/// Filter over an ordered input, applying a per-partition half-open range
/// predicate derived from runtime-discovered cuts + optional halo widths.
///
/// `partition_indices` maps this operator's local partition index (what
/// `execute(k)` receives) to the *global* partition index in the original
/// K-shape defined by `cuts`. Under [`Self::restrict_partitions`] the input
/// is sliced to a subset of the original K partitions; the RangeFilterExec
/// then reports the restricted count via `output_partitioning`, but still
/// applies the correct global predicate for each restricted slice.
///
/// Invariant: `partition_indices.len() == input.output_partitioning().partition_count()`,
/// and when `cuts` is `Some`, every entry is `< cuts.len() + 1`.
pub struct RangeFilterExec {
    input: Arc<dyn ExecutionPlan>,
    routing_expr: Arc<dyn PhysicalExpr>,
    /// Late-bound. `None` until the scheduler calls [`Self::resolve_cuts`];
    /// `execute` and serialization both refuse while unresolved.
    cuts: Arc<Mutex<Option<Vec<f64>>>>,
    halo_lo: f64,
    halo_hi: f64,
    partition_indices: Vec<usize>,
    /// True when `input.output_ordering()` leads with `routing_expr` in
    /// ascending order. Enables the min/max fast paths + binary-search slice
    /// in [`RangeFilterStream`] — with a sorted input, per-batch first/last
    /// values bound the entire batch, so most batches never touch
    /// `filter_record_batch`.
    sorted_on_key: bool,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl RangeFilterExec {
    /// Construct with cuts already resolved (adapter path — cuts arrive
    /// from `ExchangeExec::range_repartition_routing()` at Stage-2
    /// planning time).
    pub fn try_new_resolved(
        input: Arc<dyn ExecutionPlan>,
        routing_expr: Arc<dyn PhysicalExpr>,
        cuts: Vec<ScalarValue>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
    ) -> Result<Self> {
        Self::try_new_inner(input, routing_expr, Some(cuts), halo_lo, halo_hi, None)
    }

    /// Construct with an explicit `partition_indices` mapping. Wire-decoding
    /// path — restrict_partitions is preferred at plan-rewrite time.
    pub fn try_new_with_indices(
        input: Arc<dyn ExecutionPlan>,
        routing_expr: Arc<dyn PhysicalExpr>,
        cuts: Vec<ScalarValue>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
        partition_indices: Vec<usize>,
    ) -> Result<Self> {
        Self::try_new_inner(
            input,
            routing_expr,
            Some(cuts),
            halo_lo,
            halo_hi,
            Some(partition_indices),
        )
    }

    /// Construct with cuts pending (rule path — the ParallelWindow rewrite
    /// plants the operator at plan time; the scheduler resolves cuts after
    /// stage 0's stats reports merge).
    pub fn try_new_pending(
        input: Arc<dyn ExecutionPlan>,
        routing_expr: Arc<dyn PhysicalExpr>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
    ) -> Result<Self> {
        Self::try_new_inner(input, routing_expr, None, halo_lo, halo_hi, None)
    }

    fn try_new_inner(
        input: Arc<dyn ExecutionPlan>,
        routing_expr: Arc<dyn PhysicalExpr>,
        cuts: Option<Vec<ScalarValue>>,
        halo_lo: ScalarValue,
        halo_hi: ScalarValue,
        partition_indices: Option<Vec<usize>>,
    ) -> Result<Self> {
        let schema = input.schema();
        let expr_type = routing_expr.data_type(&schema)?;
        if !expr_type.is_numeric() {
            return internal_err!(
                "RangeFilterExec: routing_expr must be numeric, got {expr_type}"
            );
        }
        let partition_count = input.output_partitioning().partition_count();
        let partition_indices =
            partition_indices.unwrap_or_else(|| (0..partition_count).collect());
        if partition_indices.len() != partition_count {
            return internal_err!(
                "RangeFilterExec: partition_indices.len() ({}) does not match input partition count ({})",
                partition_indices.len(),
                partition_count
            );
        }
        let cuts_f64 = cuts
            .as_ref()
            .map(|c| c.iter().map(as_f64).collect::<Result<Vec<_>>>())
            .transpose()?;
        if let Some(cuts_f64) = &cuts_f64 {
            let global_count = cuts_f64.len() + 1;
            for &idx in &partition_indices {
                if idx >= global_count {
                    return internal_err!(
                        "RangeFilterExec: partition_indices contains {idx} but only {global_count} global partitions exist"
                    );
                }
            }
            if !cuts_f64.windows(2).all(|w| w[0] <= w[1]) {
                return internal_err!("RangeFilterExec: cuts must be monotone");
            }
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
            cuts: Arc::new(Mutex::new(cuts_f64)),
            halo_lo: halo_lo_f64,
            halo_hi: halo_hi_f64,
            partition_indices,
            sorted_on_key,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Build a restricted RangeFilterExec: the same routing_expr / cuts /
    /// halo, but sliced to a subset of the input's partitions.
    /// `restricted_input` must already have been restricted to the same
    /// `task_partitions` by the caller; the RangeFilterExec's job here is
    /// only to remap `partition_indices` so `execute(local_k)` still finds
    /// the right global cut range.
    pub fn restrict_partitions(
        &self,
        restricted_input: Arc<dyn ExecutionPlan>,
        task_partitions: &[usize],
    ) -> Result<Self> {
        let new_indices: Vec<usize> = task_partitions
            .iter()
            .map(|&local_j| self.partition_indices[local_j])
            .collect();
        let cuts_snapshot = self.cuts.lock().clone().map(|c| {
            c.into_iter()
                .map(|v| ScalarValue::Float64(Some(v)))
                .collect()
        });
        Self::try_new_inner(
            restricted_input,
            self.routing_expr.clone(),
            cuts_snapshot,
            ScalarValue::Float64(Some(self.halo_lo)),
            ScalarValue::Float64(Some(self.halo_hi)),
            Some(new_indices),
        )
    }

    /// Idempotent overwrite. Matches `ExchangeExec::resolve_range_repartition_routing`
    /// — called by the scheduler once stage-0 sketches are merged into cuts.
    /// `cuts` describes the *global* K-shape, so `cuts.len() + 1` must be at
    /// least `max(partition_indices) + 1`. Under restriction the operator's
    /// local partition count can be smaller than K.
    pub fn resolve_cuts(&self, cuts: Vec<ScalarValue>) -> Result<()> {
        let cuts_f64: Vec<f64> = cuts.iter().map(as_f64).collect::<Result<_>>()?;
        let global_count = cuts_f64.len() + 1;
        if let Some(&max_idx) = self.partition_indices.iter().max()
            && max_idx >= global_count
        {
            return internal_err!(
                "RangeFilterExec::resolve_cuts: cuts describe {global_count} global partitions but partition_indices references {max_idx}"
            );
        }
        if !cuts_f64.windows(2).all(|w| w[0] <= w[1]) {
            return internal_err!("RangeFilterExec::resolve_cuts: cuts must be monotone");
        }
        self.cuts.lock().replace(cuts_f64);
        Ok(())
    }

    /// Snapshot cuts. `None` before [`Self::resolve_cuts`] fires; `Some` after.
    pub fn cuts(&self) -> Option<Vec<ScalarValue>> {
        self.cuts.lock().clone().map(|c| {
            c.into_iter()
                .map(|v| ScalarValue::Float64(Some(v)))
                .collect()
        })
    }

    /// The physical expression whose value each row is bucketed by.
    pub fn routing_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.routing_expr
    }

    /// Halo-widening amount applied to each partition's lower bound.
    pub fn halo_lo(&self) -> ScalarValue {
        ScalarValue::Float64(Some(self.halo_lo))
    }

    /// Halo-widening amount applied to each partition's upper bound.
    pub fn halo_hi(&self) -> ScalarValue {
        ScalarValue::Float64(Some(self.halo_hi))
    }

    /// Task-local → global partition index mapping. See the struct-level
    /// invariant note.
    pub fn partition_indices(&self) -> &[usize] {
        &self.partition_indices
    }
}

impl Debug for RangeFilterExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeFilterExec")
            .field("routing_expr", &self.routing_expr.to_string())
            .field("halo_lo", &self.halo_lo)
            .field("halo_hi", &self.halo_hi)
            .field("cuts", &self.cuts.lock())
            .finish()
    }
}

impl DisplayAs for RangeFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let cuts = self.cuts.lock();
                let cut_display = match cuts.as_ref() {
                    Some(c) => format!("{:?}", c),
                    None => "pending".to_string(),
                };
                write!(
                    f,
                    "RangeFilterExec: routing={}, halo=[{}, {}], cuts={}",
                    self.routing_expr, self.halo_lo, self.halo_hi, cut_display
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
        // Preserve the cuts slot across the rewrite so a pending
        // RangeFilterExec that gets its child transformed doesn't lose
        // the eventual scheduler resolution target. Preserve
        // partition_indices too — with_new_children is a tree rewrite,
        // not a partition restriction; if a rewriter needs to change the
        // partition mapping it goes through restrict_partitions instead.
        let cuts_snapshot = self.cuts.lock().clone().map(|c| {
            c.into_iter()
                .map(|v| ScalarValue::Float64(Some(v)))
                .collect()
        });
        Ok(Arc::new(Self::try_new_inner(
            input.clone(),
            self.routing_expr.clone(),
            cuts_snapshot,
            ScalarValue::Float64(Some(self.halo_lo)),
            ScalarValue::Float64(Some(self.halo_hi)),
            Some(self.partition_indices.clone()),
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
        let cuts = self.cuts.lock().clone().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "RangeFilterExec: execute() called before resolve_cuts()".into(),
            )
        })?;
        let Some(&global_partition) = self.partition_indices.get(partition) else {
            return internal_err!(
                "RangeFilterExec: partition {} out of bounds ({} local partitions)",
                partition,
                self.partition_indices.len()
            );
        };
        let (lo, hi) =
            partition_bounds(&cuts, global_partition, self.halo_lo, self.halo_hi);
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

/// Compute partition `k`'s half-open bounds `[lo, hi)`. `None` means unbounded
/// on that side (virtual ±∞). Partition 0 is `(-∞, cuts[0] + halo_hi)`,
/// partition K-1 is `[cuts[K-2] - halo_lo, +∞)`, K == 1 is `(-∞, +∞)`.
fn partition_bounds(
    cuts: &[f64],
    partition: usize,
    halo_lo: f64,
    halo_hi: f64,
) -> (Option<f64>, Option<f64>) {
    let lo = partition
        .checked_sub(1)
        .and_then(|i| cuts.get(i).copied())
        .map(|c| c - halo_lo);
    let hi = cuts.get(partition).copied().map(|c| c + halo_hi);
    (lo, hi)
}

/// Assemble the boolean `PhysicalExpr` predicate from `[lo, hi)`. Used both by
/// [`RangeFilterStream`]'s slow path and by the tests that inspect the
/// generated expression tree.
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

/// Test-only shim preserving the pre-fast-path signature. Real code paths
/// call `partition_bounds` + `build_predicate_from_bounds` separately, since
/// the bounds are also fed to the fast path.
#[cfg(test)]
fn build_predicate(
    routing_expr: Arc<dyn PhysicalExpr>,
    cuts: &[f64],
    partition: usize,
    halo_lo: f64,
    halo_hi: f64,
) -> Arc<dyn PhysicalExpr> {
    let (lo, hi) = partition_bounds(cuts, partition, halo_lo, halo_hi);
    build_predicate_from_bounds(routing_expr, lo, hi)
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

// Silence the unused import warning when the file is compiled without
// arrow — DataType is only exercised via `data_type(...)` return checks.
#[allow(dead_code)]
fn _touch_datatype(_: DataType) {}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{Field, Schema};
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
            MemorySourceConfig::try_new(&[vec![]], schema.clone(), None).unwrap(),
        )));
        Arc::new(
            RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(partitions))
                .unwrap(),
        )
    }

    fn v_col() -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new_with_schema("v", v_schema().as_ref()).unwrap())
    }

    /// Single-partition memory source containing `batches`, declaring
    /// `sort_information` on `v` — RangeFilterExec's `sorted_on_key` detection
    /// picks this up and enables the fast path.
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
            nulls_first: false,
        }
    }

    fn batch(values: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            v_schema(),
            vec![Arc::new(Float64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn sv(v: f64) -> ScalarValue {
        ScalarValue::Float64(Some(v))
    }

    fn svs(vs: &[f64]) -> Vec<ScalarValue> {
        vs.iter().map(|&v| sv(v)).collect()
    }

    #[test]
    fn default_partition_indices_must_fit_cut_count() {
        // Default partition_indices = (0..input.partition_count()); with only 1 cut
        // there are 2 global partitions, so index 2 is out of range.
        let src = v_source(3);
        let err = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            svs(&[10.0]),
            sv(0.0),
            sv(0.0),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("only 2 global partitions"),
            "got: {err}"
        );
    }

    #[test]
    fn cuts_must_be_monotone() {
        let src = v_source(3);
        let err = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            svs(&[10.0, 5.0]),
            sv(0.0),
            sv(0.0),
        )
        .unwrap_err();
        assert!(err.to_string().contains("monotone"));
    }

    #[test]
    fn halo_must_be_non_negative() {
        let src = v_source(2);
        let err = RangeFilterExec::try_new_resolved(
            src.clone(),
            v_col(),
            svs(&[5.0]),
            sv(-1.0),
            sv(0.0),
        )
        .unwrap_err();
        assert!(err.to_string().contains("halo_lo"));
        let err = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            svs(&[5.0]),
            sv(0.0),
            sv(f64::NAN),
        )
        .unwrap_err();
        assert!(err.to_string().contains("halo_hi"));
    }

    #[test]
    fn non_float64_cuts_are_rejected() {
        let src = v_source(2);
        let cuts = vec![ScalarValue::Int64(Some(5))];
        let err = RangeFilterExec::try_new_resolved(src, v_col(), cuts, sv(0.0), sv(0.0))
            .unwrap_err();
        assert!(err.to_string().contains("only Float64"), "got: {err}");
    }

    #[test]
    fn pending_construction_defers_check() {
        let src = v_source(3);
        // 5 cuts wouldn't fit 3 partitions if we required alignment eagerly.
        let rf =
            RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0)).unwrap();
        assert!(rf.cuts().is_none());
    }

    #[tokio::test]
    async fn execute_before_resolve_errors() {
        let src = v_source(2);
        let rf =
            RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0)).unwrap();
        let ctx = SessionContext::new().task_ctx();
        let Err(err) = rf.execute(0, ctx) else {
            panic!("execute() should error before resolve_cuts")
        };
        assert!(err.to_string().contains("before resolve_cuts"));
    }

    #[test]
    fn resolve_cuts_validates() {
        let src = v_source(3);
        let rf =
            RangeFilterExec::try_new_pending(src, v_col(), sv(0.0), sv(0.0)).unwrap();
        // 3 default partition_indices = [0,1,2]; 1 cut means 2 global partitions
        // so index 2 is out of range.
        let err = rf.resolve_cuts(svs(&[1.0])).unwrap_err();
        assert!(
            err.to_string().contains("2 global partitions"),
            "got: {err}"
        );
        // Non-monotone.
        let err = rf.resolve_cuts(svs(&[5.0, 1.0])).unwrap_err();
        assert!(err.to_string().contains("monotone"));
        // Good.
        rf.resolve_cuts(svs(&[1.0, 5.0])).unwrap();
        assert_eq!(rf.cuts().unwrap(), svs(&[1.0, 5.0]));
    }

    #[test]
    fn restrict_partitions_remaps_indices() {
        // Original K=4 partitions with cuts [10, 20, 30]. Restrict to
        // task-local [1, 3] — new operator has 2 local partitions that
        // apply the predicates for global partitions 1 and 3.
        let src = v_source(4);
        let rf = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            svs(&[10.0, 20.0, 30.0]),
            sv(0.0),
            sv(0.0),
        )
        .unwrap();
        let restricted_input = v_source(2);
        let restricted = rf.restrict_partitions(restricted_input, &[1, 3]).unwrap();
        assert_eq!(restricted.partition_indices(), &[1, 3]);
        assert_eq!(restricted.cuts().unwrap(), svs(&[10.0, 20.0, 30.0]));
    }

    #[test]
    fn build_predicate_half_open_boundaries() {
        // K = 3, cuts = [10, 20], no halo:
        //   part 0: v < 10
        //   part 1: v >= 10 AND v < 20
        //   part 2: v >= 20
        let cuts = vec![10.0, 20.0];
        let p0 = build_predicate(v_col(), &cuts, 0, 0.0, 0.0).to_string();
        let p1 = build_predicate(v_col(), &cuts, 1, 0.0, 0.0).to_string();
        let p2 = build_predicate(v_col(), &cuts, 2, 0.0, 0.0).to_string();
        assert!(p0.contains("<") && p0.contains("10"));
        assert!(p1.contains("10") && p1.contains("20") && p1.contains("AND"));
        assert!(p2.contains(">=") && p2.contains("20"));
    }

    #[test]
    fn build_predicate_halo_widens_boundaries() {
        // K = 3, cuts = [10, 20], halo_lo=3, halo_hi=0:
        //   part 1: v >= (10 - 3) AND v < (20 - 0)  =>  v >= 7 AND v < 20
        let cuts = vec![10.0, 20.0];
        let p1 = build_predicate(v_col(), &cuts, 1, 3.0, 0.0).to_string();
        assert!(
            p1.contains("7") && p1.contains("20"),
            "expected halo-widened lo bound, got: {p1}"
        );
    }

    #[test]
    fn build_predicate_k1_true_when_no_cuts() {
        let expr = build_predicate(v_col(), &[], 0, 0.0, 0.0).to_string();
        assert!(expr.contains("true"), "expected lit(true), got: {expr}");
    }

    #[test]
    fn sorted_on_key_detected_when_input_ascending_on_routing_expr() {
        let src = sorted_v_source(vec![batch(&[1.0, 2.0, 3.0])], asc());
        let rf = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            svs(&[2.0]),
            sv(0.0),
            sv(0.0),
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
            svs(&[2.0]),
            sv(0.0),
            sv(0.0),
        )
        .unwrap();
        // Fast path assumes ascending — reverse order would flip min/max.
        assert!(!rf.sorted_on_key);
    }

    #[test]
    fn sorted_on_key_false_when_input_advertises_no_ordering() {
        // RepartitionExec on a single partition drops ordering information.
        let src = v_source(2);
        let rf = RangeFilterExec::try_new_resolved(
            src,
            v_col(),
            svs(&[2.0]),
            sv(0.0),
            sv(0.0),
        )
        .unwrap();
        assert!(!rf.sorted_on_key);
    }

    /// Helper to drain one partition of a resolved RangeFilterExec into a
    /// concatenated `Vec<f64>` of the output routing values, so tests can
    /// assert the exact rows that survived without needing to reason about
    /// intermediate batch boundaries.
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

    /// Build a resolved 1-local-partition RangeFilterExec that applies the
    /// predicate for global partition `global_k` of the K=cuts.len()+1 shape.
    /// The input has 1 partition, so `execute(0)` runs the chosen global
    /// predicate over the whole test batch.
    fn one_partition_rf(
        input: Arc<dyn ExecutionPlan>,
        cuts: &[f64],
        global_k: usize,
    ) -> Arc<RangeFilterExec> {
        Arc::new(
            RangeFilterExec::try_new_with_indices(
                input,
                v_col(),
                svs(cuts),
                sv(0.0),
                sv(0.0),
                vec![global_k],
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn fast_path_skips_batch_entirely_below_partition_lo() {
        // Global partition 1 window is [10, 20). Batch entirely below 10 —
        // fast path detects `last < lo` and drops the whole batch.
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
        // Global partition 0 window is (-inf, 10). Batch entirely at/above 10.
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
        // Global partition 1 window is [10, 20). Batch [11, 15, 19] is
        // entirely inside. Fast path Arc-clones — verify identical rows out.
        let input_rows = vec![11.0, 15.0, 19.0];
        let src = sorted_v_source(vec![batch(&input_rows)], asc());
        let rf = one_partition_rf(src, &[10.0, 20.0], 1);
        let rows = drain(rf, 0).await;
        assert_eq!(rows, input_rows);
    }

    #[tokio::test]
    async fn fast_path_slices_mixed_batch() {
        // Global partition 1 window is [10, 20). Batch straddles both
        // boundaries. Rows 10..20 should survive; 5, 25 dropped.
        let src =
            sorted_v_source(vec![batch(&[5.0, 10.0, 15.0, 19.0, 20.0, 25.0])], asc());
        let rf = one_partition_rf(src, &[10.0, 20.0], 1);
        let rows = drain(rf, 0).await;
        assert_eq!(rows, vec![10.0, 15.0, 19.0]);
    }

    #[tokio::test]
    async fn fast_path_slices_open_upper_bound() {
        // Global partition K-1 has hi = None (open above). Verify the
        // partition_point logic that treats missing `hi` as `n` still slices
        // the low end. Partition 2 is [20, +inf): 25, 35 survive.
        let src = sorted_v_source(vec![batch(&[5.0, 15.0, 25.0, 35.0])], asc());
        let rf = one_partition_rf(src, &[10.0, 20.0], 2);
        let rows = drain(rf, 0).await;
        assert_eq!(rows, vec![25.0, 35.0]);
    }

    #[tokio::test]
    async fn fast_path_falls_back_when_routing_column_has_nulls() {
        // Nulls in the routing column — `Float64Array::values()` returns
        // garbage for null slots, so partition_point would be meaningless.
        // Verify the slow path handles it and still produces correct rows.
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
        let rf = Arc::new(
            RangeFilterExec::try_new_with_indices(
                src,
                v_expr,
                svs(&[10.0, 20.0]),
                sv(0.0),
                sv(0.0),
                vec![1],
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
        // Same batch, same predicate, but no advertised ordering: slow path
        // via `filter_record_batch` must produce the same rows the fast path
        // would.
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
        // apply resolved cuts [10, 20], verify each partition emits only its slice.
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
                svs(&[10.0, 20.0]),
                sv(0.0),
                sv(0.0),
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
