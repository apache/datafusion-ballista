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

//! Shared building blocks for the two range-repartition operators —
//! [`UnorderedRangeRepartitionExec`] and (soon) `OrderedRangeRepartitionExec`.
//!
//! The two operators disagree substantially on execution model — the
//! unordered variant is pure scatter, the ordered one is scatter + per-output
//! k-way merge — but they agree on:
//!
//! 1. **How to find the cut boundaries at runtime** — walk the child subtree
//!    for a matching sibling [`RuntimeStatsExec`], snapshot its T-Digest,
//!    compute quantile cuts. Only descend through whitelisted
//!    distribution-preserving operators; refuse otherwise.
//! 2. **How to split one batch across K value ranges** — [`split_batch_by_range`].
//! 3. **How to broadcast a terminal error to every output channel** —
//!    [`broadcast_error`].
//!
//! Everything in this module is `pub(super)` — visible to sibling
//! `execution_plans::*` modules that own the operators, invisible outside.
//!
//! [`UnorderedRangeRepartitionExec`]: super::UnorderedRangeRepartitionExec
//! [`RuntimeStatsExec`]: super::RuntimeStatsExec

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, RecordBatch, UInt32Array};
use datafusion::arrow::compute::take_arrays;
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use futures::FutureExt;
use log::warn;
use tokio::sync::mpsc;

use crate::execution_plans::{
    BufferExec, RuntimeStatsExec, ShuffleWriterExec, SortShuffleWriterExec,
};

/// Walk `child`'s subtree for a [`RuntimeStatsExec`] that sketches on our
/// routing expression, snapshot its merged T-Digest, and compute `K - 1`
/// quantile cuts. Any failure to find a matching sketch returns an empty
/// `Vec` — the caller's `split_batch_by_range(&[])` produces a single
/// bucket and every row lands in output partition 0. Never crashes.
pub(super) fn discover_cuts(
    child: &Arc<dyn ExecutionPlan>,
    routing_expr: &dyn PhysicalExpr,
    output_partitions: usize,
) -> Vec<f64> {
    let Some(stats) = find_runtime_stats(child, routing_expr) else {
        warn!(
            "range-repartition: no matching RuntimeStatsExec found in child subtree — \
             single-bucket fallback"
        );
        return Vec::new();
    };
    // Walker returned Some → stats.order_by()'s first entry matches our
    // routing expression → RuntimeStatsExec's construction contract
    // guarantees sketch is present. Belt-and-braces arms in case that
    // invariant ever drifts, plus mutex-poisoning is theoretically possible.
    let sketch = match stats.merged_quantile_sketch() {
        Ok(Some(sketch)) => sketch,
        Ok(None) => {
            warn!(
                "range-repartition: matching RuntimeStatsExec has no sketch \
                 (RuntimeStatsExec contract broken?) — single-bucket fallback"
            );
            return Vec::new();
        }
        Err(e) => {
            warn!(
                "range-repartition: sketch snapshot failed ({e}) — single-bucket fallback"
            );
            return Vec::new();
        }
    };
    // `count()` is the sum of centroid weights — total observed row count
    // that fed the digest. Zero means no samples arrived before the
    // snapshot; degenerate cuts would follow.
    if sketch.count() == 0.0 {
        warn!(
            "range-repartition: matching sketch has no samples yet — single-bucket fallback"
        );
        return Vec::new();
    }
    // K-1 cuts at 1/K, 2/K, ..., (K-1)/K. `estimate_quantile` is monotone by
    // construction, so cuts are non-decreasing (ties possible on hot-value
    // distributions — `split_batch_by_range` handles those correctly, it
    // just skews the resulting distribution).
    let k = output_partitions as f64;
    (1..output_partitions)
        .map(|i| sketch.estimate_quantile(i as f64 / k))
        .collect()
}

/// Walks `plan`'s subtree through single-child chains only, returning the
/// first [`RuntimeStatsExec`] that sketches on `routing_expr`.
///
/// Two invariants have to hold for a sketch to be trustworthy:
/// 1. **Expression match.** A sketch of column `foo` says nothing about
///    routing on column `bar`. A `RuntimeStatsExec` sketching on a different
///    expression is treated as a plain passthrough — the walker keeps
///    descending past it looking for a matching one deeper in the chain.
/// 2. **Distribution preservation.** Any operator between us and the stats
///    that drops rows (`FilterExec`, `LimitExec`), transforms the routing
///    value (`ProjectionExec` with a computed column), or duplicates rows
///    (`JoinExec`) makes the sketch stale — the count still holds but the
///    distribution has drifted. The walker consults [`preserves_distribution`]
///    and refuses to descend past anything it doesn't know is safe.
///
/// Also stops at any branch (> 1 child) or leaf (0 children) — descending
/// into a join's sides would risk picking up a sketch of the wrong subtree.
pub(super) fn find_runtime_stats<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    routing_expr: &dyn PhysicalExpr,
) -> Option<&'a RuntimeStatsExec> {
    if let Some(stats) = plan.downcast_ref::<RuntimeStatsExec>() {
        let matches = stats
            .order_by()
            .and_then(|order_by| order_by.first())
            .is_some_and(|first| first.expr.as_ref() == routing_expr);
        if matches {
            return Some(stats);
        }
        // Non-matching stats is still a passthrough for our purposes — fall
        // through to the descent step.
    } else if !preserves_distribution(plan.as_ref()) {
        // Unrecognized node type — could change the row set or value
        // distribution of the routing key. Refuse to descend.
        return None;
    }
    let children = plan.children();
    let [only_child] = children.as_slice() else {
        return None;
    };
    find_runtime_stats(only_child, routing_expr)
}

/// Whitelist of pass-through operator types the walker will descend through
/// on its way to a matching [`RuntimeStatsExec`]. Unlisted operators might
/// drop rows, duplicate rows, or transform the routing key's value — any of
/// which would make an upstream sketch stale by the time data reaches us.
///
/// Being conservative is the safety net: unrecognized node → walker gives
/// up → single-bucket fallback. Extending this list requires positive
/// verification that the operator is a distribution-preserving passthrough
/// for the routing key. Absent an upstream `ExecutionPlan::affects_distribution()`
/// method (nice-to-have that hopefully lands one day), we maintain this by hand.
pub(super) fn preserves_distribution(plan: &dyn ExecutionPlan) -> bool {
    // Every entry here is a *claim* that the operator (1) doesn't drop
    // rows, (2) doesn't duplicate rows, (3) doesn't transform the routing
    // key's value, AND (4) doesn't change partitioning (per-partition
    // slots downstream still map to the same partitions upstream). Losing
    // any of those invalidates the sketch/count on the other side.
    //
    // Notable *exclusions*:
    //   • `SortPreservingMergeExec` — collapses N partitions to 1, so
    //     per-partition slots below it don't align with the single
    //     partition above. Values are preserved, but the partitioning
    //     invariant fails.
    //   • `ProjectionExec` — might compute a new column that shadows or
    //     replaces the routing key, transforming values invisibly.
    //   • `FilterExec`, `LimitExec`, joins — drop or duplicate rows.
    //   • Our own DRRs — repartition by value; that's the whole point.
    plan.downcast_ref::<BufferExec>().is_some()
        // `SortExec` reorders rows within each partition; row set and per-
        // partition counts unchanged — but ONLY when `preserve_partitioning`
        // is true. The `preserve_partitioning=false` variant collapses N
        // partitions to 1 (like `SortPreservingMergeExec`), which would
        // invalidate per-partition slot alignment.
        || plan
            .downcast_ref::<SortExec>()
            .is_some_and(|sort| sort.preserve_partitioning())
        // `ShuffleWriterExec` / `SortShuffleWriterExec` sit at the top of
        // every stage's plan on the executor side. They write batches to
        // disk unchanged — no transformation, no filtering.
        || plan.downcast_ref::<ShuffleWriterExec>().is_some()
        || plan.downcast_ref::<SortShuffleWriterExec>().is_some()
        // `BoundedWindowAggExec` / `WindowAggExec` are pure row-annotation:
        // each input row emits exactly one output row with a new column
        // (the window function's result). The routing key's values,
        // partitioning, and row count are all preserved verbatim.
        || plan.downcast_ref::<BoundedWindowAggExec>().is_some()
        || plan.downcast_ref::<WindowAggExec>().is_some()
}

/// Split `batch` into `K = boundaries.len() + 1` sub-batches under the
/// half-open convention: partition `p` receives rows where
/// `boundaries[p-1] <= key < boundaries[p]` (open at `-∞` on partition 0
/// and at `+∞` on partition K-1). Output vector is always length K; empty
/// buckets produce empty `RecordBatch`es rather than being omitted, so
/// callers can index by partition id.
///
/// NULL routing keys land in partition 0 today; a follow-up will honor
/// `sort_options.nulls_first`/`nulls_last` to match SQL semantics.
///
/// Boundaries are pre-extracted `f64`s; widening to other routing key
/// types generalizes this function and the discovery path together.
pub(super) fn split_batch_by_range(
    batch: &RecordBatch,
    routing_expr: &Arc<dyn PhysicalExpr>,
    boundaries: &[f64],
) -> Result<Vec<RecordBatch>> {
    let output_partitions = boundaries.len() + 1;
    let schema = batch.schema();
    if batch.num_rows() == 0 {
        return Ok((0..output_partitions)
            .map(|_| RecordBatch::new_empty(schema.clone()))
            .collect());
    }
    let evaluated = routing_expr.evaluate(batch)?;
    let array = evaluated.into_array(batch.num_rows())?;
    let keys = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            internal_datafusion_err!(
                "range-repartition: routing expr produced {:?}, expected Float64",
                array.data_type()
            )
        })?;

    // TODO: vectorized arrow computation
    let mut buckets: Vec<Vec<u32>> = (0..output_partitions).map(|_| Vec::new()).collect();
    for row in 0..batch.num_rows() {
        let target = if keys.is_null(row) {
            0
        } else {
            // partition_point returns the count of elements matching the
            // predicate — here `<= key` — which is the target partition
            // index under the half-open convention.
            boundaries.partition_point(|&cut| cut <= keys.value(row))
        };
        buckets[target].push(row as u32);
    }

    let mut result = Vec::with_capacity(output_partitions);
    for indices in buckets {
        if indices.is_empty() {
            result.push(RecordBatch::new_empty(schema.clone()));
        } else {
            let idx_array = UInt32Array::from(indices);
            let taken = take_arrays(batch.columns(), &idx_array, None)?;
            result.push(RecordBatch::try_new(schema.clone(), taken)?);
        }
    }
    Ok(result)
}

/// Best-effort broadcast of a terminal error to every output channel.
/// `DataFusionError` isn't `Clone`; serialize via `to_string` and re-wrap as
/// `Internal` on replicas beyond the first.
pub(super) async fn broadcast_error(
    senders: &[mpsc::Sender<Result<RecordBatch>>],
    err: datafusion::error::DataFusionError,
) {
    let message = err.to_string();
    let mut first = Some(err);
    for sender in senders.iter() {
        let payload = match first.take() {
            Some(original) => Err(original),
            None => Err(internal_datafusion_err!("{}", message)),
        };
        let _ = sender.send(payload).await;
    }
}

/// Run a scatter body and convert every terminal state — clean EOF, DFError,
/// or panic — into an explicit signal on the K output channels. Wraps the
/// future in `catch_unwind` so a panic inside `fut` becomes a broadcast
/// error rather than a silent sender drop (which downstream would misread
/// as a clean EOF).
///
/// `AssertUnwindSafe` is required because async futures aren't `UnwindSafe`
/// by default. Safe here because the scatter futures' captured state is
/// `Arc`s and owned locals — nothing observes post-panic state after we
/// broadcast and return.
pub(super) async fn guarded_scatter<F>(
    fut: F,
    senders: Arc<[mpsc::Sender<Result<RecordBatch>>]>,
) where
    F: Future<Output = Result<()>>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(Ok(())) => {
            // Drop the senders — receivers see clean EOF.
        }
        Ok(Err(err)) => broadcast_error(&senders, err).await,
        Err(panic_payload) => {
            let msg = panic_payload_message(panic_payload);
            broadcast_error(
                &senders,
                internal_datafusion_err!("scatter task panicked: {msg}"),
            )
            .await;
        }
    }
}

/// Extract a human-readable message from a `catch_unwind` panic payload.
/// `panic!("literal")` yields `&'static str`; `panic!("{x}")` yields
/// `String`. Anything else falls back to a placeholder.
fn panic_payload_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Shared test-only utilities for the two range-repartition operators.
/// The ordered variant will exercise the same `guarded_scatter` panic-path,
/// so the panic-source lives here rather than being duplicated per operator.
#[cfg(test)]
pub(super) mod test_util {
    use std::fmt::{self, Formatter};
    use std::sync::Arc;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{Schema, SchemaRef};
    use datafusion::common::Result;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        SendableRecordBatchStream,
    };

    /// Message the synthetic panic will surface. Exposed so tests can
    /// assert-round-trip: after `guarded_scatter`'s payload downcast, this
    /// exact string must appear in the broadcast error.
    pub(crate) const SYNTHETIC_PANIC_MESSAGE: &str =
        "PanickingSourceExec: synthetic test panic";

    /// Test-only source whose stream panics on the first poll. Used to
    /// exercise `guarded_scatter`'s `catch_unwind` → broadcast-error path
    /// end-to-end through a real `ExecutionPlan::execute()` call.
    #[derive(Debug)]
    pub(crate) struct PanickingSourceExec {
        schema: SchemaRef,
        properties: Arc<PlanProperties>,
    }

    impl PanickingSourceExec {
        pub(crate) fn new(schema: &Arc<Schema>) -> Self {
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self {
                schema: schema.clone(),
                properties,
            }
        }
    }

    impl DisplayAs for PanickingSourceExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "PanickingSourceExec")
        }
    }

    impl ExecutionPlan for PanickingSourceExec {
        fn name(&self) -> &str {
            "PanickingSourceExec"
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _ctx: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let stream = futures::stream::once(async {
                panic!("{SYNTHETIC_PANIC_MESSAGE}");
                #[allow(unreachable_code)]
                Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::col;
    use std::sync::Arc;

    fn f64_col(schema: &Schema, name: &str) -> Arc<dyn PhysicalExpr> {
        col(name, schema).unwrap()
    }

    fn schema_v2_id() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("v2", DataType::Float64, true),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn batch(schema: &Arc<Schema>, keys: Vec<Option<f64>>, ids: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(keys)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn split_conserves_rows_across_buckets() {
        let schema = schema_v2_id();
        let batch = batch(
            &schema,
            vec![
                Some(-3.0),
                Some(0.0),
                Some(1.5),
                Some(5.0),
                Some(9.9),
                Some(10.0),
                Some(100.0),
            ],
            vec![0, 1, 2, 3, 4, 5, 6],
        );
        let routing = f64_col(&schema, "v2");
        let splits = split_batch_by_range(&batch, &routing, &[0.0, 10.0]).unwrap();
        assert_eq!(splits.len(), 3, "K = boundaries.len() + 1");
        let total: usize = splits.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, batch.num_rows(), "no row lost or duplicated");
    }

    #[test]
    fn split_half_open_boundary_lands_in_higher_partition() {
        let schema = schema_v2_id();
        // 0.0 lands in partition 1 (not 0); 10.0 lands in partition 2 (not 1).
        let batch = batch(
            &schema,
            vec![Some(-0.1), Some(0.0), Some(9.999), Some(10.0)],
            vec![0, 1, 2, 3],
        );
        let routing = f64_col(&schema, "v2");
        let splits = split_batch_by_range(&batch, &routing, &[0.0, 10.0]).unwrap();
        assert_eq!(splits[0].num_rows(), 1);
        assert_eq!(splits[1].num_rows(), 2);
        assert_eq!(splits[2].num_rows(), 1);
    }

    #[test]
    fn split_routes_nulls_to_partition_zero() {
        let schema = schema_v2_id();
        let batch = batch(
            &schema,
            vec![None, Some(5.0), None, Some(50.0)],
            vec![0, 1, 2, 3],
        );
        let routing = f64_col(&schema, "v2");
        let splits = split_batch_by_range(&batch, &routing, &[10.0]).unwrap();
        assert_eq!(splits[0].num_rows(), 3);
        assert_eq!(splits[1].num_rows(), 1);
    }

    #[test]
    fn split_empty_batch_produces_k_empty_batches() {
        let schema = schema_v2_id();
        let batch = batch(&schema, vec![], vec![]);
        let routing = f64_col(&schema, "v2");
        let splits = split_batch_by_range(&batch, &routing, &[0.0, 10.0]).unwrap();
        assert_eq!(splits.len(), 3);
        assert!(splits.iter().all(|b| b.num_rows() == 0));
    }
}
