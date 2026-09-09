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

use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::{Result, ScalarValue, internal_datafusion_err};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use futures::FutureExt;
use log::warn;
use tokio::sync::mpsc;

use crate::execution_plans::RuntimeStatsExec;
use crate::execution_plans::plan_algebra::preserves_distribution;

/// Walk `child`'s subtree for a [`RuntimeStatsExec`] that sketches on our
/// routing expression, snapshot its merged sketch, and compute `K - 1`
/// quantile cuts. No sketch to read returns an empty `Vec` — the caller's
/// `split_batch_by_range(&[])` produces a single bucket and every row lands
/// in output partition 0.
///
/// Errors only when a sketch was found and contradicted itself, which the
/// single-bucket fallback would turn into a silently one-partition query.
pub(super) fn discover_cuts(
    child: &Arc<dyn ExecutionPlan>,
    routing_expr: &dyn PhysicalExpr,
    output_partitions: usize,
) -> Result<Vec<ScalarValue>> {
    let Some(stats) = find_runtime_stats(child, routing_expr) else {
        warn!(
            "range-repartition: no matching RuntimeStatsExec found in child subtree — \
             single-bucket fallback"
        );
        return Ok(Vec::new());
    };
    // Walker returned Some → stats.order_by()'s first entry matches our
    // routing expression → RuntimeStatsExec's construction contract
    // guarantees sketch is present. Belt-and-braces arms in case that
    // invariant ever drifts, plus mutex-poisoning is theoretically possible.
    let sketch = match stats.merged_sort_key_sketch() {
        Ok(Some(sketch)) => sketch,
        Ok(None) => {
            warn!(
                "range-repartition: matching RuntimeStatsExec has no sketch \
                 (RuntimeStatsExec contract broken?) — single-bucket fallback"
            );
            return Ok(Vec::new());
        }
        Err(e) => {
            warn!(
                "range-repartition: sketch snapshot failed ({e}) — single-bucket fallback"
            );
            return Ok(Vec::new());
        }
    };
    // Zero means no samples arrived before the snapshot; degenerate cuts would follow.
    if sketch.count() == 0 {
        warn!(
            "range-repartition: matching sketch has no samples yet — single-bucket fallback"
        );
        return Ok(Vec::new());
    }
    // `cuts` sizes by the whole population, keeps every boundary a real value
    // so no consumer compares against a NULL, and puts the NULL run wholly in
    // the partition at the end `nulls_first` names — which is where
    // `split_batch_by_range` sends it and where `RangeFilterExec` looks for it.
    sketch.cuts(output_partitions)
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

/// Split `batch` along boundaries under the half-open convention. Empty buckets produce empty
/// `RecordBatch`es rather than being omitted, so callers can index by partition id.
/// Boundaries must be in `options` order, which is what [`discover_cuts`] produces.
pub(super) fn split_batch_by_range(
    batch: &RecordBatch,
    routing_expr: &Arc<dyn PhysicalExpr>,
    boundaries: &[ScalarValue],
    options: SortOptions,
) -> Result<Vec<RecordBatch>> {
    let output_partitions = boundaries.len() + 1;
    let schema = batch.schema();
    if batch.num_rows() == 0 {
        return Ok((0..output_partitions)
            .map(|_| RecordBatch::new_empty(schema.clone()))
            .collect());
    }
    let [first_boundary, ..] = boundaries else {
        return Ok(vec![batch.clone()]);
    };
    let keys = routing_expr.evaluate(batch)?;
    let keys = keys.into_array(batch.num_rows())?;

    let converter = RowConverter::new(vec![SortField::new_with_options(
        keys.data_type().clone(),
        options,
    )])
    .map_err(|e| {
        internal_datafusion_err!(
            "range-repartition: {:?} has no row encoding: {e}",
            keys.data_type()
        )
    })?;
    let boundaries = converter.convert_columns(&[ScalarValue::iter_to_array(
        boundaries.iter().cloned(),
    )
    .map_err(|e| {
        internal_datafusion_err!(
            "range-repartition: boundaries starting {first_boundary:?} do not form \
             one array: {e}"
        )
    })?])?;
    let boundaries: Vec<_> = boundaries.iter().collect();
    let keys = converter.convert_columns(&[keys])?;

    let mut buckets: Vec<Vec<u32>> = (0..output_partitions).map(|_| Vec::new()).collect();
    for (row_idx, key) in keys.iter().enumerate() {
        let bucket_idx = boundaries.partition_point(|boundary| *boundary <= key);
        buckets[bucket_idx].push(row_idx as u32);
    }

    let mut result = Vec::with_capacity(output_partitions);
    for row_idxs in buckets {
        if row_idxs.is_empty() {
            result.push(RecordBatch::new_empty(schema.clone()));
        } else {
            let row_idxs = UInt32Array::from(row_idxs);
            let bucketed = take_arrays(batch.columns(), &row_idxs, None)?;
            result.push(RecordBatch::try_new(schema.clone(), bucketed)?);
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
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::{
        EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
    };
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

        /// Same as `new` but declares `order_by` as the output ordering.
        /// `OrderedRangeRepartitionExec::try_new` rejects inputs without a
        /// declared ordering, so its panic test needs this variant.
        pub(crate) fn with_ordering(
            schema: &Arc<Schema>,
            order_by: Vec<PhysicalSortExpr>,
        ) -> Self {
            let mut eq = EquivalenceProperties::new(schema.clone());
            if let Some(lex) = LexOrdering::new(order_by) {
                eq.add_ordering(lex);
            }
            let properties = Arc::new(PlanProperties::new(
                eq,
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

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
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

    /// Boundaries as `Float64` scalars, which is what discovery produces.
    fn cuts<const N: usize>(values: [f64; N]) -> Vec<ScalarValue> {
        values
            .into_iter()
            .map(|v| ScalarValue::Float64(Some(v)))
            .collect()
    }

    fn asc(nulls_first: bool) -> SortOptions {
        SortOptions {
            descending: false,
            nulls_first,
        }
    }

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
        let splits =
            split_batch_by_range(&batch, &routing, &cuts([0.0, 10.0]), asc(true))
                .unwrap();
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
        let splits =
            split_batch_by_range(&batch, &routing, &cuts([0.0, 10.0]), asc(true))
                .unwrap();
        assert_eq!(splits[0].num_rows(), 1);
        assert_eq!(splits[1].num_rows(), 2);
        assert_eq!(splits[2].num_rows(), 1);
    }

    #[test]
    fn split_routes_the_whole_null_run_to_the_end_it_occupies() {
        let schema = schema_v2_id();
        let batch = batch(
            &schema,
            vec![None, Some(5.0), None, Some(50.0)],
            vec![0, 1, 2, 3],
        );
        let routing = f64_col(&schema, "v2");
        // Two NULLs, 5.0 below the cut, 50.0 above it.
        let first =
            split_batch_by_range(&batch, &routing, &cuts([10.0]), asc(true)).unwrap();
        assert_eq!(first[0].num_rows(), 3, "NULLs + 5.0");
        assert_eq!(first[1].num_rows(), 1, "50.0");

        let last =
            split_batch_by_range(&batch, &routing, &cuts([10.0]), asc(false)).unwrap();
        assert_eq!(last[0].num_rows(), 1, "5.0");
        assert_eq!(last[1].num_rows(), 3, "50.0 + NULLs");
    }

    /// A DESC key's boundaries arrive descending, because that is the order
    /// the sketch that produced them counts in. Routing has to read them in
    /// that same order or every row lands in the mirrored partition.
    #[test]
    fn split_follows_a_descending_key() {
        let schema = schema_v2_id();
        let batch = batch(
            &schema,
            vec![Some(100.0), Some(10.0), Some(5.0), Some(-1.0)],
            vec![0, 1, 2, 3],
        );
        let routing = f64_col(&schema, "v2");
        let descending = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let splits =
            split_batch_by_range(&batch, &routing, &cuts([10.0, 0.0]), descending)
                .unwrap();
        assert_eq!(splits[0].num_rows(), 1, "100.0 sorts above the 10.0 cut");
        assert_eq!(splits[1].num_rows(), 2, "10.0 and 5.0 sit between the cuts");
        assert_eq!(splits[2].num_rows(), 1, "-1.0 sorts below the 0.0 cut");
    }

    /// No boundaries is the discovery fallback, and it has to stay the whole
    /// batch in one bucket rather than erroring on the empty boundary set.
    #[test]
    fn split_without_boundaries_keeps_one_bucket() {
        let schema = schema_v2_id();
        let batch = batch(&schema, vec![Some(1.0), None, Some(3.0)], vec![0, 1, 2]);
        let routing = f64_col(&schema, "v2");
        let splits = split_batch_by_range(&batch, &routing, &[], asc(true)).unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].num_rows(), 3);
    }

    #[test]
    fn split_empty_batch_produces_k_empty_batches() {
        let schema = schema_v2_id();
        let batch = batch(&schema, vec![], vec![]);
        let routing = f64_col(&schema, "v2");
        let splits =
            split_batch_by_range(&batch, &routing, &cuts([0.0, 10.0]), asc(true))
                .unwrap();
        assert_eq!(splits.len(), 3);
        assert!(splits.iter().all(|b| b.num_rows() == 0));
    }
}
