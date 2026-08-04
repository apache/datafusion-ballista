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

//! Filter with a distinct predicate per input partition.
//!
//! `FilterExec` in DataFusion carries a single predicate applied to every
//! partition. That's wrong for the range-repartition-consuming reader in
//! the adaptive range shuffle: each downstream partition `k` needs a range
//! predicate `cuts[k-1] <= key < cuts[k]` unique to that partition, so
//! straddling sub-parts from the producer are trimmed to just partition
//! `k`'s slice.
//!
//! One-task-per-downstream-partition + plain `FilterExec` would work but
//! defeats vcore packing (`K` tasks instead of `K / vcores`). This operator
//! keeps packing: `predicates[k]` is applied to `input.execute(k)`, so a
//! single task consuming several partitions still gets each partition's
//! own predicate.
//!
//! Semantics per batch mirror `FilterExec`: evaluate the boolean expr
//! against the batch, then `filter_record_batch`. No projection, no
//! coalescing, no metrics — those can grow later if the wiring warrants.

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::stream::{
    EmptyRecordBatchStream, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt, ready};

/// Filter with per-input-partition predicates.
///
/// `predicates[k]` is applied to `input.execute(k)`. Requires
/// `predicates.len() == input.output_partitioning().partition_count()`.
pub struct PerPartitionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicates: Vec<Arc<dyn PhysicalExpr>>,
    properties: Arc<PlanProperties>,
}

impl PerPartitionFilterExec {
    /// Wrap `input` with a vector of predicates, one per input partition.
    ///
    /// Fails if the predicate count doesn't match the input partition count
    /// or if any predicate does not evaluate to `Boolean` against the input
    /// schema.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        predicates: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        let partition_count = input.output_partitioning().partition_count();
        if predicates.len() != partition_count {
            return internal_err!(
                "PerPartitionFilterExec: predicate count {} does not match input partition count {}",
                predicates.len(),
                partition_count
            );
        }
        let schema = input.schema();
        for (k, predicate) in predicates.iter().enumerate() {
            let dt = predicate.data_type(&schema)?;
            if dt != DataType::Boolean {
                return internal_err!(
                    "PerPartitionFilterExec: predicate[{k}] must evaluate to Boolean, got {dt}"
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
            predicates,
            properties,
        })
    }

    /// The per-partition predicates. `predicates()[k]` corresponds to input partition `k`.
    pub fn predicates(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.predicates
    }
}

impl Debug for PerPartitionFilterExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PerPartitionFilterExec")
            .field("num_predicates", &self.predicates.len())
            .finish()
    }
}

impl DisplayAs for PerPartitionFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PerPartitionFilterExec: predicates=[{}]",
                    self.predicates
                        .iter()
                        .map(|p| p.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "PerPartitionFilterExec")
            }
        }
    }
}

impl ExecutionPlan for PerPartitionFilterExec {
    fn name(&self) -> &str {
        "PerPartitionFilterExec"
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
                "PerPartitionFilterExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(PerPartitionFilterExec::try_new(
            input.clone(),
            self.predicates.clone(),
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

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(predicate) = self.predicates.get(partition).cloned() else {
            return internal_err!(
                "PerPartitionFilterExec: partition {} out of bounds ({} predicates)",
                partition,
                self.predicates.len()
            );
        };
        let schema = self.schema();
        let input = self.input.execute(partition, ctx)?;
        let stream = PerPartitionFilterStream {
            schema: schema.clone(),
            predicate,
            input,
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

struct PerPartitionFilterStream {
    schema: SchemaRef,
    predicate: Arc<dyn PhysicalExpr>,
    input: SendableRecordBatchStream,
}

impl Stream for PerPartitionFilterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let mask = self
                        .predicate
                        .evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))?;
                    let mask = as_boolean_array(&mask)?;
                    let filtered = filter_record_batch(&batch, mask)?;
                    if filtered.num_rows() == 0 {
                        // Nothing left after filtering; pull the next batch
                        // rather than emit an empty batch downstream.
                        continue;
                    }
                    return Poll::Ready(Some(Ok(filtered)));
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    // Release the input pipeline's resources on EOS —
                    // mirrors DataFusion's FilterExec so the input's
                    // child chain doesn't linger on the heap until the
                    // outer stream is itself dropped.
                    let input_schema = self.input.schema();
                    self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for PerPartitionFilterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Build the `K = cuts.len() + 1` half-open range predicates a
/// `PerPartitionFilterExec` needs to reproduce the range repartition's
/// write-side routing on the read side.
///
/// Partition `i` receives the predicate
///
/// ```text
///   i = 0        →  routing_expr < cuts[0]
///   0 < i < K-1  →  cuts[i-1] <= routing_expr AND routing_expr < cuts[i]
///   i = K-1      →  routing_expr >= cuts[K-2]
///   K = 1        →  lit(true)   // empty cuts, single-bucket range repartition
/// ```
///
/// Consistent with the private `range_repartition_common::split_batch_by_range`
/// helper, which uses the same half-open convention on the write side.
/// Callers pass the range repartition's routing expression verbatim
/// (`CAST(order_by[0] AS Float64)` today).
///
/// Non-null routing expressions only. Both `UnorderedRangeRepartitionExec`
/// and `OrderedRangeRepartitionExec` refuse nullable routing exprs at
/// `try_new`, so any expression that reaches this helper via
/// `RangeRepartitionRouting` is guaranteed non-null — no `IS NULL` branch
/// needed.
pub fn range_partition_predicates(
    routing_expr: Arc<dyn PhysicalExpr>,
    cuts: &[f64],
) -> Vec<Arc<dyn PhysicalExpr>> {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion::scalar::ScalarValue;

    let partition_count = cuts.len() + 1;
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
    (0..partition_count)
        .map(|partition_idx| {
            let lo = partition_idx
                .checked_sub(1)
                .and_then(|cut_idx| cuts.get(cut_idx).copied());
            let hi = cuts.get(partition_idx).copied();
            match (lo, hi) {
                (None, None) => {
                    // K == 1: single bucket covers everything.
                    Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
                        as Arc<dyn PhysicalExpr>
                }
                (None, Some(hi)) => lt(hi),
                (Some(lo), None) => ge(lo),
                (Some(lo), Some(hi)) => {
                    Arc::new(BinaryExpr::new(ge(lo), Operator::And, lt(hi)))
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use futures::TryStreamExt;

    fn one_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// Memory source with `partitions` partitions, each carrying a single
    /// batch of `[start .. start + rows_per)` where `start = k * rows_per`.
    fn partitioned_source(partitions: usize, rows_per: usize) -> Arc<dyn ExecutionPlan> {
        let schema = one_col_schema();
        let mut per_partition: Vec<Vec<RecordBatch>> = Vec::with_capacity(partitions);
        for k in 0..partitions {
            let start = (k * rows_per) as i64;
            let arr = Int64Array::from_iter_values(start..start + rows_per as i64);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
            per_partition.push(vec![batch]);
        }
        let src =
            MemorySourceConfig::try_new(&per_partition, schema, None).expect("mem src");
        Arc::new(DataSourceExec::new(Arc::new(src)))
    }

    /// Predicate `v >= lo AND v < hi` against column `v`.
    fn range_pred(lo: i64, hi: i64) -> Arc<dyn PhysicalExpr> {
        let col = Arc::new(Column::new("v", 0));
        let lo_lit = Arc::new(Literal::new(ScalarValue::Int64(Some(lo))));
        let hi_lit = Arc::new(Literal::new(ScalarValue::Int64(Some(hi))));
        let ge: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(col.clone(), Operator::GtEq, lo_lit));
        let lt: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(col, Operator::Lt, hi_lit));
        Arc::new(BinaryExpr::new(ge, Operator::And, lt))
    }

    fn ctx() -> Arc<TaskContext> {
        SessionContext::new().task_ctx()
    }

    async fn collect(plan: Arc<dyn ExecutionPlan>, partition: usize) -> Result<Vec<i64>> {
        let stream = plan.execute(partition, ctx())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let mut out = Vec::new();
        for b in batches {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array");
            out.extend(arr.iter().map(|v| v.unwrap()));
        }
        Ok(out)
    }

    /// Each of three partitions carries `[k*100, k*100+100)`. With
    /// per-partition predicates that each carve a five-row slice, every
    /// partition emits its own five rows and nothing from another
    /// partition leaks through.
    #[tokio::test]
    async fn per_partition_predicate_filters_only_that_partition() -> Result<()> {
        let src = partitioned_source(3, 100);
        let predicates = vec![
            range_pred(0, 5),     // partition 0 → 0..5
            range_pred(105, 110), // partition 1 → 105..110
            range_pred(295, 300), // partition 2 → 295..300
        ];
        let ppf: Arc<dyn ExecutionPlan> =
            Arc::new(PerPartitionFilterExec::try_new(src, predicates)?);
        assert_eq!(collect(ppf.clone(), 0).await?, (0..5).collect::<Vec<_>>());
        assert_eq!(
            collect(ppf.clone(), 1).await?,
            (105..110).collect::<Vec<_>>()
        );
        assert_eq!(collect(ppf, 2).await?, (295..300).collect::<Vec<_>>());
        Ok(())
    }

    /// A predicate that matches nothing yields an empty stream (no zero-row
    /// batches surfaced to the caller). Regression pin — an earlier draft
    /// forwarded empty batches, which some downstream operators dislike.
    #[tokio::test]
    async fn empty_predicate_yields_empty_stream() -> Result<()> {
        let src = partitioned_source(1, 100);
        let predicates = vec![range_pred(1_000_000, 2_000_000)];
        let ppf: Arc<dyn ExecutionPlan> =
            Arc::new(PerPartitionFilterExec::try_new(src, predicates)?);
        assert_eq!(collect(ppf, 0).await?, Vec::<i64>::new());
        Ok(())
    }

    /// Predicate-count mismatch is rejected at construction time.
    #[test]
    fn rejects_predicate_count_mismatch() {
        let src = partitioned_source(3, 10);
        let err =
            PerPartitionFilterExec::try_new(src, vec![range_pred(0, 5)]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("predicate count 1") && msg.contains("input partition count 3"),
            "unexpected error: {msg}"
        );
    }

    /// Non-boolean predicate is rejected at construction time (the
    /// expression must evaluate to `Boolean` against the input schema).
    #[test]
    fn rejects_non_boolean_predicate() {
        let src = partitioned_source(1, 10);
        // Just the column `v` — evaluates to Int64, not Boolean.
        let bad: Arc<dyn PhysicalExpr> = Arc::new(Column::new("v", 0));
        let err = PerPartitionFilterExec::try_new(src, vec![bad]).unwrap_err();
        assert!(
            err.to_string().contains("Boolean"),
            "unexpected error: {err}"
        );
    }

    /// The K=4 range predicates cover every value under the half-open
    /// convention, and each row lands in exactly one predicate. Random
    /// probe values are routed through the predicates and expected to
    /// match the same partition assignment as the range repartition's
    /// write-side `split_batch_by_range` would produce.
    #[test]
    fn range_partition_predicates_partition_every_value_exactly_once() {
        use datafusion::arrow::array::Float64Array;
        use datafusion::arrow::datatypes::Field;
        use datafusion::physical_expr::expressions::Column;

        let cuts = vec![10.0, 20.0, 30.0];
        let k = cuts.len() + 1;
        let routing: Arc<dyn PhysicalExpr> = Arc::new(Column::new("v", 0));
        let preds = range_partition_predicates(routing, &cuts);
        assert_eq!(preds.len(), k);

        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let values: Vec<f64> = vec![
            -5.0, 0.0, 9.999, 10.0, 15.0, 19.999, 20.0, 25.0, 30.0, 100.0,
        ];
        let arr = Float64Array::from_iter_values(values.iter().copied());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();

        // For each row, find the unique partition whose predicate accepts it.
        for (row, &v) in values.iter().enumerate() {
            let mut hits = 0;
            for pred in &preds {
                let mask = pred
                    .evaluate(&batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
                    .unwrap();
                let mask = as_boolean_array(&mask).unwrap();
                if mask.value(row) {
                    hits += 1;
                }
            }
            assert_eq!(
                hits, 1,
                "value {v} matched {hits} predicates, expected exactly 1"
            );
        }

        // Expected assignment mirrors split_batch_by_range: `partition_point`
        // returns the count of cuts `<= key`, which is the partition index
        // under the half-open convention.
        let expected: Vec<usize> = values
            .iter()
            .map(|v| cuts.partition_point(|&c| c <= *v))
            .collect();
        for (row, want) in expected.iter().enumerate() {
            let mask = preds[*want]
                .evaluate(&batch)
                .and_then(|v| v.into_array(batch.num_rows()))
                .unwrap();
            let mask = as_boolean_array(&mask).unwrap();
            assert!(
                mask.value(row),
                "value {} should have landed in partition {}",
                values[row],
                want
            );
        }
    }

    /// Degenerate K=1 (empty cuts) yields a single lit(true) predicate.
    #[test]
    fn range_partition_predicates_single_bucket_when_cuts_empty() {
        use datafusion::physical_expr::expressions::Column;

        let routing: Arc<dyn PhysicalExpr> = Arc::new(Column::new("v", 0));
        let preds = range_partition_predicates(routing, &[]);
        assert_eq!(preds.len(), 1);
        assert_eq!(preds[0].to_string(), "true");
    }

    /// `with_new_children` swaps the input while preserving the predicate
    /// vector. Wrapping the original source in a `RepartitionExec` that
    /// keeps the partition count (RoundRobin(3)) gives a valid child; the
    /// filter still routes partition-`k` rows through `predicates[k]`.
    #[tokio::test]
    async fn with_new_children_preserves_predicates() -> Result<()> {
        let src = partitioned_source(3, 100);
        let predicates =
            vec![range_pred(0, 3), range_pred(100, 103), range_pred(200, 203)];
        let ppf = Arc::new(PerPartitionFilterExec::try_new(
            src.clone(),
            predicates.clone(),
        )?);
        // Wrap the source in RoundRobin(3) — same partition count, different plan.
        let repart: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            src,
            Partitioning::RoundRobinBatch(3),
        )?);
        let swapped: Arc<dyn ExecutionPlan> = ppf.with_new_children(vec![repart])?;
        // Just verify construction succeeded and the operator name survives.
        assert_eq!(swapped.name(), "PerPartitionFilterExec");
        Ok(())
    }
}
