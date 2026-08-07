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

//! Wrap a `BoundedWindowAggExec` and override its
//! `required_input_distribution` to `Unspecified`.
//!
//! # Status: temporary
//!
//! This is a Ballista-side wrapper because DataFusion's `BoundedWindowAggExec`
//! doesn't yet expose a way to override its `SinglePartition` requirement.
//! The upstream draft is <https://github.com/apache/datafusion/pull/23026>
//! ("Parallel bounded RANGE-frame window functions without PARTITION BY").
//! When that lands and Ballista bumps its DF pin past it, this wrapper
//! should collapse and the rule should target DF's BWAG directly.
//!
//! DataFusion's `BoundedWindowAggExec` declares
//! `Distribution::SinglePartition` when no PARTITION BY is present — a
//! correctness guard because a window frame's semantics span rows across
//! the whole input. With `ParallelWindowRule`'s range-repartition upstream,
//! each input partition IS globally range-disjoint (halo covers boundary
//! neighbours), so BWAG CAN run per-partition and produce K correct outputs.
//!
//! This wrapper flips only the distribution declaration. Everything else —
//! schema, ordering, per-partition `execute()` — delegates to a canonical
//! inner BWAG constructed at rule time. Because `children()` returns only
//! the wrapper's own input (not the inner BWAG), tree walkers like
//! `EnforceDistribution` never see BWAG and can't re-insert an
//! `SPM(K→1)` beneath it.
//!
//! # Constraints assumed by the caller
//!
//! The wrapper is safe iff the input is already range-repartitioned so that
//! each partition is a globally disjoint slice of the ORDER BY key + halo
//! for frame boundaries. Callers are responsible for this — the wrapper
//! itself doesn't (and can't) verify it. Wiring this wrapper below arbitrary
//! (non-range-partitioned) inputs will silently produce wrong window values.
//!
//! # Assumption on DataFusion internals
//!
//! `BoundedWindowAggExec::execute(i, ctx)` in DataFusion 54 processes
//! partition `i` of its input independently: no cross-partition state, no
//! spawned tasks touching sibling partitions. The wrapper depends on that
//! shape. The `per_partition_execute_running_sum_no_cross_partition_leak`
//! test exercises this — if BWAG ever gains cross-partition state, that
//! test's partition-1 sums would shift and it would fail.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{Distribution, OrderingRequirements};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InputOrderMode};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::windows::WindowExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

// The rule's `as_candidate` gates guarantee no PARTITION BY + single Column
// ORDER BY over a sorted source, so `BWAG::try_new` is always invoked with
// `InputOrderMode::Sorted` and `can_repartition=false` (partition_keys() is
// empty either way when there's no PARTITION BY). Hardcode both to keep the
// wire and the type small.
const BWAG_INPUT_ORDER_MODE: InputOrderMode = InputOrderMode::Sorted;
const BWAG_CAN_REPARTITION: bool = false;

/// Wrap a `BoundedWindowAggExec` overriding its `required_input_distribution`
/// to `Unspecified`. See module docs for what makes this safe.
#[derive(Debug, Clone)]
pub struct PartitionedBoundedWindowAggExec {
    /// Canonical inner BWAG built at construction. Not a plan-tree child —
    /// `children()` returns only [`Self::input`], so tree walkers can't
    /// reach it.
    inner_bwag: Arc<BoundedWindowAggExec>,
    /// Multi-partition input; same `Arc` `inner_bwag.input()` holds.
    input: Arc<dyn ExecutionPlan>,
}

impl PartitionedBoundedWindowAggExec {
    /// Construct the wrapper. Builds a canonical inner
    /// `BoundedWindowAggExec` from `window_expr` + `input` with the
    /// hardcoded mode/repartition constants; failures propagate verbatim so
    /// callers see the same error surface as constructing BWAG directly.
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let inner_bwag = Arc::new(BoundedWindowAggExec::try_new(
            window_expr,
            input.clone(),
            BWAG_INPUT_ORDER_MODE,
            BWAG_CAN_REPARTITION,
        )?);
        Ok(Self { inner_bwag, input })
    }

    /// The wrapped `BoundedWindowAggExec` — for accessors that don't exist
    /// on `Self` and for wire-encoding.
    pub fn inner_bwag(&self) -> &Arc<BoundedWindowAggExec> {
        &self.inner_bwag
    }

    /// The window expressions carried by the wrapped BWAG.
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        self.inner_bwag.window_expr()
    }

    /// Output schema (delegates to BWAG).
    pub fn schema(&self) -> SchemaRef {
        self.inner_bwag.schema()
    }
}

impl DisplayAs for PartitionedBoundedWindowAggExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PartitionedBoundedWindowAggExec: ")?;
        self.inner_bwag.fmt_as(t, f)
    }
}

impl ExecutionPlan for PartitionedBoundedWindowAggExec {
    fn name(&self) -> &'static str {
        "PartitionedBoundedWindowAggExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        // BWAG's properties already advertise `input.output_partitioning()`
        // as this operator's output partitioning — reuse them verbatim.
        self.inner_bwag.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // The whole point of this wrapper.
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.inner_bwag.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [new_input] = children.as_slice() else {
            return internal_err!(
                "PartitionedBoundedWindowAggExec expects exactly 1 child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(Self::try_new(
            self.window_expr().to_vec(),
            new_input.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner_bwag.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner_bwag.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner_bwag.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner_bwag.cardinality_effect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::{
        WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion::physical_expr::LexOrdering;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::windows::create_window_expr;
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use futures::TryStreamExt;

    /// Two range-disjoint partitions, both sorted on `v`. Within each
    /// partition rows are 1.0 apart so a 2.0-preceding frame spans multiple
    /// rows (running sum). The between-partition gap (97.0) is wider than
    /// the frame, so no frame ever crosses the boundary — per-partition BWAG
    /// stays correct despite the K→K shape.
    #[tokio::test]
    async fn per_partition_execute_running_sum_no_cross_partition_leak()
    -> datafusion::common::Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let partitions = vec![
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))],
            )?],
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Float64Array::from(vec![100.0, 101.0, 102.0]))],
            )?],
        ];
        let v_expr = col("v", schema.as_ref())?;
        let order_by = vec![PhysicalSortExpr {
            expr: v_expr.clone(),
            options: SortOptions::default(),
        }];
        let sort_information = vec![
            LexOrdering::new(order_by.clone())
                .expect("single sort expr is a valid LexOrdering"),
        ];
        let source = MemorySourceConfig::try_new(&partitions, schema.clone(), None)?
            .try_with_sort_information(sort_information)?;
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(source)));

        // sum(v) OVER (ORDER BY v RANGE BETWEEN 2.0 PRECEDING AND CURRENT ROW).
        // Rows within a partition are 1.0 apart so a 2.0 frame spans them;
        // partitions are 97.0 apart so no frame crosses the boundary.
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::Float64(Some(2.0))),
            WindowFrameBound::CurrentRow,
        );
        let window_expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            "sum(v)".to_string(),
            &[v_expr],
            &[],
            &order_by,
            Arc::new(frame),
            schema.clone(),
            false,
            false,
            None,
        )?;
        let pbwag = PartitionedBoundedWindowAggExec::try_new(vec![window_expr], input)?;

        // Shape: wrapper must not collapse K→1 and must hide BWAG from the tree.
        assert_eq!(
            pbwag.properties().output_partitioning().partition_count(),
            2,
            "PBWAG must not collapse partitions"
        );
        assert!(matches!(
            pbwag.required_input_distribution().as_slice(),
            [Distribution::UnspecifiedDistribution]
        ));
        assert_eq!(
            pbwag.children().len(),
            1,
            "children() must return only the input, hiding the inner BWAG"
        );

        // Execute both partitions. Expected sums are running-sums within each
        // partition; if any row from partition 0 leaked into partition 1's
        // frame (or vice versa), partition 1's first sum would be > 100.0.
        // Output schema is [v, sum(v)].
        let ctx = SessionContext::new().task_ctx();
        let pbwag: Arc<dyn ExecutionPlan> = Arc::new(pbwag);
        for (partition, expected) in [(0, [1.0, 3.0, 6.0]), (1, [100.0, 201.0, 303.0])] {
            let batches: Vec<RecordBatch> =
                pbwag.execute(partition, ctx.clone())?.try_collect().await?;
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3, "partition {partition}: row count preserved");
            let sums: Vec<f64> = batches
                .iter()
                .flat_map(|b| {
                    b.column_by_name("sum(v)")
                        .expect("sum(v) column")
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("Float64Array")
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(
                sums,
                expected.to_vec(),
                "partition {partition}: frame-of-one → sum equals input"
            );
        }
        Ok(())
    }
}
