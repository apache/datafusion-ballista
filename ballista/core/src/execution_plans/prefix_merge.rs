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
//! This operator closes that gap by merging the accumulated state from all
//! earlier partitions into partition `k`'s output. Each row's contribution
//! from prior partitions is folded in via [`Accumulator::merge_batch`]-shaped
//! composition — the same operation the group-by `Partial → Final` protocol
//! already uses to combine per-group state. Aggregate-agnostic by
//! construction: SUM adds, MIN/MAX take the extreme, sketches (KLL, TDigest,
//! HLL) merge as sketches.
//!
//! Input shape (per input partition `k`, provided in [`Self::try_new`]):
//! [`FinalizedPartitionState`] — a snapshot of every window aggregate's
//! finalized [`Accumulator::state`] for every PARTITION BY key seen while
//! draining partitions `[0..k)`. The scheduler bakes this in at plan time
//! after collecting per-task state from the upstream stage via task-status
//! transport.
//!
//! **This is scaffolding.** `execute` currently forwards batches unchanged.
//! The merge itself lands in a follow-up. The shape is committed to now so
//! the surrounding plumbing (scheduler collection, per-task state injection,
//! plan-rule placement) can be built against a stable signature.
//!
//! # Relation to DataFusion
//!
//! The per-input-partition input this operator expects is produced by
//! `BoundedWindowAggExec::finalized_partition_state`, added in
//! [apache/datafusion#24007]. Until that lands, callers can pass empty maps
//! (the operator forwards batches regardless) to exercise the plumbing.
//!
//! [`Accumulator::state`]: datafusion::logical_expr::Accumulator::state
//! [`Accumulator::merge_batch`]: datafusion::logical_expr::Accumulator::merge_batch
//! [apache/datafusion#24007]: https://github.com/apache/datafusion/pull/24007

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, ScalarValue, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::window::PartitionKey;
use datafusion::physical_expr::{Distribution, OrderingRequirements};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use std::collections::HashMap;

/// One input partition's finalized window-aggregate state, keyed by PARTITION
/// BY tuple. The inner `Vec` is indexed by window expression (same order the
/// upstream `BoundedWindowAggExec` reports in `window_expr()`); `None` at a
/// slot indicates a non-aggregate window function (`row_number`, `rank`,
/// `lead`/`lag`, ...) which contributes no state.
///
/// Must mirror `datafusion::physical_plan::windows::FinalizedPartitionState`
/// once [apache/datafusion#24007] lands; the type alias here is a local
/// stand-in so this crate can compile against stable DataFusion 54.
///
/// [apache/datafusion#24007]: https://github.com/apache/datafusion/pull/24007
pub type FinalizedPartitionState = HashMap<PartitionKey, Vec<Option<Vec<ScalarValue>>>>;

/// Merge finalized window-aggregate state from earlier partitions into each
/// partition's output. See the [module-level docs][self] for the shape of
/// the input and the AQE pipeline this fits into.
///
/// **Scaffolding.** `execute` forwards batches unchanged; the merge itself
/// lands in a follow-up.
pub struct PrefixMergeExec {
    input: Arc<dyn ExecutionPlan>,
    /// `per_partition_state[k]` is the accumulated finalized state from every
    /// input partition in `[0..k)`, ready to be merged into partition `k`'s
    /// output. Length equals `input.output_partitioning().partition_count()`.
    per_partition_state: Vec<FinalizedPartitionState>,
    properties: Arc<PlanProperties>,
}

impl PrefixMergeExec {
    /// Wrap `input` with per-input-partition prefix state.
    ///
    /// Errors if `per_partition_state.len()` doesn't match the input's
    /// partition count.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        per_partition_state: Vec<FinalizedPartitionState>,
    ) -> Result<Self> {
        let partition_count = input.output_partitioning().partition_count();
        if per_partition_state.len() != partition_count {
            return internal_err!(
                "PrefixMergeExec: per_partition_state.len() {} does not match \
                 input partition count {}",
                per_partition_state.len(),
                partition_count
            );
        }
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            per_partition_state,
            properties,
        })
    }

    /// The prefix state carried per input partition.
    /// `per_partition_state()[k]` is what will be merged into partition `k`.
    pub fn per_partition_state(&self) -> &[FinalizedPartitionState] {
        &self.per_partition_state
    }
}

impl Debug for PrefixMergeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefixMergeExec")
            .field("partition_count", &self.per_partition_state.len())
            .finish()
    }
}

impl DisplayAs for PrefixMergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PrefixMergeExec: partitions={}",
                    self.per_partition_state.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "PrefixMergeExec")
            }
        }
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
        Ok(Arc::new(PrefixMergeExec::try_new(
            input.clone(),
            self.per_partition_state.clone(),
        )?))
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    /// Every input row is emitted exactly once.
    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    /// **Scaffolding.** Forwards the input stream unchanged. The merge itself
    /// — folding `self.per_partition_state[partition]` into each row's
    /// window-aggregate columns — lands in a follow-up.
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.per_partition_state.len() {
            return internal_err!(
                "PrefixMergeExec: partition {} out of bounds ({} slots)",
                partition,
                self.per_partition_state.len()
            );
        }
        self.input.execute(partition, ctx)
    }
}

#[cfg(test)]
mod tests {
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

    /// Mismatched state length surfaces as an error rather than a panic at
    /// runtime.
    #[test]
    fn try_new_rejects_state_length_mismatch() {
        let input = partitioned_source(2, 3);
        let err = PrefixMergeExec::try_new(input, vec![FinalizedPartitionState::new()])
            .expect_err("length mismatch must surface as an error");
        assert!(
            err.to_string()
                .contains("does not match input partition count"),
            "unexpected error: {err}"
        );
    }

    /// While the merge is unimplemented, `execute` behaves as a passthrough:
    /// each partition's rows are forwarded verbatim.
    #[tokio::test]
    async fn scaffold_forwards_input_rows_unchanged() -> Result<()> {
        let input = partitioned_source(2, 3);
        let state: Vec<FinalizedPartitionState> = vec![
            FinalizedPartitionState::new(),
            FinalizedPartitionState::new(),
        ];
        let exec = Arc::new(PrefixMergeExec::try_new(input, state)?);

        let ctx = SessionContext::new().task_ctx();
        for k in 0..2 {
            let batches: Vec<RecordBatch> =
                exec.execute(k, ctx.clone())?.try_collect().await?;
            assert_eq!(batches.len(), 1);
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column");
            let expected: Vec<i64> = ((k * 3) as i64..((k + 1) * 3) as i64).collect();
            assert_eq!(arr.values(), &expected);
        }
        Ok(())
    }
}
