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
//!   of partitions `[0..k)` into a single already-merged state per PARTITION
//!   BY key and per window expression. This is the step that requires global
//!   visibility across tasks; only the scheduler has it.
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
//! # Prefix-state input
//!
//! [`FinalizedPartitionState`] — one map per input partition, holding
//! *pre-merged* [`Accumulator::state`] for every (PARTITION BY key, window
//! expression) pair. Only consumed by [`WindowApply::Aggregate`] entries;
//! [`WindowApply::Scalar`] carries its own offsets inline. The scheduler
//! bakes both when it constructs the downstream stage after the upstream
//! stage's tasks complete.
//!
//! **This is scaffolding.** `execute` currently forwards batches unchanged.
//! The row-wise fold lands in a follow-up. The shape is committed to now so
//! the surrounding plumbing (scheduler collection, per-task state injection,
//! plan-rule placement) can be built against a stable signature.
//!
//! # Relation to DataFusion
//!
//! The upstream tasks' finalized state — which the scheduler prefix-merges
//! before handing the result to this operator — is produced by
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
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::window::PartitionKey;
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use std::collections::HashMap;

/// A single already-prefix-merged window-aggregate state, keyed by PARTITION
/// BY tuple. The inner `Vec` is indexed by window expression (same order the
/// upstream `BoundedWindowAggExec` reports in `window_expr()`); `None` at a
/// slot indicates a non-aggregate window function (`row_number`, `rank`,
/// `lead`/`lag`, ...) which contributes no state.
///
/// The scheduler produces one of these per input partition, having already
/// combined the individual states from every prior partition into one merged
/// value per (key, window expr) — see the [module-level docs][self] for the
/// division of labor. This operator applies it; it does not compute it.
///
/// The type mirrors `datafusion::physical_plan::windows::FinalizedPartitionState`
/// from [apache/datafusion#24007]; the alias here is a local stand-in so
/// this crate compiles against stable DataFusion 54 until that PR lands.
///
/// [apache/datafusion#24007]: https://github.com/apache/datafusion/pull/24007
pub type FinalizedPartitionState = HashMap<PartitionKey, Vec<Option<Vec<ScalarValue>>>>;

/// How to combine each row's existing value in an output column with a
/// scheduler-provided scalar offset. The result overwrites the column.
///
/// [`Overwrite`] ignores the row's existing value and just writes the offset;
/// it's the shape needed for `first_value` / `last_value`, where the scheduler
/// picks the correct global value once and every row gets a copy.
///
/// [`Overwrite`]: ScalarOp::Overwrite
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarOp {
    /// `output := row_value + offset`. Fits SUM, COUNT, and ranking functions
    /// like `row_number` (which are effectively `COUNT(*)`).
    Add,
    /// `output := min(row_value, offset)`. Fits MIN.
    Min,
    /// `output := max(row_value, offset)`. Fits MAX.
    Max,
    /// `output := offset`. Ignores `row_value`. Fits `first_value` /
    /// `last_value`, where the scheduler picks a single global scalar and
    /// every row gets the same corrected value.
    Overwrite,
}

/// How to correct one window-function output column at row-apply time. Each
/// entry describes exactly one column PrefixMergeExec should rewrite.
///
/// Two shapes are covered by construction; anything else is out of scope
/// (`lead`/`lag`/`nth_value` are solved by halo rows in the shuffle layer, and
/// ranking-family functions like `rank`/`percent_rank`/`ntile` want a separate
/// segment-tree-plus-broadcast infrastructure).
#[derive(Debug, Clone)]
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
/// each row of the current partition's output. See the [module-level
/// docs][self] for the division of labor between scheduler and executor and
/// the AQE pipeline this fits into.
///
/// **Scaffolding.** `execute` forwards batches unchanged; the row-wise fold
/// lands in a follow-up.
pub struct PrefixMergeExec {
    input: Arc<dyn ExecutionPlan>,
    /// One entry per window-function output column that needs cross-partition
    /// correction. Non-corrected columns (e.g. `lead`/`lag` handled by halos,
    /// or ranking functions left to segment-tree infrastructure) don't appear
    /// here.
    applies: Vec<WindowApply>,
    /// `per_partition_state[k]` is the scheduler-provided *already-merged*
    /// state summarising every input partition in `[0..k)`. Only consumed by
    /// [`WindowApply::Aggregate`] entries — [`WindowApply::Scalar`] carries
    /// its own offsets. Length equals
    /// `input.output_partitioning().partition_count()`.
    per_partition_state: Vec<FinalizedPartitionState>,
    properties: Arc<PlanProperties>,
}

impl PrefixMergeExec {
    /// Wrap `input` with per-column apply descriptors and per-input-partition
    /// prefix state.
    ///
    /// Errors on any of:
    /// - `per_partition_state.len()` != input's partition count.
    /// - Any [`WindowApply::Scalar`]'s `offset.len()` != input's partition
    ///   count.
    /// - Any entry's `output_column` outside the input schema's field range.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        applies: Vec<WindowApply>,
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
            per_partition_state,
            properties,
        })
    }

    /// Per-column apply descriptors. `applies()[i]` corresponds to one
    /// output column that will be rewritten by the prefix-merge.
    pub fn applies(&self) -> &[WindowApply] {
        &self.applies
    }

    /// The prefix state carried per input partition. Only consumed by
    /// [`WindowApply::Aggregate`] entries.
    pub fn per_partition_state(&self) -> &[FinalizedPartitionState] {
        &self.per_partition_state
    }
}

impl Debug for PrefixMergeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefixMergeExec")
            .field("partition_count", &self.per_partition_state.len())
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
                    self.per_partition_state.len(),
                    self.applies.len()
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
            self.applies.clone(),
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

    /// **Scaffolding.** Forwards the input stream unchanged. The row-wise
    /// fold — applying `self.per_partition_state[partition]` (already merged
    /// upstream by the scheduler) to each row's window-aggregate columns —
    /// lands in a follow-up.
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

    fn empty_state(partitions: usize) -> Vec<FinalizedPartitionState> {
        (0..partitions)
            .map(|_| FinalizedPartitionState::new())
            .collect()
    }

    /// Mismatched state length surfaces as an error rather than a panic at
    /// runtime.
    #[test]
    fn try_new_rejects_state_length_mismatch() {
        let input = partitioned_source(2, 3);
        let err =
            PrefixMergeExec::try_new(input, vec![], vec![FinalizedPartitionState::new()])
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
        let err = PrefixMergeExec::try_new(input, vec![apply], empty_state(2))
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
        let err = PrefixMergeExec::try_new(input, vec![apply], empty_state(2))
            .expect_err("out-of-range output_column must surface as an error");
        assert!(
            err.to_string().contains("output_column 5 out of range"),
            "unexpected error: {err}"
        );
    }

    /// While the merge is unimplemented, `execute` behaves as a passthrough
    /// even with `applies` populated: each partition's rows are forwarded
    /// verbatim regardless of what the descriptors say.
    #[tokio::test]
    async fn scaffold_forwards_input_rows_unchanged() -> Result<()> {
        let input = partitioned_source(2, 3);
        let apply = WindowApply::Scalar {
            op: ScalarOp::Add,
            offset: vec![ScalarValue::Int64(Some(100)); 2],
            output_column: 0,
        };
        let exec = Arc::new(PrefixMergeExec::try_new(
            input,
            vec![apply],
            empty_state(2),
        )?);

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
            assert_eq!(
                arr.values(),
                &expected,
                "scaffold must not apply the offset yet"
            );
        }
        Ok(())
    }
}
