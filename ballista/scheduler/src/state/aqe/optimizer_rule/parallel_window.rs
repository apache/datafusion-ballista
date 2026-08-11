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

//! Rewrite bounded-RANGE-frame windows into a distributed range-shuffle so
//! `BoundedWindowAggExec`'s single-partition constraint isn't a serial
//! bottleneck.
//!
//! # Matched shape
//!
//! ```text
//! BoundedWindowAggExec [Sorted, RANGE frame, finite bounds]
//!   SortPreservingMergeExec [ORDER BY]
//!     SortExec (preserve_partitioning=true) [ORDER BY]
//!       <source with P partitions>
//! ```
//!
//! Restricted to:
//! - single window expression
//! - no PARTITION BY
//! - single-column ORDER BY on a physical `Column` (widening: multi-key,
//!   computed exprs — separate rewrites)
//! - `RANGE` frame with finite `PRECEDING` / `FOLLOWING` / `CurrentRow`
//!   bounds (UNBOUNDED frames go down a different path)
//! - ORDER BY column is `Float64` today (T-Digest restriction; lifts when
//!   the sketch swaps to KLL)
//!
//! # Rewrite
//!
//! ```text
//! RangeFilterExec (narrow, halo_lo=0, halo_hi=0, cuts=pending)
//!   PartitionedBoundedWindowAggExec (wraps BWAG; declares UnspecifiedDistribution)
//!     RangeFilterExec (wide, halo_lo, halo_hi, cuts=pending)
//!       RuntimeStatsExec #2 (per-ORRE-output-partition sketch → scheduler)
//!         OrderedRangeRepartitionExec (K outputs, walks child for RSE #1)
//!           SortExec (planted here, preserve_partitioning=true)
//!             RuntimeStatsExec #1 (local sketch — feeds ORRE's cut walker)
//!               <source>
//! ```
//!
//! RSE#1 sits *below* SortExec so the local sketch ingests the whole
//! partition while Sort buffers, giving the scheduler full-fidelity cuts
//! to hand ORRE before it starts routing. RSE#1 above Sort would force
//! ORRE to route against a still-being-built sketch → skewed shuffle files.
//!
//! The rule runs *after* DF's optimizer chain so `EnforceSorting` /
//! `RepartitionFileScans` have already materialized the SortExec placement
//! we peel here. Running earlier hits two failure modes: (a) sources with
//! `sort_order_for_reorder` set have no SortExec yet at all, and (b) DF's
//! sort-pushdown later moves any Sort we plant down through the
//! passthrough RSE#1, undoing the intended order.
//!
//! Any SPM the DF planner inserted above BWAG for its `SinglePartition`
//! requirement is dropped: the wrapper flips that declaration to
//! `UnspecifiedDistribution`, and `EnforceDistribution` doesn't re-add one.
//!
//! Both `RangeFilterExec` operators are planted with `cuts=None`. The
//! scheduler-side `resolve_range_filter_cuts` walker fills them in once
//! stage-0's `RuntimeStatsExec` reports have been merged into cuts.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{
    OrderedRangeRepartitionExec, PartitionedBoundedWindowAggExec, RangeFilterExec,
    RuntimeStatsExec,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::scalar::ScalarValue;
use log::debug;

/// Physical optimizer pass: match the parallel-window shape and rewrite each
/// hit to insert `RSE#1 → ORRE → RSE#2 → RangeFilterExec_wide` below the
/// existing SPM.
#[derive(Default, Debug)]
pub struct ParallelWindowRule;

impl PhysicalOptimizerRule for ParallelWindowRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        if !bc.parallel_window_enabled() {
            return Ok(plan);
        }
        // K = the number of range-disjoint output partitions the ORRE will
        // produce. At rule-fire time DataFusion's initial physical plan is
        // still "loose" — the DataSourceExec below BWAG has 1 file_group,
        // not the eventual `target_partitions` split (RepartitionFileScans
        // and friends run later in the AQE chain). So we can't ask the
        // plan tree for the true source width; we use the config knob that
        // those later rules also target.
        let output_partitions = config.execution.target_partitions.max(2);
        plan.transform_up(|node| match maybe_rewrite_bwag(&node, output_partitions)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(node)),
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ParallelWindow"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// True if any descendant of `nodes` is an `OrderedRangeRepartitionExec`
/// or `RangeFilterExec`. Used as an idempotency guard: those ops are what
/// our own rewrite plants, so seeing them below a BWAG means we've already
/// rewritten this candidate on a previous optimizer pass.
fn subtree_contains_our_rewrite(nodes: &[&Arc<dyn ExecutionPlan>]) -> bool {
    for node in nodes {
        if node.is::<OrderedRangeRepartitionExec>() || node.is::<RangeFilterExec>() {
            return true;
        }
        if subtree_contains_our_rewrite(node.children().as_slice()) {
            return true;
        }
    }
    false
}

/// True when the bound is `CurrentRow` or a non-null scalar offset.
/// `UNBOUNDED PRECEDING/FOLLOWING` is a typed-null scalar and returns `false`.
fn is_finite(bound: &WindowFrameBound) -> bool {
    match bound {
        WindowFrameBound::CurrentRow => true,
        WindowFrameBound::Preceding(scalar) | WindowFrameBound::Following(scalar) => {
            !scalar.is_null()
        }
    }
}

fn fmt_bound(bound: &WindowFrameBound) -> String {
    match bound {
        WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
        WindowFrameBound::Preceding(scalar) => format!("{scalar} PRECEDING"),
        WindowFrameBound::Following(scalar) => format!("{scalar} FOLLOWING"),
    }
}

/// Extract the halo width in `f64` from a bound. `CurrentRow` → `Some(0.0)`.
/// Non-numeric scalars (e.g. Interval bounds) return `None` — a shape gate,
/// widened alongside KLL.
fn halo_from_bound(bound: &WindowFrameBound) -> Option<f64> {
    let scalar = match bound {
        WindowFrameBound::CurrentRow => return Some(0.0),
        WindowFrameBound::Preceding(s) | WindowFrameBound::Following(s) => s,
    };
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt16(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Float64(Some(v)) => Some(*v),
        _ => None,
    }
}

/// Match the parallel-window shape rooted at `node` and, if it fits, splice
/// `RFE_narrow → PBWAG(BWAG) → RFE_wide → RSE#2 → ORRE → SortExec → RSE#1 →
/// <source>` in place of the DF-planted `BWAG → SPM → SortExec → <source>`
/// subtree.
///
/// - `Ok(None)`: shape gate missed (not a BWAG, PARTITION BY present, ROWS
///   frame, UNBOUNDED bound, non-Float64 ORDER BY, non-numeric bound scalar,
///   or subtree already rewritten). No log noise on the hot path.
/// - `Ok(Some(_))`: rewrite happened.
/// - `Err(_)`: an invariant the shape gates should have upheld didn't — BWAG
///   with ≠1 child, SPM/SortExec with ≠1 child, schema-lookup failure on the
///   ORDER BY expression, or a constructor `try_new` error.
///
/// The rule runs after DF's optimizer chain, so BWAG's descendants have the
/// fully-materialized `SPM → SortExec → source` shape by the time we peel
/// here (see module doc for why the SPM and SortExec get stripped).
fn maybe_rewrite_bwag(
    node: &Arc<dyn ExecutionPlan>,
    output_partitions: usize,
) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() else {
        return Ok(None);
    };
    // Shape gates as slice patterns: 0 or 2+ elements simply don't match.
    let [expr] = window.window_expr() else {
        return Ok(None);
    };
    let [] = expr.partition_by() else {
        return Ok(None);
    };
    let [order] = expr.order_by() else {
        return Ok(None);
    };
    let Some(column) = order.expr.downcast_ref::<Column>() else {
        return Ok(None);
    };
    // TODO: DESC support. SQL RANGE frame semantics invert with the sort
    // direction — `k PRECEDING` refers to *larger* values under a DESC
    // ORDER BY — so the halo must widen the upper side of each bucket
    // rather than the lower. `RangeFilterExec::sorted_on_key` also refuses
    // DESC input, so the fast path would need to grow a mirrored branch.
    // Land both together; until then, gate DESC to the serial path.
    if order.options.descending {
        return Ok(None);
    }
    let frame = expr.get_window_frame();
    let WindowFrameUnits::Range = frame.units else {
        return Ok(None);
    };
    if !is_finite(&frame.start_bound) || !is_finite(&frame.end_bound) {
        return Ok(None);
    }
    // Idempotency: re-plans (AQE fires the optimizer chain again for stage
    // N+1) would otherwise wrap another ORRE around the previous rewrite's
    // RangeFilterExec+ShuffleReader — and that ORRE's child doesn't claim
    // ordering, blowing up at execute-time.
    if subtree_contains_our_rewrite(window.children().as_slice()) {
        return Ok(None);
    }
    let (Some(halo_lo), Some(halo_hi)) = (
        halo_from_bound(&frame.start_bound),
        halo_from_bound(&frame.end_bound),
    ) else {
        return Ok(None);
    };

    let node_children = node.children();
    let [immediate] = node_children.as_slice() else {
        return datafusion::common::internal_err!(
            "ParallelWindowRule: BWAG must have exactly 1 child"
        );
    };
    // Loop tolerates any order (SPM→Sort or Sort→SPM) or partial shapes
    // (source that claims ordering natively via `sort_order_for_reorder`
    // skips the Sort entirely).
    let mut base_source: Arc<dyn ExecutionPlan> = (*immediate).clone();
    while base_source.is::<SortPreservingMergeExec>() || base_source.is::<SortExec>() {
        let children = base_source.children();
        let [inner] = children.as_slice() else {
            return datafusion::common::internal_err!(
                "ParallelWindowRule: SPM/SortExec must have exactly 1 child"
            );
        };
        base_source = (*inner).clone();
    }
    let source_schema = base_source.schema();

    // Route on the ORDER BY column. ORRE requires Float64 today (T-Digest
    // restriction; lifts when the sketch swaps to KLL).
    let routing_type = order.expr.data_type(&source_schema)?;
    if !matches!(routing_type, DataType::Float64) {
        return Ok(None);
    }

    let sort_expr = normalize_sort_expr(order);
    let rse1: Arc<dyn ExecutionPlan> = Arc::new(RuntimeStatsExec::try_new(
        base_source,
        Some(vec![sort_expr.clone()]),
    )?);
    // Plant a fresh SortExec above RSE#1 as the pipeline break: Sort
    // consumes all input before emitting the first row, so RSE#1's sketch
    // fully ingests and reports while Sort buffers — ORRE then routes
    // against final cuts instead of approximate ones (which would produce
    // skewed shuffle files).
    let sort_lex = LexOrdering::new(vec![sort_expr.clone()]).ok_or_else(|| {
        datafusion::common::DataFusionError::Internal(
            "ParallelWindowRule: could not build LexOrdering from ORDER BY".into(),
        )
    })?;
    let sorted_over_rse1: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(sort_lex, rse1).with_preserve_partitioning(true));
    let orre: Arc<dyn ExecutionPlan> = Arc::new(OrderedRangeRepartitionExec::try_new(
        sorted_over_rse1,
        vec![sort_expr.clone()],
        output_partitions,
    )?);
    let rse2: Arc<dyn ExecutionPlan> = Arc::new(RuntimeStatsExec::try_new(
        orre,
        Some(vec![sort_expr.clone()]),
    )?);
    let wide_filter: Arc<dyn ExecutionPlan> = Arc::new(RangeFilterExec::try_new_pending(
        rse2,
        sort_expr.expr.clone(),
        ScalarValue::Float64(Some(halo_lo)),
        ScalarValue::Float64(Some(halo_hi)),
    )?);

    // Wrap BWAG in PartitionedBoundedWindowAggExec instead of collapsing
    // K→1 with SPM. The wrapper declares `UnspecifiedDistribution` so
    // EnforceDistribution won't reinsert an SPM below, and BWAG's own
    // per-partition execute() runs each of the K sub-ranges independently.
    // See execution_plans::partitioned_bounded_window_agg for what makes
    // this safe (range-repartition upstream + halo).
    let partitioned_bwag: Arc<dyn ExecutionPlan> =
        Arc::new(PartitionedBoundedWindowAggExec::try_new(
            window.window_expr().to_vec(),
            wide_filter,
        )?);
    // Narrow filter above BWAG drops the halo rows the wide filter let in
    // for BWAG's frame-context. `halo_lo == halo_hi == 0.0` collapses the
    // predicate to `cuts[k-1] <= v < cuts[k]` — task k's own range.
    let narrow_filter: Arc<dyn ExecutionPlan> =
        Arc::new(RangeFilterExec::try_new_pending(
            partitioned_bwag,
            sort_expr.expr.clone(),
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some(0.0)),
        )?);

    debug!(
        "ParallelWindowRule: rewrote BWAG on `{}` (RANGE {} - {})",
        column.name(),
        fmt_bound(&frame.start_bound),
        fmt_bound(&frame.end_bound),
    );
    Ok(Some(narrow_filter))
}

/// ORRE requires `nulls_first == false` today (T-Digest has no NULL slot).
/// The BWAG's `NULLS LAST` sort expressions arrive with `nulls_first: false`
/// already, but explicit sanitization keeps the invariant obvious to future
/// readers.
fn normalize_sort_expr(expr: &PhysicalSortExpr) -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: expr.expr.clone(),
        options: SortOptions {
            descending: expr.options.descending,
            nulls_first: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::config::ExtensionOptions;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    async fn plan(sql: &str) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id1", DataType::Int64, false),
            Field::new("id2", DataType::Int64, false),
            Field::new("id3", DataType::Int64, false),
            Field::new("v2", DataType::Float64, false),
        ]));
        let ctx = SessionContext::new();
        ctx.register_table("large", Arc::new(EmptyTable::new(schema)))?;
        ctx.sql(sql).await?.create_physical_plan().await
    }

    fn optimize(
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 8;
        let mut bc = BallistaConfig::default();
        bc.set("planner.parallel_window.enabled", "true").unwrap();
        config.extensions.insert(bc);
        ParallelWindowRule.optimize(plan, &config)
    }

    #[tokio::test]
    async fn disabled_by_default() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        // No BallistaConfig extension registered → default is `false`.
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 8;
        let out = ParallelWindowRule.optimize(plan.clone(), &config)?;
        let rendered = format!("{}", displayable(out.as_ref()).indent(true));
        assert!(
            !rendered.contains("OrderedRangeRepartitionExec"),
            "flag off: rewrite must not fire:\n{rendered}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rewrites_q8_shape() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        // The rewrite must plant each of these ops. Cheap string contains —
        // exhaustive plan-shape assertions in follow-up integration tests.
        for expected in [
            "PartitionedBoundedWindowAggExec",
            "BoundedWindowAggExec",
            "RangeFilterExec",
            "RuntimeStatsExec",
            "OrderedRangeRepartitionExec",
            "SortExec",
        ] {
            assert!(
                rendered.contains(expected),
                "expected `{expected}` in rewritten plan:\n{rendered}"
            );
        }
        // BWAG's SinglePartition collapse is what this whole rewrite
        // avoids — any SPM in the output would defeat that.
        assert!(
            !rendered.contains("SortPreservingMergeExec"),
            "SortPreservingMergeExec must NOT appear in the rewritten plan:\n{rendered}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_rewrite_on_rows_frame() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT avg(v2) OVER (ORDER BY id3 \
                ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            !rendered.contains("OrderedRangeRepartitionExec"),
            "ROWS frames should not be rewritten:\n{rendered}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_rewrite_on_partition_by() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (PARTITION BY id1 ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            !rendered.contains("OrderedRangeRepartitionExec"),
            "PARTITION BY should not be rewritten:\n{rendered}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_rewrite_on_unbounded_frame() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            !rendered.contains("OrderedRangeRepartitionExec"),
            "UNBOUNDED PRECEDING should not be rewritten:\n{rendered}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_rewrite_on_descending_order_by() -> datafusion::common::Result<()> {
        // DESC + RANGE flips the SQL frame semantics: `k PRECEDING` refers to
        // larger values, not smaller. The current halo widens the lower side
        // of each bucket, so DESC would silently miss frame ancestors that
        // land in the next-higher bucket. Gate out until the halo-swap +
        // RFE fast-path DESC support land together.
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 DESC \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            !rendered.contains("OrderedRangeRepartitionExec"),
            "DESC ORDER BY should not be rewritten (halo direction bug):\n{rendered}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_rewrite_on_non_float64_order_key() -> datafusion::common::Result<()> {
        // id3 is Int64; ORRE requires Float64 today (T-Digest restriction).
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY id3 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            !rendered.contains("OrderedRangeRepartitionExec"),
            "non-Float64 order key should not be rewritten:\n{rendered}"
        );
        Ok(())
    }

    #[test]
    fn halo_from_bound_reads_all_numeric_variants() {
        assert_eq!(halo_from_bound(&WindowFrameBound::CurrentRow), Some(0.0));
        assert_eq!(
            halo_from_bound(&WindowFrameBound::Preceding(ScalarValue::Int64(Some(3)))),
            Some(3.0)
        );
        assert_eq!(
            halo_from_bound(&WindowFrameBound::Following(ScalarValue::Float64(Some(
                2.5
            )))),
            Some(2.5)
        );
        assert_eq!(
            halo_from_bound(&WindowFrameBound::Preceding(ScalarValue::Utf8(Some(
                "x".into()
            )))),
            None
        );
    }
}
