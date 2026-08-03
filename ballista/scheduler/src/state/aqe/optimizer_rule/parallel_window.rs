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
//! bottleneck. See [[parallel-range-window]] for the design.
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
//!   bounds (UNBOUNDED goes down the prefix-scan path — see
//!   [[project-prefix-scan-two-pass-rejected]])
//! - ORDER BY column is `Float64` today (T-Digest restriction; lifts with
//!   [[kll-sketch]])
//!
//! # Rewrite
//!
//! ```text
//! RangeFilterExec (narrow, halo_lo=0, halo_hi=0, cuts=pending)
//!   PartitionedBoundedWindowAggExec (wraps BWAG; declares UnspecifiedDistribution)
//!     RangeFilterExec (wide, halo_lo, halo_hi, cuts=pending)
//!       RuntimeStatsExec #2 (per-ORRE-output-partition sketch → scheduler)
//!         OrderedRangeRepartitionExec (K outputs, walks child for RSE #1)
//!           RuntimeStatsExec #1 (local sketch — feeds ORRE's cut walker)
//!             SortExec (unchanged, preserve_partitioning=true)
//!               <source>
//! ```
//!
//! Any SPM the DF planner had inserted above BWAG for its
//! `SinglePartition` requirement is dropped: the wrapper flips that
//! declaration to `UnspecifiedDistribution`, and `EnforceDistribution`
//! (running later in the AQE pass) doesn't re-add one.
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
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
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
        plan.transform_up(|node| {
            let Some(candidate) = as_candidate(node.as_ref()) else {
                return Ok(Transformed::no(node));
            };
            match rewrite_bwag(&node, &candidate, output_partitions) {
                Ok(rewritten) => {
                    debug!(
                        "ParallelWindowRule: rewrote BWAG on `{}` (RANGE {} — {})",
                        candidate.order_key,
                        fmt_bound(&candidate.start_bound),
                        fmt_bound(&candidate.end_bound),
                    );
                    Ok(Transformed::yes(rewritten))
                }
                Err(e) => {
                    debug!(
                        "ParallelWindowRule: shape matched but rewrite skipped for `{}`: {e}",
                        candidate.order_key,
                    );
                    Ok(Transformed::no(node))
                }
            }
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

/// Shape captured from a matching `BoundedWindowAggExec`. Everything the
/// rewrite needs to build the new subtree.
#[derive(Debug, Clone)]
struct WindowCandidate {
    order_key: String,
    sort_expr: PhysicalSortExpr,
    start_bound: WindowFrameBound,
    end_bound: WindowFrameBound,
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

fn as_candidate(node: &dyn ExecutionPlan) -> Option<WindowCandidate> {
    let window = node.downcast_ref::<BoundedWindowAggExec>()?;
    // Shape gates as slice patterns: 0 or 2+ elements simply don't match.
    let [expr] = window.window_expr() else {
        return None;
    };
    let [] = expr.partition_by() else {
        return None;
    };
    let [order] = expr.order_by() else {
        return None;
    };
    let column = order.expr.downcast_ref::<Column>()?;
    let frame = expr.get_window_frame();
    let WindowFrameUnits::Range = frame.units else {
        return None;
    };
    let (Some(start), Some(end)) =
        (as_finite(&frame.start_bound), as_finite(&frame.end_bound))
    else {
        return None;
    };
    // Idempotency: if the BWAG's subtree already contains our own
    // range-repartition machinery, we've already rewritten this window.
    // Re-plans (AQE fires the optimizer chain again for stage N+1) would
    // otherwise wrap another ORRE around the previous rewrite's
    // RangeFilterExec+ShuffleReader — and that ORRE's child doesn't claim
    // ordering, blowing up at execute-time.
    if subtree_contains_our_rewrite(window.children().as_slice()) {
        return None;
    }
    Some(WindowCandidate {
        order_key: column.name().to_string(),
        sort_expr: order.clone(),
        start_bound: start.clone(),
        end_bound: end.clone(),
    })
}

/// Returns the bound unchanged when it's `CurrentRow` or a non-null scalar
/// offset. `UNBOUNDED PRECEDING/FOLLOWING` is represented as a typed-null
/// scalar and returns `None`.
fn as_finite(bound: &WindowFrameBound) -> Option<&WindowFrameBound> {
    match bound {
        WindowFrameBound::CurrentRow => Some(bound),
        WindowFrameBound::Preceding(scalar) | WindowFrameBound::Following(scalar)
            if !scalar.is_null() =>
        {
            Some(bound)
        }
        _ => None,
    }
}

fn fmt_bound(bound: &WindowFrameBound) -> String {
    match bound {
        WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
        WindowFrameBound::Preceding(scalar) => format!("{scalar} PRECEDING"),
        WindowFrameBound::Following(scalar) => format!("{scalar} FOLLOWING"),
    }
}

/// Extract the halo width in `f64` from a bound. `CurrentRow` → 0.
/// Errors on non-numeric scalar (e.g. Interval bounds — future work).
fn halo_from_bound(bound: &WindowFrameBound) -> datafusion::common::Result<f64> {
    let scalar = match bound {
        WindowFrameBound::CurrentRow => return Ok(0.0),
        WindowFrameBound::Preceding(s) | WindowFrameBound::Following(s) => s,
    };
    // Widen anything Int-ish or Float-ish to f64. Interval bounds (for
    // time-typed ORDER BYs) are the widening TODO alongside KLL.
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(*v as f64),
        ScalarValue::Int16(Some(v)) => Ok(*v as f64),
        ScalarValue::Int32(Some(v)) => Ok(*v as f64),
        ScalarValue::Int64(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt8(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt16(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt32(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt64(Some(v)) => Ok(*v as f64),
        ScalarValue::Float32(Some(v)) => Ok(*v as f64),
        ScalarValue::Float64(Some(v)) => Ok(*v),
        other => datafusion::common::internal_err!(
            "ParallelWindowRule: unsupported halo bound type {other:?}"
        ),
    }
}

/// Splice `RSE#1 → ORRE → RSE#2 → RangeFilterExec_wide` below BWAG's
/// sorted source and wrap the BWAG in a `PartitionedBoundedWindowAggExec`.
/// Accepts either shape:
///
/// ```text
///   BWAG → SPM → <sorted source>          (multi-partition input)
///   BWAG → <sorted source>                 (single-partition input)
/// ```
///
/// Any pre-existing SPM below the BWAG is dropped — the wrapper's
/// `UnspecifiedDistribution` declaration keeps `EnforceDistribution` from
/// putting one back.
fn rewrite_bwag(
    bwag: &Arc<dyn ExecutionPlan>,
    candidate: &WindowCandidate,
    output_partitions: usize,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let bwag_children = bwag.children();
    let [immediate] = bwag_children.as_slice() else {
        return datafusion::common::internal_err!(
            "ParallelWindowRule: BWAG must have exactly 1 child"
        );
    };
    // Peel off any existing SPM to reveal the sorted source underneath;
    // otherwise the immediate child *is* the sorted source.
    let source: Arc<dyn ExecutionPlan> = if immediate
        .downcast_ref::<SortPreservingMergeExec>()
        .is_some()
    {
        let spm_children = immediate.children();
        let [inner] = spm_children.as_slice() else {
            return datafusion::common::internal_err!(
                "ParallelWindowRule: SPM must have exactly 1 child"
            );
        };
        (*inner).clone()
    } else {
        (*immediate).clone()
    };
    let source_schema = source.schema();

    // Route on the ORDER BY column. ORRE requires Float64 today.
    let routing_type = candidate.sort_expr.expr.data_type(&source_schema)?;
    if !matches!(routing_type, DataType::Float64) {
        return datafusion::common::internal_err!(
            "ParallelWindowRule: routing expression `{}` must be Float64, got {routing_type:?}",
            candidate.sort_expr.expr
        );
    }

    let sort_expr = normalize_sort_expr(&candidate.sort_expr);
    let rse1: Arc<dyn ExecutionPlan> = Arc::new(RuntimeStatsExec::try_new(
        source,
        Some(vec![sort_expr.clone()]),
    )?);
    let orre: Arc<dyn ExecutionPlan> = Arc::new(OrderedRangeRepartitionExec::try_new(
        rse1,
        vec![sort_expr.clone()],
        output_partitions,
    )?);
    let rse2: Arc<dyn ExecutionPlan> = Arc::new(RuntimeStatsExec::try_new(
        orre,
        Some(vec![sort_expr.clone()]),
    )?);
    let halo_lo = halo_from_bound(&candidate.start_bound)?;
    let halo_hi = halo_from_bound(&candidate.end_bound)?;
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
    let bwag_ref = bwag.downcast_ref::<BoundedWindowAggExec>().ok_or_else(|| {
        datafusion::common::DataFusionError::Internal(
            "ParallelWindowRule: rewrite_bwag caller passed non-BWAG".into(),
        )
    })?;
    let partitioned_bwag: Arc<dyn ExecutionPlan> =
        Arc::new(PartitionedBoundedWindowAggExec::try_new(
            bwag_ref.window_expr().to_vec(),
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
    Ok(narrow_filter)
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
        assert_eq!(halo_from_bound(&WindowFrameBound::CurrentRow).unwrap(), 0.0);
        assert_eq!(
            halo_from_bound(&WindowFrameBound::Preceding(ScalarValue::Int64(Some(3))))
                .unwrap(),
            3.0
        );
        assert_eq!(
            halo_from_bound(&WindowFrameBound::Following(ScalarValue::Float64(Some(
                2.5
            ))))
            .unwrap(),
            2.5
        );
        assert!(
            halo_from_bound(&WindowFrameBound::Preceding(ScalarValue::Utf8(Some(
                "x".into()
            ))))
            .is_err()
        );
    }
}
