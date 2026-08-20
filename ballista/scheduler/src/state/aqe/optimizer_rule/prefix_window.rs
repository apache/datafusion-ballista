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

//! Rewrite ever-expanding-frame windows (`UNBOUNDED PRECEDING`) into a
//! parallel prefix scan, so `BoundedWindowAggExec`'s single-partition
//! constraint isn't a serial bottleneck.
//!
//! Sibling of the halo rewrite in [`super::parallel_window`].
//!
//! # Status
//!
//! Correct end to end for the shape it gates on: no PARTITION BY, a single
//! ascending `Float64` ORDER BY column, and an ever-expanding frame.
//! `prefix_window.rs` in `ballista/client/tests` holds it to the serial
//! answer through a real cluster.
//!
//! Not yet reached by h2o Q7, which orders by an `Int64` column while ORRE
//! routes on a Float64-only T-Digest — a sketch restriction that lifts with
//! the KLL migration, not anything in this rule. See `declines_int64_routing`.
//!
//! # Why halo can't cover this
//!
//! The halo rewrite widens each range partition by the frame's reach so
//! every frame's rows are local, then drops the halo rows afterwards. That
//! needs a *finite* reach. An `UNBOUNDED PRECEDING` frame reaches back to
//! the first row of the dataset, so no halo width suffices — every
//! partition would need every prior partition. The prefix scan instead lets
//! each task compute a partition-local running aggregate, then corrects it
//! with the merged state of all prior partitions.
//!
//! # Why it's worth doing
//!
//! h2o `window.sql` Q7 at scale 1e7, 8 partitions, 2 executors × 4 vcores:
//!
//! ```sql
//! SELECT id1, id2, id3, v2,
//!        sum(v2) OVER (ORDER BY id3 ROWS BETWEEN UNBOUNDED PRECEDING
//!                                            AND CURRENT ROW) AS my_rolling_sum
//! FROM large;
//! ```
//!
//! Stage 0 sorts 8 partitions in parallel and costs `elapsed_compute` 1.94s.
//! Stage 1 merges them to one partition and runs the whole window on a
//! single core: 9.75s, 5x stage 0 for the same 10M rows.
//!
//! # Actual rule input
//!
//! Captured by logging `optimize`'s argument on the Q7 run above. The rule
//! sits after DataFusion's optimizer chain and before
//! [`DistributedExchangeRule`](super::DistributedExchangeRule), so on the
//! first pass there is no exchange or shuffle reader in the tree yet — just
//! the DataFusion plan with `EnforceSorting`'s `SortExec` placement already
//! materialized:
//!
//! ```text
//! ProjectionExec: expr=[id1@0 as id1, id2@1 as id2, id3@2 as id3, v2@3 as v2,
//!                       sum(large.v2) ORDER BY [large.id3 ASC NULLS LAST]
//!                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@4
//!                       as my_rolling_sum]
//!   BoundedWindowAggExec: wdw=[sum(large.v2) ORDER BY [large.id3 ASC NULLS LAST]
//!                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:
//!                              Field { nullable Float64 }],
//!                         frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW,
//!                         mode=[Sorted]
//!     SortPreservingMergeExec: [id3@2 ASC NULLS LAST]
//!       SortExec: expr=[id3@2 ASC NULLS LAST], preserve_partitioning=[true]
//!         DataSourceExec: file_groups={8 groups}, projection=[id1, id2, id3, v2],
//!                         file_type=parquet,
//!                         sort_order_for_reorder=[id3@2 ASC NULLS LAST]
//! ```
//!
//! Note the frame is `ROWS`, not `RANGE`. The halo rule gates on
//! `WindowFrameUnits::Range`; this one must accept both, since for an
//! unbounded start the two differ only in tie handling at the frame edge.
//!
//! ## The rule fires more than once
//!
//! AQE re-plans as stages resolve, and `optimize` is called on each pass —
//! three times for this query. Passes 2 and 3 receive the plan wrapped in
//! `AdaptiveDatafusionExec` with the source subtree already behind a
//! resolved exchange:
//!
//! ```text
//! AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending
//!   ProjectionExec: ...
//!     BoundedWindowAggExec: ...
//!       SortPreservingMergeExec: [id3@2 ASC NULLS LAST]
//!         ExchangeExec: partitioning=None, plan_id=0, stage_id=0, stage_resolved=true
//!           SortExec: expr=[id3@2 ASC NULLS LAST], preserve_partitioning=[true]
//!             DataSourceExec: ...
//! ```
//!
//! So the rewrite needs the same idempotency guard the halo rule uses
//! (`subtree_contains_our_rewrite`): without it, pass 2 would wrap the
//! output of pass 1 again.
//!
//! # Target shape
//!
//! The input partitions are **not** range-disjoint. Stage 0 is a bare
//! `SortExec` over 8 file groups, so each partition is locally sorted but
//! spans the whole value range — which is exactly why the SPM is needed for
//! correctness today. A prefix scan needs "all prior partitions" to be well
//! defined, so the rewrite has to introduce the disjointness itself, with
//! the same `RSE#1 → SortExec → ORRE → RSE#2` preamble the halo rule builds:
//!
//! ```text
//! PrefixMergeExec [per-partition state baked in by the scheduler]
//!   ExchangeExec (partitioning: None)          <- boundary 2, planted here
//!     PartitionedBoundedWindowAggExec [wraps BWAG; UnspecifiedDistribution,
//!                                      WindowStateCollector installed]
//!       RangeFilterExec (halo_lo=0, halo_hi=0, cuts=pending)
//!         [ExchangeExec]                       <- boundary 1, inserted by DER
//!           RuntimeStatsExec #2
//!             OrderedRangeRepartitionExec [K range-disjoint outputs]
//!               SortExec (preserve_partitioning=true)
//!                 RuntimeStatsExec #1 [local sketch → cuts]
//!                   <source>
//! ```
//!
//! ## Where the two stage boundaries come from
//!
//! **Boundary 1 is free.** [`DistributedExchangeRule`](super::DistributedExchangeRule)
//! walks bottom-up and, for any single-child node, tests whether that child
//! is partition-preserving *and* sits directly on an ORRE/URRE. Our
//! `RangeFilterExec`'s child (`RuntimeStatsExec #2` over the ORRE) satisfies
//! both, so DER wraps it in an `ExchangeExec`. The result is then a
//! recognized `is_stage_boundary` shape — RFE over exchange — so nothing
//! inserts a second one underneath. This is the same path the halo rule
//! rides; we plant the ORRE and DER does the rest.
//!
//! **Boundary 2 is not.** That test is two levels deep, not a walk: above
//! PBWAG there is no ORRE left (boundary 1 consumed it), and PBWAG isn't in
//! the `preserves_partitioning` whitelist anyway. So the rule plants this
//! `ExchangeExec` itself. `partitioning: None` is the passthrough encoding —
//! `ExchangeExec::new_with_details` maps it to `input.output_partitioning()`
//! and clones the input's `eq_properties`, so partition count and each
//! partition's ordering both survive. Every exchange DER creates today is
//! already a `None` one, so this is the well-trodden shape rather than a new
//! kind of boundary.
//!
//! What *is* new is the boundary's purpose. Every other boundary in Ballista
//! exists because data has to move — a fan-in, a repartition. This one moves
//! each partition to itself, and exists only so the scheduler has a
//! synchronization point at which every stage-1 task has published its
//! accumulator state.
//!
//! ## Differences from the halo rewrite
//!
//! - One `RangeFilterExec` with zero halo, not a wide/narrow pair. The trim
//!   above the shuffle reader is needed regardless — the reader delivers a
//!   superset and RFE narrows to the partition's own cut range. Halo is the
//!   *widening* on top of that trim, and an unbounded start has no finite
//!   reach to widen by; the correction rides accumulator state instead of
//!   neighbouring rows.
//! - The SPM above BWAG is dropped rather than kept — that collapse is the
//!   bottleneck being removed.
//! - Three stages rather than two, for the state round trip.
//!
//! ## Why it can't be one stage
//!
//! `PrefixMergeExec::try_new` takes its per-partition state by value, and
//! that state only exists once every upstream task has closed its window and
//! published accumulator state. So the collector rides stage 1's tasks, the
//! scheduler prefix-merges the reports as they arrive, and `PrefixMergeExec`
//! is constructed for stage 2 with the merged result baked in.
//!
//! The cost of that round trip is a full materialization of the window
//! output — for Q7, 10M rows — written and read back across boundary 2. The
//! halo path never pays it. Worth measuring against the 9.75s serial
//! baseline before assuming the parallel window is a net win.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::state::aqe::execution_plan::ExchangeExec;
use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{
    InputOrder, OrderedRangeRepartitionExec, PartitionedBoundedWindowAggExec,
    PrefixMergeExec, RangeFilterExec, RuntimeStatsExec, WindowApply,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::window::PlainAggregateWindowExpr;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::scalar::ScalarValue;
use log::debug;

/// Physical optimizer pass: rewrite `UNBOUNDED PRECEDING` window frames into
/// a parallel prefix scan. See the [module docs][self] for the measured
/// starting point and the target shape.
///
/// Shares `ballista.planner.parallel_window.enabled` with the halo rule for
/// now. The two rewrites are mutually exclusive on frame shape, so one key
/// selects both without ambiguity; whether prefix earns its own key is still
/// open.
#[derive(Debug, Clone, Default)]
pub struct PrefixWindowRule {
    /// Shared with every other rule that plants an `ExchangeExec`, so
    /// boundary 2's `plan_id` can't collide with one DER hands out.
    plan_id_generator: Arc<AtomicUsize>,
}

impl PrefixWindowRule {
    pub(crate) fn new(plan_id_generator: Arc<AtomicUsize>) -> Self {
        Self { plan_id_generator }
    }
}

impl PhysicalOptimizerRule for PrefixWindowRule {
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
        // The module docs' "Actual rule input" section is a capture of this.
        // Re-run with `RUST_LOG=ballista_scheduler=debug` to refresh it after
        // anything upstream in the optimizer chain changes shape.
        debug!(
            "PrefixWindowRule input:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        // Same K as the halo rule: at rule-fire time the source is still one
        // file_group, so the config knob is the only honest source of the
        // eventual width.
        let output_partitions = config.execution.target_partitions.max(2);
        plan.transform_up(|node| {
            match maybe_rewrite_bwag(&node, output_partitions, &self.plan_id_generator)? {
                Some(rewritten) => Ok(Transformed::yes(rewritten)),
                None => Ok(Transformed::no(node)),
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "PrefixWindow"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// True if any descendant plants what this rule plants. Idempotency guard:
/// AQE re-plans and calls `optimize` on every pass (three times for Q7), so
/// without this the second pass would wrap the first pass's output.
fn subtree_contains_our_rewrite(nodes: &[&Arc<dyn ExecutionPlan>]) -> bool {
    for node in nodes {
        if node.is::<OrderedRangeRepartitionExec>()
            || node.is::<RangeFilterExec>()
            || node.is::<PrefixMergeExec>()
        {
            return true;
        }
        if subtree_contains_our_rewrite(node.children().as_slice()) {
            return true;
        }
    }
    false
}

/// ORRE requires `nulls_first == false` (T-Digest has no NULL slot). BWAG's
/// `NULLS LAST` expressions already arrive that way; sanitize anyway so the
/// invariant is visible.
fn normalize_sort_expr(expr: &PhysicalSortExpr) -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: expr.expr.clone(),
        options: SortOptions {
            descending: expr.options.descending,
            nulls_first: false,
        },
    }
}

/// Match the prefix-scan shape rooted at `node` and splice the target from
/// the [module docs][self] in place of the DF-planted
/// `BWAG → SPM → SortExec → <source>` subtree.
///
/// - `Ok(None)`: shape gate missed. Silent — this runs on every node of
///   every plan.
/// - `Ok(Some(_))`: rewrote.
/// - `Err(_)`: a gate-guaranteed invariant didn't hold, or a constructor
///   failed.
fn maybe_rewrite_bwag(
    node: &Arc<dyn ExecutionPlan>,
    output_partitions: usize,
    plan_id_generator: &Arc<AtomicUsize>,
) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() else {
        return Ok(None);
    };
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
    // DESC support lands with the halo rule's — both need the mirrored
    // bound handling and `RangeFilterExec::sorted_on_key` refuses DESC.
    if order.options.descending {
        return Ok(None);
    }
    let frame = expr.get_window_frame();
    // The gate that splits this rule from the halo one. Unlike that rule we
    // accept both ROWS and RANGE units: with an unbounded start the two
    // differ only in tie handling at the frame edge, which the prefix
    // correction doesn't observe.
    if !frame.start_bound.is_unbounded() {
        return Ok(None);
    }
    // An unbounded *end* means the window sees rows it hasn't reached, so
    // `uses_bounded_memory()` is false and DataFusion plans a `WindowAggExec`
    // instead — which carries no observer and never reaches us. Guard anyway.
    if frame.end_bound.is_unbounded() {
        return Ok(None);
    }
    if subtree_contains_our_rewrite(window.children().as_slice()) {
        return Ok(None);
    }

    let node_children = node.children();
    let [immediate] = node_children.as_slice() else {
        return datafusion::common::internal_err!(
            "PrefixWindowRule: BWAG must have exactly 1 child"
        );
    };
    // Peel whatever SPM/Sort combination EnforceSorting materialized. A
    // source claiming its order natively via `sort_order_for_reorder` may
    // have no SortExec at all.
    let mut base_source: Arc<dyn ExecutionPlan> = (*immediate).clone();
    while base_source.is::<SortPreservingMergeExec>() || base_source.is::<SortExec>() {
        let children = base_source.children();
        let [inner] = children.as_slice() else {
            return datafusion::common::internal_err!(
                "PrefixWindowRule: SPM/SortExec must have exactly 1 child"
            );
        };
        base_source = (*inner).clone();
    }
    let source_schema = base_source.schema();

    // ORRE routes on the ORDER BY column, Float64-only today (T-Digest).
    let routing_type = order.expr.data_type(&source_schema)?;
    if !matches!(routing_type, DataType::Float64) {
        return Ok(None);
    }

    let sort_expr = normalize_sort_expr(order);

    // RSE#1 below the pipeline-breaking Sort so its sketch fully ingests and
    // reports while Sort buffers — ORRE then routes against final cuts.
    let rse1: Arc<dyn ExecutionPlan> = Arc::new(RuntimeStatsExec::try_new(
        base_source,
        Some(vec![sort_expr.clone()]),
    )?);
    let sort_lex = LexOrdering::new(vec![sort_expr.clone()]).ok_or_else(|| {
        datafusion::common::DataFusionError::Internal(
            "PrefixWindowRule: could not build LexOrdering from ORDER BY".into(),
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
    // Zero halo: the shuffle reader delivers a superset and this trims each
    // task to its own cut range. There is no halo to widen by — the prefix
    // correction rides accumulator state, not neighbouring rows.
    let trim: Arc<dyn ExecutionPlan> = Arc::new(RangeFilterExec::try_new_pending(
        rse2,
        sort_expr.expr.clone(),
        ScalarValue::Float64(Some(0.0)),
        ScalarValue::Float64(Some(0.0)),
        Some(InputOrder::Ordered(sort_expr.options)),
    )?);
    // DER inserts boundary 1 under this trim, because its child (RSE#2) is
    // partition-preserving and sits directly on the ORRE.

    let partitioned_bwag: Arc<dyn ExecutionPlan> = Arc::new(
        PartitionedBoundedWindowAggExec::try_new(window.window_expr().to_vec(), trim)?,
    );

    // Boundary 2. `None` partitioning is the passthrough encoding: partition
    // count and per-partition ordering both carry across. Exists so the
    // scheduler gets a synchronization point where every task on the stage
    // below has published its accumulator state, not to move data.
    let state_boundary: Arc<dyn ExecutionPlan> = Arc::new(ExchangeExec::new(
        partitioned_bwag,
        None,
        plan_id_generator.fetch_add(1, Ordering::Relaxed),
    ));

    // BWAG appends its window columns after the input's, so expression `i`
    // lands at `input_field_count + i` and the input columns keep their
    // indices — which is why the aggregate's own argument expressions carry
    // over unchanged despite being resolved against the input schema.
    //
    // SUM goes through the Aggregate path even though the cheaper Scalar path
    // covers it: seeding an accumulator and replaying rows is the shape
    // non-decomposable aggregates need, and exercising it where the answer is
    // independently checkable beats the arrow-kernel shortcut. Choosing Scalar
    // where it applies is a later optimization, worth measuring.
    let input_field_count = window.input().schema().fields().len();
    let mut applies = Vec::new();
    for (expr_index, expr) in window.window_expr().iter().enumerate() {
        // Non-aggregate window functions publish no state to merge, so they
        // get no apply — `lead`/`lag` are handled by halos and the ranking
        // family needs separate infrastructure.
        let Some(plain) = expr.as_any().downcast_ref::<PlainAggregateWindowExpr>() else {
            continue;
        };
        let aggregate = plain.get_aggregate_expr();
        applies.push(WindowApply::Aggregate {
            udf: Arc::new(aggregate.fun().clone()),
            args: aggregate.expressions(),
            output_column: input_field_count + expr_index,
            window_expr_index: expr_index,
        });
    }

    // State is pending: it only exists once every task in the stage below
    // has closed its window and reported. The scheduler resolves it via
    // `PrefixMergeExec::resolve_state` when that stage completes.
    let prefix_merge: Arc<dyn ExecutionPlan> =
        Arc::new(PrefixMergeExec::try_new_pending(state_boundary, applies)?);

    debug!(
        "PrefixWindowRule: rewrote BWAG on `{}` ({} UNBOUNDED PRECEDING - {:?})",
        column.name(),
        frame.units,
        frame.end_bound,
    );
    Ok(Some(prefix_merge))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::config::ExtensionOptions;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// Plan `sql` over an 8-partition source, so the physical plan matches
    /// the shape captured from the real Q7 run: a per-partition `SortExec`
    /// with `preserve_partitioning=true` under a `SortPreservingMergeExec`
    /// that collapses to one partition for BWAG. A single-partition source
    /// produces neither, which would make the "SPM is gone" assertion pass
    /// against an input that never had one.
    async fn plan(sql: &str) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        const PARTITIONS: usize = 8;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id1", DataType::Int64, false),
            Field::new("id2", DataType::Int64, false),
            Field::new("id3", DataType::Int64, false),
            Field::new("v2", DataType::Float64, false),
        ]));
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(PARTITIONS),
        );
        let table = MemTable::try_new(Arc::clone(&schema), vec![Vec::new(); PARTITIONS])?;
        ctx.register_table("large", Arc::new(table))?;
        ctx.sql(sql).await?.create_physical_plan().await
    }

    /// Runs the rule with the shared parallel-window flag on, so
    /// `ignores_finite_frame_shape` exercises the frame gate rather than the
    /// config gate.
    fn optimize(
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 8;
        let mut bc = BallistaConfig::default();
        bc.set("planner.parallel_window.enabled", "true")?;
        config.extensions.insert(bc);
        PrefixWindowRule::default().optimize(plan, &config)
    }

    /// `UNBOUNDED PRECEDING` frame, no PARTITION BY, single ascending
    /// ORDER BY on a `Float64` column. Asserts the target from the module
    /// docs.
    ///
    /// Orders by `v2` rather than h2o Q7's `id3` because the routing gate is
    /// `Float64`-only today — see `declines_int64_routing`, which pins Q7's
    /// actual shape and why it doesn't rewrite.
    ///
    /// Only this rule runs here, so boundary 1's `ExchangeExec` is absent —
    /// `DistributedExchangeRule` inserts that one later in the chain. What
    /// the rule itself owns is the ORRE preamble, the zero-halo trim, the
    /// PBWAG swap, boundary 2, and the correction on top.
    #[tokio::test]
    async fn rewrites_unbounded_preceding_shape() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));

        assert!(
            rendered.contains("OrderedRangeRepartitionExec"),
            "prefix scan needs range-disjoint partitions for \"all prior \
             partitions\" to be well defined:\n{rendered}"
        );
        assert!(
            rendered.contains("RuntimeStatsExec"),
            "ORRE routes against sketched cuts, which RSE produces:\n{rendered}"
        );
        assert!(
            rendered.contains("RangeFilterExec"),
            "the shuffle reader delivers a superset; each task must be \
             trimmed to its own cut range:\n{rendered}"
        );
        assert!(
            rendered.contains("PartitionedBoundedWindowAggExec"),
            "BWAG must be wrapped so it runs per-partition:\n{rendered}"
        );
        assert!(
            rendered.contains("PrefixMergeExec"),
            "per-partition results need a downstream prefix correction:\n{rendered}"
        );
        assert!(
            rendered.contains("ExchangeExec"),
            "boundary 2 carries accumulator state back to the scheduler and \
             is planted by this rule, not by DER:\n{rendered}"
        );
        assert!(
            !rendered.contains("SortPreservingMergeExec"),
            "SPM collapses K partitions to 1, which is the bottleneck being \
             removed:\n{rendered}"
        );
        Ok(())
    }

    /// h2o Q7 verbatim. It has the right frame and the right partitioning,
    /// but orders by `id3`, which is `Int64` in the h2o schema — and both
    /// window rules gate routing on `Float64`, a T-Digest restriction that
    /// lifts with the KLL migration.
    ///
    /// So Q7 is blocked on the sketch, not on anything in this rule. When
    /// KLL lands this test starts failing, which is the signal to widen the
    /// gate and delete it.
    #[tokio::test]
    async fn declines_int64_routing() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY id3 \
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            !rendered.contains("PrefixMergeExec"),
            "ORRE cannot route on a non-Float64 key until the sketch \
             widens:\n{rendered}"
        );
        Ok(())
    }

    /// The halo rule's shape — finite `PRECEDING` start — must fall through
    /// untouched. The two rules partition the frame space between them, so a
    /// hit here would mean both fire on the same plan.
    #[tokio::test]
    async fn ignores_finite_frame_shape() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            !rendered.contains("PrefixMergeExec"),
            "finite-frame windows belong to the halo rewrite:\n{rendered}"
        );
        Ok(())
    }
}
