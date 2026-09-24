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
//! # The shape it gates on
//!
//! No PARTITION BY, a single ascending ORDER BY column, and an ever-expanding
//! frame. The ORDER BY column's type is gated by asking
//! [`SortKeyCodec::try_new`] rather than by a type list here, because that
//! codec is what ORRE's routing sketch encodes keys with — widening the codec
//! widens this rule.
//!
//! # Actual rule input
//!
//! Captured by logging `optimize`'s argument on h2o q7. The rule
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
//! # Target shape
//!
//! The input partitions are **not** range-disjoint. Stage 0 is a bare
//! `SortExec` over 8 file groups, so each partition is locally sorted but
//! spans the whole value range. A prefix scan needs "all prior partitions" to be well
//! defined, so the rewrite has to introduce the disjointness itself, with
//! the same `RSE#1 → SortExec → ORRE → RSE#2` preamble the halo rule builds:
//!
//! ```text
//! PrefixMergeExec [per-partition state baked in by the scheduler]
//!   ExchangeExec (partitioning: None)          <- boundary 2, to exchange prefixes with scheduler
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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::state::aqe::execution_plan::ExchangeExec;
use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{
    InputOrder, OrderedRangeRepartitionExec, PartitionedBoundedWindowAggExec,
    PrefixMergeExec, RangeFilterExec, RuntimeStatsExec, WindowApply,
};
use ballista_core::sort_key::SortKeyCodec;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::window::PlainAggregateWindowExpr;
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
/// now.
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

    // ORRE routes on the ORDER BY column via a `SortKeySketch`, so the gate
    // is whatever that sketch's codec can encode — asked here rather than
    // spelled as a type list, so widening the codec widens this rule with no
    // edit. `SortKeyCodec` gives NULLs a position per `nulls_first`, so a
    // nullable key needs no separate gate.
    let routing_type = order.expr.data_type(&source_schema)?;
    if SortKeyCodec::try_new(&routing_type, order.options).is_none() {
        return Ok(None);
    }

    let sort_expr = order.clone();

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
    let trim: Arc<dyn ExecutionPlan> = Arc::new(RangeFilterExec::try_new_pending(
        rse2,
        sort_expr.expr.clone(),
        ScalarValue::new_zero(&routing_type)?,
        ScalarValue::new_zero(&routing_type)?,
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
    // For now SUM goes through the Aggregate path even though the cheaper Scalar path
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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
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
            // Not in h2o's schema. The sketch codec places a NULL run per
            // `nulls_first`, so nullability is not part of the type gate —
            // `rewrites_nullable_routing` is what holds that open.
            Field::new("nullable_key", DataType::Int64, true),
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

    /// h2o Q7 verbatim, ordering by `Int64 id3`. The rewrite must not be
    /// specific to the `Float64` key `rewrites_unbounded_preceding_shape`
    /// uses: the gate is what the routing sketch's codec encodes, and that
    /// covers every fixed-width type.
    #[tokio::test]
    async fn rewrites_int64_routing() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY id3 \
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            rendered.contains("PrefixMergeExec"),
            "an Int64 routing key is one the sketch codec encodes:\n{rendered}"
        );
        Ok(())
    }

    /// A nullable routing key. NULLs do not break the prefix: they sort as one
    /// run to the end `nulls_first` names, so the run lands wholly inside one
    /// partition and "every partition before k" stays well defined.
    #[tokio::test]
    async fn rewrites_nullable_routing() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY nullable_key \
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = optimize(plan)?;
        let rendered = format!("{}", displayable(rewritten.as_ref()).indent(true));
        assert!(
            rendered.contains("PrefixMergeExec"),
            "a nullable routing key is routable — the codec gives the NULL \
             run a position:\n{rendered}"
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
