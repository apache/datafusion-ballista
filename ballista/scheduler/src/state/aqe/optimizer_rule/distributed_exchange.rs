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

use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{
    OrderedRangeRepartitionExec, RangeFilterExec, UnorderedRangeRepartitionExec,
    preserves_partitioning,
};
use datafusion::common::plan_err;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions, execution_plan,
};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

enum ExchangeStatus {
    Absent,
    Resolved,
    Unresolved,
}

#[derive(Debug, Clone, Default)]
pub struct DistributedExchangeRule {
    plan_id_generator: Arc<AtomicUsize>,
}

impl DistributedExchangeRule {
    pub(crate) fn new(plan_id_generator: Arc<AtomicUsize>) -> Self {
        Self { plan_id_generator }
    }

    pub(crate) fn transform(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        // DataFusion's null-aware hash join coordinates visited rows and
        // probe-side NULL state with in-process atomics. A CollectLeft join
        // running once per probe partition therefore produces duplicate or
        // incorrect output. This is the final plan-mutating rule, so enforce an
        // explicit probe-side coalesce here after DataFusion's optimizers have
        // finished. Add an exchange when the subtree has no existing boundary.
        if let Some(hash_join) = execution_plan.downcast_ref::<HashJoinExec>()
            && hash_join.null_aware
            && *hash_join.partition_mode() == PartitionMode::CollectLeft
            && hash_join
                .right()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_none()
        {
            let left = hash_join.left().clone();
            let right = hash_join.right().clone();
            let right = if right.downcast_ref::<ExchangeExec>().is_none()
                && !matches!(nearest_exchange_status(&right), ExchangeStatus::Unresolved)
            {
                Arc::new(ExchangeExec::new(
                    right,
                    None,
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                )) as Arc<dyn ExecutionPlan>
            } else {
                right
            };
            let right = Arc::new(CoalescePartitionsExec::new(right));
            return Ok(Transformed::yes(execution_plan.replace_children(
                vec![left, right],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?));
        }

        if let Some(coalesce) = execution_plan.downcast_ref::<CoalescePartitionsExec>() {
            let input = coalesce.input();
            if input.downcast_ref::<ExchangeExec>().is_none()
                && !matches!(nearest_exchange_status(input), ExchangeStatus::Unresolved)
                && !is_inlinable_scan_pipeline(input, broadcast_threshold_bytes(config))?
            {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    None,
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(execution_plan.replace_children(
                    vec![Arc::new(exchange_exec)],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )?));
            }
        } else if let Some(sort_preserving_merge) =
            execution_plan.downcast_ref::<SortPreservingMergeExec>()
        {
            let input = sort_preserving_merge.input();
            if !is_stage_boundary(input)
                && !matches!(nearest_exchange_status(input), ExchangeStatus::Unresolved)
            {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    None,
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(execution_plan.replace_children(
                    vec![Arc::new(exchange_exec)],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )?));
            }
        } else if let Some(repartition) = execution_plan.downcast_ref::<RepartitionExec>()
            && let execution_plan::Partitioning::Hash(_, _) = repartition.partitioning()
        {
            let input = repartition.input();
            if !matches!(nearest_exchange_status(input), ExchangeStatus::Unresolved) {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    Some(repartition.partitioning().clone()),
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(Arc::new(exchange_exec)));
            }
        } else if !execution_plan.is::<ExchangeExec>() {
            let children = execution_plan.children();
            match children.as_slice() {
                [] => {}
                [child] => {
                    if can_be_range_repartitioned(child)? {
                        let exchange_exec = ExchangeExec::new(
                            Arc::clone(child),
                            None,
                            self.plan_id_generator
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                        );
                        return Ok(Transformed::yes(execution_plan.replace_children(
                            vec![Arc::new(exchange_exec)],
                            ReplaceChildrenOptions::new(
                                ChildrenPropertiesMode::Recompute,
                            ),
                        )?));
                    }
                }
                many => {
                    let mut any_range = false;
                    for c in many {
                        any_range |= can_be_range_repartitioned(c)?;
                    }
                    if any_range {
                        return plan_err!(
                            "range-repartitioned child under multi-child parent `{}`: \
                             cross-stage cut coordination is not yet implemented",
                            execution_plan.name()
                        );
                    }
                }
            }
        }
        Ok(Transformed::no(execution_plan))
    }
}

/// Reuses the broadcast budget: a scan cheap enough to sit in every probe task
/// is cheap enough to re-read there. `0` disables inlining.
fn broadcast_threshold_bytes(config: &datafusion::config::ConfigOptions) -> usize {
    config
        .extensions
        .get::<BallistaConfig>()
        .cloned()
        .unwrap_or_default()
        .broadcast_join_threshold_bytes()
}

/// True when every task of the consuming stage can read `plan` directly, making
/// a stage boundary under it pure cost: the `CoalescePartitionsExec` DataFusion
/// plants over a `CollectLeft` build side gathers inside one task either way.
///
/// Filtered scans are excluded — the staged copy is then much smaller than the
/// scan, so rebuilding it per task costs more than reading it back (inlining
/// q19's filtered `part` cost 14%). `Precision::Exact` is the test, since the
/// predicate is often pushed into the scan with no `FilterExec` to look for.
fn is_inlinable_scan_pipeline(
    plan: &Arc<dyn ExecutionPlan>,
    max_bytes: usize,
) -> datafusion::error::Result<bool> {
    if max_bytes == 0 {
        return Ok(false);
    }
    let mut scan_bytes = 0usize;
    let mut leaves = 0usize;
    if !collect_scan_bytes(plan, &mut scan_bytes, &mut leaves)? {
        return Ok(false);
    }
    Ok(leaves > 0 && scan_bytes <= max_bytes)
}

/// Sums the leaf scans' bytes; `false` on any other operator or inexact leaf.
fn collect_scan_bytes(
    plan: &Arc<dyn ExecutionPlan>,
    total_bytes: &mut usize,
    leaves: &mut usize,
) -> datafusion::error::Result<bool> {
    if plan.is::<DataSourceExec>() {
        let stats =
            StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new())?;
        let Precision::Exact(bytes) = stats.total_byte_size else {
            return Ok(false);
        };
        *total_bytes = total_bytes.saturating_add(bytes);
        *leaves += 1;
        return Ok(true);
    }
    if !plan.is::<ProjectionExec>() {
        return Ok(false);
    }
    for child in plan.children() {
        if !collect_scan_bytes(child, total_bytes, leaves)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Returns whether a plan should have distributed range-repartitioning added:
///
/// `Ok(true)` - `plan` has a "range-repartitioned child" that should have an ExchangeExec
///
/// `Ok(false)` - no range-repartition was present
///
/// `Err(_)` - there IS a URRE/ORRE below, but an intermediate op
/// disturbs the routing expression enough that we can't safely route
/// through it
fn can_be_range_repartitioned(
    plan: &Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<bool> {
    if !preserves_partitioning(plan.as_ref()) {
        // We've hit some other repartitioner, not range-repartitioned
        return Ok(false);
    }
    let children = plan.children();
    let [child] = children.as_slice() else {
        // We don't support multi-legged plans for now (SMJ, etc)
        return Ok(false);
    };
    if !child.is::<UnorderedRangeRepartitionExec>()
        && !child.is::<OrderedRangeRepartitionExec>()
    {
        // Not range-repartitioned
        return Ok(false);
    }
    // We are range repartitioned, but make sure the routing expression survives
    if plan.is::<ProjectionExec>() {
        // TODO: verify by checking expression itself
        return plan_err!(
            "range-repartitioned child under `{}`: routing expression \
             cannot be safely remapped to the boundary schema",
            plan.name()
        );
    }
    // We can range-repartition this
    Ok(true)
}

impl PhysicalOptimizerRule for DistributedExchangeRule {
    fn optimize(
        &self,
        execution_plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<
        std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    > {
        let result = execution_plan.transform_up(|p| self.transform(p, config))?;

        if result
            .data
            .downcast_ref::<AdaptiveDatafusionExec>()
            .is_some()
        {
            return Ok(result.data);
        }

        // A range-repartitioned root is never visited as a child by
        // `transform_up`, so wrap it here before the outer
        // `AdaptiveDatafusionExec` goes on.
        //
        // TODO: kill this branch — and the range-repart arm in
        // `transform()` above — when `ExchangeExec` carries a
        // range-cuts partitioning variant the way it carries
        // `Partitioning::Hash`. URRE will replace itself with an
        // ExchangeExec (like the Hash arm does), `transform_up`'s
        // output will already have an ExchangeExec at the range-repart
        // position, and the plain `AdaptiveDatafusionExec` wrap below
        // handles the root case with no extra ceremony.
        let inner = if can_be_range_repartitioned(&result.data)? {
            let id = self
                .plan_id_generator
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Arc::new(ExchangeExec::new(result.data, None, id)) as Arc<dyn ExecutionPlan>
        } else {
            result.data
        };
        let plan_id = self
            .plan_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(Arc::new(AdaptiveDatafusionExec::new(plan_id, inner)))
    }

    fn name(&self) -> &str {
        "DistributedExchangeRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// True when `node` is (or transparently sits on) a stage boundary.
/// `RangeFilterExec` counts because we chose not to fold range-filtering
/// into `ShuffleReader`/`ExchangeExec` — the operator is part of the
/// boundary shape by design.
fn is_stage_boundary(node: &Arc<dyn ExecutionPlan>) -> bool {
    if node.is::<ExchangeExec>() {
        return true;
    }
    if node.is::<RangeFilterExec>()
        && let [child] = node.children().as_slice()
    {
        return child.is::<ExchangeExec>();
    }
    false
}

/// Scans the subtree for the nearest `ExchangeExec` in each path and returns the
/// aggregate status. Stops recursing at `ExchangeExec` boundaries so that only the
/// shallowest exchange in each branch is considered.
///
/// Returns `Unresolved` as soon as any branch contains an unresolved exchange
/// (short-circuits), `Resolved` if every branch that has an exchange has a resolved
/// one, and `None` if no exchange is found anywhere.
fn nearest_exchange_status(plan: &Arc<dyn ExecutionPlan>) -> ExchangeStatus {
    if let Some(exchange) = plan.downcast_ref::<ExchangeExec>() {
        if exchange.shuffle_created() && !exchange.inactive_stage {
            ExchangeStatus::Resolved
        } else {
            ExchangeStatus::Unresolved
        }
    } else {
        let mut found_resolved = false;
        for child in plan.children() {
            match nearest_exchange_status(child) {
                ExchangeStatus::Unresolved => return ExchangeStatus::Unresolved,
                ExchangeStatus::Resolved => found_resolved = true,
                ExchangeStatus::Absent => {}
            }
        }
        if found_resolved {
            ExchangeStatus::Resolved
        } else {
            ExchangeStatus::Absent
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
    use ballista_core::assert_plan;
    use ballista_core::execution_plans::{
        RuntimeStatsExec, UnorderedRangeRepartitionExec,
    };
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::execution_plan::Partitioning;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::test::exec::StatisticsExec;
    use std::sync::Arc;

    fn leaf_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let stats = Statistics {
            num_rows: Default::default(),
            total_byte_size: Default::default(),
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };
        Arc::new(StatisticsExec::new(stats, schema))
    }

    fn config() -> ConfigOptions {
        ConfigOptions::new()
    }

    fn unresolved_exchange(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(ExchangeExec::new(input, None, 0))
    }

    fn resolved_exchange(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let exchange = ExchangeExec::new(input, None, 0);
        exchange.resolve_shuffle_partitions(vec![]);
        Arc::new(exchange)
    }

    fn sort_preserving_merge(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let sort_expr = PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)));
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        Arc::new(SortPreservingMergeExec::new(ordering, input))
    }

    #[test]
    fn null_aware_join_coalesces_probe_after_other_optimizers() {
        use datafusion::common::{JoinType, NullEquality};

        let left = leaf_exec();
        let right = leaf_exec();
        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(
                    Arc::new(Column::new("a", 0)) as _,
                    Arc::new(Column::new("a", 0)) as _,
                )],
                None,
                &JoinType::LeftAnti,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                true,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let result = DistributedExchangeRule::default()
            .optimize(join, &config())
            .unwrap();

        assert_plan!(result.as_ref(), @ r"
        AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending, stage_resolved=false
          HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(a@0, a@0)], null_aware
            StatisticsExec: col_count=1, row_count=Absent
            CoalescePartitionsExec
              ExchangeExec: partitioning=None, plan_id=0, stage_id=pending, stage_resolved=false
                StatisticsExec: col_count=1, row_count=Absent
        ");
    }

    // --- CoalescePartitionsExec ---

    #[test]
    fn coalesce_inserts_exchange_for_bare_leaf() {
        let rule = DistributedExchangeRule::default();
        let input =
            Arc::new(CoalescePartitionsExec::new(leaf_exec())) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(input, &config()).unwrap();

        assert_plan!(result.as_ref(), @ r"
        AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending, stage_resolved=false
          CoalescePartitionsExec
            ExchangeExec: partitioning=None, plan_id=0, stage_id=pending, stage_resolved=false
              StatisticsExec: col_count=1, row_count=Absent
        ");
    }

    /// Scan with exact bytes, like a parquet scan with collected statistics.
    fn scan_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn config_with_broadcast_threshold(bytes: usize) -> ConfigOptions {
        let mut config = ConfigOptions::new();
        config.extensions.insert(BallistaConfig::default());
        config
            .set(
                "ballista.optimizer.broadcast_join_threshold_bytes",
                &bytes.to_string(),
            )
            .unwrap();
        config
    }

    #[test]
    fn coalesce_reads_small_scan_inline_instead_of_staging_it() {
        let rule = DistributedExchangeRule::default();
        let input =
            Arc::new(CoalescePartitionsExec::new(scan_exec())) as Arc<dyn ExecutionPlan>;

        let result = rule
            .optimize(input, &config_with_broadcast_threshold(64 * 1024 * 1024))
            .unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let coalesce = adaptive
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            coalesce.children()[0].is::<DataSourceExec>(),
            "small scan should stay inline, got {}",
            coalesce.children()[0].name()
        );
    }

    #[test]
    fn coalesce_stages_scan_larger_than_the_broadcast_threshold() {
        let rule = DistributedExchangeRule::default();
        let input =
            Arc::new(CoalescePartitionsExec::new(scan_exec())) as Arc<dyn ExecutionPlan>;

        let result = rule
            .optimize(input, &config_with_broadcast_threshold(1))
            .unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let coalesce = adaptive
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            coalesce.children()[0].is::<ExchangeExec>(),
            "oversized scan should keep its stage boundary, got {}",
            coalesce.children()[0].name()
        );
    }

    #[test]
    fn coalesce_stages_scan_when_broadcast_promotion_is_disabled() {
        let rule = DistributedExchangeRule::default();
        let input =
            Arc::new(CoalescePartitionsExec::new(scan_exec())) as Arc<dyn ExecutionPlan>;

        let result = rule
            .optimize(input, &config_with_broadcast_threshold(0))
            .unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let coalesce = adaptive
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            coalesce.children()[0].is::<ExchangeExec>(),
            "threshold 0 disables inlining, got {}",
            coalesce.children()[0].name()
        );
    }

    #[test]
    fn coalesce_stages_scan_behind_a_selective_filter() {
        // Filtering marks the estimate inexact (TPC-H q19's build side).
        let rule = DistributedExchangeRule::default();
        let scan = scan_exec();
        let filter = Arc::new(
            FilterExec::try_new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                )),
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let input =
            Arc::new(CoalescePartitionsExec::new(filter)) as Arc<dyn ExecutionPlan>;

        let result = rule
            .optimize(input, &config_with_broadcast_threshold(64 * 1024 * 1024))
            .unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let coalesce = adaptive
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            coalesce.children()[0].is::<ExchangeExec>(),
            "selective filter should keep its stage boundary, got {}",
            coalesce.children()[0].name()
        );
    }

    #[test]
    fn coalesce_stages_pipeline_that_is_not_a_plain_scan() {
        let rule = DistributedExchangeRule::default();
        let scan = scan_exec();
        let aggregate = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::new_single(vec![(
                    Arc::new(Column::new("a", 0)) as _,
                    "a".to_string(),
                )]),
                vec![],
                vec![],
                scan.clone(),
                scan.schema(),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let input =
            Arc::new(CoalescePartitionsExec::new(aggregate)) as Arc<dyn ExecutionPlan>;

        let result = rule
            .optimize(input, &config_with_broadcast_threshold(64 * 1024 * 1024))
            .unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let coalesce = adaptive
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            coalesce.children()[0].is::<ExchangeExec>(),
            "aggregate pipeline should keep its stage boundary, got {}",
            coalesce.children()[0].name()
        );
    }

    #[test]
    fn coalesce_with_direct_exchange_not_double_wrapped() {
        let rule = DistributedExchangeRule::default();
        let coalesce = Arc::new(CoalescePartitionsExec::new(unresolved_exchange(
            leaf_exec(),
        ))) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(coalesce, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let coalesce_out = adaptive
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let child = coalesce_out.children()[0];
        assert!(
            child.downcast_ref::<ExchangeExec>().is_some(),
            "direct child should remain ExchangeExec"
        );
        assert!(
            child.children()[0].downcast_ref::<ExchangeExec>().is_none(),
            "ExchangeExec should not wrap another ExchangeExec"
        );
    }

    #[test]
    fn coalesce_skips_injection_when_unresolved_exchange_in_subtree() {
        // outer coalesce should NOT get ExchangeExec injected because
        // the subtree (inner coalesce → unresolved exchange) blocks stage splitting
        let rule = DistributedExchangeRule::default();
        let inner = Arc::new(CoalescePartitionsExec::new(
            unresolved_exchange(leaf_exec()),
        )) as Arc<dyn ExecutionPlan>;
        let outer =
            Arc::new(CoalescePartitionsExec::new(inner)) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(outer, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let outer_coalesce = adaptive
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            outer_coalesce.children()[0]
                .downcast_ref::<ExchangeExec>()
                .is_none(),
            "should not inject ExchangeExec when unresolved exchange is in subtree"
        );
    }

    #[test]
    fn coalesce_injects_exchange_when_subtree_has_only_resolved_exchanges() {
        // outer coalesce SHOULD get ExchangeExec injected because
        // all exchanges in the subtree are already resolved
        let rule = DistributedExchangeRule::default();
        let inner = Arc::new(CoalescePartitionsExec::new(resolved_exchange(leaf_exec())))
            as Arc<dyn ExecutionPlan>;
        let outer =
            Arc::new(CoalescePartitionsExec::new(inner)) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(outer, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let outer_coalesce = adaptive
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            outer_coalesce.children()[0]
                .downcast_ref::<ExchangeExec>()
                .is_some(),
            "should inject ExchangeExec when subtree only has resolved exchanges"
        );
    }

    // --- SortPreservingMergeExec ---

    #[test]
    fn spm_inserts_exchange_for_bare_leaf() {
        let rule = DistributedExchangeRule::default();
        let input = sort_preserving_merge(leaf_exec());

        let result = rule.optimize(input, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let spm = adaptive
            .input()
            .downcast_ref::<SortPreservingMergeExec>()
            .expect("child should be SortPreservingMergeExec");
        assert!(
            spm.children()[0].downcast_ref::<ExchangeExec>().is_some(),
            "SortPreservingMergeExec should have ExchangeExec injected as its child"
        );
    }

    #[test]
    fn spm_with_direct_exchange_not_double_wrapped() {
        let rule = DistributedExchangeRule::default();
        let input = sort_preserving_merge(unresolved_exchange(leaf_exec()));

        let result = rule.optimize(input, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let spm = adaptive
            .input()
            .downcast_ref::<SortPreservingMergeExec>()
            .unwrap();
        let child = spm.children()[0];
        assert!(child.downcast_ref::<ExchangeExec>().is_some());
        assert!(child.children()[0].downcast_ref::<ExchangeExec>().is_none());
    }

    #[test]
    fn spm_skips_injection_when_unresolved_exchange_in_subtree() {
        let rule = DistributedExchangeRule::default();
        let inner = Arc::new(CoalescePartitionsExec::new(
            unresolved_exchange(leaf_exec()),
        )) as Arc<dyn ExecutionPlan>;
        let input = sort_preserving_merge(inner);

        let result = rule.optimize(input, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let spm = adaptive
            .input()
            .downcast_ref::<SortPreservingMergeExec>()
            .unwrap();
        assert!(
            spm.children()[0].downcast_ref::<ExchangeExec>().is_none(),
            "should not inject ExchangeExec when unresolved exchange is in subtree"
        );
    }

    #[test]
    fn spm_skips_when_range_filter_covers_exchange() {
        // ParallelWindowRule plants a RangeFilterExec directly on the
        // resolved range-repartition ExchangeExec with SPM above. The
        // filter must count as part of the boundary — otherwise DE
        // inserts another ExchangeExec between SPM and the filter,
        // collapsing K partitions into a single outer-stage task.
        use ballista_core::execution_plans::RangeFilterExec;
        use datafusion::scalar::ScalarValue;

        let rule = DistributedExchangeRule::default();
        let exchange = resolved_exchange(float_leaf_exec());
        let filter: Arc<dyn ExecutionPlan> = Arc::new(
            RangeFilterExec::try_new_pending(
                exchange,
                Arc::new(Column::new("v", 0)),
                ScalarValue::Float64(Some(0.0)),
                ScalarValue::Float64(Some(0.0)),
                None,
            )
            .unwrap(),
        );
        let sort_expr = PhysicalSortExpr::new_default(Arc::new(Column::new("v", 0)));
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let spm: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, filter));

        let result = rule.optimize(spm, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let spm_out = adaptive
            .input()
            .downcast_ref::<SortPreservingMergeExec>()
            .unwrap();
        let below_spm = spm_out.children()[0];
        assert!(
            below_spm.downcast_ref::<RangeFilterExec>().is_some(),
            "SPM's direct child should remain RangeFilterExec, not a new ExchangeExec"
        );
        assert!(
            below_spm.children()[0]
                .downcast_ref::<ExchangeExec>()
                .is_some(),
            "resolved ExchangeExec should remain under the RangeFilterExec"
        );
    }

    // --- RepartitionExec ---

    #[test]
    fn hash_repartition_is_replaced_with_exchange() {
        let rule = DistributedExchangeRule::default();
        let col = Arc::new(Column::new("a", 0)) as _;
        let repartition = Arc::new(
            RepartitionExec::try_new(leaf_exec(), Partitioning::Hash(vec![col], 4))
                .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(repartition, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        let exchange = adaptive
            .input()
            .downcast_ref::<ExchangeExec>()
            .expect("Hash RepartitionExec should be replaced with ExchangeExec");
        assert!(
            matches!(exchange.partitioning, Some(Partitioning::Hash(_, 4))),
            "ExchangeExec should carry the hash partitioning"
        );
    }

    #[test]
    fn round_robin_repartition_is_not_replaced() {
        let rule = DistributedExchangeRule::default();
        let repartition = Arc::new(
            RepartitionExec::try_new(leaf_exec(), Partitioning::RoundRobinBatch(4))
                .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(repartition, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        assert!(
            adaptive.input().downcast_ref::<RepartitionExec>().is_some(),
            "RoundRobin repartition should be kept as-is (not replaced)"
        );
    }

    #[test]
    fn hash_repartition_skips_when_unresolved_exchange_in_input() {
        let rule = DistributedExchangeRule::default();
        let col = Arc::new(Column::new("a", 0)) as _;
        let repartition = Arc::new(
            RepartitionExec::try_new(
                unresolved_exchange(leaf_exec()),
                Partitioning::Hash(vec![col], 4),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(repartition, &config()).unwrap();

        let adaptive = result.downcast_ref::<AdaptiveDatafusionExec>().unwrap();
        assert!(
            adaptive.input().downcast_ref::<RepartitionExec>().is_some(),
            "Hash repartition should be kept when input has an unresolved exchange"
        );
    }

    // --- optimize() root wrapping ---

    #[test]
    fn optimize_wraps_plan_in_adaptive_exec() {
        let rule = DistributedExchangeRule::default();
        let result = rule.optimize(leaf_exec(), &config()).unwrap();
        assert!(
            result.downcast_ref::<AdaptiveDatafusionExec>().is_some(),
            "optimize should always wrap the result in AdaptiveDatafusionExec"
        );
    }

    #[test]
    fn optimize_does_not_double_wrap_existing_adaptive_exec() {
        let rule = DistributedExchangeRule::default();
        let adaptive = Arc::new(AdaptiveDatafusionExec::new(0, leaf_exec()))
            as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(adaptive, &config()).unwrap();

        let outer = result
            .downcast_ref::<AdaptiveDatafusionExec>()
            .expect("result should be AdaptiveDatafusionExec");
        assert!(
            outer
                .input()
                .downcast_ref::<AdaptiveDatafusionExec>()
                .is_none(),
            "existing AdaptiveDatafusionExec should not be wrapped in another one"
        );
    }

    // --- find_exchange_status ---

    #[test]
    fn exchange_status_none_for_plan_with_no_exchanges() {
        let plan = leaf_exec();
        assert!(matches!(
            nearest_exchange_status(&plan),
            ExchangeStatus::Absent
        ));
    }

    #[test]
    fn exchange_status_unresolved_for_unresolved_exchange() {
        let plan = unresolved_exchange(leaf_exec());
        assert!(matches!(
            nearest_exchange_status(&plan),
            ExchangeStatus::Unresolved
        ));
    }

    #[test]
    fn exchange_status_resolved_for_resolved_exchange() {
        let plan = resolved_exchange(leaf_exec());
        assert!(matches!(
            nearest_exchange_status(&plan),
            ExchangeStatus::Resolved
        ));
    }

    #[test]
    fn exchange_status_propagates_through_non_exchange_nodes() {
        let coalesce_resolved =
            Arc::new(CoalescePartitionsExec::new(resolved_exchange(leaf_exec())))
                as Arc<dyn ExecutionPlan>;
        assert!(matches!(
            nearest_exchange_status(&coalesce_resolved),
            ExchangeStatus::Resolved
        ));

        let coalesce_unresolved = Arc::new(CoalescePartitionsExec::new(
            unresolved_exchange(leaf_exec()),
        )) as Arc<dyn ExecutionPlan>;
        assert!(matches!(
            nearest_exchange_status(&coalesce_unresolved),
            ExchangeStatus::Unresolved
        ));
    }

    #[test]
    fn exchange_status_stops_at_exchange_boundary() {
        // An unresolved exchange wrapping a resolved one: the outer (unresolved)
        // is the shallowest and determines the result — Unresolved.
        let inner_resolved = resolved_exchange(leaf_exec());
        let outer_unresolved = unresolved_exchange(inner_resolved);
        assert!(matches!(
            nearest_exchange_status(&outer_unresolved),
            ExchangeStatus::Unresolved
        ));

        // A resolved exchange wrapping an unresolved one: the outer (resolved)
        // is the shallowest — result is Resolved, the inner is not inspected.
        let inner_unresolved = unresolved_exchange(leaf_exec());
        let outer_resolved = resolved_exchange(inner_unresolved);
        assert!(matches!(
            nearest_exchange_status(&outer_resolved),
            ExchangeStatus::Resolved
        ));
    }

    // --- plan_id counter monotonicity ---

    #[test]
    fn plan_ids_are_assigned_sequentially_across_optimize_calls() {
        // The rule's plan_id counter is shared across multiple optimize calls.
        // First call: CoalescePartitionsExec(leaf) → ExchangeExec(plan_id=0), AdaptiveExec(plan_id=1)
        // Second call on the same rule: counter continues from 2,
        //   so the new ExchangeExec gets plan_id=2.
        let rule = DistributedExchangeRule::default();

        let result1 = rule
            .optimize(
                Arc::new(CoalescePartitionsExec::new(leaf_exec())),
                &config(),
            )
            .unwrap();
        let exchange1 = result1
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap()
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap()
            .children()[0]
            .downcast_ref::<ExchangeExec>()
            .unwrap();
        assert_eq!(
            0, exchange1.plan_id,
            "first optimize: ExchangeExec should get plan_id=0"
        );

        let result2 = rule
            .optimize(
                Arc::new(CoalescePartitionsExec::new(leaf_exec())),
                &config(),
            )
            .unwrap();
        let exchange2 = result2
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap()
            .input()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap()
            .children()[0]
            .downcast_ref::<ExchangeExec>()
            .unwrap();
        assert_eq!(
            2, exchange2.plan_id,
            "second optimize on same rule: ExchangeExec should get plan_id=2 (counter at 2 after first call used 0 and 1)"
        );
    }

    // --- range-repartition ---

    fn float_leaf_exec() -> Arc<dyn ExecutionPlan> {
        // URRE routing today requires Float64 non-nullable.
        let schema = Schema::new(vec![Field::new("v", DataType::Float64, false)]);
        let stats = Statistics {
            num_rows: Default::default(),
            total_byte_size: Default::default(),
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };
        Arc::new(StatisticsExec::new(stats, schema))
    }

    fn stats_over_urre_over_leaf() -> Arc<dyn ExecutionPlan> {
        let sort_expr = PhysicalSortExpr {
            expr: Arc::new(Column::new("v", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        };
        let urre: Arc<dyn ExecutionPlan> = Arc::new(
            UnorderedRangeRepartitionExec::try_new(
                float_leaf_exec(),
                vec![sort_expr.clone()],
                4,
            )
            .unwrap(),
        );
        Arc::new(RuntimeStatsExec::try_new(urre, Some(vec![sort_expr])).unwrap())
    }

    fn count_exchanges(plan: &dyn ExecutionPlan) -> usize {
        let here = usize::from(plan.is::<ExchangeExec>());
        here + plan
            .children()
            .iter()
            .map(|c| count_exchanges(c.as_ref()))
            .sum::<usize>()
    }

    fn display_plan(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        )
    }

    /// A bug in either half of the idempotency guard (the
    /// `!.is::<ExchangeExec>()` check on the visited node, or
    /// `is_range_repartitioned` accidentally recognising `ExchangeExec`)
    /// would double-wrap on every AQE replan.
    #[test]
    fn range_repartition_optimize_is_idempotent() {
        let rule = DistributedExchangeRule::default();

        let first = rule
            .optimize(stats_over_urre_over_leaf(), &config())
            .unwrap();
        let second = rule.optimize(first.clone(), &config()).unwrap();

        assert_eq!(
            count_exchanges(first.as_ref()),
            1,
            "first pass must insert exactly one ExchangeExec above the range-repartitioned root"
        );
        assert_eq!(
            count_exchanges(second.as_ref()),
            1,
            "second pass must not insert a second ExchangeExec"
        );
        assert_eq!(
            display_plan(&first),
            display_plan(&second),
            "second DER pass over its own output must be a no-op"
        );
    }

    /// A range-repartition at the plan root — the canonical shape a
    /// range-repartition-inserting rule emits, with nothing above it —
    /// must still get an `ExchangeExec` wrapped above it. Without it,
    /// `set_repartition_routing` has no parking slot for the recovered
    /// cuts and downstream never gets a `RangeFilterExec` to
    /// trim straddler duplication.
    #[test]
    fn range_repartition_at_plan_root_gets_exchange_inserted() {
        let rule = DistributedExchangeRule::default();
        let root = stats_over_urre_over_leaf();

        let result = rule.optimize(root, &config()).unwrap();

        let adaptive = result
            .downcast_ref::<AdaptiveDatafusionExec>()
            .expect("DER wraps its output in AdaptiveDatafusionExec");
        let below = adaptive.input();
        assert!(
            below.is::<ExchangeExec>(),
            "expected an ExchangeExec between the AdaptiveDatafusionExec \
             wrapper and the range-repartitioned subtree; got {}",
            below.name()
        );
        let rse = below.children()[0];
        assert!(rse.is::<RuntimeStatsExec>());
        assert!(rse.children()[0].is::<UnorderedRangeRepartitionExec>());
        assert_eq!(count_exchanges(result.as_ref()), 1);
    }

    /// A range-repartitioned chain on each branch of a `UnionExec` is
    /// rejected at plan time — the machinery needs cross-stage cut
    /// coordination (see doc-comment on `is_range_repartitioned`), and
    /// silently producing independently-cut shuffles would misroute.
    #[test]
    fn range_repartition_under_union_is_rejected() {
        let rule = DistributedExchangeRule::default();
        let union: Arc<dyn ExecutionPlan> =
            datafusion::physical_plan::union::UnionExec::try_new(vec![
                stats_over_urre_over_leaf(),
                stats_over_urre_over_leaf(),
            ])
            .unwrap();

        let err = rule.optimize(union, &config()).unwrap_err().to_string();

        assert!(
            err.contains("range-repartitioned child under multi-child parent"),
            "unexpected error: {err}"
        );
        assert!(err.contains("UnionExec"), "unexpected error: {err}");
    }

    /// TODO: SMJ over range-repartitioned inputs is a motivating
    /// consumer for cross-stage cut coordination — both sides must
    /// share one cut set (derived from both sketches, parked on both
    /// boundary exchanges) for equijoin keys to land in the same
    /// downstream partition. Until that code exists, DER rejects the
    /// shape at plan time.
    #[test]
    fn range_repartition_under_sort_merge_join_is_rejected() {
        use datafusion::common::{JoinType, NullEquality};
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_plan::joins::SortMergeJoinExec;

        let rule = DistributedExchangeRule::default();
        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
            vec![(Arc::new(Column::new("v", 0)), Arc::new(Column::new("v", 0)))];
        let smj: Arc<dyn ExecutionPlan> = Arc::new(
            SortMergeJoinExec::try_new(
                stats_over_urre_over_leaf(),
                stats_over_urre_over_leaf(),
                on,
                None,
                JoinType::Inner,
                vec![SortOptions {
                    descending: false,
                    nulls_first: false,
                }],
                NullEquality::NullEqualsNothing,
            )
            .unwrap(),
        );

        let err = rule.optimize(smj, &config()).unwrap_err().to_string();

        assert!(
            err.contains("range-repartitioned child under multi-child parent"),
            "unexpected error: {err}"
        );
        assert!(err.contains("SortMergeJoinExec"), "unexpected error: {err}");
    }

    /// A `ProjectionExec` between (O/U)RRE and the boundary could
    /// reindex, drop, or shadow the routing expression's referenced
    /// columns — the read-side `RangeFilterExec` would evaluate
    /// against the wrong column and silently misroute. DER rejects the
    /// shape at plan time; the fix will be revisited when arbitrary
    /// routing expressions replace the current single-column form.
    ///
    /// See https://github.com/apache/datafusion-ballista/pull/2196#discussion_r3705634907
    #[test]
    fn range_repartition_under_projection_is_rejected() {
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_plan::projection::ProjectionExec;

        let rule = DistributedExchangeRule::default();
        let schema = Schema::new(vec![
            Field::new("v", DataType::Float64, false),
            Field::new("tag", DataType::Float64, false),
        ]);
        let stats = Statistics {
            num_rows: Default::default(),
            total_byte_size: Default::default(),
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(stats, schema));
        let sort_expr = PhysicalSortExpr {
            expr: Arc::new(Column::new("v", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        };
        let urre: Arc<dyn ExecutionPlan> = Arc::new(
            UnorderedRangeRepartitionExec::try_new(leaf, vec![sort_expr], 4).unwrap(),
        );
        // Swap: post-projection output is (tag, v) — routing key `v`
        // moves from index 0 to index 1.
        let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("tag", 1)), "tag".to_string()),
            (Arc::new(Column::new("v", 0)), "v".to_string()),
        ];
        let proj: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(proj_exprs, urre).unwrap());

        let err = rule.optimize(proj, &config()).unwrap_err().to_string();

        assert!(
            err.contains("routing expression cannot be safely remapped"),
            "unexpected error: {err}"
        );
        assert!(err.contains("ProjectionExec"), "unexpected error: {err}");
    }
}

#[cfg(test)]
mod debug_stats {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::filter::FilterExec;

    #[test]
    fn print_stats() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let scan =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let s = StatisticsContext::new()
            .compute(scan.as_ref(), &StatisticsArgs::new())
            .unwrap();
        println!("SCAN rows={:?} bytes={:?}", s.num_rows, s.total_byte_size);
        let filter = Arc::new(
            FilterExec::try_new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                )),
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let f = StatisticsContext::new()
            .compute(filter.as_ref(), &StatisticsArgs::new())
            .unwrap();
        println!("FILTER rows={:?} bytes={:?}", f.num_rows, f.total_byte_size);
    }
}
