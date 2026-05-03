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
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, execution_plan};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

enum ExchangeStatus {
    None,
    Resolved,
    Unresolved,
}

#[derive(Debug, Clone, Default)]
pub struct DistributedExchangeRule {
    plan_id_generator: Arc<AtomicUsize>,
}

impl DistributedExchangeRule {

    pub(crate) fn transform(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(coalesce) = execution_plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
        {
            let input = coalesce.input();
            if input.as_any().downcast_ref::<ExchangeExec>().is_none()
                && !matches!(find_exchange_status(input), ExchangeStatus::Unresolved)
            {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    None,
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(
                    execution_plan.with_new_children(vec![Arc::new(exchange_exec)])?,
                ));
            }
        } else if let Some(sort_preserving_merge) = execution_plan
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>(
        ) {
            let input = sort_preserving_merge.input();
            if input.as_any().downcast_ref::<ExchangeExec>().is_none()
                && !matches!(find_exchange_status(input), ExchangeStatus::Unresolved)
            {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    None,
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(
                    execution_plan.with_new_children(vec![Arc::new(exchange_exec)])?,
                ));
            }
        } else if let Some(repartition) =
            execution_plan.as_any().downcast_ref::<RepartitionExec>()
            && let execution_plan::Partitioning::Hash(_, _) = repartition.partitioning()
        {
            let input = repartition.input();
            if !matches!(find_exchange_status(input), ExchangeStatus::Unresolved) {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    Some(repartition.partitioning().clone()),
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(Arc::new(exchange_exec)));
            }
        }
        Ok(Transformed::no(execution_plan))
    }
}

impl PhysicalOptimizerRule for DistributedExchangeRule {
    fn optimize(
        &self,
        execution_plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<
        std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    > {
        let result = execution_plan.transform_up(|p| self.transform(p))?;

        if result
            .data
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .is_some()
        {
            Ok(result.data)
        } else {
            let plan_id = self
                .plan_id_generator
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            Ok(Arc::new(AdaptiveDatafusionExec::new(plan_id, result.data)))
        }
    }

    fn name(&self) -> &str {
        "DistributedExchangeRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Scans the subtree for the nearest `ExchangeExec` in each path and returns the
/// aggregate status. Stops recursing at `ExchangeExec` boundaries so that only the
/// shallowest exchange in each branch is considered.
///
/// Returns `Unresolved` as soon as any branch contains an unresolved exchange
/// (short-circuits), `Resolved` if every branch that has an exchange has a resolved
/// one, and `None` if no exchange is found anywhere.
fn find_exchange_status(plan: &Arc<dyn ExecutionPlan>) -> ExchangeStatus {
    if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>() {
        if exchange.shuffle_created() {
            ExchangeStatus::Resolved
        } else {
            ExchangeStatus::Unresolved
        }
    } else {
        let mut found_resolved = false;
        for child in plan.children() {
            match find_exchange_status(child) {
                ExchangeStatus::Unresolved => return ExchangeStatus::Unresolved,
                ExchangeStatus::Resolved => found_resolved = true,
                ExchangeStatus::None => {}
            }
        }
        if found_resolved {
            ExchangeStatus::Resolved
        } else {
            ExchangeStatus::None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_plan;
    use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::config::ConfigOptions;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::execution_plan::Partitioning;
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

    // --- CoalescePartitionsExec ---

    #[test]
    fn coalesce_inserts_exchange_for_bare_leaf() {
        let rule = DistributedExchangeRule::default();
        let input =
            Arc::new(CoalescePartitionsExec::new(leaf_exec())) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(input, &config()).unwrap();

        assert_plan!(result.as_ref(), @ r"
        AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending
          CoalescePartitionsExec
            ExchangeExec: partitioning=None, plan_id=0, stage_id=pending, stage_resolved=false
              StatisticsExec: col_count=1, row_count=Absent
        ");
    }

    #[test]
    fn coalesce_with_direct_exchange_not_double_wrapped() {
        let rule = DistributedExchangeRule::default();
        let coalesce = Arc::new(CoalescePartitionsExec::new(unresolved_exchange(
            leaf_exec(),
        ))) as Arc<dyn ExecutionPlan>;

        let result = rule.optimize(coalesce, &config()).unwrap();

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        let coalesce_out = adaptive
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let child = coalesce_out.children()[0];
        assert!(
            child.as_any().downcast_ref::<ExchangeExec>().is_some(),
            "direct child should remain ExchangeExec"
        );
        assert!(
            child.children()[0]
                .as_any()
                .downcast_ref::<ExchangeExec>()
                .is_none(),
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

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        let outer_coalesce = adaptive
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            outer_coalesce.children()[0]
                .as_any()
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

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        let outer_coalesce = adaptive
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            outer_coalesce.children()[0]
                .as_any()
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

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        let spm = adaptive
            .input()
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .expect("child should be SortPreservingMergeExec");
        assert!(
            spm.children()[0]
                .as_any()
                .downcast_ref::<ExchangeExec>()
                .is_some(),
            "SortPreservingMergeExec should have ExchangeExec injected as its child"
        );
    }

    #[test]
    fn spm_with_direct_exchange_not_double_wrapped() {
        let rule = DistributedExchangeRule::default();
        let input = sort_preserving_merge(unresolved_exchange(leaf_exec()));

        let result = rule.optimize(input, &config()).unwrap();

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        let spm = adaptive
            .input()
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .unwrap();
        let child = spm.children()[0];
        assert!(child.as_any().downcast_ref::<ExchangeExec>().is_some());
        assert!(
            child.children()[0]
                .as_any()
                .downcast_ref::<ExchangeExec>()
                .is_none()
        );
    }

    #[test]
    fn spm_skips_injection_when_unresolved_exchange_in_subtree() {
        let rule = DistributedExchangeRule::default();
        let inner = Arc::new(CoalescePartitionsExec::new(
            unresolved_exchange(leaf_exec()),
        )) as Arc<dyn ExecutionPlan>;
        let input = sort_preserving_merge(inner);

        let result = rule.optimize(input, &config()).unwrap();

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        let spm = adaptive
            .input()
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .unwrap();
        assert!(
            spm.children()[0]
                .as_any()
                .downcast_ref::<ExchangeExec>()
                .is_none(),
            "should not inject ExchangeExec when unresolved exchange is in subtree"
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

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        let exchange = adaptive
            .input()
            .as_any()
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

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        assert!(
            adaptive
                .input()
                .as_any()
                .downcast_ref::<RepartitionExec>()
                .is_some(),
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

        let adaptive = result
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap();
        assert!(
            adaptive
                .input()
                .as_any()
                .downcast_ref::<RepartitionExec>()
                .is_some(),
            "Hash repartition should be kept when input has an unresolved exchange"
        );
    }

    // --- optimize() root wrapping ---

    #[test]
    fn optimize_wraps_plan_in_adaptive_exec() {
        let rule = DistributedExchangeRule::default();
        let result = rule.optimize(leaf_exec(), &config()).unwrap();
        assert!(
            result
                .as_any()
                .downcast_ref::<AdaptiveDatafusionExec>()
                .is_some(),
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
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .expect("result should be AdaptiveDatafusionExec");
        assert!(
            outer
                .input()
                .as_any()
                .downcast_ref::<AdaptiveDatafusionExec>()
                .is_none(),
            "existing AdaptiveDatafusionExec should not be wrapped in another one"
        );
    }

    // --- find_exchange_status ---

    #[test]
    fn exchange_status_none_for_plan_with_no_exchanges() {
        let plan = leaf_exec();
        assert!(matches!(find_exchange_status(&plan), ExchangeStatus::None));
    }

    #[test]
    fn exchange_status_unresolved_for_unresolved_exchange() {
        let plan = unresolved_exchange(leaf_exec());
        assert!(matches!(
            find_exchange_status(&plan),
            ExchangeStatus::Unresolved
        ));
    }

    #[test]
    fn exchange_status_resolved_for_resolved_exchange() {
        let plan = resolved_exchange(leaf_exec());
        assert!(matches!(
            find_exchange_status(&plan),
            ExchangeStatus::Resolved
        ));
    }

    #[test]
    fn exchange_status_propagates_through_non_exchange_nodes() {
        let coalesce_resolved =
            Arc::new(CoalescePartitionsExec::new(resolved_exchange(leaf_exec())))
                as Arc<dyn ExecutionPlan>;
        assert!(matches!(
            find_exchange_status(&coalesce_resolved),
            ExchangeStatus::Resolved
        ));

        let coalesce_unresolved = Arc::new(CoalescePartitionsExec::new(
            unresolved_exchange(leaf_exec()),
        )) as Arc<dyn ExecutionPlan>;
        assert!(matches!(
            find_exchange_status(&coalesce_unresolved),
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
            find_exchange_status(&outer_unresolved),
            ExchangeStatus::Unresolved
        ));

        // A resolved exchange wrapping an unresolved one: the outer (resolved)
        // is the shallowest — result is Resolved, the inner is not inspected.
        let inner_unresolved = unresolved_exchange(leaf_exec());
        let outer_resolved = resolved_exchange(inner_unresolved);
        assert!(matches!(
            find_exchange_status(&outer_resolved),
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
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap()
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap()
            .children()[0]
            .as_any()
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
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .unwrap()
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap()
            .children()[0]
            .as_any()
            .downcast_ref::<ExchangeExec>()
            .unwrap();
        assert_eq!(
            2, exchange2.plan_id,
            "second optimize on same rule: ExchangeExec should get plan_id=2 (counter at 2 after first call used 0 and 1)"
        );
    }
}
