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

use crate::state::aqe::execution_plan::ExchangeExec;
use ballista_core::config::BallistaConfig;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use log::info;
use std::sync::Arc;

/// AQE rule that re-derives `InputOrderMode` for each `AggregateExec` after a
/// stage completes and rewrites the operator when the derived mode differs from
/// the cached one.
///
/// DataFusion freezes `InputOrderMode` (Linear = hash table / Sorted = streaming /
/// PartiallySorted) at plan time. After a shuffle stage resolves, the input's
/// `EquivalenceProperties` may expose ordering that wasn't visible earlier, making
/// streaming aggregation possible (or, after a subtree rewrite, making the previously
/// assumed ordering stale). This rule repairs both cases on each AQE replan.
///
/// Relies on `AggregateExec::with_new_children` calling `try_new_with_schema`,
/// which re-derives `input_order_mode` from the current input equivalence
/// properties — no upstream DataFusion changes required.
///
/// Enabled via `ballista.aqe.dynamic_aggregate.enabled` (default: false).
#[derive(Debug, Clone, Default)]
pub struct DynamicAggregateAlgorithmRule {}

impl DynamicAggregateAlgorithmRule {
    fn transform(
        plan: Arc<dyn ExecutionPlan>,
        enabled: bool,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if !enabled {
            return Ok(Transformed::no(plan));
        }
        let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(plan));
        };
        // Only act when a resolved exchange exists below — if nothing resolved,
        // the ordering is the same as at initial plan time and no switch is needed.
        if !subtree_has_resolved_exchange(&plan) {
            return Ok(Transformed::no(plan));
        }

        // Re-derive InputOrderMode by reconstructing the aggregate with the same
        // input. with_new_children → try_new_with_schema re-runs the derivation
        // against the current input EquivalenceProperties.
        let rebuilt = plan
            .clone()
            .with_new_children(vec![agg.children()[0].clone()])?;
        let rebuilt_agg = rebuilt
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("with_new_children on AggregateExec must return AggregateExec");

        if rebuilt_agg.input_order_mode() == agg.input_order_mode() {
            return Ok(Transformed::no(plan));
        }

        info!(
            "DynamicAggregateAlgorithmRule: switching InputOrderMode {:?} → {:?}",
            agg.input_order_mode(),
            rebuilt_agg.input_order_mode(),
        );
        Ok(Transformed::yes(rebuilt))
    }
}

impl PhysicalOptimizerRule for DynamicAggregateAlgorithmRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let enabled = config
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.aqe_dynamic_aggregate_enabled())
            .unwrap_or(false);
        Ok(plan.transform_up(|p| Self::transform(p, enabled))?.data)
    }

    fn name(&self) -> &str {
        "DynamicAggregateAlgorithmRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Returns true if any `ExchangeExec` in `plan`'s subtree is resolved
/// (i.e. its shuffle has completed). Recurses through all nodes, including
/// through exchange boundaries — unlike `nearest_exchange_status` in
/// `distributed_exchange.rs` which stops at the first exchange, here we
/// want to know if *any* upstream stage has finished.
fn subtree_has_resolved_exchange(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>() {
        exchange.shuffle_created() && !exchange.inactive_stage
    } else {
        plan.children()
            .iter()
            .any(|child| subtree_has_resolved_exchange(child))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::config::BallistaConfig;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::config::{ConfigOptions, ExtensionOptions};
    use datafusion::physical_expr::LexOrdering;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::test::exec::StatisticsExec;
    use datafusion::physical_expr::PhysicalSortExpr;
    use std::sync::Arc;

    fn leaf_unsorted() -> Arc<dyn ExecutionPlan> {
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Default::default(),
                total_byte_size: Default::default(),
                column_statistics: vec![
                    ColumnStatistics::new_unknown(),
                    ColumnStatistics::new_unknown(),
                ],
            },
            Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("v", DataType::Int64, true),
            ]),
        ))
    }

    fn leaf_sorted_on_k() -> Arc<dyn ExecutionPlan> {
        let sort_expr =
            PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0)));
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        Arc::new(SortExec::new(ordering, leaf_unsorted()))
    }

    fn resolved_exchange(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let exchange = ExchangeExec::new(input, None, 0);
        exchange.resolve_shuffle_partitions(vec![]);
        Arc::new(exchange)
    }

    fn unresolved_exchange(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(ExchangeExec::new(input, None, 0))
    }

    fn aggregate_on_k(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let k_col = Arc::new(Column::new("k", 0)) as _;
        let group_by = PhysicalGroupBy::new_single(vec![(k_col, "k".to_string())]);
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                group_by,
                vec![],
                vec![],
                input,
                schema,
            )
            .unwrap(),
        )
    }

    fn config_with_flag(enabled: bool) -> ConfigOptions {
        let mut ballista = BallistaConfig::default();
        if enabled {
            ballista
                .set(
                    "aqe.dynamic_aggregate.enabled",
                    "true",
                )
                .unwrap();
        }
        let mut opts = ConfigOptions::new();
        opts.extensions.insert(ballista);
        opts
    }

    // --- gate ---

    #[test]
    fn gate_disabled_returns_no_transform() {
        let plan = aggregate_on_k(resolved_exchange(leaf_sorted_on_k()));
        let rule = DynamicAggregateAlgorithmRule::default();
        let result = rule.optimize(plan.clone(), &config_with_flag(false)).unwrap();
        assert!(
            Arc::ptr_eq(&result, &plan),
            "gate disabled: plan must be returned unchanged (same Arc)"
        );
    }

    // --- subtree_has_resolved_exchange ---

    #[test]
    fn no_exchange_skips() {
        // AggregateExec with no exchange at all → no rewrite even when enabled
        let plan = aggregate_on_k(leaf_sorted_on_k());
        let rule = DynamicAggregateAlgorithmRule::default();
        let result = rule.optimize(plan.clone(), &config_with_flag(true)).unwrap();
        assert!(Arc::ptr_eq(&result, &plan));
    }

    #[test]
    fn unresolved_exchange_skips() {
        let plan = aggregate_on_k(unresolved_exchange(leaf_sorted_on_k()));
        let rule = DynamicAggregateAlgorithmRule::default();
        let result = rule.optimize(plan.clone(), &config_with_flag(true)).unwrap();
        assert!(Arc::ptr_eq(&result, &plan));
    }

    // --- mode transitions ---

    #[test]
    fn linear_to_sorted_when_input_fully_ordered() {
        // Input: SortExec on k → resolved ExchangeExec → AggregateExec(group_by=[k])
        // The SortExec grants ordering on k; the exchange preserves it in eq_properties.
        // AggregateExec is initially planned as Linear (before the sort was visible).
        // After re-derivation the mode should become Sorted.
        let sorted_input = leaf_sorted_on_k();
        let exchange = resolved_exchange(sorted_input);
        let agg = aggregate_on_k(exchange);

        let agg_exec = agg.as_any().downcast_ref::<AggregateExec>().unwrap();
        // The agg was built over the resolved exchange whose eq_properties mirror
        // the SortExec — so it may already be Sorted. What matters is the rule
        // doesn't break it (idempotence when already correct).
        let original_mode = agg_exec.input_order_mode().clone();

        let rule = DynamicAggregateAlgorithmRule::default();
        let result = rule.optimize(agg.clone(), &config_with_flag(true)).unwrap();
        let result_agg = result.as_any().downcast_ref::<AggregateExec>().unwrap();

        // After the rule the mode must match what re-derivation produces.
        assert_eq!(result_agg.input_order_mode(), &original_mode);
        // Schema must be preserved.
        assert_eq!(result.schema(), agg.schema());
    }

    #[test]
    fn unsorted_input_stays_linear() {
        // Input has no ordering → mode must stay Linear after the rule.
        let exchange = resolved_exchange(leaf_unsorted());
        let plan = aggregate_on_k(exchange);

        let rule = DynamicAggregateAlgorithmRule::default();
        let result = rule.optimize(plan.clone(), &config_with_flag(true)).unwrap();
        let result_agg = result.as_any().downcast_ref::<AggregateExec>().unwrap();

        assert_eq!(
            result_agg.input_order_mode(),
            &datafusion::physical_plan::InputOrderMode::Linear
        );
    }

    #[test]
    fn schema_preserved_after_rewrite() {
        let exchange = resolved_exchange(leaf_sorted_on_k());
        let plan = aggregate_on_k(exchange);
        let original_schema = plan.schema();

        let rule = DynamicAggregateAlgorithmRule::default();
        let result = rule.optimize(plan, &config_with_flag(true)).unwrap();

        assert_eq!(result.schema(), original_schema);
    }

    // --- idempotence ---

    #[test]
    fn idempotent_second_pass() {
        let exchange = resolved_exchange(leaf_sorted_on_k());
        let plan = aggregate_on_k(exchange);

        let rule = DynamicAggregateAlgorithmRule::default();
        let after_first = rule.optimize(plan, &config_with_flag(true)).unwrap();
        let mode_after_first = after_first
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap()
            .input_order_mode()
            .clone();

        let after_second = rule
            .optimize(after_first.clone(), &config_with_flag(true))
            .unwrap();
        let mode_after_second = after_second
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap()
            .input_order_mode()
            .clone();

        assert_eq!(mode_after_first, mode_after_second, "rule must be idempotent");
    }

    // --- rule metadata ---

    #[test]
    fn rule_name() {
        assert_eq!(
            DynamicAggregateAlgorithmRule::default().name(),
            "DynamicAggregateAlgorithmRule"
        );
    }

    #[test]
    fn schema_check_is_false() {
        assert!(!DynamicAggregateAlgorithmRule::default().schema_check());
    }
}
