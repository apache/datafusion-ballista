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

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::ChaosExec;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::sync::Arc;

/// Optimizer rule that randomly injects a single [`ChaosExec`] into the physical plan.
///
/// Controlled by four Ballista config keys:
///
/// - `ballista.testing.chaos_execution.enabled` (bool, default `false`)
/// - `ballista.testing.chaos_execution.probability` (f64 in 0.0–1.0, default `0.25`)
/// - `ballista.testing.chaos_execution.fault_type` (str, default `"transient"`) — one of `transient`, `fatal`, `panic`, `delay`
/// - `ballista.testing.chaos_execution.seed` (str, default `""`) — optional u64 seed for reproducible runs
///
/// When enabled, the rule walks the plan in pre-order, picks a random node, and wraps it
/// with [`ChaosExec`]. Exactly one injection per `optimize` call.
///
/// This optimizer rule is not idempotent and should not be called multiple times
/// on the same plan.
#[derive(Debug, Default)]
pub struct ChaosCreatingRule {}

impl PhysicalOptimizerRule for ChaosCreatingRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();

        if !bc.chaos_execution_enabled() {
            return Ok(plan);
        }

        let failure_probability = bc.chaos_execution_probability();
        let fault_type = bc.chaos_execution_fault_type();
        let seed = bc.chaos_execution_seed();

        // Count total nodes (pre-order DFS).
        let mut node_count = 0usize;
        plan.apply(|_| {
            node_count += 1;
            Ok(TreeNodeRecursion::Continue)
        })?;

        // Pick a uniformly random target. % 1 == 0 always, so single-node plans
        // are deterministic (index 0), which makes tests predictable.
        let target_idx = match seed {
            Some(s) => (StdRng::seed_from_u64(s).random::<u64>() as usize) % node_count,
            None => (rand::random::<u64>() as usize) % node_count,
        };
        let mut current_idx = 0usize;

        // transform_down visits nodes in the same pre-order as apply, so
        // target_idx addresses the same node in both passes.
        let result = plan.transform_down(|node| {
            let idx = current_idx;
            current_idx += 1;
            if idx == target_idx {
                let chaos =
                    Arc::new(ChaosExec::new(node, failure_probability, &fault_type, seed)?)
                        as Arc<dyn ExecutionPlan>;
                log::warn!("A ChaosExec node has been injected in your execution plan, making execution undeterministic");
                Ok(Transformed::yes(chaos))
            } else {
                Ok(Transformed::no(node))
            }
        })?;

        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ChaosCreatingRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_plan;
    use ballista_core::config::BallistaConfig;
    use ballista_core::execution_plans::ChaosExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::config::{ConfigOptions, ExtensionOptions};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
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

    fn chaos_config(probability: f64) -> ConfigOptions {
        let mut bc = BallistaConfig::default();
        bc.set("testing.chaos_execution.enabled", "true").unwrap();
        bc.set(
            "testing.chaos_execution.probability",
            &probability.to_string(),
        )
        .unwrap();
        let mut config = ConfigOptions::default();
        config.extensions.insert(bc);
        config
    }

    #[test]
    fn chaos_rule_disabled_is_noop() {
        let rule = ChaosCreatingRule::default();
        let plan = leaf_exec();
        let result = rule.optimize(plan, &ConfigOptions::default()).unwrap();
        assert_plan!(result.as_ref(), @ "StatisticsExec: col_count=1, row_count=Absent");
    }

    // Single-node plan: target_idx = rand % 1 = 0 always — injection is deterministic.
    #[test]
    fn chaos_rule_wraps_single_node() {
        let rule = ChaosCreatingRule::default();
        let plan = leaf_exec();
        let result = rule.optimize(plan, &chaos_config(1.0)).unwrap();
        assert_plan!(result.as_ref(), @ r"
        ChaosExec: failure_probability=1, fault_type=transient
          StatisticsExec: col_count=1, row_count=Absent
        ");
    }

    // Multi-node plan: target node is random, so we only assert count, not shape.
    #[test]
    fn chaos_rule_injects_exactly_once() {
        let rule = ChaosCreatingRule::default();
        let plan =
            Arc::new(CoalescePartitionsExec::new(leaf_exec())) as Arc<dyn ExecutionPlan>;

        for _ in 0..20 {
            let result = rule.optimize(plan.clone(), &chaos_config(0.5)).unwrap();
            let mut count = 0usize;
            result
                .apply(|node| {
                    if node.as_any().downcast_ref::<ChaosExec>().is_some() {
                        count += 1;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
            assert_eq!(1, count, "exactly one ChaosExec must be present");
        }
    }

    #[test]
    fn chaos_rule_probability_is_passed_through() {
        let rule = ChaosCreatingRule::default();
        let plan = leaf_exec();
        let result = rule.optimize(plan, &chaos_config(0.42)).unwrap();
        let chaos = result
            .as_any()
            .downcast_ref::<ChaosExec>()
            .expect("single-node plan always wraps root");
        assert!(
            (chaos.failure_probability() - 0.42).abs() < 1e-5,
            "probability should round-trip through f64→f32 cast"
        );
    }
}
