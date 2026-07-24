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
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::physical_plan::StatisticsArgs;
use datafusion::{
    catalog::memory::DataSourceExec,
    common::tree_node::{Transformed, TreeNode},
    error::DataFusionError,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan,
        joins::{HashJoinExec, PartitionMode, SortMergeJoinExec},
    },
};
use std::sync::{Arc, atomic::AtomicUsize};

use crate::state::aqe::execution_plan::{
    DynamicJoinSelectionExec, ExchangeExec, JoinSelectionAction,
};

#[derive(Debug, Default)]
pub struct DelayJoinSelectionRule {
    plan_id_generator: Arc<AtomicUsize>,
}

impl DelayJoinSelectionRule {
    pub(crate) fn new(plan_id_generator: Arc<AtomicUsize>) -> Self {
        DelayJoinSelectionRule { plan_id_generator }
    }

    fn plan_id(&self) -> usize {
        self.plan_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

impl PhysicalOptimizerRule for DelayJoinSelectionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        if bc.adaptive_join_enabled() {
            let result = plan.transform_up(|node| {
                if let Some(hj) = node.downcast_ref::<HashJoinExec>() {
                    let dynamic =
                        DynamicJoinSelectionExec::from_hash_join(hj, self.plan_id())?;
                    Ok(Transformed::yes(dynamic as Arc<dyn ExecutionPlan>))
                } else if let Some(smj) = node.downcast_ref::<SortMergeJoinExec>() {
                    let dynamic =
                        DynamicJoinSelectionExec::from_sort_join(smj, self.plan_id())?;
                    Ok(Transformed::yes(dynamic as Arc<dyn ExecutionPlan>))
                } else {
                    Ok(Transformed::no(node))
                }
            })?;
            Ok(result.data)
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "DelayJoinSelectionRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
pub struct SelectJoinRule {
    plan_id_generator: Arc<AtomicUsize>,
}

impl SelectJoinRule {
    pub(crate) fn new(plan_id_generator: Arc<AtomicUsize>) -> Self {
        SelectJoinRule { plan_id_generator }
    }

    fn plan_id(&self) -> usize {
        self.plan_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn supports_swap_join_order(
        left: &dyn ExecutionPlan,
        right: &dyn ExecutionPlan,
    ) -> datafusion::common::Result<bool> {
        // Get the left and right table's total bytes
        // If both the left and right tables contain total_byte_size statistics,
        // use `total_byte_size` to determine `should_swap_join_order`, else use `num_rows`
        let left_stats = StatisticsContext::new().compute(left, &StatisticsArgs::new())?;
        let right_stats = StatisticsContext::new().compute(right, &StatisticsArgs::new())?;
        // First compare `total_byte_size` of left and right side,
        // if information in this field is insufficient fallback to the `num_rows`
        match (
            left_stats.total_byte_size.get_value(),
            right_stats.total_byte_size.get_value(),
        ) {
            (Some(l), Some(r)) => Ok(l > r),
            _ => match (
                left_stats.num_rows.get_value(),
                right_stats.num_rows.get_value(),
            ) {
                (Some(l), Some(r)) => Ok(l > r),
                _ => Ok(false),
            },
        }
    }
}

impl PhysicalOptimizerRule for SelectJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        if bc.adaptive_join_enabled() {
            let result = plan.transform_up(|node| {
                if let Some(dynamic_join) =
                    node.downcast_ref::<DynamicJoinSelectionExec>()
                {
                    if dynamic_join.upstream_resolved() {
                        match dynamic_join.to_actual_join(config)? {
                            // at this point we know there are two exchanges
                            // as we added them beforehand
                            JoinSelectionAction::LateCollectLeft(hash_join_exec) => {
                                if Self::supports_swap_join_order(
                                    hash_join_exec.left.as_ref(),
                                    hash_join_exec.right.as_ref(),
                                )? {
                                    let left = hash_join_exec.left.clone();
                                    let right = hash_join_exec.right.clone();

                                    let right = right
                                    .downcast_ref::<ExchangeExec>()
                                    .ok_or(DataFusionError::Execution(
                                        "ExchangeExec is expected at this point (right)"
                                            .to_owned(),
                                    ))?
                                    .to_broadcast(self.plan_id());
                                    let right = Arc::new(right);

                                    let join = hash_join_exec
                                        .with_new_children(vec![left, right])?;

                                    let join = join
                                        .downcast_ref::<HashJoinExec>()
                                        .ok_or(DataFusionError::Execution(
                                        "HashJoinExec is expected at this point (right)"
                                            .to_owned(),
                                    ))?;
                                    let join =
                                        join.swap_inputs(PartitionMode::CollectLeft)?;
                                    Ok(Transformed::yes(join))
                                } else {
                                    let left = hash_join_exec
                                    .left
                                    .downcast_ref::<ExchangeExec>()
                                    .ok_or(DataFusionError::Execution(
                                        "ExchangeExec is expected at this point (left)"
                                            .to_owned(),
                                    ))?
                                    .to_broadcast(self.plan_id());
                                    let left = Arc::new(left);

                                    let right = hash_join_exec.right.clone();

                                    let join = hash_join_exec
                                        .with_new_children(vec![left, right])?;
                                    Ok(Transformed::yes(join))
                                }
                            }

                            JoinSelectionAction::CollectLeft(hash_join_exec) => {
                                let plan = if Self::supports_swap_join_order(
                                    hash_join_exec.left.as_ref(),
                                    hash_join_exec.right.as_ref(),
                                )? {
                                    hash_join_exec
                                        .swap_inputs(PartitionMode::CollectLeft)?
                                } else {
                                    hash_join_exec
                                };

                                let exec = plan.transform_up(|p| {
                                    if let Some(hash_join) =
                                        p.downcast_ref::<HashJoinExec>()
                                        && matches!(
                                            hash_join.partition_mode(),
                                            PartitionMode::CollectLeft
                                        )
                                    {
                                        if let Some(data_source) = hash_join
                                            .left
                                            .downcast_ref::<DataSourceExec>(
                                        ) && let Ok(Some(left)) =
                                            data_source.repartitioned(1, config)
                                        {
                                            let right = hash_join.right.clone();
                                            let p =
                                                p.with_new_children(vec![left, right])?;
                                            Ok(Transformed::yes(p))
                                        } else if hash_join
                                            .left
                                            .properties()
                                            .partitioning
                                            .partition_count()
                                            > 1
                                        {
                                            let left = if let Some(exchange) = hash_join
                                                .left
                                                .downcast_ref::<ExchangeExec>()
                                            {
                                                Arc::new(
                                                    exchange.to_broadcast(self.plan_id()),
                                                )
                                            } else {
                                                Arc::new(ExchangeExec::new_broadcast(
                                                    hash_join.left.clone(),
                                                    None,
                                                    self.plan_id(),
                                                ))
                                            };
                                            let right = hash_join.right.clone();
                                            let p =
                                                p.with_new_children(vec![left, right])?;
                                            Ok(Transformed::yes(p))
                                        } else {
                                            Ok(Transformed::no(p))
                                        }
                                    } else {
                                        Ok(Transformed::no(p))
                                    }
                                })?;

                                Ok(Transformed::yes(exec.data))
                            }
                            JoinSelectionAction::Repartition(dynamic_join) => {
                                let partition_count = config.execution.target_partitions;
                                let partitioning = dynamic_join
                                    ._required_input_distribution()
                                    .iter()
                                    .map(|d| {
                                        d.clone().create_partitioning(partition_count)
                                    })
                                    .collect::<Vec<_>>();

                                let left = dynamic_join.left.clone();
                                let right = dynamic_join.right.clone();

                                let left = Arc::new(ExchangeExec::new(
                                    left,
                                    Some(partitioning[0].clone()),
                                    self.plan_id(),
                                ));

                                let right = Arc::new(ExchangeExec::new(
                                    right,
                                    Some(partitioning[1].clone()),
                                    self.plan_id(),
                                ));

                                let dynamic_join =
                                    dynamic_join.with_new_children(vec![left, right])?;

                                Ok(Transformed::yes(dynamic_join))
                            }
                            JoinSelectionAction::Hash(hash_join_exec) => {
                                let hash_join_exec = if Self::supports_swap_join_order(
                                    hash_join_exec.left.as_ref(),
                                    hash_join_exec.right.as_ref(),
                                )? {
                                    hash_join_exec
                                        .swap_inputs(*hash_join_exec.partition_mode())?
                                } else {
                                    hash_join_exec
                                };

                                Ok(Transformed::yes(hash_join_exec))
                            }
                            JoinSelectionAction::Sort(sort_merge_join_exec) => {
                                Ok(Transformed::yes(sort_merge_join_exec))
                            }
                        }
                    } else {
                        Ok(Transformed::no(node))
                    }
                } else {
                    Ok(Transformed::no(node))
                }
            })?;
            Ok(result.data)
        } else {
            // disabled using configuration
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "SelectJoinRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::assert_plan;
    use ballista_core::config::BallistaConfig;
    use datafusion::{
        arrow::{
            array::{Int32Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        },
        common::config::ConfigOptions,
        config::ExtensionOptions,
        datasource::MemTable,
        execution::{
            SessionStateBuilder, config::SessionConfig, context::SessionContext,
        },
        physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    };

    fn make_table(schema: Arc<Schema>) -> Arc<MemTable> {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    fn make_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let ctx = SessionContext::new();
        ctx.register_table("t1", make_table(Arc::clone(&schema)))
            .unwrap();
        ctx.register_table("t2", make_table(schema)).unwrap();
        ctx
    }

    fn make_ctx_3tables() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let ctx = SessionContext::new();
        ctx.register_table("t1", make_table(Arc::clone(&schema)))
            .unwrap();
        ctx.register_table("t2", make_table(Arc::clone(&schema)))
            .unwrap();
        ctx.register_table("t3", make_table(schema)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_hash_join_replaced_with_dynamic() {
        let ctx = make_ctx();
        let state = SessionStateBuilder::new()
            .with_physical_optimizer_rules(vec![])
            .build();
        let lp = ctx
            .sql("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&lp, &state)
            .await
            .unwrap();

        assert_plan!(plan.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          HashJoinExec: mode=Auto, join_type=Inner, on=[(id@0, id@0)]
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");

        let optimized = DelayJoinSelectionRule::default()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_plan!(optimized.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    #[tokio::test]
    async fn test_sort_merge_join_replaced_with_dynamic() {
        let ctx = make_ctx();
        let config =
            SessionConfig::new().set_bool("datafusion.optimizer.prefer_hash_join", false);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_physical_optimizer_rules(vec![])
            .build();

        let lp = ctx
            .sql("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&lp, &state)
            .await
            .unwrap();

        assert_plan!(plan.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          SortMergeJoinExec: join_type=Inner, on=[(id@0, id@0)]
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");

        let optimized = DelayJoinSelectionRule::default()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_plan!(optimized.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    #[tokio::test]
    async fn test_resolve_replaces_dynamic_with_sort_merge_join() {
        let ctx = make_ctx();
        let config =
            SessionConfig::new().set_bool("datafusion.optimizer.prefer_hash_join", false);
        let state = SessionStateBuilder::new()
            .with_config(config.clone())
            .with_physical_optimizer_rules(vec![])
            .build();
        let lp = ctx
            .sql("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&lp, &state)
            .await
            .unwrap();

        let dynamic_plan = DelayJoinSelectionRule::default()
            .optimize(plan, config.options())
            .unwrap();

        assert_plan!(dynamic_plan.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");

        let resolved = SelectJoinRule::default()
            .optimize(dynamic_plan, config.options())
            .unwrap();

        assert_plan!(resolved.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)]
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    #[tokio::test]
    async fn test_resolve_nested_dynamic_joins() {
        let ctx = make_ctx_3tables();
        let config =
            SessionConfig::new().set_bool("datafusion.optimizer.prefer_hash_join", false);

        let state = SessionStateBuilder::new()
            .with_physical_optimizer_rules(vec![])
            .with_config(config.clone())
            .build();

        let lp = ctx
            .sql("SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();

        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&lp, &state)
            .await
            .unwrap();

        let dynamic_plan = DelayJoinSelectionRule::default()
            .optimize(plan, config.options())
            .unwrap();

        assert_plan!(dynamic_plan.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id]
          DynamicJoinSelectionExec: plan_id=1, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
            ProjectionExec: expr=[id@0 as id]
              DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
                DataSourceExec: partitions=1, partition_sizes=[1]
                DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");

        let resolved = SelectJoinRule::default()
            .optimize(dynamic_plan, config.options())
            .unwrap();

        assert_plan!(resolved.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)]
            ProjectionExec: expr=[id@0 as id]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)]
                DataSourceExec: partitions=1, partition_sizes=[1]
                DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    #[tokio::test]
    async fn test_resolve_non_join_plan_untouched() {
        let ctx = make_ctx();
        let state = SessionStateBuilder::new()
            .with_physical_optimizer_rules(vec![])
            .build();
        let lp = ctx
            .sql("SELECT id FROM t1")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&lp, &state)
            .await
            .unwrap();

        let resolved = SelectJoinRule::default()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_plan!(resolved.as_ref(), @ "DataSourceExec: partitions=1, partition_sizes=[1]");
    }

    #[tokio::test]
    async fn test_non_join_plan_untouched() {
        let ctx = make_ctx();
        let state = SessionStateBuilder::new()
            .with_physical_optimizer_rules(vec![])
            .build();
        let lp = ctx
            .sql("SELECT id FROM t1")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&lp, &state)
            .await
            .unwrap();

        let optimized = DelayJoinSelectionRule::default()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_plan!(optimized.as_ref(), @ "DataSourceExec: partitions=1, partition_sizes=[1]");
    }

    /// When `ballista.planner.adaptive_join.enabled = false` the `DelayJoinSelectionRule`
    /// must be a no-op: a plan containing a `DynamicJoinSelectionExec` node must
    /// be returned unchanged.
    #[tokio::test]
    async fn test_select_join_rule_disabled_via_config() {
        let ctx = make_ctx();

        let state = SessionStateBuilder::new()
            .with_physical_optimizer_rules(vec![])
            .build();

        let lp = ctx
            .sql("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();

        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&lp, &state)
            .await
            .unwrap();

        let dynamic_plan = DelayJoinSelectionRule::default()
            .optimize(plan.clone(), &ConfigOptions::default())
            .unwrap();

        assert_plan!(dynamic_plan.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");

        let resolve_plan = SelectJoinRule::default()
            .optimize(dynamic_plan.clone(), &ConfigOptions::default())
            .unwrap();

        assert_plan!(resolve_plan.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)]
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");

        //
        // Build a ConfigOptions with adaptive_join disabled.
        //
        let mut bc = BallistaConfig::default();
        bc.set("planner.adaptive_join.enabled", "false").unwrap();
        let mut config_disabled = ConfigOptions::default();
        config_disabled.extensions.insert(bc);

        let resolve_plan = SelectJoinRule::default()
            .optimize(dynamic_plan.clone(), &config_disabled)
            .unwrap();

        assert_plan!(resolve_plan.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");

        let dynamic_plan = DelayJoinSelectionRule::default()
            .optimize(plan, &config_disabled)
            .unwrap();

        assert_plan!(dynamic_plan.as_ref(), @ r"
        ProjectionExec: expr=[id@0 as id, val@2 as val]
          HashJoinExec: mode=Auto, join_type=Inner, on=[(id@0, id@0)]
            DataSourceExec: partitions=1, partition_sizes=[1]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }
}
