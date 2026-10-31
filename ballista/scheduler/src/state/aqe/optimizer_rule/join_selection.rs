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
use datafusion::physical_plan::StatisticsArgs;
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::{
    catalog::memory::DataSourceExec,
    common::tree_node::{Transformed, TreeNode},
    error::DataFusionError,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ChildrenPropertiesMode, ExecutionPlan, Partitioning, ReplaceChildrenOptions,
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
        let left_stats =
            StatisticsContext::new().compute(left, &StatisticsArgs::new())?;
        let right_stats =
            StatisticsContext::new().compute(right, &StatisticsArgs::new())?;
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

/// The shuffle partitioning a join's `(left, right)` inputs need to be
/// co-partitioned on its keys.
fn join_key_partitioning(
    join: &DynamicJoinSelectionExec,
    partition_count: usize,
) -> (Partitioning, Partitioning) {
    let mut required = join
        ._required_input_distribution()
        .into_iter()
        .map(|d| d.create_partitioning(partition_count));
    let left = required.next().expect("a join requires two distributions");
    let right = required.next().expect("a join requires two distributions");
    (left, right)
}

impl SelectJoinRule {
    /// Wraps `child` in an `ExchangeExec` shuffling on `partitioning`, unless it
    /// already is one that does.
    ///
    /// `StageBuildSide` shuffles the build side on the join key ahead of the
    /// decision, so when the measured size then turns out to be too large to
    /// broadcast and the join falls back to `Repartition`, that side is already
    /// where it needs to be. Wrapping it again would nest one stage boundary
    /// inside another and move the same rows twice.
    ///
    /// The match is structural equality, deliberately narrower than
    /// [`DynamicJoinSelectionExec::inputs_already_partitioned`], which asks the
    /// same question through the equivalence-aware `Partitioning::satisfaction`.
    /// The only exchange worth reusing here is one this rule itself just placed
    /// on these join keys, so the plainer comparison is the honest test and the
    /// two should not be unified.
    fn ensure_exchange(
        &self,
        child: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Arc<dyn ExecutionPlan> {
        if let Some(exchange) = child.downcast_ref::<ExchangeExec>()
            && exchange.partitioning.as_ref() == Some(&partitioning)
        {
            return child;
        }
        Arc::new(ExchangeExec::new(child, Some(partitioning), self.plan_id()))
    }
}

/// Whether giving the build side its own broadcast stage is worth it.
fn broadcast_build_side_pays_off(
    build: &dyn ExecutionPlan,
    probe: &dyn ExecutionPlan,
) -> bool {
    build.properties().partitioning.partition_count() > 1
        && probe.properties().partitioning.partition_count() > 1
}

/// Rebuilds `plan` on top of new `left` and `right` children, recomputing
/// its properties.
fn with_children(
    plan: Arc<dyn ExecutionPlan>,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    plan.replace_children(
        vec![left, right],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

impl SelectJoinRule {
    /// Whether `join` should have its inputs swapped so that the smaller side
    /// builds. Null-aware joins are never swapped.
    fn should_swap(join: &HashJoinExec) -> datafusion::common::Result<bool> {
        Ok(!join.null_aware
            && Self::supports_swap_join_order(join.left.as_ref(), join.right.as_ref())?)
    }

    /// Converts `plan`, which has to be an `ExchangeExec`, into a broadcast
    /// exchange. `side` only names the join input in the error message.
    fn broadcast_exchange(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        side: &str,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let exchange = plan.downcast_ref::<ExchangeExec>().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ExchangeExec is expected at this point ({side})"
            ))
        })?;
        Ok(Arc::new(exchange.to_broadcast(self.plan_id())))
    }

    /// Picks the join for a resolved `DynamicJoinSelectionExec` (`node`).
    /// Any other node, or a dynamic join whose inputs are not resolved yet, is
    /// left untouched.
    fn select_join(
        &self,
        node: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(dynamic_join) = node.downcast_ref::<DynamicJoinSelectionExec>() else {
            return Ok(Transformed::no(node));
        };
        if !dynamic_join.upstream_resolved() {
            return Ok(Transformed::no(node));
        }

        let plan = match dynamic_join.to_actual_join(config)? {
            JoinSelectionAction::LateCollectLeft(hash_join_exec) => {
                self.late_collect_left(hash_join_exec)?
            }
            JoinSelectionAction::CollectLeft(hash_join_exec) => {
                self.collect_left(hash_join_exec, config)?
            }
            JoinSelectionAction::StageBuildSide { build_is_right } => {
                self.stage_build_side(&node, dynamic_join, build_is_right, config)?
            }
            JoinSelectionAction::Repartition(dynamic_join) => {
                self.repartition(dynamic_join, config)?
            }
            JoinSelectionAction::Hash(hash_join_exec) => Self::hash(hash_join_exec)?,
            JoinSelectionAction::Sort(sort_merge_join_exec) => sort_merge_join_exec,
        };
        Ok(Transformed::yes(plan))
    }

    /// Broadcasts the build side of a join whose inputs are both exchanges
    /// already (added beforehand), swapping the inputs first when the right
    /// side is the smaller one.
    fn late_collect_left(
        &self,
        hash_join_exec: Arc<HashJoinExec>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if Self::should_swap(&hash_join_exec)? {
            let left = hash_join_exec.left.clone();
            let right = self.broadcast_exchange(&hash_join_exec.right, "right")?;
            let join = with_children(hash_join_exec, left, right)?;
            let join =
                join.downcast_ref::<HashJoinExec>()
                    .ok_or(DataFusionError::Execution(
                        "HashJoinExec is expected at this point (right)".to_owned(),
                    ))?;
            join.swap_inputs(PartitionMode::CollectLeft)
        } else {
            let left = self.broadcast_exchange(&hash_join_exec.left, "left")?;
            let right = hash_join_exec.right.clone();
            with_children(hash_join_exec, left, right)
        }
    }

    /// Plans a `CollectLeft` join, swapping the inputs when the right side is
    /// the smaller one, then decides how each `CollectLeft` build side in the
    /// result is collected.
    fn collect_left(
        &self,
        hash_join_exec: Arc<HashJoinExec>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> = if Self::should_swap(&hash_join_exec)? {
            hash_join_exec.swap_inputs(PartitionMode::CollectLeft)?
        } else {
            hash_join_exec
        };

        Ok(plan
            .transform_up(|p| self.collect_build_side(p, config))?
            .data)
    }

    /// For a `CollectLeft` hash join, reads a data source build side as a
    /// single partition, or otherwise gives the build side its own broadcast
    /// stage when that pays off. Any other node is left untouched.
    fn collect_build_side(
        &self,
        p: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(hash_join) = p.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(p));
        };
        if !matches!(hash_join.partition_mode(), PartitionMode::CollectLeft) {
            return Ok(Transformed::no(p));
        }

        if let Some(data_source) = hash_join.left.downcast_ref::<DataSourceExec>()
            && let Ok(Some(left)) = data_source.repartitioned(1, config)
        {
            let right = hash_join.right.clone();
            return Ok(Transformed::yes(with_children(p, left, right)?));
        }

        // Broadcasting the build side buys nothing when the probe has a single
        // partition.
        if !broadcast_build_side_pays_off(
            hash_join.left.as_ref(),
            hash_join.right.as_ref(),
        ) {
            return Ok(Transformed::no(p));
        }

        let left: Arc<dyn ExecutionPlan> =
            if let Some(exchange) = hash_join.left.downcast_ref::<ExchangeExec>() {
                Arc::new(exchange.to_broadcast(self.plan_id()))
            } else {
                Arc::new(ExchangeExec::new_broadcast(
                    hash_join.left.clone(),
                    None,
                    self.plan_id(),
                ))
            };
        let right = hash_join.right.clone();
        Ok(Transformed::yes(with_children(p, left, right)?))
    }

    /// Puts an exchange on the build side only. The probe side is left as it
    /// is, so no stage is created for it and nothing of it is shuffled until
    /// the join is decided for real.
    fn stage_build_side(
        &self,
        node: &Arc<dyn ExecutionPlan>,
        dynamic_join: &DynamicJoinSelectionExec,
        build_is_right: bool,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (left_key, right_key) =
            join_key_partitioning(dynamic_join, config.execution.target_partitions);

        let (left, right): (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) =
            if build_is_right {
                (
                    dynamic_join.left.clone(),
                    Arc::new(ExchangeExec::new(
                        dynamic_join.right.clone(),
                        Some(right_key),
                        self.plan_id(),
                    )),
                )
            } else {
                (
                    Arc::new(ExchangeExec::new(
                        dynamic_join.left.clone(),
                        Some(left_key),
                        self.plan_id(),
                    )),
                    dynamic_join.right.clone(),
                )
            };

        with_children(Arc::clone(node), left, right)
    }

    /// Co-partitions both inputs of the join on its keys.
    fn repartition(
        &self,
        dynamic_join: Arc<DynamicJoinSelectionExec>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (left_key, right_key) =
            join_key_partitioning(&dynamic_join, config.execution.target_partitions);

        let left = self.ensure_exchange(dynamic_join.left.clone(), left_key);
        let right = self.ensure_exchange(dynamic_join.right.clone(), right_key);

        with_children(dynamic_join, left, right)
    }

    /// Plans a partitioned hash join, swapping the inputs when the right side
    /// is the smaller one.
    fn hash(
        hash_join_exec: Arc<HashJoinExec>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if Self::should_swap(&hash_join_exec)? {
            hash_join_exec.swap_inputs(*hash_join_exec.partition_mode())
        } else {
            Ok(hash_join_exec)
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
        if !bc.adaptive_join_enabled() {
            // disabled using configuration
            return Ok(plan);
        }

        Ok(plan
            .transform_up(|node| self.select_join(node, config))?
            .data)
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

    use ballista_core::assert_plan;
    use ballista_core::config::BallistaConfig;
    use datafusion::physical_plan::empty::EmptyExec;
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

    fn key_partitioning(name: &str, schema: &Schema) -> Partitioning {
        Partitioning::Hash(
            vec![Arc::new(
                datafusion::physical_plan::expressions::Column::new_with_schema(
                    name, schema,
                )
                .unwrap(),
            )],
            4,
        )
    }

    // A build side that `StageBuildSide` already shuffled on the join key must
    // not be shuffled again when the join later falls back to `Repartition`.
    // Wrapping it a second time would nest one stage boundary inside another
    // and move the same rows twice.
    #[test]
    fn exchange_on_reuses_a_matching_exchange() {
        let schema = Schema::new(vec![Field::new("k", DataType::Int32, false)]);
        let partitioning = key_partitioning("k", &schema);
        let staged: Arc<dyn ExecutionPlan> = Arc::new(ExchangeExec::new(
            Arc::new(EmptyExec::new(Arc::new(schema))),
            Some(partitioning.clone()),
            0,
        ));

        let reused =
            SelectJoinRule::default().ensure_exchange(Arc::clone(&staged), partitioning);

        assert!(Arc::ptr_eq(&staged, &reused));
    }

    // An exchange on a different key is not reusable, so the join key's own
    // exchange still has to be added on top.
    #[test]
    fn exchange_on_wraps_an_exchange_on_a_different_key() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("other", DataType::Int32, false),
        ]);
        let staged: Arc<dyn ExecutionPlan> = Arc::new(ExchangeExec::new(
            Arc::new(EmptyExec::new(Arc::new(schema.clone()))),
            Some(key_partitioning("other", &schema)),
            0,
        ));

        let wrapped = SelectJoinRule::default()
            .ensure_exchange(staged, key_partitioning("k", &schema));
        let outer = wrapped.downcast_ref::<ExchangeExec>().unwrap();

        assert!(outer.input().is::<ExchangeExec>());
    }

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
        // Pinned: the staged exchange is hash-partitioned to `target_partitions`,
        // whose default follows the machine's core count.
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .set_bool("datafusion.optimizer.prefer_hash_join", false);

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

        assert_plan!(resolved.as_ref(), @ "
        ProjectionExec: expr=[id@0 as id]
          DynamicJoinSelectionExec: plan_id=1, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
            ExchangeExec: partitioning=Hash([id@0], 4), plan_id=0, stage_id=pending, stage_resolved=false
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

    #[test]
    fn null_aware_anti_join_is_not_swapped_by_aqe() {
        use datafusion::physical_expr::expressions::Column;
        use datafusion::{
            arrow::datatypes::{DataType, Field, Schema},
            common::{
                ColumnStatistics, JoinType, NullEquality, Statistics, stats::Precision,
            },
            physical_plan::{
                Partitioning,
                joins::{HashJoinExec, PartitionMode},
                repartition::RepartitionExec,
                test::exec::StatisticsExec,
            },
        };

        fn stats_exec(name: &str, bytes: usize) -> Arc<dyn ExecutionPlan> {
            Arc::new(StatisticsExec::new(
                Statistics {
                    num_rows: Precision::Inexact(bytes / 4),
                    total_byte_size: Precision::Inexact(bytes),
                    column_statistics: vec![ColumnStatistics::new_unknown()],
                },
                Schema::new(vec![Field::new(name, DataType::Int32, true)]),
            ))
        }

        // A normal join would swap these inputs to build from the smaller right
        // side. Doing that to a null-aware LeftAnti creates an invalid
        // null-aware RightAnti join.
        let left = stats_exec("big_key", 1024 * 1024);
        let right = Arc::new(
            RepartitionExec::try_new(
                stats_exec("small_key", 1024),
                Partitioning::RoundRobinBatch(4),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let join = HashJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            vec![(
                Arc::new(Column::new("big_key", 0)) as _,
                Arc::new(Column::new("small_key", 0)) as _,
            )],
            None,
            &JoinType::LeftAnti,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            true,
        )
        .unwrap();
        let dynamic = DynamicJoinSelectionExec::from_hash_join(&join, 0).unwrap()
            as Arc<dyn ExecutionPlan>;

        let resolved = SelectJoinRule::default()
            .optimize(dynamic, &ConfigOptions::default())
            .expect("AQE must not swap a null-aware LeftAnti join");
        let hash_join = resolved
            .downcast_ref::<HashJoinExec>()
            .expect("AQE should resolve to HashJoinExec");

        assert_eq!(*hash_join.join_type(), JoinType::LeftAnti);
        assert_eq!(*hash_join.partition_mode(), PartitionMode::CollectLeft);
        assert!(hash_join.null_aware);
        assert_eq!(hash_join.left().schema().field(0).name(), "big_key");
        assert_eq!(hash_join.right().schema().field(0).name(), "small_key");
    }

    #[test]
    fn null_aware_anti_join_rejects_known_oversized_build_side_in_aqe() {
        use datafusion::physical_expr::expressions::Column;
        use datafusion::{
            arrow::datatypes::{DataType, Field, Schema},
            common::{
                ColumnStatistics, JoinType, NullEquality, Statistics, stats::Precision,
            },
            physical_plan::{
                joins::{HashJoinExec, PartitionMode},
                test::exec::StatisticsExec,
            },
        };

        fn stats_exec(name: &str, bytes: usize) -> Arc<dyn ExecutionPlan> {
            Arc::new(StatisticsExec::new(
                Statistics {
                    num_rows: Precision::Inexact(bytes / 4),
                    total_byte_size: Precision::Inexact(bytes),
                    column_statistics: vec![ColumnStatistics::new_unknown()],
                },
                Schema::new(vec![Field::new(name, DataType::Int32, true)]),
            ))
        }

        // Over `broadcast_join_threshold_bytes`, whose default is 128 MB.
        let join = HashJoinExec::try_new(
            stats_exec("big_key", 256 * 1024 * 1024),
            stats_exec("small_key", 1024),
            vec![(
                Arc::new(Column::new("big_key", 0)) as _,
                Arc::new(Column::new("small_key", 0)) as _,
            )],
            None,
            &JoinType::LeftAnti,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            true,
        )
        .unwrap();
        let dynamic = DynamicJoinSelectionExec::from_hash_join(&join, 0).unwrap()
            as Arc<dyn ExecutionPlan>;

        let error = SelectJoinRule::default()
            .optimize(dynamic, &ConfigOptions::default())
            .unwrap_err();
        assert!(
            error.to_string().contains(
                "build side does not fit ballista.optimizer.broadcast_join_threshold_bytes"
            ),
            "{error}"
        );
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
    #[test]
    fn broadcast_pays_off_only_when_both_sides_are_partitioned() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let one = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let many = Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(4))
            as Arc<dyn ExecutionPlan>;

        assert!(
            broadcast_build_side_pays_off(many.as_ref(), many.as_ref()),
            "both sides partitioned: the build is worth its own stage"
        );
        assert!(
            !broadcast_build_side_pays_off(many.as_ref(), one.as_ref()),
            "single-partition probe pins the join to one task either way"
        );
        assert!(
            !broadcast_build_side_pays_off(one.as_ref(), many.as_ref()),
            "single-partition build needs no stage of its own"
        );
    }
}
