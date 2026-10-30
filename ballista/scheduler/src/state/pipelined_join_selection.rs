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

//! Size-aware join selection for pipelined jobs.
//!
//! A pipelined job is planned with the static planner, so AQE never chooses
//! its join strategies. The scheduler plans it with hash joins, and
//! [`PipelinedJoinSelection`] then turns every `Partitioned` hash join whose
//! build side is not known to fit
//! `ballista.optimizer.hash_join_max_build_partition_bytes` per partition back
//! into a `SortMergeJoinExec`, which can spill. Small build sides stay hash
//! joins, and the distributed planner promotes those under the broadcast
//! threshold to `CollectLeft` as usual.
//!
//! The rule runs among the physical optimizer rules, right after
//! DataFusion's `JoinSelection`, so that the later rules sort and partition
//! the inputs of the sort-merge joins it creates. Rewriting a finished plan
//! instead would lose orderings that a parent relied on: a hash join keeps
//! its probe side's order, so the planner may have dropped a sort above it.
//!
//! This is stricter than AQE, which keeps a hash join when the size is
//! unknown: a pipelined job holds every hash table of the job at once, a hash
//! build cannot spill, and a failed pipelined task fails the job, so a build of
//! unknown size is treated as too large.
//!
//! The limit is per join and per partition. It does not bound the total of all
//! the hash tables a pipelined job holds at once, so a job with many joins just
//! under the limit can still need more memory than the limit suggests.
//!
//! Joins that are `CollectLeft` when this rule runs are not checked. The
//! distributed planner can later turn such a join back into a `Partitioned`
//! one, when its join type is unsafe to broadcast or its build side is over
//! `ballista.optimizer.broadcast_join_threshold_bytes`. Those build sides are
//! still under DataFusion's `hash_join_single_partition_threshold`, so they
//! are small unless that threshold has been raised well above its default.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use datafusion::arrow::compute::SortOptions;
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, Partitioning, StatisticsArgs,
};
use log::{debug, warn};

use crate::state::aqe::execution_plan::estimate_output_byte_size;

/// Replaces `Partitioned` hash joins whose build side may not fit in memory
/// with sort-merge joins. It must run before the rules that enforce
/// distribution and sorting; see the module docs.
#[derive(Debug, Default)]
pub struct PipelinedJoinSelection;

impl PipelinedJoinSelection {
    /// Creates a new `PipelinedJoinSelection` rule.
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for PipelinedJoinSelection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let max_build_bytes = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default()
            .hash_join_max_build_partition_bytes();
        // 0 disables the check, as it does under AQE.
        if max_build_bytes == 0 {
            return Ok(plan);
        }

        plan.transform_up(|plan| {
            let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };
            // `CollectLeft` joins are left to the distributed planner's
            // broadcast check, and a null-aware anti join has no sort-merge
            // equivalent.
            if *join.partition_mode() != PartitionMode::Partitioned || join.null_aware {
                return Ok(Transformed::no(plan));
            }

            let partitions =
                join_input_partitions(join, config.execution.target_partitions);
            let build_partition_bytes = estimated_bytes(join.left().as_ref())
                .map(|bytes| bytes.div_ceil(partitions));
            if build_partition_bytes.is_some_and(|bytes| bytes <= max_build_bytes) {
                return Ok(Transformed::no(plan));
            }

            let sort_merge_join = to_sort_merge_join(join)?;
            debug!(
                "pipelined join selection: demoting {:?} hash join to sort-merge join, \
                 estimated build bytes per partition={build_partition_bytes:?}, \
                 hash_join_max_build_partition_bytes={max_build_bytes}",
                join.join_type(),
            );
            Ok(Transformed::yes(sort_merge_join))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "PipelinedJoinSelection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Plans a pipelined job's logical plan with size-aware join selection.
///
/// Without AQE there are no measured sizes to pick join strategies from, so
/// every join is planned as a hash join, as AQE does whatever
/// `datafusion.optimizer.prefer_hash_join` says, and [`PipelinedJoinSelection`]
/// falls back to a sort-merge join wherever the build side may not fit.
pub(crate) async fn create_pipelined_physical_plan(
    state: &SessionState,
    logical_plan: &LogicalPlan,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut rules = state.physical_optimizers().to_vec();
    // Without `JoinSelection` the partition modes stay `Auto`, so the rule
    // would find no `Partitioned` join to check. Forcing hash joins would then
    // drop the session's own `prefer_hash_join` choice for nothing, so plan the
    // job as the static planner does.
    let Some(index) = rules
        .iter()
        .position(|rule| rule.name() == "join_selection")
    else {
        warn!(
            "pipelined join selection: no join_selection rule in the session, \
             so the job is planned without size-aware join selection"
        );
        return state.create_physical_plan(logical_plan).await;
    };
    rules.insert(index + 1, Arc::new(PipelinedJoinSelection::new()));
    let mut planning_state = SessionStateBuilder::new_from_existing(state.clone())
        .with_physical_optimizer_rules(rules)
        .build();
    planning_state
        .config_mut()
        .options_mut()
        .optimizer
        .prefer_hash_join = true;
    planning_state.create_physical_plan(logical_plan).await
}

/// The number of partitions a `Partitioned` join will run with.
///
/// The inputs are not repartitioned yet, and `EnforceDistribution` will
/// repartition them into `target_partitions` partitions. An input that is
/// already hash partitioned may be kept as it is, with its own partition
/// count, so the smallest such count is used when it is lower.
fn join_input_partitions(join: &HashJoinExec, target_partitions: usize) -> usize {
    [join.left(), join.right()]
        .into_iter()
        .filter_map(|input| match input.output_partitioning() {
            Partitioning::Hash(_, partitions) => Some(*partitions),
            _ => None,
        })
        .fold(target_partitions, usize::min)
        .max(1)
}

/// Estimated output size of `plan` in bytes, or `None` if unknown.
///
/// Uses `total_byte_size` when it is known, otherwise estimates it from the
/// row count as AQE's broadcast check does. An inexact zero is treated as
/// unknown, since statistics can report zero for inputs they did not measure.
/// An exact zero is a known empty input.
fn estimated_bytes(plan: &dyn ExecutionPlan) -> Option<usize> {
    let stats = StatisticsContext::new()
        .compute(plan, &StatisticsArgs::new())
        .ok()?;
    if stats.num_rows == Precision::Exact(0)
        || stats.total_byte_size == Precision::Exact(0)
    {
        return Some(0);
    }
    if let Some(&bytes) = stats.total_byte_size.get_value()
        && bytes != 0
    {
        return Some(bytes);
    }
    let num_rows = *stats.num_rows.get_value()?;
    if num_rows == 0 {
        return None;
    }
    estimate_output_byte_size(&plan.schema(), num_rows, &stats.column_statistics)
}

/// The sort-merge join equivalent of a `Partitioned` hash join, over the same
/// inputs. The later optimizer rules sort and partition them on the join keys.
/// The hash join's projection, if any, is kept as a `ProjectionExec` on top.
fn to_sort_merge_join(join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let sort_merge_join = Arc::new(SortMergeJoinExec::try_new(
        Arc::clone(join.left()),
        Arc::clone(join.right()),
        join.on().to_vec(),
        join.filter().cloned(),
        *join.join_type(),
        vec![SortOptions::default(); join.on().len()],
        join.null_equality,
    )?) as Arc<dyn ExecutionPlan>;

    let Some(projection) = join.projection.as_deref() else {
        return Ok(sort_merge_join);
    };
    let schema = sort_merge_join.schema();
    let exprs = projection.iter().map(|&index| {
        let name = schema.field(index).name();
        (
            Arc::new(Column::new(name, index)) as PhysicalExprRef,
            name.clone(),
        )
    });
    Ok(Arc::new(ProjectionExec::try_new(exprs, sort_merge_join)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::config::BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES;
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::common::{ColumnStatistics, JoinType, NullEquality, Statistics};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::test::exec::StatisticsExec;
    use datafusion::prelude::{SessionConfig, SessionContext, col};

    const PARTITIONS: usize = 16;

    /// An input with the given size statistics. The rule runs before the
    /// inputs are repartitioned, so it sees them bare.
    fn input(
        name: &str,
        num_rows: Precision<usize>,
        bytes: Precision<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(format!("{name}_key"), DataType::Int64, false),
            Field::new(format!("{name}_value"), DataType::Int64, false),
        ]);
        let stats = Statistics {
            num_rows,
            total_byte_size: bytes,
            column_statistics: vec![ColumnStatistics::new_unknown(); 2],
        };
        Arc::new(StatisticsExec::new(stats, schema))
    }

    fn hash_join(
        build: Arc<dyn ExecutionPlan>,
        mode: PartitionMode,
        projection: Option<Vec<usize>>,
    ) -> Arc<dyn ExecutionPlan> {
        let probe = input(
            "probe",
            Precision::Exact(1 << 30),
            Precision::Exact(16 << 30),
        );
        let on = vec![(
            Arc::new(Column::new("build_key", 0)) as PhysicalExprRef,
            Arc::new(Column::new("probe_key", 0)) as PhysicalExprRef,
        )];
        Arc::new(
            HashJoinExec::try_new(
                build,
                probe,
                on,
                None,
                &JoinType::Inner,
                projection,
                mode,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    /// 4 GiB of build side: 256 MiB per partition, over the 64 MiB default.
    fn large_build() -> Arc<dyn ExecutionPlan> {
        input(
            "build",
            Precision::Exact(1 << 28),
            Precision::Exact(4 << 30),
        )
    }

    /// 160 MiB of build side: 10 MiB per partition.
    fn small_build() -> Arc<dyn ExecutionPlan> {
        input(
            "build",
            Precision::Exact(10 << 20),
            Precision::Exact(160 << 20),
        )
    }

    fn optimize(
        plan: Arc<dyn ExecutionPlan>,
        config: SessionConfig,
    ) -> Arc<dyn ExecutionPlan> {
        let config = config.with_target_partitions(PARTITIONS);
        PipelinedJoinSelection::new()
            .optimize(plan, config.options())
            .unwrap()
    }

    /// The later optimizer rules sort the inputs, so the rule leaves them as
    /// they are.
    fn assert_sort_merge_join_over_inputs(plan: &Arc<dyn ExecutionPlan>) {
        let smj = plan
            .downcast_ref::<SortMergeJoinExec>()
            .expect("expected a SortMergeJoinExec");
        for child in [smj.left(), smj.right()] {
            assert!(child.is::<StatisticsExec>());
        }
    }

    #[test]
    fn demotes_hash_join_with_oversized_build() {
        let plan = hash_join(large_build(), PartitionMode::Partitioned, None);
        let schema = plan.schema();
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert_sort_merge_join_over_inputs(&optimized);
        assert_eq!(optimized.schema(), schema);
    }

    #[test]
    fn keeps_hash_join_with_small_build() {
        let plan = hash_join(small_build(), PartitionMode::Partitioned, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        let join = optimized
            .downcast_ref::<HashJoinExec>()
            .expect("a small build side keeps its hash join");
        assert_eq!(*join.partition_mode(), PartitionMode::Partitioned);
    }

    #[test]
    fn demotes_hash_join_with_unknown_build_size() {
        let build = input("build", Precision::Absent, Precision::Absent);
        let plan = hash_join(build, PartitionMode::Partitioned, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert_sort_merge_join_over_inputs(&optimized);
    }

    #[test]
    fn keeps_hash_join_with_known_empty_build() {
        let build = input("build", Precision::Exact(0), Precision::Exact(0));
        let plan = hash_join(build, PartitionMode::Partitioned, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert!(optimized.downcast_ref::<HashJoinExec>().is_some());

        // An inexact zero may be an input that was not measured.
        let build = input("build", Precision::Inexact(0), Precision::Inexact(0));
        let plan = hash_join(build, PartitionMode::Partitioned, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert!(optimized.is::<SortMergeJoinExec>());
    }

    #[test]
    fn uses_the_partition_count_of_hash_partitioned_inputs() {
        // 160 MiB is 10 MiB per partition over 16 partitions, but an input
        // already hash partitioned into 1 partition may be kept that way.
        let build = Arc::new(
            RepartitionExec::try_new(
                small_build(),
                Partitioning::Hash(vec![Arc::new(Column::new("build_key", 0))], 1),
            )
            .unwrap(),
        );
        let plan = hash_join(build, PartitionMode::Partitioned, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert!(optimized.is::<SortMergeJoinExec>());
    }

    #[test]
    fn estimates_build_size_from_row_count() {
        // No byte size: 2^28 rows of two Int64 columns is 4 GiB.
        let build = input("build", Precision::Inexact(1 << 28), Precision::Absent);
        let plan = hash_join(build, PartitionMode::Partitioned, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert_sort_merge_join_over_inputs(&optimized);

        // 2^20 rows is 16 MiB, 1 MiB per partition.
        let build = input("build", Precision::Inexact(1 << 20), Precision::Absent);
        let plan = hash_join(build, PartitionMode::Partitioned, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert!(optimized.downcast_ref::<HashJoinExec>().is_some());
    }

    #[test]
    fn keeps_projection_when_demoting() {
        let plan = hash_join(large_build(), PartitionMode::Partitioned, Some(vec![3, 0]));
        let schema = plan.schema();
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        let projection = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("the hash join's projection must be kept");
        assert_sort_merge_join_over_inputs(projection.input());
        assert_eq!(optimized.schema(), schema);
    }

    #[test]
    fn zero_limit_disables_the_check() {
        let plan = hash_join(large_build(), PartitionMode::Partitioned, None);
        let config = SessionConfig::new_with_ballista()
            .set_usize(BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, 0);
        let optimized = optimize(plan, config);
        assert!(optimized.downcast_ref::<HashJoinExec>().is_some());
    }

    /// Names of the join operators in `plan`, top-down.
    fn join_operators(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let mut joins = vec![];
        plan.apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                joins.push(format!("HashJoinExec({:?})", join.partition_mode()));
            } else if node.is::<SortMergeJoinExec>() {
                joins.push("SortMergeJoinExec".to_string());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        joins
    }

    /// Plans `sql` over tables `a` and `b`, each with columns `k` (0..1000) and
    /// `v` (1000 down to 1), declared sorted on `v`.
    async fn plan_pipelined(
        config: SessionConfig,
        sql: &str,
    ) -> (SessionContext, Arc<dyn ExecutionPlan>) {
        let config = config
            .with_target_partitions(4)
            .set_bool("datafusion.optimizer.prefer_hash_join", false)
            // Keep the join partitioned, so that broadcast selection does not
            // decide it.
            .set_usize(
                "datafusion.optimizer.hash_join_single_partition_threshold",
                0,
            )
            .set_usize(
                "datafusion.optimizer.hash_join_single_partition_threshold_rows",
                0,
            );
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        for name in ["a", "b"] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..1000)),
                    Arc::new(Int64Array::from_iter_values((1..=1000).rev())),
                ],
            )
            .unwrap();
            let table = MemTable::try_new(schema, vec![vec![batch]])
                .unwrap()
                .with_sort_order(vec![vec![col("v").sort(true, false)]]);
            ctx.register_table(name, Arc::new(table)).unwrap();
        }
        let logical_plan = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
        let plan = create_pipelined_physical_plan(&ctx.state(), &logical_plan)
            .await
            .unwrap();
        (ctx, plan)
    }

    async fn plan_join(config: SessionConfig) -> Vec<String> {
        let (_, plan) =
            plan_pipelined(config, "SELECT a.k FROM a JOIN b ON a.k = b.k").await;
        join_operators(&plan)
    }

    #[tokio::test]
    async fn plans_small_joins_as_hash_joins_whatever_prefer_hash_join_says() {
        assert_eq!(
            plan_join(SessionConfig::new_with_ballista()).await,
            vec!["HashJoinExec(Partitioned)"]
        );
    }

    #[tokio::test]
    async fn plans_joins_over_the_build_limit_as_sort_merge_joins() {
        let config = SessionConfig::new_with_ballista()
            .set_usize(BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, 1);
        assert_eq!(plan_join(config).await, vec!["SortMergeJoinExec"]);
    }

    // A hash join keeps its probe side's order, so planning with hash joins
    // can drop the sort for an `ORDER BY` on probe columns. Demoting the join
    // must not lose that order.
    #[tokio::test]
    async fn demoting_keeps_the_order_a_parent_relies_on() {
        let config = SessionConfig::new_with_ballista()
            .set_usize(BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, 1);
        let (ctx, plan) =
            plan_pipelined(config, "SELECT b.v FROM a JOIN b ON a.k = b.k ORDER BY b.v")
                .await;
        assert_eq!(join_operators(&plan), vec!["SortMergeJoinExec"]);

        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, (1..=1000).collect::<Vec<_>>());
    }

    /// Runs `plan` and returns its rows as sorted, formatted lines.
    async fn sorted_rows(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Vec<String> {
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let mut rows: Vec<String> = pretty_format_batches(&batches)
            .unwrap()
            .to_string()
            .lines()
            .map(str::to_string)
            .collect();
        rows.sort();
        rows
    }

    // The sort-merge join applies a join filter differently for outer joins,
    // so a demoted outer join with a filter must return what the hash join
    // returns.
    #[tokio::test]
    async fn demoted_outer_joins_with_filters_match_hash_joins() {
        for join in ["LEFT JOIN", "FULL JOIN"] {
            let sql = format!(
                "SELECT a.k, a.v, b.k, b.v FROM a {join} b ON a.k = b.k AND b.v > 500"
            );
            let hash_config = SessionConfig::new_with_ballista()
                .set_usize(BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, 0);
            let (ctx, hash_plan) = plan_pipelined(hash_config, &sql).await;
            assert_eq!(
                join_operators(&hash_plan),
                vec!["HashJoinExec(Partitioned)"]
            );
            let expected = sorted_rows(&ctx, hash_plan).await;

            let demoted_config = SessionConfig::new_with_ballista()
                .set_usize(BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, 1);
            let (ctx, demoted_plan) = plan_pipelined(demoted_config, &sql).await;
            assert_eq!(join_operators(&demoted_plan), vec!["SortMergeJoinExec"]);
            assert_eq!(sorted_rows(&ctx, demoted_plan).await, expected, "{join}");
        }
    }

    #[test]
    fn leaves_collect_left_joins_alone() {
        // A build side this large would be demoted if it were `Partitioned`.
        let plan = hash_join(large_build(), PartitionMode::CollectLeft, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        let join = optimized
            .downcast_ref::<HashJoinExec>()
            .expect("CollectLeft joins are left to the broadcast check");
        assert_eq!(*join.partition_mode(), PartitionMode::CollectLeft);
    }
}
