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
//! This is stricter than AQE, which keeps a hash join when the size is
//! unknown: a pipelined job holds every hash table of the job at once, a hash
//! build cannot spill, and a failed pipelined task fails the job, so a build of
//! unknown size is treated as too large.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use datafusion::arrow::compute::SortOptions;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExprRef, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, StatisticsArgs};
use log::debug;

use crate::state::aqe::execution_plan::estimate_output_byte_size;

/// Replaces `Partitioned` hash joins whose build side may not fit in memory
/// with sort-merge joins. See the module docs.
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

            let build_partition_bytes = estimated_bytes(join.left().as_ref()).map(|bytes| {
                bytes.div_ceil(join.left().output_partitioning().partition_count().max(1))
            });
            if build_partition_bytes.is_some_and(|bytes| bytes <= max_build_bytes) {
                return Ok(Transformed::no(plan));
            }

            match to_sort_merge_join(join) {
                Ok(sort_merge_join) => {
                    debug!(
                        "pipelined join selection: demoting {:?} hash join to sort-merge join, \
                         estimated build bytes per partition={build_partition_bytes:?}, \
                         hash_join_max_build_partition_bytes={max_build_bytes}",
                        join.join_type(),
                    );
                    Ok(Transformed::yes(sort_merge_join))
                }
                // A join type the sort-merge join does not support keeps its
                // hash join.
                Err(e) => {
                    debug!(
                        "pipelined join selection: keeping {:?} hash join, no sort-merge equivalent: {e}",
                        join.join_type(),
                    );
                    Ok(Transformed::no(plan))
                }
            }
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
    let mut planning_state = state.clone();
    planning_state
        .config_mut()
        .options_mut()
        .optimizer
        .prefer_hash_join = true;
    let physical_plan = planning_state.create_physical_plan(logical_plan).await?;
    PipelinedJoinSelection::new().optimize(physical_plan, state.config().options())
}

/// Estimated output size of `plan` in bytes, or `None` if unknown.
///
/// Uses `total_byte_size` when it is known, otherwise estimates it from the
/// row count as AQE's broadcast check does. A zero is treated as unknown, since
/// statistics can report zero for inputs they did not measure.
fn estimated_bytes(plan: &dyn ExecutionPlan) -> Option<usize> {
    let stats = StatisticsContext::new()
        .compute(plan, &StatisticsArgs::new())
        .ok()?;
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
/// (already hash-partitioned) inputs, each sorted on its join keys. The hash
/// join's projection, if any, is kept as a `ProjectionExec` on top.
fn to_sort_merge_join(join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let (left_keys, right_keys): (Vec<_>, Vec<_>) = join.on().iter().cloned().unzip();
    let sort_merge_join = Arc::new(SortMergeJoinExec::try_new(
        sorted_on(join.left(), left_keys)?,
        sorted_on(join.right(), right_keys)?,
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

fn sorted_on(
    input: &Arc<dyn ExecutionPlan>,
    keys: Vec<PhysicalExprRef>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ordering = LexOrdering::new(keys.into_iter().map(PhysicalSortExpr::new_default))
        .ok_or_else(|| internal_datafusion_err!("hash join has no join keys"))?;
    Ok(Arc::new(
        SortExec::new(ordering, Arc::clone(input)).with_preserve_partitioning(true),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::config::BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES;
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::common::{ColumnStatistics, JoinType, NullEquality, Statistics};
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::test::exec::StatisticsExec;
    use datafusion::prelude::{SessionConfig, SessionContext};

    const PARTITIONS: usize = 16;

    /// A hash-partitioned input on `key` with the given size statistics.
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
        let source = Arc::new(StatisticsExec::new(stats, schema));
        let key = Arc::new(Column::new(&format!("{name}_key"), 0)) as PhysicalExprRef;
        Arc::new(
            RepartitionExec::try_new(source, Partitioning::Hash(vec![key], PARTITIONS))
                .unwrap(),
        )
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
        PipelinedJoinSelection::new()
            .optimize(plan, config.options())
            .unwrap()
    }

    fn assert_sort_merge_join_over_sorts(plan: &Arc<dyn ExecutionPlan>) {
        let smj = plan
            .downcast_ref::<SortMergeJoinExec>()
            .expect("expected a SortMergeJoinExec");
        for child in [smj.left(), smj.right()] {
            let sort = child
                .downcast_ref::<SortExec>()
                .expect("each sort-merge join input must be sorted");
            assert!(sort.preserve_partitioning());
            assert_eq!(
                sort.input().output_partitioning().partition_count(),
                PARTITIONS
            );
        }
    }

    #[test]
    fn demotes_hash_join_with_oversized_build() {
        let plan = hash_join(large_build(), PartitionMode::Partitioned, None);
        let schema = plan.schema();
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert_sort_merge_join_over_sorts(&optimized);
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
        assert_sort_merge_join_over_sorts(&optimized);
    }

    #[test]
    fn estimates_build_size_from_row_count() {
        // No byte size: 2^28 rows of two Int64 columns is 4 GiB.
        let build = input("build", Precision::Inexact(1 << 28), Precision::Absent);
        let plan = hash_join(build, PartitionMode::Partitioned, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        assert_sort_merge_join_over_sorts(&optimized);

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
        assert_sort_merge_join_over_sorts(projection.input());
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

    async fn plan_join(config: SessionConfig) -> Vec<String> {
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
            let schema =
                Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(0..1000))],
            )
            .unwrap();
            let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            ctx.register_table(name, Arc::new(table)).unwrap();
        }
        let logical_plan = ctx
            .sql("SELECT a.k FROM a JOIN b ON a.k = b.k")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let plan = create_pipelined_physical_plan(&ctx.state(), &logical_plan)
            .await
            .unwrap();
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

    #[test]
    fn leaves_collect_left_joins_alone() {
        let plan = hash_join(small_build(), PartitionMode::CollectLeft, None);
        let optimized = optimize(plan, SessionConfig::new_with_ballista());
        let join = optimized
            .downcast_ref::<HashJoinExec>()
            .expect("CollectLeft joins are left to the broadcast check");
        assert_eq!(*join.partition_mode(), PartitionMode::CollectLeft);
    }
}
