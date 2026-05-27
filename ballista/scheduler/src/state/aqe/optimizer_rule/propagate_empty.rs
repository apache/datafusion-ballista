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
use datafusion::common::JoinType;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use std::sync::Arc;

macro_rules! is_empty_exec {
    ($e:expr) => {
        $e.as_any().downcast_ref::<EmptyExec>().is_some()
    };
}

macro_rules! empty_exec {
    ($e:expr) => {
        Ok(Transformed::yes(Arc::new(
            EmptyExec::new($e.schema())
                .with_partitions($e.properties().output_partitioning().partition_count()),
        )))
    };
}

/// Holding information about the subsequent join operation
pub struct JoinInfo<'a> {
    pub join_type: JoinType,
    pub left: &'a Arc<dyn ExecutionPlan>,
    pub right: &'a Arc<dyn ExecutionPlan>,
}

/// This is [datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation] rule with difference
/// it has been implemented for physical plan
// TODO we need to decide if we want to use this rule
//      with empty exec or we should make empty exchange (to be decided)
//      if we keep it on empty exec we need additional rule
#[derive(Debug, Clone, Default)]
pub struct PropagateEmptyExecRule {}

impl PropagateEmptyExecRule {
    #[allow(deprecated)]
    fn transform(
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>()
            && is_empty_exec!(filter.input())
        {
            Ok(Transformed::yes(filter.input().clone()))
        } else if let Some(coalesce) = plan.as_any().downcast_ref::<CoalesceBatchesExec>()
            && is_empty_exec!(coalesce.input())
        {
            Ok(Transformed::yes(coalesce.input().clone()))
        } else if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>()
            && is_empty_exec!(exchange.input())
        {
            Ok(Transformed::yes(exchange.input().clone()))
        } else if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>()
            && is_empty_exec!(projection.input())
        {
            empty_exec!(projection)
        } else if let Some(limit) = plan.as_any().downcast_ref::<GlobalLimitExec>()
            && is_empty_exec!(limit.input())
        {
            Ok(Transformed::yes(limit.input().clone()))
        } else if let Some(limit) = plan.as_any().downcast_ref::<LocalLimitExec>()
            && is_empty_exec!(limit.input())
        {
            Ok(Transformed::yes(limit.input().clone()))
        } else if let Some(aggregation) = plan.as_any().downcast_ref::<AggregateExec>()
            && is_empty_exec!(aggregation.input())
        {
            empty_exec!(aggregation)
        } else if let Some(join) = as_join(&plan) {
            let left_empty = is_guaranteed_empty(join.left);
            let right_empty = is_guaranteed_empty(join.right);

            // Checking whether join would produce an empty result
            let should_eliminate = match join.join_type {
                JoinType::Inner => left_empty || right_empty,
                JoinType::Full => left_empty && right_empty,
                JoinType::Left
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark => left_empty,
                JoinType::Right
                | JoinType::LeftSemi
                | JoinType::RightAnti
                | JoinType::RightMark => right_empty,
            };

            if should_eliminate {
                Ok(Transformed::yes(Arc::new(
                    EmptyExec::new(plan.schema()).with_partitions(
                        plan.properties().output_partitioning().partition_count(),
                    ),
                )))
            } else {
                Ok(Transformed::no(plan))
            }
        } else if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>() {
            let stats = exchange.partition_statistics(None)?;
            match stats.num_rows {
                Precision::Exact(0) => empty_exec!(plan),
                _ => Ok(Transformed::no(plan)),
            }
        }
        // TODO: implement others
        else {
            Ok(Transformed::no(plan))
        }
    }
}
impl PhysicalOptimizerRule for PropagateEmptyExecRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan.transform_up(Self::transform)?.data)
    }

    fn name(&self) -> &str {
        "PropagateEmptyExecRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Extracting the physical join information from the incoming [ExecutionPlan]
pub fn as_join(plan: &Arc<dyn ExecutionPlan>) -> Option<JoinInfo<'_>> {
    let any = plan.as_any();

    if let Some(join) = any.downcast_ref::<HashJoinExec>() {
        return Some(JoinInfo {
            join_type: join.join_type,
            left: join.left(),
            right: join.right(),
        });
    }
    if let Some(join) = any.downcast_ref::<SortMergeJoinExec>() {
        return Some(JoinInfo {
            join_type: join.join_type,
            left: join.left(),
            right: join.right(),
        });
    }

    None
}

/// Getting sure that incoming plan (either from left or right) is empty
///
/// Note that when we have [Precision::Absent] it means we know nothing about the value
/// Hence we can't decide whether it is empty
pub fn is_guaranteed_empty(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let Ok(stats) = plan.partition_statistics(None) else {
        return false;
    };

    match stats.num_rows {
        Precision::Exact(n) => n == 0,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::{ColumnStatistics, JoinType, NullEquality, Statistics},
        physical_plan::{
            expressions::Column, joins::PartitionMode, test::exec::StatisticsExec,
        },
    };

    /// Helpers
    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]))
    }

    pub fn empty_stats_exec() -> Arc<dyn ExecutionPlan> {
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(0),
                total_byte_size: Precision::Exact(0),
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            Schema::new(vec![Field::new("a", DataType::Int32, true)]),
        ))
    }

    pub fn non_empty_stats_exec() -> Arc<dyn ExecutionPlan> {
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(100),
                total_byte_size: Precision::Exact(800),
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            Schema::new(vec![Field::new("a", DataType::Int32, true)]),
        ))
    }

    pub fn unknown_stats_exec() -> Arc<dyn ExecutionPlan> {
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Absent,
                total_byte_size: Precision::Absent,
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            Schema::new(vec![Field::new("a", DataType::Int32, true)]),
        ))
    }

    pub fn join_on() -> Vec<(
        Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    )> {
        vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("a", 0)))]
    }

    pub fn hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                join_on(),
                None,
                &join_type,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    pub fn sort_merge_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_options = vec![Default::default()];
        Arc::new(
            SortMergeJoinExec::try_new(
                left,
                right,
                join_on(),
                None,
                join_type,
                sort_options,
                NullEquality::NullEqualsNothing,
            )
            .unwrap(),
        )
    }

    #[test]
    fn guaranteed_empty_exact_zero() {
        assert!(is_guaranteed_empty(&empty_stats_exec()));
    }

    #[test]
    fn guaranteed_empty_non_zero() {
        assert!(!is_guaranteed_empty(&non_empty_stats_exec()));
    }

    #[test]
    fn guaranteed_empty_unknown_stats() {
        // Precision::Absent — must NOT claim empty, otherwise we would make false claims about it being empty
        assert!(!is_guaranteed_empty(&unknown_stats_exec()));
    }

    #[test]
    fn guaranteed_empty_literal_empty_exec() {
        let plan = Arc::new(EmptyExec::new(schema())) as Arc<dyn ExecutionPlan>;
        assert!(is_guaranteed_empty(&plan));
    }

    #[test]
    fn as_join_recognises_hash_join() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Inner,
        );
        let info = as_join(&plan);
        assert!(info.is_some());
        assert_eq!(info.unwrap().join_type, JoinType::Inner);
    }

    #[test]
    fn as_join_recognises_sort_merge_join() {
        let plan = sort_merge_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Left,
        );
        let info = as_join(&plan);
        assert!(info.is_some());
        assert_eq!(info.unwrap().join_type, JoinType::Left);
    }

    #[test]
    fn as_join_returns_none_for_non_join() {
        let plan = non_empty_stats_exec();
        assert!(as_join(&plan).is_none());
    }

    // Testing correctness of empty join eliminating logic
    #[test]
    fn inner_left_empty_eliminated() {
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Inner);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_some());
    }

    #[test]
    fn inner_right_empty_eliminated() {
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Inner);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_some());
    }

    #[test]
    fn inner_neither_empty_not_eliminated() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Inner,
        );
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_none());
    }

    #[test]
    fn full_both_empty_eliminated() {
        let plan = hash_join(empty_stats_exec(), empty_stats_exec(), JoinType::Full);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_some());
    }

    #[test]
    fn full_only_left_empty_not_eliminated() {
        // Full join preserves rows from both sides — one empty side is not enough
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Full);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_none());
    }

    #[test]
    fn left_join_left_empty_eliminated() {
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Left);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_some());
    }

    #[test]
    fn left_join_right_empty_not_eliminated() {
        // Left join preserves all left rows even when right is empty
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Left);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_none());
    }

    #[test]
    fn right_join_right_empty_eliminated() {
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Right);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_some());
    }

    #[test]
    fn smj_inner_left_empty_eliminated() {
        let plan =
            sort_merge_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Inner);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_some());
    }

    #[test]
    fn smj_left_join_right_empty_not_eliminated() {
        let plan =
            sort_merge_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Left);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_none());
    }

    #[test]
    fn smj_full_only_one_side_empty_not_eliminated() {
        let plan =
            sort_merge_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Full);
        let result = PropagateEmptyExecRule::transform(plan).unwrap();
        assert!(result.data.as_any().downcast_ref::<EmptyExec>().is_none());
    }
}
