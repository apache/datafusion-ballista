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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::JoinType;
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
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
    pub schema: SchemaRef,
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
        } else if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>()
            && is_empty_exec!(repartition.input())
        {
            empty_exec!(repartition)
        } else if let Some(coalesce_partition) =
            plan.as_any().downcast_ref::<CoalescePartitionsExec>()
            && is_empty_exec!(coalesce_partition.input())
        {
            empty_exec!(coalesce_partition)
        } else if let Some(sort_preserving_merge) =
            plan.as_any().downcast_ref::<SortPreservingMergeExec>()
            && is_empty_exec!(sort_preserving_merge.input())
        {
            empty_exec!(sort_preserving_merge)
        } else if let Some(join) = as_join(&plan) {
            let left_empty = is_guaranteed_empty(join.left);
            let right_empty = is_guaranteed_empty(join.right);

            let left_field_count = join.left.schema().fields.len();

            // Checking whether join would produce an empty result
            match join.join_type {
                JoinType::Inner if left_empty || right_empty => empty_exec!(plan),
                JoinType::Left if left_empty => empty_exec!(plan),
                // Left Join with empty right: all left rows survive
                // with NULLs for right columns.
                JoinType::Left if right_empty => {
                    Ok(Transformed::yes(build_null_padded_projection(
                        Arc::clone(&join.left),
                        join.schema,
                        left_field_count,
                        true,
                    )?))
                }
                JoinType::Right if right_empty => empty_exec!(plan),
                // Right Join with empty left: all right rows survive
                // with NULLs for left columns.
                JoinType::Right if left_empty => {
                    Ok(Transformed::yes(build_null_padded_projection(
                        Arc::clone(&join.right),
                        join.schema,
                        left_field_count,
                        false,
                    )?))
                }
                JoinType::LeftSemi if left_empty || right_empty => empty_exec!(plan),
                JoinType::RightSemi if left_empty || right_empty => empty_exec!(plan),
                JoinType::LeftAnti if left_empty => empty_exec!(plan),
                JoinType::LeftAnti if right_empty => {
                    Ok(Transformed::yes((*join.left).clone()))
                }
                JoinType::RightAnti if right_empty => empty_exec!(plan),
                JoinType::RightAnti if left_empty => {
                    Ok(Transformed::yes((*join.right).clone()))
                }
                // Return empty if both sides are empty
                JoinType::Full if left_empty && right_empty => empty_exec!(plan),
                // For Full Join, if one side is empty, replace with a
                // Projection that null-pads the empty side's columns.
                JoinType::Full if right_empty && is_guaranteed_non_empty(join.left) => {
                    Ok(Transformed::yes(build_null_padded_projection(
                        Arc::clone(&join.left),
                        join.schema.clone(),
                        left_field_count,
                        true,
                    )?))
                }
                JoinType::Full if left_empty && is_guaranteed_non_empty(join.right) => {
                    Ok(Transformed::yes(build_null_padded_projection(
                        Arc::clone(&join.right),
                        join.schema.clone(),
                        left_field_count,
                        false,
                    )?))
                }
                _ => Ok(Transformed::no(plan)),
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
            schema: join.schema(),
        });
    }
    if let Some(join) = any.downcast_ref::<SortMergeJoinExec>() {
        return Some(JoinInfo {
            join_type: join.join_type,
            left: join.left(),
            right: join.right(),
            schema: join.schema(),
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

/// Compagnion function for branching
/// Returns true only when we have exact stats confirming the plan is non-empty.
/// Precision::Absent means we know nothing — we cannot claim non-empty.
pub fn is_guaranteed_non_empty(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let Ok(stats) = plan.partition_statistics(None) else {
        return false;
    };

    match stats.num_rows {
        Precision::Exact(n) => n > 0,
        _ => false,
    }
}

pub fn build_null_padded_projection(
    surviving_exec: Arc<dyn ExecutionPlan>,
    join_schema: SchemaRef,
    left_field_count: usize,
    empty_side_is_right: bool,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let surviving_schema = surviving_exec.schema();

    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = join_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let on_empty_side = if empty_side_is_right {
                i >= left_field_count
            } else {
                i < left_field_count
            };

            let expr: Arc<dyn PhysicalExpr> = if on_empty_side {
                // Replace empty side with a typed NULL literal
                Arc::new(Literal::new(ScalarValue::try_from(field.data_type())?))
            } else {
                // Map surviving side column by position within surviving schema.
                // When empty_side_is_right - the surviving side is left
                // when empty_side_is_right is false - the surviving side is right
                // (index i - left_field_count).
                let col_idx = if empty_side_is_right {
                    i
                } else {
                    i - left_field_count
                };
                Arc::new(Column::new(surviving_schema.field(col_idx).name(), col_idx))
            };

            Ok((expr, field.name().clone()))
        })
        .collect::<datafusion::error::Result<_>>()?;

    Ok(Arc::new(ProjectionExec::try_new(exprs, surviving_exec)?))
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

    /// Assert the result is an EmptyExec.
    fn assert_empty(result: &Arc<dyn ExecutionPlan>) {
        assert!(
            result.as_any().downcast_ref::<EmptyExec>().is_some(),
            "expected EmptyExec, got {:?}",
            result.name()
        );
    }

    /// Assert the result is a ProjectionExec (null-padded surviving side).
    fn assert_projection(result: &Arc<dyn ExecutionPlan>) {
        assert!(
            result.as_any().downcast_ref::<ProjectionExec>().is_some(),
            "expected ProjectionExec, got {:?}",
            result.name()
        );
    }

    /// Assert the result is neither EmptyExec nor ProjectionExec —
    /// i.e. the join was left untouched.
    fn assert_untouched(result: &Arc<dyn ExecutionPlan>) {
        assert!(
            result.as_any().downcast_ref::<EmptyExec>().is_none()
                && result.as_any().downcast_ref::<ProjectionExec>().is_none(),
            "expected join to be untouched, got {:?}",
            result.name()
        );
    }

    fn transform(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        PropagateEmptyExecRule::transform(plan).unwrap().data
    }

    // ── is_guaranteed_empty ──────────────────────────────────────────────────

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
        // Precision::Absent — must NOT claim empty; we know nothing
        assert!(!is_guaranteed_empty(&unknown_stats_exec()));
    }

    #[test]
    fn guaranteed_empty_literal_empty_exec() {
        let plan = Arc::new(EmptyExec::new(schema())) as Arc<dyn ExecutionPlan>;
        assert!(is_guaranteed_empty(&plan));
    }

    // ── as_join ──────────────────────────────────────────────────────────────

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
        assert!(as_join(&non_empty_stats_exec()).is_none());
    }

    // ── Inner join ───────────────────────────────────────────────────────────

    #[test]
    fn inner_left_empty_eliminated() {
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Inner);
        assert_empty(&transform(plan));
    }

    #[test]
    fn inner_right_empty_eliminated() {
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Inner);
        assert_empty(&transform(plan));
    }

    #[test]
    fn inner_both_empty_eliminated() {
        let plan = hash_join(empty_stats_exec(), empty_stats_exec(), JoinType::Inner);
        assert_empty(&transform(plan));
    }

    #[test]
    fn inner_neither_empty_untouched() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Inner,
        );
        assert_untouched(&transform(plan));
    }

    // ── Left join ────────────────────────────────────────────────────────────

    #[test]
    fn left_join_left_empty_eliminated() {
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Left);
        assert_empty(&transform(plan));
    }

    #[test]
    fn left_join_right_empty_produces_null_padded_projection() {
        // All left rows survive; right columns become NULLs — must be a ProjectionExec
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Left);
        assert_projection(&transform(plan));
    }

    #[test]
    fn left_join_right_empty_schema_matches_join_schema() {
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Left);
        let join_schema = plan.schema();
        let result = transform(plan);
        assert_eq!(result.schema(), join_schema);
    }

    #[test]
    fn left_join_neither_empty_untouched() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Left,
        );
        assert_untouched(&transform(plan));
    }

    // ── Right join ───────────────────────────────────────────────────────────

    #[test]
    fn right_join_right_empty_eliminated() {
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Right);
        assert_empty(&transform(plan));
    }

    #[test]
    fn right_join_left_empty_produces_null_padded_projection() {
        // All right rows survive; left columns become NULLs — must be a ProjectionExec
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Right);
        assert_projection(&transform(plan));
    }

    #[test]
    fn right_join_left_empty_schema_matches_join_schema() {
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Right);
        let join_schema = plan.schema();
        let result = transform(plan);
        assert_eq!(result.schema(), join_schema);
    }

    #[test]
    fn right_join_neither_empty_untouched() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Right,
        );
        assert_untouched(&transform(plan));
    }

    // ── Full join ────────────────────────────────────────────────────────────

    #[test]
    fn full_join_both_empty_eliminated() {
        let plan = hash_join(empty_stats_exec(), empty_stats_exec(), JoinType::Full);
        assert_empty(&transform(plan));
    }

    #[test]
    fn full_join_right_empty_produces_null_padded_projection() {
        // Left rows survive; right columns become NULLs
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Full);
        assert_projection(&transform(plan));
    }

    #[test]
    fn full_join_right_empty_schema_matches_join_schema() {
        let plan = hash_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Full);
        let join_schema = plan.schema();
        let result = transform(plan);
        assert_eq!(result.schema(), join_schema);
    }

    #[test]
    fn full_join_left_empty_produces_null_padded_projection() {
        // Right rows survive; left columns become NULLs
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Full);
        assert_projection(&transform(plan));
    }

    #[test]
    fn full_join_left_empty_schema_matches_join_schema() {
        let plan = hash_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Full);
        let join_schema = plan.schema();
        let result = transform(plan);
        assert_eq!(result.schema(), join_schema);
    }

    #[test]
    fn full_join_neither_empty_untouched() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Full,
        );
        assert_untouched(&transform(plan));
    }

    // ── LeftSemi / RightSemi ─────────────────────────────────────────────────

    #[test]
    fn left_semi_left_empty_eliminated() {
        let plan = hash_join(
            empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::LeftSemi,
        );
        assert_empty(&transform(plan));
    }

    #[test]
    fn left_semi_right_empty_eliminated() {
        let plan = hash_join(
            non_empty_stats_exec(),
            empty_stats_exec(),
            JoinType::LeftSemi,
        );
        assert_empty(&transform(plan));
    }

    #[test]
    fn left_semi_neither_empty_untouched() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::LeftSemi,
        );
        assert_untouched(&transform(plan));
    }

    #[test]
    fn right_semi_left_empty_eliminated() {
        let plan = hash_join(
            empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::RightSemi,
        );
        assert_empty(&transform(plan));
    }

    #[test]
    fn right_semi_right_empty_eliminated() {
        let plan = hash_join(
            non_empty_stats_exec(),
            empty_stats_exec(),
            JoinType::RightSemi,
        );
        assert_empty(&transform(plan));
    }

    #[test]
    fn right_semi_neither_empty_untouched() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::RightSemi,
        );
        assert_untouched(&transform(plan));
    }

    // ── LeftAnti / RightAnti ─────────────────────────────────────────────────

    #[test]
    fn left_anti_left_empty_eliminated() {
        let plan = hash_join(
            empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::LeftAnti,
        );
        assert_empty(&transform(plan));
    }

    #[test]
    fn left_anti_right_empty_returns_left_child() {
        // Empty right means no rows are excluded — the result IS the left child
        let left = non_empty_stats_exec();
        let plan = hash_join(Arc::clone(&left), empty_stats_exec(), JoinType::LeftAnti);
        let result = transform(plan);
        // Should be the left child itself, not wrapped in anything
        assert!(result.as_any().downcast_ref::<StatisticsExec>().is_some());
    }

    #[test]
    fn left_anti_neither_empty_untouched() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::LeftAnti,
        );
        assert_untouched(&transform(plan));
    }

    #[test]
    fn right_anti_right_empty_eliminated() {
        let plan = hash_join(
            non_empty_stats_exec(),
            empty_stats_exec(),
            JoinType::RightAnti,
        );
        assert_empty(&transform(plan));
    }

    #[test]
    fn right_anti_left_empty_returns_right_child() {
        // Empty left means no rows are excluded — the result IS the right child
        let right = non_empty_stats_exec();
        let plan = hash_join(empty_stats_exec(), Arc::clone(&right), JoinType::RightAnti);
        let result = transform(plan);
        assert!(result.as_any().downcast_ref::<StatisticsExec>().is_some());
    }

    #[test]
    fn right_anti_neither_empty_untouched() {
        let plan = hash_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::RightAnti,
        );
        assert_untouched(&transform(plan));
    }

    // ── SortMergeJoin — mirrors the HashJoin matrix ──────────────────────────

    #[test]
    fn smj_inner_left_empty_eliminated() {
        let plan =
            sort_merge_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Inner);
        assert_empty(&transform(plan));
    }

    #[test]
    fn smj_inner_right_empty_eliminated() {
        let plan =
            sort_merge_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Inner);
        assert_empty(&transform(plan));
    }

    #[test]
    fn smj_left_join_left_empty_eliminated() {
        let plan =
            sort_merge_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Left);
        assert_empty(&transform(plan));
    }

    #[test]
    fn smj_left_join_right_empty_produces_null_padded_projection() {
        let plan =
            sort_merge_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Left);
        assert_projection(&transform(plan));
    }

    #[test]
    fn smj_right_join_right_empty_eliminated() {
        let plan =
            sort_merge_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Right);
        assert_empty(&transform(plan));
    }

    #[test]
    fn smj_right_join_left_empty_produces_null_padded_projection() {
        let plan =
            sort_merge_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Right);
        assert_projection(&transform(plan));
    }

    #[test]
    fn smj_full_both_empty_eliminated() {
        let plan =
            sort_merge_join(empty_stats_exec(), empty_stats_exec(), JoinType::Full);
        assert_empty(&transform(plan));
    }

    #[test]
    fn smj_full_right_empty_produces_null_padded_projection() {
        let plan =
            sort_merge_join(non_empty_stats_exec(), empty_stats_exec(), JoinType::Full);
        assert_projection(&transform(plan));
    }

    #[test]
    fn smj_full_left_empty_produces_null_padded_projection() {
        let plan =
            sort_merge_join(empty_stats_exec(), non_empty_stats_exec(), JoinType::Full);
        assert_projection(&transform(plan));
    }

    #[test]
    fn smj_full_neither_empty_untouched() {
        let plan = sort_merge_join(
            non_empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Full,
        );
        assert_untouched(&transform(plan));
    }

    #[test]
    fn smj_left_anti_right_empty_returns_left_child() {
        let plan = sort_merge_join(
            non_empty_stats_exec(),
            empty_stats_exec(),
            JoinType::LeftAnti,
        );
        let result = transform(plan);
        assert!(result.as_any().downcast_ref::<StatisticsExec>().is_some());
    }

    #[test]
    fn smj_right_anti_left_empty_returns_right_child() {
        let plan = sort_merge_join(
            empty_stats_exec(),
            non_empty_stats_exec(),
            JoinType::RightAnti,
        );
        let result = transform(plan);
        assert!(result.as_any().downcast_ref::<StatisticsExec>().is_some());
    }

    // ── unknown stats — never optimised ─────────────────────────────────────

    #[test]
    fn unknown_stats_inner_join_untouched() {
        // Precision::Absent on either side must not trigger elimination
        let plan = hash_join(
            unknown_stats_exec(),
            non_empty_stats_exec(),
            JoinType::Inner,
        );
        assert_untouched(&transform(plan));
    }

    #[test]
    fn left_join_left_exact_zero_right_absent_eliminated() {
        // Left is confirmed empty, right stats unknown — Left join result is still empty
        let plan = hash_join(empty_stats_exec(), unknown_stats_exec(), JoinType::Left);
        assert_empty(&transform(plan));
    }

    #[test]
    fn right_join_right_exact_zero_left_absent_eliminated() {
        // Right is confirmed empty, left stats unknown — Right join result is still empty
        let plan = hash_join(unknown_stats_exec(), empty_stats_exec(), JoinType::Right);
        assert_empty(&transform(plan));
    }

    #[test]
    fn inner_join_left_exact_zero_right_absent_eliminated() {
        // Inner: left empty is sufficient, right stats don't matter
        let plan = hash_join(empty_stats_exec(), unknown_stats_exec(), JoinType::Inner);
        assert_empty(&transform(plan));
    }

    #[test]
    fn inner_join_right_exact_zero_left_absent_eliminated() {
        let plan = hash_join(unknown_stats_exec(), empty_stats_exec(), JoinType::Inner);
        assert_empty(&transform(plan));
    }

    #[test]
    fn full_join_one_side_absent_untouched() {
        // Full needs BOTH sides empty — if one is unknown, can't eliminate
        let plan = hash_join(empty_stats_exec(), unknown_stats_exec(), JoinType::Full);
        assert_untouched(&transform(plan));
    }

    #[test]
    fn left_anti_empty_left_under_partitioned_exchange_preserves_partition_count() {
        // Simulates: ExchangeExec(Hash, 16) → EmptyExec
        // After propagation the Exchange becomes empty.
        // The LeftAnti join above must produce an EmptyExec with the
        // join's partition count, NOT the Exchange's input partition count.
        use datafusion::physical_plan::Partitioning;
        use datafusion::physical_plan::repartition::RepartitionExec;

        // Build: LeftAnti join where left = RepartitionExec(16) over EmptyExec
        let empty = Arc::new(EmptyExec::new(schema()).with_partitions(1));
        let repartitioned = Arc::new(
            RepartitionExec::try_new(
                empty,
                Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 16),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let plan = hash_join(repartitioned, non_empty_stats_exec(), JoinType::LeftAnti);

        // The rule should fire (left is empty via RepartitionExec over EmptyExec)
        // and produce an EmptyExec with the join's partition count
        let result = PropagateEmptyExecRule {}
            .optimize(plan.clone(), &ConfigOptions::default())
            .unwrap();

        let empty_exec = result.as_any().downcast_ref::<EmptyExec>();
        assert!(empty_exec.is_some(), "expected EmptyExec");
        assert_eq!(
            empty_exec
                .unwrap()
                .properties()
                .output_partitioning()
                .partition_count(),
            plan.properties().output_partitioning().partition_count(),
            "EmptyExec partition count must match the original join's partition count"
        );
    }
}
