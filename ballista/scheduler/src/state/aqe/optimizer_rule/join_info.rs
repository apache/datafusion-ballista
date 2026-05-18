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

use datafusion::common::stats::Precision;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::{common::JoinType, physical_plan::ExecutionPlan};
use std::sync::Arc;

pub struct JoinInfo<'a> {
    pub join_type: JoinType,
    pub left: &'a Arc<dyn ExecutionPlan>,
    pub right: &'a Arc<dyn ExecutionPlan>,
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
/// Hence we can decide whether it is empty
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
pub(crate) mod test_helpers {
    use super::*;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::{ColumnStatistics, JoinType, NullEquality, Statistics},
        physical_plan::{
            expressions::Column, joins::PartitionMode, test::exec::StatisticsExec,
        },
    };

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::optimizer_rule::join_info::test_helpers::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinType;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use std::sync::Arc;

    // Helpers
    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]))
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
}
