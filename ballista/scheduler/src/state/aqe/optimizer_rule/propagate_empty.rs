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
use crate::state::aqe::optimizer_rule::join_info::{as_join, is_guaranteed_empty};
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
        Ok(Transformed::yes(Arc::new(EmptyExec::new($e.schema()))))
    };
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
                Ok(Transformed::yes(Arc::new(EmptyExec::new(plan.schema()))))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::optimizer_rule::join_info::test_helpers::*;
    use datafusion::common::JoinType;

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
