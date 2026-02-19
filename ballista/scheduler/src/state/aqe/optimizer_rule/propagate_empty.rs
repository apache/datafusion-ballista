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
use datafusion::common::JoinType::Inner;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
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
        } else if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>()
            // TODO: - we need other joins, this one is used for testing cancellation
            && hash_join.join_type == Inner
            && (is_empty_exec!(hash_join.left) || is_empty_exec!(hash_join.right))
        {
            empty_exec!(hash_join)
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
