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

use crate::execution_plans::{CoalesceTasksExec, ShuffleWriterExec};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct OptimizeTaskGroup {
    partitions: Vec<usize>,
}

impl OptimizeTaskGroup {
    pub fn new(partitions: Vec<usize>) -> Self {
        Self { partitions }
    }

    fn transform_node(
        &self,
        node: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exec) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            return Ok(Transformed::Yes(Arc::new(
                exec.with_partitions(self.partitions.clone())?,
            )));
        }

        if node.children().is_empty() {
            return Ok(Transformed::Yes(Arc::new(CoalesceTasksExec::new(
                node,
                self.partitions.clone(),
            ))));
        }

        let children = node.children();

        if is_mapping(node.as_ref())
            && children.len() == 1
            && children[0].as_any().is::<CoalesceTasksExec>()
        {
            let coalesce = children[0]
                .as_any()
                .downcast_ref::<CoalesceTasksExec>()
                .unwrap();

            let mut new_plan: Arc<dyn ExecutionPlan> = Arc::new(CoalesceTasksExec::new(
                node.clone().with_new_children(coalesce.children())?,
                self.partitions.clone(),
            ));

            // As we combine partitions in CoalesceTasksExec, add another top-level
            // LocalLimit to reduce the output size
            // and potentially abort execution early
            if node.as_any().is::<LocalLimitExec>() {
                new_plan = node.with_new_children(vec![new_plan])?;
            }

            Ok(Transformed::Yes(new_plan))
        } else if node.as_any().is::<UnionExec>()
            && children
                .iter()
                .all(|child| child.as_any().is::<CoalesceTasksExec>())
        {
            let new_children =
                children.iter().flat_map(|child| child.children()).collect();

            let new_plan: Arc<dyn ExecutionPlan> = Arc::new(CoalesceTasksExec::new(
                node.clone().with_new_children(new_children)?,
                self.partitions.clone(),
            ));

            Ok(Transformed::Yes(new_plan))
        } else {
            Ok(Transformed::No(node))
        }
    }
}

impl PhysicalOptimizerRule for OptimizeTaskGroup {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform(&|node| self.transform_node(node))
    }

    fn name(&self) -> &str {
        "optimize_task_group"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// Returns true for nodes that are mapping tasks (filter, projection, local-limit, coalesce-batches)
fn is_mapping(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().is::<FilterExec>()
        || plan.as_any().is::<CoalesceBatchesExec>()
        || plan.as_any().is::<ProjectionExec>()
        || plan.as_any().is::<LocalLimitExec>()
        || is_partial_aggregate(plan)
}

fn is_partial_aggregate(plan: &dyn ExecutionPlan) -> bool {
    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
        return *aggregate.mode() == AggregateMode::Partial;
    };
    false
}
