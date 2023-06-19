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
use datafusion::physical_plan::joins::HashJoinExec;
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

    fn transform_down(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.insert_coalesce(plan)? {
            Transformed::Yes(node) => {
                if let Some(coalesce) = node.as_any().downcast_ref::<CoalesceTasksExec>()
                {
                    if let Some(original_node) = coalesce.children().first().cloned() {
                        // we need to traverse on the children of the original node and not transformed,
                        // as we fall in to an infinite loop otherwise
                        return node.with_new_children(vec![original_node
                            .map_children(|node| self.transform_down(node))?]);
                    }
                }

                node.map_children(|node| self.transform_down(node))
            }
            Transformed::No(node) => node.map_children(|node| self.transform_down(node)),
        }
    }

    fn insert_coalesce(
        &self,
        node: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exec) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            return Ok(Transformed::Yes(Arc::new(
                exec.with_partitions(self.partitions.clone())?,
            )));
        }

        if node.children().is_empty() || node.as_any().is::<UnionExec>() {
            return Ok(Transformed::Yes(Arc::new(CoalesceTasksExec::new(
                node,
                self.partitions.clone(),
            ))));
        }

        Ok(Transformed::No(node))
    }

    fn optimize_node(
        &self,
        node: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let children = node.children();

        if is_mapping(node.as_ref()) && children.len() == 1 {
            if let Some(coalesce) =
                children[0].as_any().downcast_ref::<CoalesceTasksExec>()
            {
                let mut new_plan: Arc<dyn ExecutionPlan> =
                    Arc::new(CoalesceTasksExec::new(
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
            } else {
                Ok(Transformed::No(node))
            }
        } else {
            let is_union = node.as_any().is::<UnionExec>();
            let all_children_are_coalesce = node
                .children()
                .iter()
                .all(|child| child.as_any().is::<CoalesceTasksExec>());

            if (is_union || is_hash_join_no_partitioning(node.as_ref()))
                && all_children_are_coalesce
            {
                let new_children =
                    children.iter().flat_map(|child| child.children()).collect();

                let new_plan: Arc<dyn ExecutionPlan> = Arc::new(CoalesceTasksExec::new(
                    node.clone().with_new_children(new_children)?,
                    self.partitions.clone(),
                ));

                return Ok(Transformed::Yes(new_plan));
            }

            Ok(Transformed::No(node))
        }
    }
}

fn is_hash_join_no_partitioning(node: &dyn ExecutionPlan) -> bool {
    // only push down hash join if the input is unpartitioned
    // (so parent nodes won't rely on the output partitioning)
    if let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() {
        return matches!(
            hash_join.output_partitioning(),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(_)
        );
    }
    false
}

impl PhysicalOptimizerRule for OptimizeTaskGroup {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inserted = self.transform_down(plan)?;
        inserted.transform_up(&|node| self.optimize_node(node))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::Schema,
        physical_plan::{limit::LocalLimitExec, union::UnionExec},
    };

    use crate::execution_plans::{CoalesceTasksExec, ShuffleReaderExec};

    use super::OptimizeTaskGroup;

    #[test]
    fn test_optimizer_insert_coalesce_on_top_of_the_leaf() {
        let optimizer = OptimizeTaskGroup::new(Vec::default());
        let input = Arc::new(
            ShuffleReaderExec::try_new(Vec::default(), Arc::new(Schema::empty()))
                .unwrap(),
        );

        let optiized = optimizer.insert_coalesce(input).unwrap().into();
        assert!(optiized.as_ref().as_any().is::<CoalesceTasksExec>());

        let children = optiized.children();
        assert_eq!(children.len(), 1);

        let original_node = &children[0];
        assert_eq!(original_node.children().len(), 0);
        assert!(original_node.as_ref().as_any().is::<ShuffleReaderExec>());
    }

    #[test]
    fn test_optimizer_insert_coalesce_on_top_of_the_union() {
        let optimizer = OptimizeTaskGroup::new(Vec::default());
        let input = Arc::new(UnionExec::new(vec![
            Arc::new(
                ShuffleReaderExec::try_new(Vec::default(), Arc::new(Schema::empty()))
                    .unwrap(),
            ),
            Arc::new(
                ShuffleReaderExec::try_new(Vec::default(), Arc::new(Schema::empty()))
                    .unwrap(),
            ),
        ]));

        let optimized = optimizer.transform_down(input).unwrap();
        let children = optimized.children();
        assert_eq!(children.len(), 1);
        assert!(optimized.as_ref().as_any().is::<CoalesceTasksExec>());

        let nested_union = &children[0];
        assert_eq!(nested_union.children().len(), 2);
        assert!(nested_union.as_ref().as_any().is::<UnionExec>());
    }

    #[test]
    fn test_optimizer_insert_coalesce_should_do_nothing() {
        let optimizer = OptimizeTaskGroup::new(Vec::default());
        let input = Arc::new(LocalLimitExec::new(
            Arc::new(
                ShuffleReaderExec::try_new(Vec::default(), Arc::new(Schema::empty()))
                    .unwrap(),
            ),
            10,
        ));

        assert!(optimizer
            .transform_down(input)
            .unwrap()
            .as_any()
            .is::<LocalLimitExec>())
    }

    #[test]
    fn test_optimize_union_plan_flat_children() {
        let optimizer = OptimizeTaskGroup::new(Vec::default());
        let input = Arc::new(UnionExec::new(vec![
            Arc::new(CoalesceTasksExec::new(
                Arc::new(
                    ShuffleReaderExec::try_new(Vec::default(), Arc::new(Schema::empty()))
                        .unwrap(),
                ),
                Vec::default(),
            )),
            Arc::new(CoalesceTasksExec::new(
                Arc::new(
                    ShuffleReaderExec::try_new(Vec::default(), Arc::new(Schema::empty()))
                        .unwrap(),
                ),
                Vec::default(),
            )),
        ]));

        let optimized = optimizer.optimize_node(input).unwrap().into();
        let children = optimized.children();
        assert_eq!(children.len(), 1);
        assert!(optimized.as_ref().as_any().is::<CoalesceTasksExec>());

        let nested_union = &children[0];
        let children = nested_union.children();
        assert_eq!(children.len(), 2);
        assert!(nested_union.as_ref().as_any().is::<UnionExec>());
        assert!(children
            .iter()
            .all(|c| c.as_any().is::<ShuffleReaderExec>()))
    }
}
