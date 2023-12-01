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
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
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
            Transformed::Yes(node) => Ok(node),
            Transformed::No(node) => node.map_children(|node| self.transform_down(node)),
        }
    }

    fn insert_coalesce(
        &self,
        node: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exec) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            return Ok(Transformed::No(Arc::new(
                exec.with_partitions(self.partitions.clone())?,
            )));
        }

        if insert_coalesce(node.as_ref()) {
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
            Ok(Transformed::No(node))
        }
    }
}

// fn is_hash_join_no_partitioning(node: &dyn ExecutionPlan) -> bool {
//     // only push down hash join if the input is unpartitioned
//     // (so parent nodes won't rely on the output partitioning)
//     if let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() {
//         return matches!(
//             hash_join.output_partitioning(),
//             datafusion::physical_plan::Partitioning::UnknownPartitioning(_)
//         );
//     }
//     false
// }

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

/// Return whether we should insert a `CoalesceTasksExec` above this node.
fn insert_coalesce(plan: &dyn ExecutionPlan) -> bool {
    plan.children().is_empty()
        || plan.as_any().is::<UnionExec>()
        || plan.as_any().is::<SortPreservingMergeExec>()
        || plan.as_any().is::<CoalescePartitionsExec>()
        || plan.as_any().is::<HashJoinExec>()
        || is_aggregation(plan, AggregateMode::Final)
        || is_aggregation(plan, AggregateMode::FinalPartitioned)
}

// Returns true for nodes that are mapping tasks (filter, projection, local-limit, coalesce-batches)
fn is_mapping(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().is::<FilterExec>()
        || plan.as_any().is::<CoalesceBatchesExec>()
        || plan.as_any().is::<ProjectionExec>()
        || plan.as_any().is::<LocalLimitExec>()
        || is_aggregation(plan, AggregateMode::Partial)
}

fn is_aggregation(plan: &dyn ExecutionPlan, mode: AggregateMode) -> bool {
    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
        return *aggregate.mode() == mode;
    };
    false
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::{
        arrow::datatypes::Schema,
        physical_plan::{limit::LocalLimitExec, union::UnionExec},
    };

    use crate::execution_plans::{
        CoalesceTasksExec, ShuffleReaderExec, ShuffleWriterExec,
    };
    use crate::serde::scheduler::{
        ExecutorMetadata, ExecutorSpecification, PartitionLocation, PartitionStats,
    };
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

    use super::OptimizeTaskGroup;

    fn scan(partitions: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            EmptyExec::new(
                true,
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::UInt32, false),
                    Field::new("b", DataType::UInt32, false),
                    Field::new("c", DataType::UInt32, false),
                ])),
            )
            .with_partitions(partitions),
        )
    }

    fn shuffle_write(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            ShuffleWriterExec::try_new(
                "job".into(),
                1,
                vec![],
                plan,
                "/work".into(),
                None,
            )
            .unwrap(),
        )
    }

    fn shuffle_reader(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let locations = (0..partitions)
            .map(|part| PartitionLocation {
                job_id: "job".into(),
                stage_id: 0,
                map_partitions: vec![],
                output_partition: part,
                executor_meta: ExecutorMetadata {
                    id: "".into(),
                    host: "".into(),
                    port: 0,
                    grpc_port: 0,
                    specification: ExecutorSpecification {
                        task_slots: 0,
                        version: "0".to_string(),
                    },
                },
                partition_stats: PartitionStats {
                    num_rows: None,
                    num_batches: None,
                    num_bytes: None,
                },
                path: "/path/to/file".into(),
            })
            .collect();

        let partitions = std::iter::repeat(locations).take(partitions).collect();
        Arc::new(
            ShuffleReaderExec::try_new(
                partitions,
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::UInt32, false),
                    Field::new("b", DataType::UInt32, false),
                    Field::new("c", DataType::UInt32, false),
                ])),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_optimizer_sort_merge_exec() {
        let optimizer = OptimizeTaskGroup::new(vec![0]);

        let input = shuffle_write(Arc::new(SortPreservingMergeExec::new(
            vec![],
            shuffle_reader(10),
        )));

        let optimized = optimizer
            .optimize(input, &ConfigOptions::default())
            .unwrap();

        println!(
            "{}",
            DisplayableExecutionPlan::new(optimized.as_ref()).indent(false)
        );

        assert!(optimized.as_any().is::<ShuffleWriterExec>());
        assert_eq!(optimized.children().len(), 1);

        let child = optimized.children()[0].clone();
        assert_eq!(child.children().len(), 1);
        assert!(child.as_any().is::<CoalesceTasksExec>());

        let child = child.children()[0].clone();
        assert_eq!(child.children().len(), 1);
        assert!(child.as_any().is::<SortPreservingMergeExec>());

        let child = child.children()[0].clone();
        assert!(child.children().is_empty());
        assert!(child.as_any().is::<ShuffleReaderExec>());
    }

    #[test]
    fn test_optimizer_hash_join_exec() {
        let optimizer = OptimizeTaskGroup::new(vec![0]);

        let input = shuffle_write(Arc::new(
            HashJoinExec::try_new(
                scan(10),
                scan(10),
                vec![(Column::new("a", 0), Column::new("a", 0))],
                None,
                &JoinType::Left,
                PartitionMode::CollectLeft,
                true,
            )
            .unwrap(),
        ));

        let optimized = optimizer
            .optimize(input, &ConfigOptions::default())
            .unwrap();

        assert!(optimized.as_any().is::<ShuffleWriterExec>());
        assert_eq!(optimized.children().len(), 1);

        let child = optimized.children()[0].clone();
        assert_eq!(child.children().len(), 1);
        assert!(child.as_any().is::<CoalesceTasksExec>());

        let child = child.children()[0].clone();
        assert_eq!(child.children().len(), 2);
        assert!(child.as_any().is::<HashJoinExec>());
    }

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
        let optimizer = OptimizeTaskGroup::new((0..20).collect());
        let input = shuffle_write(Arc::new(UnionExec::new(vec![scan(10), scan(10)])));

        let optimized = optimizer.optimize(input, &ConfigOptions::new()).unwrap();

        assert!(optimized.as_any().is::<ShuffleWriterExec>());
        assert_eq!(optimized.children().len(), 1);

        let child = optimized.children()[0].clone();

        assert!(child.as_any().is::<CoalesceTasksExec>());
        assert_eq!(child.children().len(), 1);

        let child = child.children()[0].clone();

        assert!(child.as_any().is::<UnionExec>());
        assert_eq!(child.children().len(), 2);
    }

    #[test]
    fn test_optimizer_push_down_limit() {
        let optimizer = OptimizeTaskGroup::new((0..10).collect());
        let input = shuffle_write(Arc::new(LocalLimitExec::new(scan(10), 10)));

        let optimized = optimizer.optimize(input, &ConfigOptions::new()).unwrap();

        println!(
            "{}",
            DisplayableExecutionPlan::new(optimized.as_ref()).indent(false)
        );

        assert!(optimized.as_any().is::<ShuffleWriterExec>());
        assert_eq!(optimized.children().len(), 1);

        let child = optimized.children()[0].clone();

        assert!(child.as_any().is::<LocalLimitExec>());
        assert_eq!(child.children().len(), 1);

        let child = child.children()[0].clone();

        assert!(child.as_any().is::<CoalesceTasksExec>());
        assert_eq!(child.children().len(), 1);

        let child = child.children()[0].clone();

        assert!(child.as_any().is::<LocalLimitExec>());
        assert_eq!(child.children().len(), 1);
    }

    #[test]
    fn test_optimizer_push_partial_agg() {
        let optimizer = OptimizeTaskGroup::new((0..10).collect());
        let input = shuffle_write(Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::new(vec![], vec![], vec![]),
                vec![],
                vec![],
                vec![],
                scan(10),
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::UInt32, false),
                    Field::new("b", DataType::UInt32, false),
                    Field::new("c", DataType::UInt32, false),
                ])),
            )
            .unwrap(),
        ));

        let optimized = optimizer.optimize(input, &ConfigOptions::new()).unwrap();

        println!(
            "{}",
            DisplayableExecutionPlan::new(optimized.as_ref()).indent(false)
        );

        assert!(optimized.as_any().is::<ShuffleWriterExec>());
        assert_eq!(optimized.children().len(), 1);

        let child = optimized.children()[0].clone();

        assert!(child.as_any().is::<CoalesceTasksExec>());
        assert_eq!(child.children().len(), 1);

        let child = child.children()[0].clone();

        assert!(child.as_any().is::<AggregateExec>());
        assert_eq!(child.children().len(), 1);
    }
}
