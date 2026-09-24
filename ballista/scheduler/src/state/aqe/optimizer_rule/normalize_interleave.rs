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

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use std::sync::Arc;

/// Rewrites every [`InterleaveExec`] back into a [`UnionExec`].
///
/// `InterleaveExec` requires all children to share one `Hash`/`Range`
/// partitioning and asserts on rebuild. AQE rewrites union branches
/// independently (pruning an empty one, resolving sibling joins differently),
/// and `TreeNode` rebuilds a parent as soon as a child changes, so the
/// assertion fires before any rule can see the parent and aborts the replan
/// (#2047). `UnionExec` has no such invariant; `EnforceDistribution` re-forms
/// the interleave later via its `can_interleave`-guarded constructor.
#[derive(Debug, Clone, Default)]
pub struct NormalizeInterleaveRule {}

impl PhysicalOptimizerRule for NormalizeInterleaveRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Top-down so a nested interleave is replaced rather than rebuilt
        // as a changed child.
        Ok(plan
            .transform_down(|node| {
                let Some(interleave) = node.downcast_ref::<InterleaveExec>() else {
                    return Ok(Transformed::no(node));
                };
                Ok(Transformed::yes(UnionExec::try_new(
                    interleave.inputs().clone(),
                )?))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "NormalizeInterleaveRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{
        ChildrenPropertiesMode, ExecutionPlanProperties, Partitioning,
        ReplaceChildrenOptions,
    };

    fn recompute() -> ReplaceChildrenOptions {
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute)
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]))
    }

    /// Two branches hash-partitioned the same way, so `InterleaveExec` is valid.
    fn hash_branch(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(
                Arc::new(EmptyExec::new(Arc::clone(schema))),
                Partitioning::Hash(vec![col("a", schema).unwrap()], 4),
            )
            .unwrap(),
        )
    }

    #[test]
    fn rewrites_interleave_into_union() {
        let schema = schema();
        let interleave: Arc<dyn ExecutionPlan> = Arc::new(
            InterleaveExec::try_new(vec![hash_branch(&schema), hash_branch(&schema)])
                .unwrap(),
        );

        let out = NormalizeInterleaveRule::default()
            .optimize(interleave, &ConfigOptions::default())
            .unwrap();

        assert!(out.downcast_ref::<UnionExec>().is_some());
        assert!(out.downcast_ref::<InterleaveExec>().is_none());
    }

    /// The point of the rule: once it has run, swapping one branch for a
    /// differently-partitioned plan must not blow up on rebuild.
    #[test]
    fn union_tolerates_a_branch_whose_partitioning_diverges() {
        let schema = schema();
        let interleave: Arc<dyn ExecutionPlan> = Arc::new(
            InterleaveExec::try_new(vec![hash_branch(&schema), hash_branch(&schema)])
                .unwrap(),
        );

        // Rebuilding the interleave itself with a mismatched child is what
        // aborts an AQE replan today.
        let divergent: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(4));
        assert!(
            Arc::clone(&interleave)
                .replace_children(
                    vec![hash_branch(&schema), Arc::clone(&divergent)],
                    recompute()
                )
                .is_err()
        );

        let union = NormalizeInterleaveRule::default()
            .optimize(interleave, &ConfigOptions::default())
            .unwrap();
        assert!(
            union
                .replace_children(vec![hash_branch(&schema), divergent], recompute())
                .is_ok()
        );
    }

    #[test]
    fn leaves_plans_without_an_interleave_alone() {
        let schema = schema();
        let plan = hash_branch(&schema);
        let out = NormalizeInterleaveRule::default()
            .optimize(Arc::clone(&plan), &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            out.output_partitioning().partition_count(),
            plan.output_partitioning().partition_count()
        );
    }
}
