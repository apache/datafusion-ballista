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
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions,
};
use std::sync::Arc;

/// Reads a broadcast exchange on the probe side of a `CollectLeft` join
/// partitioned instead.
///
/// When a broadcast build side measures larger than expected, `join_selection`
/// swaps it onto the probe side, where its single partition would pin the join
/// stage to one task. Runs straight after `join_selection`.
///
/// Null-aware joins track probe-side NULLs in-process, so they keep a single
/// task. Broadcasts over ordered inputs are left to the k-way merge reader.
#[derive(Debug, Default)]
pub struct PartitionProbeSideBroadcastRule {}

/// `node`'s children with its probe-side broadcast read partitioned, if it has
/// one to replace.
fn with_partitioned_probe(
    node: &Arc<dyn ExecutionPlan>,
) -> Option<Vec<Arc<dyn ExecutionPlan>>> {
    let join = node.downcast_ref::<HashJoinExec>()?;
    if join.null_aware || *join.partition_mode() != PartitionMode::CollectLeft {
        return None;
    }
    let probe = join.right().downcast_ref::<ExchangeExec>()?;
    if !probe.broadcast || probe.input().properties().output_ordering().is_some() {
        return None;
    }
    Some(vec![
        Arc::clone(join.left()),
        Arc::new(probe.to_partitioned()?),
    ])
}

impl PhysicalOptimizerRule for PartitionProbeSideBroadcastRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(children) = with_partitioned_probe(&node) else {
                return Ok(Transformed::no(node));
            };
            node.replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .map(Transformed::yes)
        })
        .data()
    }

    fn name(&self) -> &str {
        "PartitionProbeSideBroadcastRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::assert_plan;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality, Statistics};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::test::exec::StatisticsExec;

    fn leaf() -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema,
        ))
    }

    /// Optimizes a `CollectLeft` join whose probe side broadcasts `probe_input`.
    fn optimize(
        probe_input: Arc<dyn ExecutionPlan>,
        null_aware: bool,
    ) -> Arc<dyn ExecutionPlan> {
        optimize_exchange(
            ExchangeExec::new_broadcast(probe_input, None, 0),
            null_aware,
        )
    }

    /// Optimizes a `CollectLeft` join whose probe side is `probe`.
    fn optimize_exchange(
        probe: ExchangeExec,
        null_aware: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let a = Arc::new(Column::new("a", 0));
        let join = HashJoinExec::try_new(
            leaf(),
            Arc::new(probe),
            vec![(a.clone(), a)],
            None,
            &JoinType::LeftAnti,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            null_aware,
        )
        .unwrap();
        PartitionProbeSideBroadcastRule::default()
            .optimize(Arc::new(join), &ConfigOptions::new())
            .unwrap()
    }

    #[test]
    fn probe_side_broadcast_is_read_partitioned() {
        assert_plan!(optimize(leaf(), false).as_ref(), @ r"
        HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(a@0, a@0)]
          StatisticsExec: col_count=1, row_count=Absent
          ExchangeExec: partitioning=None, plan_id=0, stage_id=pending, stage_resolved=false
            StatisticsExec: col_count=1, row_count=Absent
        ");
    }

    #[test]
    fn null_aware_join_keeps_probe_side_broadcast() {
        assert_plan!(optimize(leaf(), true).as_ref(), @ r"
        HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(a@0, a@0)], null_aware
          StatisticsExec: col_count=1, row_count=Absent
          ExchangeExec: partitioning=None, plan_id=0, stage_id=pending, stage_resolved=false, broadcast=true
            StatisticsExec: col_count=1, row_count=Absent
        ");
    }

    #[test]
    fn ordered_probe_side_broadcast_is_left_to_the_merge_reader() {
        let a = PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)));
        let sorted = SortExec::new(LexOrdering::new(vec![a]).unwrap(), leaf());
        assert_plan!(optimize(Arc::new(sorted), false).as_ref(), @ r"
        HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(a@0, a@0)]
          StatisticsExec: col_count=1, row_count=Absent
          ExchangeExec: partitioning=None, plan_id=0, stage_id=pending, stage_resolved=false, broadcast=true
            SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
              StatisticsExec: col_count=1, row_count=Absent
        ");
    }

    #[test]
    fn broadcast_written_as_a_shuffle_is_not_read_partitioned() {
        let written_as_hash_shuffle = ExchangeExec::new_broadcast(leaf(), None, 0);
        written_as_hash_shuffle.resolve_shuffle_partitions(vec![vec![]; 3]);
        assert_plan!(optimize_exchange(written_as_hash_shuffle, false).as_ref(), @ r"
        HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(a@0, a@0)]
          StatisticsExec: col_count=1, row_count=Absent
          ExchangeExec: partitioning=None, plan_id=0, stage_id=pending, stage_resolved=true, broadcast=true
            StatisticsExec: col_count=1, row_count=Absent
        ");
    }
}
