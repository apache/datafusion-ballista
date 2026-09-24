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

use crate::physical_optimizer::join_selection::collect_left_broadcast_safe;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{
    HashJoinExec, HashJoinExecBuilder, PartitionMode,
};
use log::debug;
use std::sync::Arc;

/// Demotes a `CollectLeft` hash join whose join type is not broadcast-safe back
/// to `Partitioned`.
///
/// DataFusion's `join_selection` promotes any small-build-side join to
/// `CollectLeft` regardless of join type — correct in one process, but Ballista
/// runs one task per probe partition, each with a full copy of the build side,
/// so a join type that emits build-side rows (`Left`, `Full`, semi/anti/mark)
/// emits them once per task. The static planner guards this in
/// `maybe_promote_to_broadcast`. Runs between `join_selection` and
/// `EnsureRequirements`, which then re-adds the needed repartitions.
#[derive(Debug, Default)]
pub struct DemoteUnsafeBroadcastJoinRule {}

impl PhysicalOptimizerRule for DemoteUnsafeBroadcastJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_up(|node| {
                let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                    return Ok(Transformed::no(node));
                };
                // Null-aware anti joins require single-task `CollectLeft`.
                if join.null_aware
                    || *join.partition_mode() != PartitionMode::CollectLeft
                    || collect_left_broadcast_safe(*join.join_type())
                {
                    return Ok(Transformed::no(node));
                }

                debug!(
                    "demoting broadcast-unsafe CollectLeft {:?} join to Partitioned",
                    join.join_type()
                );
                let demoted = HashJoinExecBuilder::new(
                    Arc::clone(&join.left),
                    Arc::clone(&join.right),
                    join.on.clone(),
                    *join.join_type(),
                )
                .with_filter(join.filter.clone())
                .with_projection_ref(join.projection.clone())
                .with_partition_mode(PartitionMode::Partitioned)
                .with_null_equality(join.null_equality)
                .with_null_aware(join.null_aware)
                .build()?;

                Ok(Transformed::yes(Arc::new(demoted) as Arc<dyn ExecutionPlan>))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "DemoteUnsafeBroadcastJoinRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
