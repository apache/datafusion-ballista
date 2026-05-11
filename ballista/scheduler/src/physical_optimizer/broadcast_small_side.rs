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

//! Broadcast small-side optimizer rule.
//!
//! Converts a [`HashJoinExec`] in [`PartitionMode::Partitioned`] mode to
//! [`PartitionMode::CollectLeft`] (broadcast) when one side's total byte
//! size is below the configured threshold. Avoids the network shuffle on
//! the larger side when the smaller side fits.
//!
//! Restricted to `JoinType::Inner` because CollectLeft has known
//! correctness issues for Left/Full outer joins in Ballista
//! ([issue #1055](https://github.com/apache/datafusion-ballista/issues/1055)).
//! `should_swap_join_order` from the existing JoinSelection rule chooses
//! which side to put on the build side after the conversion.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use datafusion::common::JoinType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

use crate::physical_optimizer::join_selection::should_swap_join_order;

/// Optimizer rule that promotes small-side broadcasts.
#[derive(Debug)]
pub struct BroadcastSmallSideRule {
    threshold_bytes: usize,
}

impl BroadcastSmallSideRule {
    /// Build the rule with an explicit threshold (in bytes).
    pub fn with_threshold(threshold_bytes: usize) -> Self {
        Self { threshold_bytes }
    }

    /// Build the rule from a [`SessionConfig`]'s broadcast-threshold setting.
    /// Returns a rule with `threshold = 0` (no-op) if the session config has
    /// no [`BallistaConfig`] extension — Ballista features should only fire
    /// when the user has explicitly opted into Ballista.
    pub fn from_session_config(config: &datafusion::prelude::SessionConfig) -> Self {
        let threshold = config
            .options()
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.broadcast_join_threshold_bytes())
            .unwrap_or(0);
        Self::with_threshold(threshold)
    }
}

impl PhysicalOptimizerRule for BroadcastSmallSideRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.threshold_bytes == 0 {
            return Ok(plan);
        }
        let threshold = self.threshold_bytes;
        plan.transform_up(|p| try_broadcast_small_side(p, threshold))
            .data()
    }

    fn name(&self) -> &str {
        "BroadcastSmallSideRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn try_broadcast_small_side(
    plan: Arc<dyn ExecutionPlan>,
    threshold_bytes: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !matches!(join.partition_mode(), PartitionMode::Partitioned) {
        return Ok(Transformed::no(plan));
    }
    if !matches!(join.join_type(), JoinType::Inner) {
        return Ok(Transformed::no(plan));
    }
    if join.null_aware {
        // CollectLeft needs global probe-side state for null-aware semantics.
        return Ok(Transformed::no(plan));
    }

    let left_bytes = side_byte_size(join.left());
    let right_bytes = side_byte_size(join.right());

    let left_fits = left_bytes.is_some_and(|b| b > 0 && b < threshold_bytes);
    let right_fits = right_bytes.is_some_and(|b| b > 0 && b < threshold_bytes);

    let new_plan: Arc<dyn ExecutionPlan> = match (left_fits, right_fits) {
        (false, false) => return Ok(Transformed::no(plan)),
        (true, true) | (true, false) => {
            // Left fits → keep it on the build side.
            Arc::new(
                join.builder()
                    .with_partition_mode(PartitionMode::CollectLeft)
                    .build()?,
            )
        }
        (false, true) => {
            // Right fits → swap so the small side becomes the build side.
            if !join.join_type().supports_swap()
                || should_swap_join_order(&**join.left(), &**join.right())?
            {
                join.swap_inputs(PartitionMode::CollectLeft)?
            } else {
                Arc::new(
                    join.builder()
                        .with_partition_mode(PartitionMode::CollectLeft)
                        .build()?,
                )
            }
        }
    };
    Ok(Transformed::yes(new_plan))
}

fn side_byte_size(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    plan.partition_statistics(None)
        .ok()
        .and_then(|s| s.total_byte_size.get_value().copied())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::joins::HashJoinExecBuilder;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::test::exec::StatisticsExec;
    use datafusion::physical_plan::Partitioning;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn stats_exec(byte_size: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(byte_size / 4),
                total_byte_size: Precision::Inexact(byte_size),
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            (*schema()).clone(),
        ))
    }

    fn col(name: &str, idx: usize) -> PhysicalExprRef {
        Arc::new(Column::new(name, idx))
    }

    fn build_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        let on = vec![(col("id", 0), col("id", 0))];
        let (lk, rk): (Vec<_>, Vec<_>) = on.iter().cloned().unzip();
        let left = Arc::new(
            RepartitionExec::try_new(left, Partitioning::Hash(lk, 4)).unwrap(),
        );
        let right = Arc::new(
            RepartitionExec::try_new(right, Partitioning::Hash(rk, 4)).unwrap(),
        );
        Arc::new(
            HashJoinExecBuilder::new(left, right, on, join_type)
                .with_partition_mode(PartitionMode::Partitioned)
                .build()
                .unwrap(),
        )
    }

    fn join_partition_mode(plan: &Arc<dyn ExecutionPlan>) -> PartitionMode {
        // After a swap, HashJoinExec may sit under a ProjectionExec that
        // restores the original column order. Walk the tree to find it.
        let mut cur: Arc<dyn ExecutionPlan> = Arc::clone(plan);
        loop {
            if let Some(j) = cur.as_any().downcast_ref::<HashJoinExec>() {
                return *j.partition_mode();
            }
            let children = cur.children();
            assert_eq!(
                children.len(),
                1,
                "expected HashJoinExec along single-child path"
            );
            cur = Arc::clone(children[0]);
        }
    }

    #[test]
    fn broadcasts_when_left_below_threshold() {
        let small = stats_exec(1024);
        let big = stats_exec(100 * 1024 * 1024);
        let join = build_join(small, big, JoinType::Inner);
        let optimized = BroadcastSmallSideRule::with_threshold(1024 * 1024)
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(join_partition_mode(&optimized), PartitionMode::CollectLeft);
    }

    #[test]
    fn broadcasts_and_swaps_when_right_below_threshold() {
        let big = stats_exec(100 * 1024 * 1024);
        let small = stats_exec(1024);
        let join = build_join(big, small, JoinType::Inner);
        let optimized = BroadcastSmallSideRule::with_threshold(1024 * 1024)
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(join_partition_mode(&optimized), PartitionMode::CollectLeft);
    }

    #[test]
    fn skips_when_neither_side_fits() {
        let big1 = stats_exec(50 * 1024 * 1024);
        let big2 = stats_exec(100 * 1024 * 1024);
        let join = build_join(big1, big2, JoinType::Inner);
        let optimized = BroadcastSmallSideRule::with_threshold(1024 * 1024)
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(join_partition_mode(&optimized), PartitionMode::Partitioned);
    }

    #[test]
    fn skips_outer_join() {
        let small = stats_exec(1024);
        let big = stats_exec(100 * 1024 * 1024);
        let join = build_join(small, big, JoinType::Left);
        let optimized = BroadcastSmallSideRule::with_threshold(1024 * 1024)
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(join_partition_mode(&optimized), PartitionMode::Partitioned);
    }

    #[test]
    fn disabled_when_threshold_is_zero() {
        let small = stats_exec(1024);
        let big = stats_exec(100 * 1024 * 1024);
        let join = build_join(small, big, JoinType::Inner);
        let optimized = BroadcastSmallSideRule::with_threshold(0)
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(join_partition_mode(&optimized), PartitionMode::Partitioned);
    }
}
