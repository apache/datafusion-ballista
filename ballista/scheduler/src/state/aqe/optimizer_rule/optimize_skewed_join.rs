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

//! Split skewed hash buckets into mapper ranges and replicate the matching
//! join input. Decisions are attached to exchanges before adapter conversion.
//!
//! Supports a single partitioned join connected by projections, filters and
//! per-partition sorts. Final aggregates, windows, limits and additional joins
//! can depend on whole hash buckets, so splitting through them would change
//! query results.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::SkewJoinPlan;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::common::JoinType;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};
use log::debug;

use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::skew_join::{
    map_ranges_for_upstream, pair_shards, skew_threshold, sort_mapper_outputs,
};

#[derive(Debug, Default)]
pub struct OptimizeSkewedJoinRule;

impl PhysicalOptimizerRule for OptimizeSkewedJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        if !bc.skew_join_enabled() {
            return Ok(plan);
        }
        // Partition-routed dynamic filters assume hash(key) % partition_count.
        // Shards deliberately break that invariant. Disable the umbrella option
        // datafusion.optimizer.enable_dynamic_filter_pushdown to use this rule.
        if config.optimizer.enable_join_dynamic_filter_pushdown {
            debug!("skew join skipped: join dynamic-filter pushdown is enabled");
            return Ok(plan);
        }

        let input = if let Some(exchange) = plan.downcast_ref::<ExchangeExec>() {
            // A passthrough boundary still advertises its original partition
            // count to the next stage. Only rewrite an intermediate stage when
            // its writer repartitions the shards back to that declared width.
            if exchange.partitioning.is_none() {
                return Ok(plan);
            }
            exchange.input()
        } else if let Some(adaptive) = plan.downcast_ref::<AdaptiveDatafusionExec>() {
            adaptive.input()
        } else {
            return Ok(plan);
        };
        // Stop at the first non-passthrough operator, including an exchange:
        // its input belongs to another stage and must not be rewritten here.
        let mut join = skip_passthrough(input);
        // Partial aggregates can emit more partial groups: the later final
        // aggregate merges them after the stage's repartitioning boundary.
        while let Some(aggregate) = join.downcast_ref::<AggregateExec>() {
            if aggregate.mode() != &AggregateMode::Partial || aggregate.fetch().is_some()
            {
                return Ok(plan);
            }
            join = skip_passthrough(aggregate.input());
        }
        let Some(join_type) = join_type_of(join) else {
            return Ok(plan);
        };
        let (split_left, split_right) = match join_type {
            JoinType::Inner => (true, true),
            JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark => (true, false),
            JoinType::Right => (false, true),
            _ => return Ok(plan),
        };
        let children = join.children();
        let Some(left) = skip_passthrough(children[0]).downcast_ref::<ExchangeExec>()
        else {
            return Ok(plan);
        };
        let Some(right) = skip_passthrough(children[1]).downcast_ref::<ExchangeExec>()
        else {
            return Ok(plan);
        };
        if [left, right].iter().any(|exchange| {
            exchange.coalesce().is_some()
                || exchange.skew_join().is_some()
                || exchange.input().output_ordering().is_some()
                || exchange.broadcast
                || exchange.range_repartition_routing().is_some()
                || !matches!(
                    exchange.properties().output_partitioning(),
                    Partitioning::Hash(_, _)
                )
        }) {
            return Ok(plan);
        }
        let (Some(mut left_parts), Some(mut right_parts)) =
            (left.shuffle_partitions(), right.shuffle_partitions())
        else {
            return Ok(plan);
        };
        let partition_count = left_parts.len();
        if right_parts.len() != partition_count {
            return Ok(plan);
        }
        sort_mapper_outputs(&mut left_parts);
        sort_mapper_outputs(&mut right_parts);
        let left_ranges = build_ranges(&left_parts, split_left, &bc);
        let right_ranges = build_ranges(&right_parts, split_right, &bc);
        let (left_shards, right_shards) = pair_shards(&left_ranges, &right_ranges);
        if left_shards.len() == partition_count {
            return Ok(plan);
        }
        debug!(
            "attaching SkewJoinPlan: partitions={partition_count}, shards={}",
            left_shards.len()
        );
        left.set_skew_join(Arc::new(SkewJoinPlan {
            upstream_partition_count: partition_count as u32,
            shards: left_shards,
        }));
        right.set_skew_join(Arc::new(SkewJoinPlan {
            upstream_partition_count: partition_count as u32,
            shards: right_shards,
        }));
        Ok(plan)
    }

    fn name(&self) -> &str {
        "OptimizeSkewedJoinRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// These operators can process each shard independently unless they carry a
/// fetch limit: applying that limit to each shard changes the selected rows.
fn skip_passthrough(mut plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    while plan.fetch().is_none()
        && (plan.is::<ProjectionExec>()
            || plan.is::<FilterExec>()
            || plan
                .downcast_ref::<SortExec>()
                .is_some_and(|sort| sort.preserve_partitioning()))
    {
        plan = plan.children()[0];
    }
    plan
}

fn join_type_of(plan: &Arc<dyn ExecutionPlan>) -> Option<JoinType> {
    if plan.fetch().is_some() {
        return None;
    }
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        return (*join.partition_mode() == PartitionMode::Partitioned)
            .then(|| *join.join_type());
    }
    plan.downcast_ref::<SortMergeJoinExec>()
        .map(|join| join.join_type())
}

fn build_ranges(
    partitions: &[Vec<PartitionLocation>],
    can_split: bool,
    config: &BallistaConfig,
) -> Vec<Vec<(u32, u32)>> {
    let sizes: Vec<u64> = partitions
        .iter()
        .map(|locations| {
            locations
                .iter()
                .filter_map(|location| location.partition_stats.num_bytes())
                .sum()
        })
        .collect();
    let threshold = skew_threshold(
        &sizes,
        config.skew_join_skewed_partition_factor(),
        config.skew_join_partition_threshold_bytes() as u64,
    );
    let advisory_bytes = config.skew_join_advisory_partition_bytes() as u64;
    let small_factor = config.skew_join_small_partition_factor();
    partitions
        .iter()
        .zip(sizes)
        .map(|(locations, bytes)| {
            if can_split && bytes > threshold {
                let mapper_bytes: Vec<_> = locations
                    .iter()
                    .map(|location| location.partition_stats.num_bytes().unwrap_or(0))
                    .collect();
                map_ranges_for_upstream(&mapper_bytes, advisory_bytes, small_factor)
            } else {
                vec![(0, locations.len() as u32)]
            }
        })
        .collect()
}
