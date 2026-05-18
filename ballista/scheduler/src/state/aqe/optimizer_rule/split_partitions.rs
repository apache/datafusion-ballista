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

//! AQE rule that splits skewed shuffle partitions after upstream stages finalize.
//!
//! [`SplitPartitionsRule`] runs once per `replan_stages()` pass on a stage
//! subtree whose root is either an [`ExchangeExec`] (intermediate stage) or
//! an [`AdaptiveDatafusionExec`] (final stage). The rule mirrors
//! [`CoalescePartitionsRule`](super::CoalescePartitionsRule) — same per-stage
//! invocation, same alignment-group leaf walk — but applies the inverse
//! transform: when one upstream partition is far larger than the median, fan
//! it out across multiple downstream reader tasks instead of folding small
//! partitions together.
//!
//! # v1 mechanism: file-list sharding
//!
//! The reader side already lists multiple `PartitionLocation`s per output
//! partition (one per upstream map task). Splitting just means handing those
//! locations to several reader tasks via round-robin assignment. No row-range
//! reads, no protobuf changes, no executor changes.
//!
//! Tradeoff: a partition backed by only one file can't be split this way —
//! the rule bails on that idx (factor stays at 1). v2 row-range reads lift
//! this restriction.
//!
//! # Why bail on hash/single-partition consumers
//!
//! File-list sharding produces `UnknownPartitioning(K')` output. Rows that
//! used to land in the same hash bucket on the M-side are now scattered
//! across multiple K'-side partitions (the round-robin assignment is
//! file-keyed, not row-keyed). That breaks:
//!
//! - `HashJoinExec(Partitioned)` and `SortMergeJoinExec` — both legs must
//!   have matching hash buckets, scattering one side desynchronises the join.
//! - `AggregateExec(FinalPartitioned)` — assumes each downstream partition
//!   holds a closed set of group keys. Scattering keys across K' partitions
//!   makes the final emit duplicate rows per group.
//! - Any operator with `Distribution::HashPartitioned(_)` or
//!   `Distribution::SinglePartition` in `required_input_distribution()`.
//!
//! Rather than enumerate operator types, the rule walks the stage subtree
//! and checks `required_input_distribution()` directly. If any node above
//! the leaves demands hash or single-partition input, the whole stage bails.
//! This is strictly correct and accommodates new DataFusion operators
//! without source changes.
//!
//! # The alignment group (same invariant as coalesce)
//!
//! Even though we bail on joins, multi-leaf non-join stages (e.g. UNION ALL)
//! still need a *single* split decision applied uniformly to every leaf —
//! otherwise downstream K' would differ across legs, which the planner
//! cannot represent. The rule sums byte sizes element-wise across all
//! leaves (just like coalesce) and attaches the same `SplitPlan` to each.
//!
//! # Default off
//!
//! `ballista.planner.split.enabled = false` by default. v1 has narrow
//! real-world applicability (most queries hit a hash/single distribution
//! requirement on the path and bail), so opt-in keeps the cost-of-being-
//! enabled at zero for users who don't have the workload it helps.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::SplitPlan;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{Distribution, ExecutionPlan};
use log::debug;

use crate::state::aqe::execution_plan::AdaptiveDatafusionExec;
use crate::state::aqe::execution_plan::ExchangeExec;
use crate::state::aqe::split::{decide_split_factors, factors_to_shards};

/// AQE rule that attaches a split decision to every leaf `ExchangeExec`
/// feeding the current stage when one upstream partition is far larger than
/// the median.
///
/// See module docs for design intent.
#[derive(Debug, Default)]
pub struct SplitPartitionsRule;

impl PhysicalOptimizerRule for SplitPartitionsRule {
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
        if !bc.split_enabled() {
            return Ok(plan);
        }
        let skew_factor = bc.split_skew_factor();
        let min_split_bytes = bc.split_min_split_bytes();
        let max_split_factor = bc.split_max_split_factor();

        debug!(
            "[split-rule] fire: skew_factor={skew_factor} min_split_bytes={min_split_bytes} \
             max_split_factor={max_split_factor}",
        );

        // Get the subtree below the root. Two root kinds, same outcome —
        // same pattern as `CoalescePartitionsRule`.
        let input = if let Some(ex) = plan.as_any().downcast_ref::<ExchangeExec>() {
            debug!(
                "[split-rule] root=ExchangeExec plan_id={} stage_id={:?} stage_resolved={}",
                ex.plan_id,
                ex.stage_id(),
                ex.shuffle_partitions().is_some(),
            );
            ex.input().clone()
        } else if let Some(adp) = plan.as_any().downcast_ref::<AdaptiveDatafusionExec>() {
            debug!(
                "[split-rule] root=AdaptiveDatafusionExec stage_id={:?}",
                adp.stage_id(),
            );
            adp.input().clone()
        } else {
            debug!(
                "[split-rule] root is neither ExchangeExec nor AdaptiveDatafusionExec; bail"
            );
            return Ok(plan);
        };

        // Single subtree walk that does two things at once:
        //   - Distribution-safety check: if any operator demands hash or
        //     single-partition input, bail (splitting would scatter rows
        //     that operator needs colocated — catches HashJoinExec
        //     (Partitioned), SortMergeJoinExec, AggregateExec
        //     (FinalPartitioned), and any future operator with the same
        //     requirement).
        //   - Leaf collection: every `ExchangeExec` becomes a member of
        //     the alignment group. `Jump` after each hit stops descent
        //     into the upstream stage's compute.
        let mut leaves: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let mut unsafe_dist = false;
        input.apply(|node| {
            if node.as_any().is::<ExchangeExec>() {
                leaves.push(node.clone());
                return Ok(TreeNodeRecursion::Jump);
            }
            for dist in node.required_input_distribution() {
                if matches!(
                    dist,
                    Distribution::HashPartitioned(_) | Distribution::SinglePartition
                ) {
                    unsafe_dist = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if unsafe_dist {
            debug!("[split-rule] subtree has hash/single-partition consumer; bail");
            return Ok(plan);
        }

        // Helper: downcast each Arc back to &ExchangeExec.
        fn as_exchange(arc: &Arc<dyn ExecutionPlan>) -> &ExchangeExec {
            arc.as_any()
                .downcast_ref::<ExchangeExec>()
                .expect("filtered to ExchangeExec above")
        }

        debug!(
            "[split-rule] collected {} leaf ExchangeExec(s)",
            leaves.len()
        );

        // Leaf-scan stage with no upstream Exchanges → nothing to split.
        if leaves.is_empty() {
            debug!("[split-rule] no leaves; bail");
            return Ok(plan);
        }

        // Conflict / idempotence guard: another rule (coalesce) or a prior
        // pass already wrote a decision to one of the leaves. Either way,
        // don't fight it.
        if leaves.iter().any(|l| {
            as_exchange(l).coalesce().is_some() || as_exchange(l).split().is_some()
        }) {
            debug!("[split-rule] at least one leaf already has coalesce/split set; bail");
            return Ok(plan);
        }

        // Alignment-group invariant: all leaves share `M`. Same Q22 guard as
        // coalesce.
        let m = as_exchange(&leaves[0])
            .properties()
            .partitioning
            .partition_count();
        if leaves
            .iter()
            .any(|arc| as_exchange(arc).properties().partitioning.partition_count() != m)
        {
            debug!("[split-rule] heterogeneous M across leaves; bail");
            return Ok(plan);
        }

        // Sum byte sizes element-wise across the alignment group; capture
        // the first leaf's file-count vector for the v1 single-file guard.
        // If any leaf is still unresolved, bail — the rule reruns on the
        // next replan_stages pass anyway.
        let mut summed = vec![0u64; m];
        let mut file_counts: Vec<usize> = vec![0; m];
        for (leaf_ix, arc) in leaves.iter().enumerate() {
            let ex = as_exchange(arc);
            let Some(parts) = ex.shuffle_partitions() else {
                debug!(
                    "[split-rule] leaf plan_id={} unresolved; bail entire group",
                    ex.plan_id
                );
                return Ok(plan);
            };
            for (i, locs) in parts.iter().enumerate() {
                summed[i] += locs
                    .iter()
                    .filter_map(|l| l.partition_stats.num_bytes())
                    .sum::<u64>();
                if leaf_ix == 0 {
                    file_counts[i] = locs.len();
                }
            }
        }
        debug!(
            "[split-rule] summed bytes per upstream partition: {summed:?}, \
             leaf-0 file_counts: {file_counts:?}"
        );

        let factors = decide_split_factors(
            &summed,
            &file_counts,
            skew_factor,
            min_split_bytes,
            max_split_factor,
        );
        debug!("[split-rule] decision factors per upstream partition: {factors:?}");

        if factors.iter().all(|&f| f == 1) {
            debug!("[split-rule] all factors == 1 (no skewed partition qualifies); bail");
            return Ok(plan);
        }

        // Attach the same `SplitPlan` to every member of the alignment group.
        // Sharing the plan (not just the K' value) keeps the per-idx fan-out
        // identical across leaves — even non-join multi-leaf shapes (UNION)
        // need this for the downstream K' to be uniform.
        let sp = Arc::new(SplitPlan {
            upstream_partition_count: m as u32,
            shards: factors_to_shards(&factors),
        });
        let k_prime = sp.shards.len();
        for arc in &leaves {
            let ex = as_exchange(arc);
            debug!(
                "[split-rule] set_split(K'={k_prime}) on plan_id={} (M={m})",
                ex.plan_id,
            );
            ex.set_split(sp.clone());
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "SplitPartitionsRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
