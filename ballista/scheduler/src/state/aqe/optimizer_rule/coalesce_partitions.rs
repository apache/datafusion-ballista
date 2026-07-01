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

//! AQE rule that coalesces shuffle partitions after upstream stages finalize.
//!
//! [`CoalescePartitionsRule`] runs once per `replan_stages()` pass on a
//! stage subtree whose root is either an [`ExchangeExec`] (intermediate
//! stage) or an [`AdaptiveDatafusionExec`] (final stage). The rule walks the
//! subtree, collects every leaf [`ExchangeExec`] — the resolved upstream
//! shuffles feeding this stage — and decides whether to coalesce.
//!
//! # The alignment group
//!
//! Every leaf `ExchangeExec` in a single stage subtree forms one **alignment
//! group**. Why one group, not one decision per leaf?
//!
//! - Hash-partitioned joins (`HashJoinExec(Partitioned)`, `SortMergeJoinExec`)
//!   require their two inputs to have the *same partition count* and to be
//!   hash-partitioned on the join key. If we coalesced left to `K=4` and
//!   right to `K=2`, DataFusion's `EnforceDistribution` would either reject
//!   the plan or insert remediation repartitions that undo the optimization.
//! - Both join legs read shuffle output from upstream stages that wrote
//!   `M` partitions using the *same* hash function on the *same* key
//!   (that's what made them joinable in the first place). So upstream
//!   partition `i` of the left and upstream partition `i` of the right
//!   hold rows that must meet at downstream partition `f(i)`. Coalescing
//!   them with the *same* mapping `i → group(i)` keeps that meeting point
//!   consistent; coalescing them with different mappings scatters it.
//!
//! Practically: we treat all leaf Exchanges as a single workload, sum their
//! per-partition byte counts element-wise, bin-pack the summed sizes once,
//! and attach the *same* `CoalescePlan` to every leaf. Joins with two leaves
//! and chains of joins with three or more leaves all go through the same
//! code path — there is no per-leaf decision.
//!
//! Concretely for `[25; 8]` bytes per partition on both sides of a join:
//! summed `[50; 8]`, bin-pack at target `200` produces `K=2` (4 upstream
//! partitions per group), both leaves get `coalesce=2 of 8`, the downstream
//! join runs with 2 partitions on each side, hash buckets stay aligned.
//!
//! # Default off
//!
//! `ballista.planner.coalesce.enabled` is `false` by default. The rule is an opt-in
//! trade — coalescing reduces task overhead and IPC cost, but at the price
//! of less downstream parallelism. Users who want the trade explicitly turn
//! the rule on. When off, the rule short-circuits at the first statement
//! of `optimize()` and the plan flows through untouched.
//!
//! Conceptually:
//!   - `coalesce.enabled=false` (default) ≈ Spark's `parallelismFirst=true`
//!     outcome — partitions preserved, no packing.
//!   - `coalesce.enabled=true` (opt-in) ≈ Spark's `parallelismFirst=false`
//!     outcome — pack toward the advisory target, accept fewer/larger tasks.
//!
//! # Algorithm
//!
//!   1. Find leaf `ExchangeExec`s — the alignment group. If empty, this
//!      stage reads from scans and has nothing to coalesce.
//!   2. All leaves share the upstream partition count `M` (the writer side).
//!   3. Sum per-partition byte sizes element-wise across the group to get
//!      combined work per upstream index.
//!   4. Bin-pack the summed sizes into `K` buckets near
//!      `target_partition_bytes` (Spark's `advisoryPartitionSizeInBytes`,
//!      64 MB by default) using `split_size_list_by_target_size`.
//!   5. If `K >= M` or `K <= 1`, the rewrite is degenerate and is skipped.
//!   6. Otherwise, attach a shared [`CoalescePlan`] (with `K` partition
//!      groups) to every leaf `ExchangeExec` via `set_coalesce(..)`. The
//!      adapter consumes that decision when it builds the downstream
//!      `ShuffleReaderExec`s.
//!
//! # Carrier semantics
//!
//! The `CoalescePlan` lives on the upstream `ExchangeExec`; the rule does
//! not rewrite the plan tree. Idempotency is structural — `set_coalesce`
//! overwrites the slot with an equivalent plan on re-entry, and the
//! bin-pack is a pure function of the resolved byte sizes, so the second
//! pass produces the same decision.
//!
//! # Grouping discipline
//!
//! The bin-pack groups **neighboring** upstream partitions only — each
//! output partition `k` covers a contiguous index range `[start_k,
//! start_k+1)` over the input. Non-adjacent partitions are never folded
//! together, even when that would yield a tighter byte fit. This matches
//! Spark's `CoalesceShufflePartitions` and is what keeps hash
//! co-partitioning intact across the rewrite: a hash bucket that used to
//! live at index `i` still lives in the single output group that covers
//! `i`, on every leaf of the alignment group.
//!
//! # Behavior preservation
//!
//! When `ballista.planner.coalesce.enabled=false`, when the subtree has no
//! leaf Exchanges, or when the bin-pack returns a degenerate `K`, the rule
//! is a no-op and returns the input `Arc` verbatim (preserving
//! `Arc::ptr_eq`).

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::CoalescePlan;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;

use crate::state::aqe::coalesce::{
    split_size_list_by_target_size, start_indices_to_partition_groups,
};
use crate::state::aqe::execution_plan::AdaptiveDatafusionExec;
use crate::state::aqe::execution_plan::ExchangeExec;

/// AQE rule that attaches a coalesce decision to every leaf `ExchangeExec`
/// feeding the current stage, so the downstream reader exposes `K < M`
/// partitions.
///
/// See module docs for design intent.
#[derive(Debug, Default)]
pub struct CoalescePartitionsRule;

impl PhysicalOptimizerRule for CoalescePartitionsRule {
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
        if !bc.coalesce_enabled() {
            return Ok(plan);
        }
        let target = bc.coalesce_target_partition_bytes();
        let small = bc.coalesce_small_partition_factor();
        let merged = bc.coalesce_merged_partition_factor();

        debug!(
            "[coalesce-rule] fire: target_partition_bytes={target} small_factor={small} merged_factor={merged}",
        );

        // Get the subtree below the root. Two root kinds, same outcome.
        let input = if let Some(ex) = plan.downcast_ref::<ExchangeExec>() {
            debug!(
                "[coalesce-rule] root=ExchangeExec plan_id={} stage_id={:?} stage_resolved={}",
                ex.plan_id,
                ex.stage_id(),
                ex.shuffle_partitions().is_some(),
            );
            ex.input().clone()
        } else if let Some(adp) = plan.downcast_ref::<AdaptiveDatafusionExec>() {
            debug!(
                "[coalesce-rule] root=AdaptiveDatafusionExec stage_id={:?}",
                adp.stage_id(),
            );
            adp.input().clone()
        } else {
            debug!(
                "[coalesce-rule] root is neither ExchangeExec nor AdaptiveDatafusionExec; bail"
            );
            return Ok(plan); // unexpected root — adapter will fail anyway, just bail
        };

        // Collect the alignment group: every leaf `ExchangeExec` feeding
        // this stage. `Jump` after each hit stops the walk from descending
        // into the upstream stage's compute — those nodes aren't part of
        // *this* stage's group, they belong to whatever stage wrote them.
        let mut leaves: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        input.apply(|node| {
            if node.is::<ExchangeExec>() {
                leaves.push(node.clone());
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        // Helper: downcast each Arc back to &ExchangeExec.
        fn as_exchange(arc: &Arc<dyn ExecutionPlan>) -> &ExchangeExec {
            arc.downcast_ref::<ExchangeExec>()
                .expect("filtered to ExchangeExec above")
        }

        debug!(
            "[coalesce-rule] collected {} leaf ExchangeExec(s)",
            leaves.len()
        );
        for arc in &leaves {
            let ex = as_exchange(arc);
            debug!(
                "[coalesce-rule]   leaf: plan_id={} stage_id={:?} partitioning={} M={} resolved={} existing_coalesce={:?}",
                ex.plan_id,
                ex.stage_id(),
                ex.properties().partitioning,
                ex.properties().partitioning.partition_count(),
                ex.shuffle_partitions().is_some(),
                ex.coalesce()
                    .as_ref()
                    .map(|cp| (cp.groups.len(), cp.upstream_partition_count)),
            );
        }

        // Leaf-scan stage with no upstream Exchanges → nothing to coalesce.
        if leaves.is_empty() {
            debug!("[coalesce-rule] no leaves; bail");
            return Ok(plan);
        }

        // this is temporary fix until we figure it out how to
        // make this work with broadcast
        if leaves.iter().any(|arc| as_exchange(arc).broadcast) {
            debug!("[coalesce-rule] broadcast leaf present; bail entire group");
            return Ok(plan);
        }

        // The alignment-group invariant assumes a shared `M`. In every plan
        // shape we currently produce, all leaves of one stage subtree are
        // hash-partitioned by the same target_partitions setting upstream,
        // so reading `M` from leaf 0 is sufficient.
        let m = as_exchange(&leaves[0])
            .properties()
            .partitioning
            .partition_count();

        // TODO: per-M subgrouping; for now bail on heterogeneous M (Q22 panic guard).
        if leaves
            .iter()
            .any(|arc| as_exchange(arc).properties().partitioning.partition_count() != m)
        {
            return Ok(plan);
        }

        // Sum byte sizes element-wise across the alignment group. Upstream
        // partition `i` is the same logical hash bucket on every leaf, so
        // `summed[i]` is the total downstream work for that bucket. If any
        // leaf is still unresolved we bail — early `replan_stages()` passes
        // run before all upstream stages finalize, and the rule reruns on
        // every later pass anyway, so the no-op is free.
        let mut summed = vec![0u64; m];
        for arc in &leaves {
            let ex = as_exchange(arc);
            let Some(parts) = ex.shuffle_partitions() else {
                debug!(
                    "[coalesce-rule] leaf plan_id={} unresolved; bail entire group",
                    ex.plan_id
                );
                return Ok(plan);
            };
            for (i, locs) in parts.iter().enumerate() {
                summed[i] += locs
                    .iter()
                    .filter_map(|l| l.partition_stats.num_bytes())
                    .sum::<u64>();
            }
        }
        debug!("[coalesce-rule] summed bytes per upstream partition: {summed:?}");

        // One bin-pack decision for the whole alignment group, packing toward
        // `target_partition_bytes` (Spark's `advisoryPartitionSizeInBytes`).
        // The rule is opt-in (`coalesce.enabled=false` by default), so users
        // get parallelism preservation unless they explicitly trade it for
        // larger tasks. This corresponds to Spark's
        // `parallelismFirst=false` mode — direct advisory-driven packing.
        let starts = split_size_list_by_target_size(&summed, target, small, merged);
        let k = starts.len();
        debug!("[coalesce-rule] bin-pack result: K={k} M={m}");
        if k >= m || k <= 1 {
            debug!(
                "[coalesce-rule] K degenerate (K>=M or K<=1); bail without setting coalesce"
            );
            return Ok(plan);
        }

        // Attach the same `CoalescePlan` to every member of the alignment
        // group. Sharing the plan (not just the K value) keeps the upstream
        // index → group mapping identical across leaves — hash buckets that
        // were aligned at M stay aligned at K, and the join's
        // partition-count requirement still holds after the rewrite.
        let cp = Arc::new(CoalescePlan {
            upstream_partition_count: m as u32,
            groups: start_indices_to_partition_groups(&starts, m),
        });
        for arc in &leaves {
            let ex = as_exchange(arc);
            debug!(
                "[coalesce-rule] set_coalesce(K={k}) on plan_id={} (was {:?})",
                ex.plan_id,
                ex.coalesce().as_ref().map(|cp| cp.groups.len()),
            );
            ex.set_coalesce(cp.clone());
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "CoalescePartitionsRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
