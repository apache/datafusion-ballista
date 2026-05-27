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

//! AQE rule that splits skewed inputs of a partitioned hash / sort-merge join
//! and replicates the matching partition on the other side.
//!
//! [`OptimizeSkewedJoinRule`] runs once per `replan_stages()` pass on a stage
//! subtree whose root is either an [`ExchangeExec`] (intermediate stage) or
//! an [`AdaptiveDatafusionExec`] (final stage). Port of Spark's
//! `OptimizeSkewedJoin` AQE rule.
//!
//! # Mechanism
//!
//!   1. **Find** the binary join in the stage subtree
//!      (`HashJoinExec(Partitioned)` or `SortMergeJoinExec`). v1 handles
//!      stages with exactly one binary join; multi-join stages bail.
//!   2. **Apply** Spark's per-join-type split-side allowlist. `Full` is
//!      always skipped — splitting either side drops unmatched rows.
//!   3. **Descend** each side of the join through "passthrough" wrappers
//!      (`SortExec`, `ProjectionExec`, `FilterExec`, …) to the unique leaf
//!      [`ExchangeExec`]. Bails when a side has zero or more than one.
//!   4. **Detect skew** per side, per upstream index: a partition is skewed
//!      iff its bytes exceed BOTH `factor * median(per-side sizes)` AND an
//!      absolute `threshold_bytes`.
//!   5. **Split** each skewed upstream's per-mapper byte vector into
//!      contiguous `[start_map_idx, end_map_idx)` ranges via the coalesce
//!      module's `split_size_list_by_target_size`. Non-skewed upstreams stay
//!      as one passthrough range covering all mappers.
//!   6. **Cartesian-pair** the per-upstream left/right ranges: every left
//!      range pairs with every right range, producing `|L| × |R|` downstream
//!      tasks per upstream. When only one side is split N-way, the other
//!      side is read N times (Spark's "replicate the matching partition").
//!   7. **Attach** the two paired `SkewJoinPlan`s — one per leg — onto the
//!      two leaf Exchanges. The adapter consumes them at conversion time.
//!
//! # Correctness
//!
//! Each (left-shard-j, right-shard-j) pair is a downstream task. By
//! construction every (left-row, right-row) join pair is produced exactly
//! once:
//!
//! - When only the left is split N-way, the right bucket is replicated N
//!   times. Each right row appears in N tasks but is joined against a
//!   disjoint slice of left rows each time, so it pairs with each matching
//!   left row exactly once.
//! - For `LeftOuter`: every left row lives in exactly one shard; that
//!   shard's task does its own left-outer against the full right replica,
//!   so unmatched left rows are NULL-extended exactly once.
//! - For `RightOuter`: mirror argument on the right side.
//! - `Full` is excluded — symmetric replication would over-emit unmatched
//!   rows on whichever side gets replicated, and Spark also doesn't handle
//!   it here.
//!
//! # Why bail on multi-join stages
//!
//! Two joins sharing an Exchange in the same subtree would need a single
//! skew decision applied consistently across both joins' requirements —
//! beyond the scope of this v1 port. Spark handles it via a separate
//! per-join evaluation pass.
//!
//! # Default off
//!
//! `ballista.planner.skew_join.enabled = false`. The rule is an opt-in
//! trade — splitting skewed partitions adds scheduling overhead and
//! re-reads the replicated side's shuffle blocks, but eliminates the
//! straggler that comes from one fat join key. Workloads with no skew get
//! no benefit, only the overhead of the safety walk.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::SkewJoinPlan;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::common::JoinType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use log::debug;

use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::skew_join::{is_skewed, map_ranges_for_upstream, pair_shards};

/// AQE rule that attaches paired `SkewJoinPlan`s to the two leaf
/// `ExchangeExec`s of a binary join when one (or both) sides have a skewed
/// partition.
///
/// See module docs for design intent.
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

        // Mutual-exclusion guard with DataFusion's HashJoin dynamic-filter
        // pushdown. The dynamic filter builds a CASE expression keyed on
        // `hash(join_keys) % K'` that routes each probe row to one
        // partition's bounds. The skew rewrite intentionally violates the
        // hash-co-location invariant that routing assumes (the same key
        // now lives in multiple partitions of the split side), so the
        // routed CASE would filter out probe rows whose matches live in
        // a different partition than `hash(key) % K'` selects → silent
        // wrong results.
        //
        // The default for `enable_join_dynamic_filter_pushdown` in DF
        // 53.1 is `true`, so users wanting skew rewrite must explicitly
        // set it to `false`. Picked mutual exclusion (option 3 below)
        // for v1.
        //
        // Alternatives considered for future work:
        //   - **Plan mutation**: walk the subtree and rebuild any
        //     HashJoinExec with `.with_dynamic_filter(None)` before
        //     attaching skew_join. Targeted but more invasive — requires
        //     re-running `with_new_children` up the chain and reasoning
        //     about Arc identity for the carrier slots.
        //   - **Upstream DF fix**: add a "skew-compatible" fallback in
        //     `shared_bounds.rs` that uses union bounds instead of
        //     partition-routed bounds. Once landed, this guard becomes a
        //     `pass is_hash_co_located=false` call instead of a bail.
        //
        // Placed before the plan walk so we don't pay traversal cost
        // when the user has both flags on (a real misconfiguration the
        // log explicitly calls out).
        if config.optimizer.enable_join_dynamic_filter_pushdown {
            debug!(
                "[skew-join-rule] optimizer.enable_join_dynamic_filter_pushdown=true; \
                 mutually exclusive with skew_join rewrite (DataFusion's per-partition \
                 dynamic-filter routing is incompatible with split shards). \
                 Bail. Disable one or the other to proceed."
            );
            return Ok(plan);
        }

        let factor = bc.skew_join_skewed_partition_factor();
        let threshold_bytes = bc.skew_join_skewed_partition_threshold_bytes();
        let advisory_bytes = bc.skew_join_advisory_partition_bytes();
        let small_factor = bc.skew_join_small_partition_factor();

        debug!(
            "[skew-join-rule] fire: factor={factor} threshold_bytes={threshold_bytes} \
             advisory_bytes={advisory_bytes} small_factor={small_factor}",
        );

        // Two root kinds, same outcome — same convention as CoalescePartitionsRule.
        let input = if let Some(ex) = plan.as_any().downcast_ref::<ExchangeExec>() {
            ex.input().clone()
        } else if let Some(adp) = plan.as_any().downcast_ref::<AdaptiveDatafusionExec>() {
            adp.input().clone()
        } else {
            debug!(
                "[skew-join-rule] root is neither ExchangeExec nor AdaptiveDatafusionExec; bail"
            );
            return Ok(plan);
        };

        // Find the binary join. v1 requires exactly one; multi-join stages
        // bail (Spark iterates per-join — a future pass can do the same).
        let mut joins: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        input.apply(|node| {
            if join_type_of(node).is_some() {
                joins.push(node.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if joins.len() != 1 {
            debug!(
                "[skew-join-rule] subtree has {} binary joins (need exactly 1); bail",
                joins.len()
            );
            return Ok(plan);
        }
        let join = &joins[0];
        let jt = join_type_of(join).expect("filtered above to Some");

        let (can_split_left, can_split_right) = split_allowlist(jt);
        if !can_split_left && !can_split_right {
            debug!("[skew-join-rule] join_type={jt:?} disallows splitting either side; bail");
            return Ok(plan);
        }

        // Descend each leg to the unique ExchangeExec leaf.
        let join_children = join.children();
        debug_assert_eq!(
            join_children.len(),
            2,
            "binary joins (HashJoinExec/SortMergeJoinExec) must have exactly 2 children",
        );
        let Some(left_leaf_arc) = single_exchange_leaf(join_children[0])? else {
            debug!("[skew-join-rule] left side has no unique ExchangeExec leaf; bail");
            return Ok(plan);
        };
        let Some(right_leaf_arc) = single_exchange_leaf(join_children[1])? else {
            debug!("[skew-join-rule] right side has no unique ExchangeExec leaf; bail");
            return Ok(plan);
        };
        let left_leaf = as_exchange(&left_leaf_arc);
        let right_leaf = as_exchange(&right_leaf_arc);

        // Idempotence guard: don't fight a coalesce or a prior skew-join pass.
        if left_leaf.coalesce().is_some()
            || left_leaf.skew_join().is_some()
            || right_leaf.coalesce().is_some()
            || right_leaf.skew_join().is_some()
        {
            debug!("[skew-join-rule] at least one leaf already has coalesce/skew_join set; bail");
            return Ok(plan);
        }

        // Both leaves must be resolved with matching M. Heterogeneous M
        // between legs is a Q22-style edge case the rule isn't designed
        // for — coalesce bails on it too.
        let Some(left_parts) = left_leaf.shuffle_partitions() else {
            debug!("[skew-join-rule] left leaf unresolved; bail (will rerun next pass)");
            return Ok(plan);
        };
        let Some(right_parts) = right_leaf.shuffle_partitions() else {
            debug!("[skew-join-rule] right leaf unresolved; bail (will rerun next pass)");
            return Ok(plan);
        };
        let m = left_parts.len();
        if right_parts.len() != m {
            debug!(
                "[skew-join-rule] heterogeneous M (left={} right={}); bail",
                left_parts.len(),
                right_parts.len()
            );
            return Ok(plan);
        }

        // Per-side per-upstream byte totals.
        let left_sizes: Vec<u64> = left_parts.iter().map(|locs| sum_bytes(locs)).collect();
        let right_sizes: Vec<u64> =
            right_parts.iter().map(|locs| sum_bytes(locs)).collect();

        // Per-side per-upstream skew decisions, gated by the allowlist.
        let left_skewed: Vec<bool> = left_sizes
            .iter()
            .map(|&b| can_split_left && is_skewed(b, &left_sizes, factor, threshold_bytes))
            .collect();
        let right_skewed: Vec<bool> = right_sizes
            .iter()
            .map(|&b| {
                can_split_right && is_skewed(b, &right_sizes, factor, threshold_bytes)
            })
            .collect();

        if left_skewed.iter().all(|&b| !b) && right_skewed.iter().all(|&b| !b) {
            debug!("[skew-join-rule] no upstream qualifies as skewed; bail");
            return Ok(plan);
        }

        // Build per-upstream sub-ranges for each side. Skewed → bin-pack
        // per-mapper bytes near advisory_bytes; non-skewed → passthrough.
        let left_ranges = build_ranges(
            &left_parts,
            &left_skewed,
            advisory_bytes,
            small_factor,
        );
        let right_ranges = build_ranges(
            &right_parts,
            &right_skewed,
            advisory_bytes,
            small_factor,
        );

        let (left_shards, right_shards) = pair_shards(&left_ranges, &right_ranges);
        let k_prime = left_shards.len();
        debug_assert_eq!(k_prime, right_shards.len());

        // No fan-out means no benefit, only overhead — bail.
        if k_prime <= m {
            debug!("[skew-join-rule] K'={k_prime} <= M={m}; no benefit, bail");
            return Ok(plan);
        }

        debug!(
            "[skew-join-rule] attaching SkewJoinPlan: M={m} K'={k_prime} \
             (skew bits left={left_skewed:?} right={right_skewed:?})"
        );
        left_leaf.set_skew_join(Arc::new(SkewJoinPlan {
            upstream_partition_count: m as u32,
            shards: left_shards,
        }));
        right_leaf.set_skew_join(Arc::new(SkewJoinPlan {
            upstream_partition_count: m as u32,
            shards: right_shards,
        }));
        Ok(plan)
    }

    fn name(&self) -> &str {
        "OptimizeSkewedJoinRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Returns the join type if `node` is a binary join eligible for the skew
/// rewrite. Broadcast hash joins (`PartitionMode::CollectLeft`) and
/// pre-broadcast `Auto` modes are excluded — they have no per-side shuffle
/// the rule could split.
fn join_type_of(node: &Arc<dyn ExecutionPlan>) -> Option<JoinType> {
    if let Some(h) = node.as_any().downcast_ref::<HashJoinExec>() {
        return match h.partition_mode() {
            PartitionMode::Partitioned => Some(*h.join_type()),
            _ => None,
        };
    }
    if let Some(s) = node.as_any().downcast_ref::<SortMergeJoinExec>() {
        return Some(s.join_type());
    }
    None
}

/// Spark's per-join-type split-side allowlist: `(can_split_left, can_split_right)`.
///
/// Source: `OptimizeSkewedJoin.scala` — `canSplitLeftSide` / `canSplitRightSide`.
/// `Full` returns `(false, false)`. Right-side semi/anti/mark variants fall
/// into the conservative catch-all and are not split.
fn split_allowlist(jt: JoinType) -> (bool, bool) {
    match jt {
        JoinType::Inner => (true, true),
        JoinType::Left => (true, false),
        JoinType::Right => (false, true),
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => (true, false),
        _ => (false, false),
    }
}

/// Walks `root` and returns the unique descendant `ExchangeExec` Arc, or
/// `None` if zero or more than one is present. Stops descending at every
/// Exchange (the upstream stage's compute belongs to a different subtree).
fn single_exchange_leaf(
    root: &Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
    let mut exchanges: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    root.apply(|node| {
        if node.as_any().is::<ExchangeExec>() {
            exchanges.push(node.clone());
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(if exchanges.len() == 1 {
        Some(exchanges.remove(0))
    } else {
        None
    })
}

/// Same one-liner used by the coalesce rule. The cast is infallible because
/// the caller filtered to `ExchangeExec` already.
fn as_exchange(arc: &Arc<dyn ExecutionPlan>) -> &ExchangeExec {
    arc.as_any()
        .downcast_ref::<ExchangeExec>()
        .expect("filtered to ExchangeExec above")
}

/// Build the `(start_map_idx, end_map_idx)` ranges per upstream for one side.
fn build_ranges(
    parts: &[Vec<PartitionLocation>],
    skewed: &[bool],
    advisory_bytes: u64,
    small_factor: f64,
) -> Vec<Vec<(u32, u32)>> {
    parts
        .iter()
        .zip(skewed.iter())
        .map(|(locs, &is_skewed)| {
            if is_skewed {
                let per_mapper: Vec<u64> = locs
                    .iter()
                    .map(|loc| loc.partition_stats.num_bytes().unwrap_or(0))
                    .collect();
                map_ranges_for_upstream(&per_mapper, advisory_bytes, small_factor)
            } else {
                vec![(0, locs.len() as u32)]
            }
        })
        .collect()
}

fn sum_bytes(locs: &[PartitionLocation]) -> u64 {
    locs.iter()
        .filter_map(|l| l.partition_stats.num_bytes())
        .sum()
}
