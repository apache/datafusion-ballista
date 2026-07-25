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
//! # Alignment groups
//!
//! Leaf Exchanges are grouped by upstream partition count `M`, and each group
//! is decided independently.
//!
//! Grouping exists because hash-partitioned joins (`HashJoinExec(Partitioned)`,
//! `SortMergeJoinExec`) require their two inputs to have the same partition
//! count and the same hash mapping. Both legs read shuffle output written with
//! the same hash function on the same key, so upstream partition `i` of the
//! left and upstream partition `i` of the right hold rows that must meet at one
//! downstream partition. Coalescing them with the same `i → group(i)` mapping
//! keeps that meeting point; coalescing them differently scatters it. Every
//! member of a group therefore gets the *same* `CoalescePlan`, not merely the
//! same `K`.
//!
//! Grouping by `M` is sufficient, not merely convenient. DataFusion's
//! `EnforceDistribution` guarantees both sides of a `Partitioned` join have
//! equal partition counts, so two leaves feeding one partitioned join always
//! share an `M` and always land in the same group. Two leaves with different
//! `M` provably are not co-partitioned siblings of one join.
//!
//! Broadcast leaves are excluded from every group. A broadcast reader flattens
//! all upstream locations into a single output partition, so there is nothing
//! to coalesce, and a `CollectLeft` join requires `SinglePartition` on the
//! build side and `UnspecifiedDistribution` on the probe side, so a broadcast
//! leaf is never a co-partitioned sibling.
//!
//! The grouping is conservative in the other direction: leaves that share an
//! `M` without sharing any co-partitioning requirement, such as the two arms of
//! a union, still land in one group and have their sizes summed. That inflates
//! the per-index totals and under-coalesces, which is safe.
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
//!   1. Collect leaf `ExchangeExec`s. If none, this stage reads from scans and
//!      has nothing to coalesce.
//!   2. Clear every leaf's coalesce slot, so the pass recomputes wholesale and
//!      no decision survives from an earlier pass.
//!   3. Classify each leaf. Broadcast leaves drop out; unresolved, statistics-
//!      free, and inconsistent leaves make their group undecidable.
//!   4. Group the rest by `M`.
//!   5. Per group: sum per-partition byte sizes element-wise, then bin-pack the
//!      sums toward `target_partition_bytes` (Spark's
//!      `advisoryPartitionSizeInBytes`, 64 MB by default) with
//!      `split_size_list_by_target_size`.
//!   6. If `K >= M` the rewrite is not a reduction and the group is left alone.
//!      Otherwise attach one shared [`CoalescePlan`] to every member. `K == 1`
//!      is a legitimate outcome for a stage that fits in one partition.
//!
//! # Carrier semantics
//!
//! The `CoalescePlan` lives on the upstream `ExchangeExec`; the rule does not
//! rewrite the plan tree. Idempotency comes from the clear-then-set contract in
//! step 2 combined with the bin-pack being a pure function of resolved byte
//! sizes, which do not change once a stage has finalized.
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
//! The rule never rewrites the plan tree; decisions travel on each leaf's
//! interior-mutable slot and the input `Arc` is returned verbatim, preserving
//! `Arc::ptr_eq`. When `ballista.planner.coalesce.enabled=false` the rule
//! short-circuits before touching any slot.

use std::collections::BTreeMap;
use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::CoalescePlan;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, warn};

use crate::state::aqe::coalesce::{
    split_size_list_by_target_size, start_indices_to_partition_groups,
};
use crate::state::aqe::execution_plan::AdaptiveDatafusionExec;
use crate::state::aqe::execution_plan::ExchangeExec;

/// What one leaf `ExchangeExec` contributes to its alignment group.
#[derive(Debug, PartialEq, Eq)]
enum LeafKind {
    /// A broadcast exchange. Its reader flattens every upstream location into a
    /// single output partition, so there is no partition structure to coalesce,
    /// and a `CollectLeft` join never requires it to be co-partitioned with
    /// anything. Excluded from every alignment group.
    Broadcast,
    /// The upstream stage has not finalized. The group defers to a later pass.
    Unresolved,
    /// At least one location reports no `num_bytes`.
    UnknownBytes,
    /// The resolved location vector's length disagrees with the declared
    /// partition count. Should be unreachable for a non-broadcast exchange.
    Inconsistent { declared: usize, resolved: usize },
    /// Per-upstream-partition byte counts. Length equals the declared count.
    Sizes(Vec<u64>),
}

/// A leaf `ExchangeExec` reduced to what the rule needs: its alignment-group
/// key and its contribution to that group.
#[derive(Debug)]
struct ClassifiedLeaf {
    /// Upstream partition count `M`, the alignment-group key.
    m: usize,
    kind: LeafKind,
}

/// Reduce one leaf `ExchangeExec` to a [`ClassifiedLeaf`].
///
/// `m` is the declared partition count. For a `Sizes` leaf the resolved shape
/// is checked to match it, so `m == sizes.len()` holds for every leaf that
/// reaches the summing step and the summed vector can never be indexed out of
/// bounds.
fn classify_leaf(ex: &ExchangeExec) -> ClassifiedLeaf {
    let m = ex.properties().partitioning.partition_count();
    if ex.broadcast {
        return ClassifiedLeaf {
            m,
            kind: LeafKind::Broadcast,
        };
    }
    let Some(parts) = ex.shuffle_partitions() else {
        return ClassifiedLeaf {
            m,
            kind: LeafKind::Unresolved,
        };
    };
    if parts.len() != m {
        return ClassifiedLeaf {
            m,
            kind: LeafKind::Inconsistent {
                declared: m,
                resolved: parts.len(),
            },
        };
    }
    let mut sizes = Vec::with_capacity(parts.len());
    for locations in &parts {
        let mut total = 0u64;
        for location in locations {
            match location.partition_stats.num_bytes() {
                Some(bytes) => total = total.saturating_add(bytes),
                None => {
                    return ClassifiedLeaf {
                        m,
                        kind: LeafKind::UnknownBytes,
                    };
                }
            }
        }
        sizes.push(total);
    }
    ClassifiedLeaf {
        m,
        kind: LeafKind::Sizes(sizes),
    }
}

/// Partition classified leaves into alignment groups keyed by upstream
/// partition count `M`.
///
/// Broadcast leaves are dropped: they carry no partition structure to coalesce
/// and are never a co-partitioned join sibling, so excluding them cannot break
/// an alignment invariant.
///
/// A group maps to `Some(indices)` when every member carries usable sizes, and
/// to `None` when any member is unresolved, missing byte statistics, or
/// inconsistent. Skipping is per group: one undecidable leaf no longer
/// suppresses coalescing for leaves it has no relationship with.
///
/// `BTreeMap` rather than `HashMap` so iteration order, and therefore the debug
/// log and the order decisions are applied, is stable across passes.
fn group_by_upstream_count(
    leaves: &[ClassifiedLeaf],
) -> BTreeMap<usize, Option<Vec<usize>>> {
    let mut groups: BTreeMap<usize, Option<Vec<usize>>> = BTreeMap::new();
    for (idx, leaf) in leaves.iter().enumerate() {
        if leaf.kind == LeafKind::Broadcast {
            continue;
        }
        let entry = groups.entry(leaf.m).or_insert_with(|| Some(Vec::new()));
        match &leaf.kind {
            LeafKind::Sizes(_) => {
                if let Some(members) = entry {
                    members.push(idx);
                }
            }
            _ => *entry = None,
        }
    }
    groups
}

/// Unwrap a stage root to the subtree the rule inspects.
///
/// The root is either an `ExchangeExec` (intermediate stage) or an
/// `AdaptiveDatafusionExec` (final stage). Anything else means the adapter is
/// about to fail anyway, so the rule declines rather than guessing.
fn stage_input(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(exchange) = plan.downcast_ref::<ExchangeExec>() {
        Some(exchange.input().clone())
    } else if let Some(adaptive) = plan.downcast_ref::<AdaptiveDatafusionExec>() {
        Some(adaptive.input().clone())
    } else {
        None
    }
}

/// Collect every leaf `ExchangeExec` feeding this stage.
///
/// `Jump` after each hit stops the walk from descending into the upstream
/// stage's compute: those nodes belong to whatever stage wrote them, not to
/// this one.
fn collect_leaf_exchanges(
    input: &Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Vec<Arc<dyn ExecutionPlan>>> {
    let mut leaves: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    input.apply(|node| {
        if node.is::<ExchangeExec>() {
            leaves.push(node.clone());
            Ok(TreeNodeRecursion::Jump)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(leaves)
}

/// Downcast a collected leaf back to `&ExchangeExec`.
fn as_exchange(arc: &Arc<dyn ExecutionPlan>) -> &ExchangeExec {
    arc.downcast_ref::<ExchangeExec>()
        .expect("collect_leaf_exchanges filters to ExchangeExec")
}

/// Sum an alignment group's per-upstream-partition byte counts element-wise.
///
/// `summed[i]` is the total downstream work for upstream index `i` across the
/// whole group: for a partitioned join, the task reading group `i` reads that
/// index from both sides, so their sizes add.
///
/// `m` is the group's upstream partition count. Slices are zipped against the
/// accumulator rather than indexed, so a shorter or longer member cannot index
/// out of bounds. Addition saturates.
fn sum_sizes(sizes: &[&[u64]], m: usize) -> Vec<u64> {
    let mut summed = vec![0u64; m];
    for leaf in sizes {
        for (acc, &size) in summed.iter_mut().zip(leaf.iter()) {
            *acc = acc.saturating_add(size);
        }
    }
    summed
}

/// Bin-pack a summed size list into a coalesce decision.
///
/// Returns `None` when the rewrite would not reduce the partition count
/// (`K >= M`). That single test covers every degenerate case: an empty list and
/// a one-partition input both pack to `K = 1`, which is not below their `M`.
/// A `K` of 1 over a larger `M` *is* a reduction and is returned.
fn decide(
    summed: &[u64],
    target: u64,
    small_factor: f64,
    merged_factor: f64,
) -> Option<CoalescePlan> {
    let m = summed.len();
    let starts =
        split_size_list_by_target_size(summed, target, small_factor, merged_factor);
    let k = starts.len();
    if k >= m {
        debug!("[coalesce-rule] K={k} is not a reduction from M={m}; no decision");
        return None;
    }
    Some(CoalescePlan {
        upstream_partition_count: m as u32,
        groups: start_indices_to_partition_groups(&starts, m),
    })
}

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
        let Some(input) = stage_input(&plan) else {
            debug!(
                "[coalesce-rule] root is neither ExchangeExec nor AdaptiveDatafusionExec; bail"
            );
            return Ok(plan);
        };

        let leaves = collect_leaf_exchanges(&input)?;
        debug!(
            "[coalesce-rule] collected {} leaf ExchangeExec(s)",
            leaves.len()
        );
        if leaves.is_empty() {
            debug!("[coalesce-rule] no leaves; bail");
            return Ok(plan);
        }

        // Wholesale recompute: clear every leaf before deciding anything, so a
        // group can never be left with one member carrying a decision from an
        // earlier pass and another carrying none.
        for arc in &leaves {
            as_exchange(arc).set_coalesce(None);
        }

        let classified: Vec<ClassifiedLeaf> = leaves
            .iter()
            .map(|arc| classify_leaf(as_exchange(arc)))
            .collect();
        for (arc, leaf) in leaves.iter().zip(&classified) {
            let ex = as_exchange(arc);
            debug!(
                "[coalesce-rule]   leaf: plan_id={} stage_id={:?} partitioning={} M={} kind={:?}",
                ex.plan_id,
                ex.stage_id(),
                ex.properties().partitioning,
                leaf.m,
                leaf.kind,
            );
            if let LeafKind::Inconsistent { declared, resolved } = &leaf.kind {
                warn!(
                    "[coalesce-rule] leaf plan_id={} declares {declared} partitions but \
                     resolved {resolved}; skipping its alignment group",
                    ex.plan_id
                );
            }
        }

        for (m, members) in group_by_upstream_count(&classified) {
            let Some(members) = members else {
                debug!("[coalesce-rule] group M={m} has an unusable leaf; skipping");
                continue;
            };
            let sizes: Vec<&[u64]> = members
                .iter()
                .filter_map(|&idx| match &classified[idx].kind {
                    LeafKind::Sizes(sizes) => Some(sizes.as_slice()),
                    _ => None,
                })
                .collect();
            let summed = sum_sizes(&sizes, m);
            debug!("[coalesce-rule] group M={m} summed bytes: {summed:?}");

            let Some(cp) = decide(&summed, target, small, merged) else {
                continue;
            };
            let cp = Arc::new(cp);
            for &idx in &members {
                let ex = as_exchange(&leaves[idx]);
                debug!(
                    "[coalesce-rule] set_coalesce(K={}) on plan_id={}",
                    cp.groups.len(),
                    ex.plan_id,
                );
                ex.set_coalesce(Some(cp.clone()));
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The production defaults, at the byte scale the existing snapshot tests
    /// use, so a trace here lines up with a trace there.
    const TARGET: u64 = 200;
    const SMALL: f64 = 0.2;
    const MERGED: f64 = 1.2;

    fn decide_at_defaults(summed: &[u64]) -> Option<CoalescePlan> {
        decide(summed, TARGET, SMALL, MERGED)
    }

    #[test]
    fn sum_sizes_adds_element_wise() {
        let a: &[u64] = &[1, 2, 3];
        let b: &[u64] = &[10, 20, 30];
        assert_eq!(sum_sizes(&[a, b], 3), vec![11, 22, 33]);
    }

    #[test]
    fn sum_sizes_of_no_leaves_is_all_zero() {
        assert_eq!(sum_sizes(&[], 3), vec![0, 0, 0]);
    }

    #[test]
    fn sum_sizes_saturates_instead_of_overflowing() {
        // Byte counts come off the wire; a corrupt pair must not panic a
        // release-mode scheduler differently from a debug one.
        let a: &[u64] = &[u64::MAX];
        let b: &[u64] = &[1];
        assert_eq!(sum_sizes(&[a, b], 1), vec![u64::MAX]);
    }

    #[test]
    fn decide_declines_when_every_partition_is_already_full() {
        // 300 > 200 so each partition flushes on its own and the post-flush
        // merge is rejected: K = M = 8, no reduction, nothing to do.
        assert!(decide_at_defaults(&[300; 8]).is_none());
    }

    #[test]
    fn decide_declines_for_a_single_upstream_partition() {
        // M = 1 always packs to K = 1, which is not a reduction.
        assert!(decide_at_defaults(&[10]).is_none());
    }

    #[test]
    fn decide_declines_for_an_empty_size_list() {
        assert!(decide_at_defaults(&[]).is_none());
    }

    #[test]
    fn decide_packs_eight_fiftys_into_two_partitions() {
        // 4 x 50 fills a bucket to exactly 200; the fifth overshoots and
        // flushes; the remaining 4 fill the second bucket; the post-loop merge
        // is rejected (400 >= 200 * 1.2). This is the trace the existing
        // end-to-end snapshot asserts.
        let plan = decide_at_defaults(&[50; 8]).expect("K=2 is a reduction from M=8");
        assert_eq!(plan.upstream_partition_count, 8);
        assert_eq!(plan.groups.len(), 2);
        assert_eq!(plan.groups[0].upstream_indices, vec![0, 1, 2, 3]);
        assert_eq!(plan.groups[1].upstream_indices, vec![4, 5, 6, 7]);
    }

    #[test]
    fn decide_packs_a_tiny_stage_into_a_single_partition() {
        // 8 x 10 = 80 never reaches the 200 target, so the whole stage becomes
        // one downstream task. Previously refused by a `K <= 1` guard, which is
        // exactly the case where per-task overhead dominates.
        let plan = decide_at_defaults(&[10; 8]).expect("K=1 is a reduction from M=8");
        assert_eq!(plan.upstream_partition_count, 8);
        assert_eq!(plan.groups.len(), 1);
        assert_eq!(
            plan.groups[0].upstream_indices,
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    use crate::state::aqe::test::{
        mock_schema, partitions_with_byte_sizes, partitions_with_optional_byte_sizes,
    };
    use ballista_core::serde::scheduler::PartitionLocation;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::empty::EmptyExec;

    /// A shuffle exchange declaring `m` partitions, optionally resolved.
    fn shuffle_exchange(
        m: usize,
        resolved: Option<Vec<Vec<PartitionLocation>>>,
    ) -> ExchangeExec {
        let exchange = ExchangeExec::new(
            Arc::new(EmptyExec::new(mock_schema())),
            Some(Partitioning::UnknownPartitioning(m)),
            0,
        );
        if let Some(parts) = resolved {
            exchange.resolve_shuffle_partitions(parts);
        }
        exchange
    }

    #[test]
    fn classify_reads_sizes_from_the_resolved_shuffle_shape() {
        let exchange =
            shuffle_exchange(3, Some(partitions_with_byte_sizes(&[10, 20, 30])));

        let leaf = classify_leaf(&exchange);

        assert_eq!(leaf.m, 3);
        assert_eq!(leaf.kind, LeafKind::Sizes(vec![10, 20, 30]));
    }

    #[test]
    fn classify_reports_broadcast_regardless_of_resolved_shape() {
        // The Q22 regression guard. A broadcast exchange declares
        // `UnknownPartitioning(1)` but resolves to one entry per upstream
        // partition. Reading M from the declaration and indexing by the
        // resolution is what panicked; classification never lets a broadcast
        // leaf reach the summing step at all.
        let exchange =
            ExchangeExec::new_broadcast(Arc::new(EmptyExec::new(mock_schema())), None, 0);
        exchange.resolve_shuffle_partitions(partitions_with_byte_sizes(&[50; 8]));

        let leaf = classify_leaf(&exchange);

        assert_eq!(leaf.kind, LeafKind::Broadcast);
    }

    #[test]
    fn classify_reports_inconsistent_when_the_resolved_shape_disagrees() {
        let exchange = shuffle_exchange(8, Some(partitions_with_byte_sizes(&[50; 4])));

        let leaf = classify_leaf(&exchange);

        assert_eq!(
            leaf.kind,
            LeafKind::Inconsistent {
                declared: 8,
                resolved: 4
            }
        );
    }

    #[test]
    fn classify_reports_unresolved_before_the_upstream_stage_finalizes() {
        let exchange = shuffle_exchange(8, None);

        assert_eq!(classify_leaf(&exchange).kind, LeafKind::Unresolved);
    }

    #[test]
    fn classify_reports_unknown_bytes_when_a_location_has_no_size() {
        // One missing value makes the leaf's totals untrustworthy: counting it
        // as zero would let real partitions pack into an oversized task.
        let exchange = shuffle_exchange(
            3,
            Some(partitions_with_optional_byte_sizes(&[
                Some(10),
                None,
                Some(30),
            ])),
        );

        assert_eq!(classify_leaf(&exchange).kind, LeafKind::UnknownBytes);
    }

    fn sized(m: usize, sizes: Vec<u64>) -> ClassifiedLeaf {
        ClassifiedLeaf {
            m,
            kind: LeafKind::Sizes(sizes),
        }
    }

    fn broadcast(m: usize) -> ClassifiedLeaf {
        ClassifiedLeaf {
            m,
            kind: LeafKind::Broadcast,
        }
    }

    fn unresolved(m: usize) -> ClassifiedLeaf {
        ClassifiedLeaf {
            m,
            kind: LeafKind::Unresolved,
        }
    }

    #[test]
    fn grouping_splits_leaves_by_upstream_partition_count() {
        let leaves = vec![
            sized(8, vec![50; 8]),
            sized(8, vec![50; 8]),
            sized(4, vec![50; 4]),
            sized(4, vec![50; 4]),
        ];

        let groups = group_by_upstream_count(&leaves);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups.get(&8), Some(&Some(vec![0, 1])));
        assert_eq!(groups.get(&4), Some(&Some(vec![2, 3])));
    }

    #[test]
    fn grouping_drops_broadcast_leaves_without_disturbing_their_siblings() {
        // The #2166 case: a broadcast leaf must not take the shuffle leaves in
        // the same stage down with it.
        let leaves = vec![broadcast(1), sized(8, vec![50; 8]), sized(8, vec![50; 8])];

        let groups = group_by_upstream_count(&leaves);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups.get(&8), Some(&Some(vec![1, 2])));
        assert!(!groups.contains_key(&1));
    }

    #[test]
    fn grouping_skips_only_the_group_holding_an_unusable_leaf() {
        let leaves = vec![sized(8, vec![50; 8]), unresolved(8), sized(4, vec![50; 4])];

        let groups = group_by_upstream_count(&leaves);

        assert_eq!(groups.get(&8), Some(&None));
        assert_eq!(groups.get(&4), Some(&Some(vec![2])));
    }

    #[test]
    fn grouping_of_only_broadcast_leaves_is_empty() {
        assert!(group_by_upstream_count(&[broadcast(1)]).is_empty());
    }
}
