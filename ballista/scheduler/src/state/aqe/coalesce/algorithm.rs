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

//! Bin-packing helpers that turn per-partition byte sizes into coalesce
//! decisions.
//!
//! `split_size_list_by_target_size` walks the size list left-to-right,
//! accumulating into a bucket and flushing when adding the next size would
//! overshoot `target`. Two refinements smooth out pathological shapes that
//! pure overshoot-flushing produces:
//!
//! - **merged-factor early flush** — when a small flushed bucket sits next
//!   to another small one, fold them together rather than leaving two tiny
//!   downstream tasks.
//! - **small-tail folding** — the post-loop pass folds a small final bucket
//!   into its predecessor.
//!
//! The implementation derives from Spark's `ShufflePartitionsUtil`; both
//! refinements are load-bearing on bursty workloads and aren't optional
//! polish.
//!
//! Float arithmetic is intentional: every comparison casts `u64 → f64` so
//! `target * factor` matches the original semantics exactly. Don't
//! pre-compute integer thresholds.

use ballista_core::execution_plans::PartitionGroup;

/// Pack `sizes` into bins whose total bytes approach `target` and return
/// each bin's start index.
///
/// Output `starts` defines bins as: bin `k` covers
/// `sizes[starts[k]..starts.get(k+1).unwrap_or(&sizes.len())]`. Always
/// returns at least `vec![0]`, including for an empty input.
///
/// `small_factor` (default 0.2) and `merged_factor` (default 1.2) tune the
/// merge-on-flush refinement: a bucket is merged back into its predecessor
/// when their combined size is below `target * merged_factor`, or when
/// either is below `target * small_factor`. The rule wires these from
/// `ConfigOptions`; tests pass them inline.
pub fn split_size_list_by_target_size(
    sizes: &[u64],
    target: u64,
    small_factor: f64,
    merged_factor: f64,
) -> Vec<usize> {
    let mut starts: Vec<usize> = vec![0];
    let mut current: u64 = 0;
    // Last flushed bucket's size, or None before the first flush.
    let mut last: Option<u64> = None;

    for (i, &size) in sizes.iter().enumerate() {
        // Strict `>`: if the next size would push current PAST target, flush.
        if i > 0 && current + size > target {
            try_merge_partitions(
                &mut starts,
                current,
                &mut last,
                target,
                small_factor,
                merged_factor,
            );
            starts.push(i);
            current = size;
        } else {
            current += size;
        }
    }
    // Unconditional post-loop merge so the small tail bucket has a chance to
    // fold back into its predecessor.
    try_merge_partitions(
        &mut starts,
        current,
        &mut last,
        target,
        small_factor,
        merged_factor,
    );
    starts
}

// Decide whether to fold `current` back into the previous bucket. Only
// callable from `split_size_list_by_target_size`. Float casts are
// intentional — see module docs.
fn try_merge_partitions(
    starts: &mut Vec<usize>,
    current: u64,
    last: &mut Option<u64>,
    target: u64,
    small_factor: f64,
    merged_factor: f64,
) {
    // Skipped on the first flush, when there's nothing to merge into.
    let should_merge = match *last {
        None => false,
        Some(l) => {
            let combined = (current + l) as f64;
            let curr_f = current as f64;
            let last_f = l as f64;
            let target_f = target as f64;
            combined < target_f * merged_factor
                || curr_f < target_f * small_factor
                || last_f < target_f * small_factor
        }
    };
    if should_merge {
        // Pop the last start — merging the current bucket back into the previous.
        starts.pop();
        // Safe because should_merge implies last.is_some().
        *last = Some(last.expect("should_merge guarantees Some") + current);
    } else {
        *last = Some(current);
    }
}

/// Expand the start-index output of [`split_size_list_by_target_size`]
/// into `PartitionGroup`s.
///
/// Group `k` covers `[starts[k], starts.get(k+1).copied().unwrap_or(n))`.
/// Produces only contiguous ranges; non-contiguous groups stay representable
/// in proto for future strategies but this algorithm never emits them.
///
/// `n` is the total number of upstream partitions (M) — the same value the
/// `CoalescePlan::upstream_partition_count` field will carry.
pub fn start_indices_to_partition_groups(
    starts: &[usize],
    n: usize,
) -> Vec<PartitionGroup> {
    starts
        .iter()
        .enumerate()
        .map(|(k, &start)| {
            let end = starts.get(k + 1).copied().unwrap_or(n);
            let upstream_indices: Vec<u32> = (start..end).map(|i| i as u32).collect();
            PartitionGroup { upstream_indices }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Defaults the rule passes in real life. Pulled out so each test below
    // reads as "this input → this start-indices vector at the production
    // configuration", and so a reader doesn't have to wonder whether a
    // tweaked factor is what's driving the assertion.
    const TARGET: u64 = 1024;
    const SMALL: f64 = 0.2;
    const MERGED: f64 = 1.2;

    fn pack(sizes: &[u64]) -> Vec<usize> {
        split_size_list_by_target_size(sizes, TARGET, SMALL, MERGED)
    }

    #[test]
    fn empty_input_returns_single_bucket() {
        // No partitions → one (empty) bucket. The `[0]` seed survives the
        // post-loop merge because `last=None` skips it.
        assert_eq!(pack(&[]), vec![0]);
    }

    #[test]
    fn single_partition_returns_single_bucket() {
        // Single 2048-byte partition (≥ target). The `i > 0` guard never
        // fires so no flush happens mid-loop; one bucket.
        assert_eq!(pack(&[2048]), vec![0]);
    }

    #[test]
    fn all_zero_sizes_collapse_to_one_bucket() {
        // 0 + 0 + 0 + 0 never exceeds target; never flushes.
        assert_eq!(pack(&[0, 0, 0, 0]), vec![0]);
    }

    #[test]
    fn sum_exactly_target_does_not_split() {
        // 512 + 512 = 1024. The comparison is strict `>`, so 1024 > 1024
        // is false; the bucket fills to target without flushing.
        assert_eq!(pack(&[512, 512]), vec![0]);
    }

    #[test]
    fn one_byte_over_target_flushes_then_post_merges() {
        // 512 + 513 = 1025 > 1024 → mid-loop flush. The post-loop merge
        // then folds the second bucket back in (combined 1025 < 1024*1.2),
        // so the final result is one bucket.
        assert_eq!(pack(&[512, 513]), vec![0]);
    }

    #[test]
    fn small_tail_folds_into_predecessor() {
        // 1000 fills bucket 0, 50 starts bucket 1, post-loop sees a
        // 50-byte tail next to a 1000-byte previous. Combined 1050 <
        // 1024 * 1.2 = 1228.8 → merge. One bucket.
        assert_eq!(pack(&[1000, 50]), vec![0]);
    }

    #[test]
    fn alternating_pattern_keeps_buckets_separate() {
        // 800 + 100 = 900 in bucket 0. Adding the third 800 would push to
        // 1700 > 1024, so bucket 0 closes and bucket 1 starts with 800;
        // the trailing 100 fills bucket 1 to 900. Post-loop merge is
        // rejected (1800 ≥ 1228.8; both buckets ≥ small threshold of 205).
        assert_eq!(pack(&[800, 100, 800, 100]), vec![0, 2]);
    }

    #[test]
    fn factor_zero_disables_merging() {
        // With small=0 and merged=1, only the strict `>` overshoot drives
        // boundaries; the merging refinement is fully suppressed. Each
        // 600-byte partition gets its own bucket.
        let starts = split_size_list_by_target_size(&[600, 600, 600], 1024, 0.0, 1.0);
        assert_eq!(starts, vec![0, 1, 2]);
    }

    #[test]
    fn start_indices_expand_into_contiguous_partition_groups() {
        // [0, 3, 5] over n=8 means: group 0 covers indices [0,1,2],
        // group 1 covers [3,4], group 2 covers [5,6,7]. Pure unpacking,
        // no algorithm logic.
        let groups = start_indices_to_partition_groups(&[0, 3, 5], 8);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].upstream_indices, vec![0, 1, 2]);
        assert_eq!(groups[1].upstream_indices, vec![3, 4]);
        assert_eq!(groups[2].upstream_indices, vec![5, 6, 7]);
    }
}
