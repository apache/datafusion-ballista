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

//! Per-upstream skew detection and shard-pairing for `OptimizeSkewedJoinRule`.
//!
//! Mirrors Spark's `OptimizeSkewedJoin` + `ShufflePartitionsUtil`
//! (`createSkewPartitionSpecs`, `splitSizeListByTargetSize`):
//!
//! 1. A side's partition `i` is skewed iff its total bytes exceed BOTH
//!    `factor * median(per-side sizes)` AND an absolute `threshold_bytes`.
//! 2. When a side is skewed on partition `i`, that side's mapper-output
//!    byte vector is greedy-packed into contiguous mapper-index groups whose
//!    cumulative bytes ≈ `advisory_bytes` (the same bin-packer the coalesce
//!    rule uses). When the side is not skewed, it stays as one passthrough
//!    range `[0, num_mappers)`.
//! 3. Per-upstream `i` the left and right ranges are cartesian-paired: every
//!    left range pairs with every right range, producing `|L| × |R|`
//!    downstream tasks for that upstream index. When neither side is split
//!    `|L| × |R| = 1` (passthrough). When one side is split N-way and the
//!    other isn't, that's an N×1 = N-way fan-out where the non-split side
//!    is read N times (Spark's "replicate the matching partition" mechanic).
//!
//! Float arithmetic for the skew check is intentional — every comparison
//! casts `u64 → f64` so `factor * median` matches Spark's reference
//! semantics exactly.

use ballista_core::execution_plans::SkewJoinShard;

use crate::state::aqe::coalesce::split_size_list_by_target_size;

/// Is per-side partition byte total `bytes` skewed?
///
/// Skewed iff `bytes > factor * median(sizes)` AND `bytes > threshold_bytes`.
/// The dual guard prevents fan-out of trivially small partitions (Spark's
/// `skewedPartitionThresholdInBytes` and `skewedPartitionFactor`).
///
/// Caller passes the side's full size vector (used for the median) — that
/// side is checked independently of the other side.
pub fn is_skewed(
    bytes: u64,
    sizes: &[u64],
    factor: f64,
    threshold_bytes: u64,
) -> bool {
    if bytes < threshold_bytes {
        return false;
    }
    let med = robust_median(sizes) as f64;
    (bytes as f64) > factor * med
}

/// Bin-pack a per-mapper byte vector into contiguous map-index ranges whose
/// cumulative bytes target `advisory_bytes`.
///
/// Reuses the coalesce rule's `split_size_list_by_target_size` so the
/// skewed-join sub-shard packer matches the coalesce packer exactly
/// (Spark uses the same helper for both).
///
/// `merged_partition_factor` is hard-coded to `1.0` — coalesce raises it
/// above 1.0 to fold neighboring small partitions together on flush, but for
/// skew sub-sharding we *want* small sub-shards near the advisory size,
/// so the neighbor-merge refinement is disabled. The `small_partition_factor`
/// passed through still controls tail-merge.
///
/// Returns a `Vec<(start_map_idx, end_map_idx)>` (exclusive end) of length
/// ≥ 1. When `per_mapper_bytes` is empty, returns `vec![(0, 0)]` to keep the
/// downstream pairing logic correctness (one passthrough shard for an empty
/// upstream is benign — the adapter produces an empty read).
pub fn map_ranges_for_upstream(
    per_mapper_bytes: &[u64],
    advisory_bytes: u64,
    small_partition_factor: f64,
) -> Vec<(u32, u32)> {
    // merged_partition_factor=1.0 disables the coalesce-style neighbor merge.
    let starts =
        split_size_list_by_target_size(per_mapper_bytes, advisory_bytes, small_partition_factor, 1.0);
    let total = per_mapper_bytes.len();
    starts
        .iter()
        .enumerate()
        .map(|(k, &start)| {
            let end = starts.get(k + 1).copied().unwrap_or(total);
            (start as u32, end as u32)
        })
        .collect()
}

/// Cartesian-pair per-upstream left/right map-ranges into the matched K'
/// shard lists for both sides of the join.
///
/// For each upstream index `i` and each `(left_range, right_range)` pair, two
/// `SkewJoinShard`s are appended — one to each output list. The two output
/// lists have the same length (= K') and index-aligned entries so the
/// adapter can build matched join inputs by zipping them.
///
/// `left_ranges_per_upstream` and `right_ranges_per_upstream` must have the
/// same outer length (= M, the upstream partition count). When a side is
/// not split on upstream `i`, callers pass `vec![(0, num_mappers_i)]` — one
/// passthrough range. The cartesian product correctly handles split×split
/// (full N×M), split×passthrough (N×1 = N), passthrough×split (1×M = M), and
/// passthrough×passthrough (1×1 = 1) shapes per upstream.
pub fn pair_shards(
    left_ranges_per_upstream: &[Vec<(u32, u32)>],
    right_ranges_per_upstream: &[Vec<(u32, u32)>],
) -> (Vec<SkewJoinShard>, Vec<SkewJoinShard>) {
    debug_assert_eq!(
        left_ranges_per_upstream.len(),
        right_ranges_per_upstream.len(),
        "left and right per-upstream range lists must have equal length (= M)",
    );

    let mut left_shards = Vec::new();
    let mut right_shards = Vec::new();
    for (i, (l_ranges, r_ranges)) in left_ranges_per_upstream
        .iter()
        .zip(right_ranges_per_upstream.iter())
        .enumerate()
    {
        let i = i as u32;
        for &(ls, le) in l_ranges {
            for &(rs, re) in r_ranges {
                left_shards.push(SkewJoinShard {
                    upstream_idx: i,
                    start_map_idx: ls,
                    end_map_idx: le,
                });
                right_shards.push(SkewJoinShard {
                    upstream_idx: i,
                    start_map_idx: rs,
                    end_map_idx: re,
                });
            }
        }
    }
    (left_shards, right_shards)
}

/// Median of a byte-size slice, robust against the single-outlier case the
/// skew check is designed to catch.
///
/// Returns 0 for empty input — combined with the `threshold_bytes` guard in
/// `is_skewed`, an all-zero or empty `sizes` slice never reports skewed.
fn robust_median(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted: Vec<u64> = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    // Match Spark defaults so the tests double as documentation of the
    // production configuration semantics.
    const FACTOR: f64 = 5.0;
    const THRESHOLD: u64 = 256 * 1024 * 1024;
    const ADVISORY: u64 = 64 * 1024 * 1024;
    const SMALL: f64 = 0.2;

    #[test]
    fn is_skewed_requires_both_factor_and_threshold() {
        // 8 partitions: seven small, one large. Median = small.
        let sizes = vec![10 * 1024 * 1024; 7]
            .into_iter()
            .chain(std::iter::once(2 * 1024 * 1024 * 1024))
            .collect::<Vec<u64>>();

        // The large one qualifies on both ratio (2 GB / 10 MB = 200× > 5)
        // and threshold (2 GB > 256 MB) → skewed.
        assert!(is_skewed(2 * 1024 * 1024 * 1024, &sizes, FACTOR, THRESHOLD));

        // A small one fails the threshold even though everything's small —
        // ratio is 1× which fails factor anyway.
        assert!(!is_skewed(10 * 1024 * 1024, &sizes, FACTOR, THRESHOLD));
    }

    #[test]
    fn is_skewed_fails_threshold_with_huge_ratio() {
        // Median = 10 KB; outlier = 100 MB. Ratio is 10000× (well over 5),
        // but 100 MB < threshold (256 MB) — bail. Prevents fan-out of
        // trivially small "skewed" partitions.
        let sizes = vec![10 * 1024, 10 * 1024, 10 * 1024, 100 * 1024 * 1024];
        assert!(!is_skewed(100 * 1024 * 1024, &sizes, FACTOR, THRESHOLD));
    }

    #[test]
    fn is_skewed_fails_factor_with_uniform_sizes() {
        // All equal — ratio is exactly 1, fails factor of 5.
        let sizes = vec![1024 * 1024 * 1024; 8];
        assert!(!is_skewed(1024 * 1024 * 1024, &sizes, FACTOR, THRESHOLD));
    }

    #[test]
    fn is_skewed_handles_zero_median() {
        // Lots of empty partitions; one moderately large.
        // Median = 0, so factor check is bytes > 0 (trivially yes for the
        // outlier). Threshold still applies — must be > 256 MB.
        let sizes = vec![0, 0, 0, 0, 0, 0, 0, 512 * 1024 * 1024];
        assert!(is_skewed(512 * 1024 * 1024, &sizes, FACTOR, THRESHOLD));
        // But a 100 MB outlier with the same zero-median fails threshold.
        assert!(!is_skewed(100 * 1024 * 1024, &sizes, FACTOR, THRESHOLD));
    }

    #[test]
    fn map_ranges_passthrough_when_total_under_advisory() {
        // 4 mappers, each 10 MB → 40 MB total < 64 MB target. The bin-packer
        // never flushes; one passthrough range covers all mappers.
        let per_mapper = vec![10 * 1024 * 1024; 4];
        let ranges = map_ranges_for_upstream(&per_mapper, ADVISORY, SMALL);
        assert_eq!(ranges, vec![(0, 4)]);
    }

    #[test]
    fn map_ranges_splits_at_advisory_boundary() {
        // 8 mappers, each 50 MB → 400 MB total. Advisory = 64 MB.
        // Bin-pack: 50 fills; +50=100 > 64 flush at idx 1; restart; +50=100 > 64
        // flush at idx 2; etc. → boundaries [0,1,2,3,4,5,6,7] → 8 single-mapper shards.
        let per_mapper = vec![50 * 1024 * 1024; 8];
        let ranges = map_ranges_for_upstream(&per_mapper, ADVISORY, SMALL);
        assert_eq!(
            ranges,
            vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8)]
        );
    }

    #[test]
    fn map_ranges_empty_input_yields_single_zero_range() {
        // Documents the guard: empty per_mapper still returns one range so
        // pair_shards' zipped iteration shape is preserved.
        let ranges = map_ranges_for_upstream(&[], ADVISORY, SMALL);
        assert_eq!(ranges, vec![(0, 0)]);
    }

    #[test]
    fn pair_shards_cartesian_when_only_left_split() {
        // M = 1 upstream. Left split 3-way, right passthrough.
        // Expected K' = 3×1 = 3 — same range on left repeated against the
        // full right replica.
        let left = vec![vec![(0, 2), (2, 4), (4, 6)]];
        let right = vec![vec![(0, 6)]];
        let (l, r) = pair_shards(&left, &right);
        assert_eq!(
            l,
            vec![
                SkewJoinShard { upstream_idx: 0, start_map_idx: 0, end_map_idx: 2 },
                SkewJoinShard { upstream_idx: 0, start_map_idx: 2, end_map_idx: 4 },
                SkewJoinShard { upstream_idx: 0, start_map_idx: 4, end_map_idx: 6 },
            ]
        );
        assert_eq!(
            r,
            vec![
                SkewJoinShard { upstream_idx: 0, start_map_idx: 0, end_map_idx: 6 },
                SkewJoinShard { upstream_idx: 0, start_map_idx: 0, end_map_idx: 6 },
                SkewJoinShard { upstream_idx: 0, start_map_idx: 0, end_map_idx: 6 },
            ]
        );
    }

    #[test]
    fn pair_shards_cartesian_when_both_split() {
        // M = 1 upstream. Left split 2-way, right split 3-way.
        // Expected K' = 2×3 = 6 — full cartesian.
        let left = vec![vec![(0, 4), (4, 8)]];
        let right = vec![vec![(0, 2), (2, 5), (5, 8)]];
        let (l, r) = pair_shards(&left, &right);
        assert_eq!(l.len(), 6);
        assert_eq!(r.len(), 6);
        // Layout: for each left range, pair with each right range in order.
        assert_eq!(l[0].end_map_idx, 4); assert_eq!(r[0].end_map_idx, 2);
        assert_eq!(l[1].end_map_idx, 4); assert_eq!(r[1].end_map_idx, 5);
        assert_eq!(l[2].end_map_idx, 4); assert_eq!(r[2].end_map_idx, 8);
        assert_eq!(l[3].end_map_idx, 8); assert_eq!(r[3].end_map_idx, 2);
        assert_eq!(l[4].end_map_idx, 8); assert_eq!(r[4].end_map_idx, 5);
        assert_eq!(l[5].end_map_idx, 8); assert_eq!(r[5].end_map_idx, 8);
    }

    #[test]
    fn pair_shards_passthrough_passthrough_yields_one_pair() {
        // Neither side split — one (passthrough, passthrough) pair, just
        // like the no-skew baseline.
        let left = vec![vec![(0, 4)]];
        let right = vec![vec![(0, 4)]];
        let (l, r) = pair_shards(&left, &right);
        assert_eq!(l.len(), 1);
        assert_eq!(r.len(), 1);
        assert_eq!(l[0].start_map_idx, 0);
        assert_eq!(l[0].end_map_idx, 4);
    }

    #[test]
    fn pair_shards_multi_upstream_accumulates() {
        // M = 3 upstreams. Mixed shapes:
        //   upstream 0: left split 2, right passthrough → 2 pairs
        //   upstream 1: both passthrough → 1 pair
        //   upstream 2: right split 3, left passthrough → 3 pairs
        // Total K' = 6.
        let left = vec![
            vec![(0, 2), (2, 4)],
            vec![(0, 4)],
            vec![(0, 4)],
        ];
        let right = vec![
            vec![(0, 4)],
            vec![(0, 4)],
            vec![(0, 1), (1, 2), (2, 4)],
        ];
        let (l, r) = pair_shards(&left, &right);
        assert_eq!(l.len(), 6);
        assert_eq!(r.len(), 6);
        // upstream_idx packed in expected sequence.
        let upstream_ids: Vec<u32> = l.iter().map(|s| s.upstream_idx).collect();
        assert_eq!(upstream_ids, vec![0, 0, 1, 2, 2, 2]);
    }
}
