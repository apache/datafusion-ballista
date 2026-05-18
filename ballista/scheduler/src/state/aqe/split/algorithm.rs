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

//! Per-partition split-factor decision and shard expansion.
//!
//! `decide_split_factors` mirrors Spark's `OptimizeSkewedJoin` skew-detection
//! formula: a partition is "skewed" if its byte size exceeds
//! `skew_factor * median(byte_sizes)` AND exceeds an absolute
//! `min_split_bytes` floor (so we don't fan-out a few-byte partition just
//! because the median is zero). For skewed partitions we fan out
//! `ceil(byte_size / median)`, capped at `max_split_factor` to avoid
//! overwhelming the executor.
//!
//! v1 caveat: file-list sharding can only split a partition whose upstream
//! `Vec<PartitionLocation>` has length `>= 2`. A single-file partition stays
//! at factor 1 even when it qualifies on bytes. (Row-range reads would lift
//! this restriction; that work belongs in v2.)
//!
//! Float arithmetic is intentional and matches `coalesce/algorithm.rs` style:
//! every comparison promotes `u64 → f64` so `skew_factor * median` matches
//! Spark's reference semantics exactly. Don't pre-compute integer thresholds.

use ballista_core::execution_plans::SplitShard;

/// Decide a split factor per upstream partition.
///
/// Returns a `Vec<u32>` of length `summed_bytes.len()` where each entry is
/// the number of output partitions that upstream idx should fan into.
/// `1` means passthrough (no split); values `> 1` are bounded by
/// `max_split_factor`.
///
/// Inputs:
/// - `summed_bytes[i]` — byte size of upstream partition `i` summed across
///   the alignment group (see `SplitPartitionsRule` docs).
/// - `file_counts[i]` — number of `PartitionLocation`s backing upstream
///   partition `i` on leaf 0 of the alignment group. v1 requires
///   `file_counts[i] >= 2` to actually split — a single-file partition can't
///   be sharded by file-list assignment.
/// - `skew_factor` — multiplier over the median (Spark default 5.0).
/// - `min_split_bytes` — absolute floor; partitions smaller than this are
///   never split regardless of skew ratio (Spark default 256 MB; ours 64 MB
///   to match the coalesce target).
/// - `max_split_factor` — upper bound on per-partition fan-out (Spark default
///   no explicit cap; we set 8 to match the doc and limit executor pressure).
///
/// Edge cases:
/// - Empty input or `summed_bytes` of all zeros → all factors `1` (median is
///   0, the multiplicative check short-circuits via the `min_split_bytes` floor).
/// - `summed_bytes.len() != file_counts.len()` → panics in debug; in release
///   the shorter slice wins. Callers must enforce equal length.
pub fn decide_split_factors(
    summed_bytes: &[u64],
    file_counts: &[usize],
    skew_factor: f64,
    min_split_bytes: u64,
    max_split_factor: u32,
) -> Vec<u32> {
    debug_assert_eq!(
        summed_bytes.len(),
        file_counts.len(),
        "decide_split_factors: summed_bytes and file_counts must agree in length"
    );

    let median = robust_median(summed_bytes);

    summed_bytes
        .iter()
        .zip(file_counts.iter())
        .map(|(&bytes, &files)| {
            // v1 file-list sharding can't subdivide a single file.
            if files <= 1 {
                return 1u32;
            }
            if bytes < min_split_bytes {
                return 1u32;
            }
            let bytes_f = bytes as f64;
            let median_f = median as f64;
            if bytes_f < skew_factor * median_f {
                return 1u32;
            }
            // Skew qualified — must split at least 2-ways; cap at max.
            let raw = if median == 0 {
                max_split_factor
            } else {
                (bytes_f / median_f).ceil() as u32
            };
            raw.clamp(2, max_split_factor)
        })
        .collect()
}

/// Expand per-idx split factors into a flat `Vec<SplitShard>`.
///
/// Order is `upstream_idx` ascending, then `shard_idx` ascending within each
/// upstream. For factor `f`, the idx contributes `f` consecutive shards with
/// `shard_idx` in `0..f`. Factor `1` contributes one passthrough entry with
/// `shard_idx = 0, split_factor = 1`.
///
/// Result length equals `factors.iter().sum::<u32>() as usize` — this is K'.
pub fn factors_to_shards(factors: &[u32]) -> Vec<SplitShard> {
    let total: usize = factors.iter().map(|&f| f as usize).sum();
    let mut shards = Vec::with_capacity(total);
    for (idx, &f) in factors.iter().enumerate() {
        // Skipped on factor=0 (which decide_split_factors never produces, but
        // is defensive against future bugs).
        for shard_idx in 0..f {
            shards.push(SplitShard {
                upstream_idx: idx as u32,
                shard_idx,
                split_factor: f,
            });
        }
    }
    shards
}

/// Median of a byte-size slice, robust against the single-outlier case the
/// rule is designed to catch.
///
/// Returns 0 for empty input (treated as "no skew" by `decide_split_factors`).
/// Uses the lower-mid for even-length input — exact-quantile precision isn't
/// needed; what matters is that one huge value doesn't shift the median up.
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

    // Defaults the rule passes in real life. Pulled out so each test below
    // reads as "this input → this factors vector at the production
    // configuration", same convention as `coalesce/algorithm.rs`.
    const SKEW: f64 = 5.0;
    const MIN_BYTES: u64 = 64 * 1024 * 1024;
    const MAX_FACTOR: u32 = 8;

    fn factor(summed: &[u64], files: &[usize]) -> Vec<u32> {
        decide_split_factors(summed, files, SKEW, MIN_BYTES, MAX_FACTOR)
    }

    #[test]
    fn no_skew_returns_all_ones() {
        // Uniform sizes — median equals every entry, ratio is exactly 1 < 5.
        // Every entry returns factor 1 regardless of file count.
        let sizes = vec![100 * 1024 * 1024; 8];
        let files = vec![4; 8];
        assert_eq!(factor(&sizes, &files), vec![1; 8]);
    }

    #[test]
    fn single_outlier_splits_only_that_idx() {
        // Four partitions: three at 10 MB, one at 2 GB. Median = 10 MB.
        // 2 GB / 10 MB = 200 (way above max_factor=8) → factor capped at 8.
        // Files = 8 each so the file-count guard doesn't fire.
        let sizes = vec![
            10 * 1024 * 1024,
            10 * 1024 * 1024,
            10 * 1024 * 1024,
            2 * 1024 * 1024 * 1024,
        ];
        let files = vec![8; 4];
        // The three small partitions are below the 64 MB floor → factor 1.
        // The outlier qualifies on skew ratio and bytes → factor 8 (cap).
        assert_eq!(factor(&sizes, &files), vec![1, 1, 1, 8]);
    }

    #[test]
    fn below_min_bytes_never_splits() {
        // Huge ratio (1000×) but the largest is 10 MB, below the 64 MB floor.
        // No split happens — protects us from fanning out trivially small work.
        let sizes = vec![10 * 1024, 10 * 1024, 10 * 1024 * 1024];
        let files = vec![4; 3];
        assert_eq!(factor(&sizes, &files), vec![1, 1, 1]);
    }

    #[test]
    fn single_file_cannot_split() {
        // Outlier qualifies on bytes and ratio, but it's backed by 1 file.
        // v1 file-list sharding can't subdivide a single file; bail.
        let sizes = vec![10 * 1024 * 1024, 10 * 1024 * 1024, 2 * 1024 * 1024 * 1024];
        let files = vec![4, 4, 1]; // outlier has only 1 file
        assert_eq!(factor(&sizes, &files), vec![1, 1, 1]);
    }

    #[test]
    fn max_split_factor_caps_output() {
        // Five partitions: four at 10 MB, one at 1 GB. Median (lower-mid of
        // sorted [10MB, 10MB, 10MB, 10MB, 1GB]) = 10 MB. Ratio = 100 — well
        // above max_factor=4 — so factor clamps to 4. (The small ones stay
        // at 1: they're below the 64 MB floor.)
        let sizes = vec![
            10 * 1024 * 1024,
            10 * 1024 * 1024,
            10 * 1024 * 1024,
            10 * 1024 * 1024,
            1024 * 1024 * 1024,
        ];
        let files = vec![8; 5];
        let factors = decide_split_factors(&sizes, &files, SKEW, MIN_BYTES, 4);
        assert_eq!(factors, vec![1, 1, 1, 1, 4]);
    }

    #[test]
    fn zero_median_does_not_panic_or_overshoot() {
        // Three zeros and one large entry — median = 0. Without the
        // min_split_bytes guard a naive `ceil(bytes / 0)` would be UB; the
        // guard ensures we exit before that math runs.
        let sizes = vec![0, 0, 0, 10 * 1024 * 1024 * 1024];
        let files = vec![4; 4];
        // The big one qualifies (well above 64 MB and skew=5 × 0 = 0 is trivially
        // exceeded); factor goes to the max because the divisor is zero.
        assert_eq!(factor(&sizes, &files), vec![1, 1, 1, 8]);
    }

    #[test]
    fn empty_input_returns_empty_factors() {
        let factors = decide_split_factors(&[], &[], SKEW, MIN_BYTES, MAX_FACTOR);
        assert!(factors.is_empty());
    }

    #[test]
    fn factors_to_shards_passthrough_and_split_mix() {
        // [1, 1, 3, 1] over upstream idx 0..3 → 6 shards total:
        // (idx=0, shard=0, factor=1), (idx=1, shard=0, factor=1),
        // (idx=2, shard=0..2, factor=3) × 3, (idx=3, shard=0, factor=1).
        let shards = factors_to_shards(&[1, 1, 3, 1]);
        assert_eq!(shards.len(), 6);
        assert_eq!(
            shards[0],
            SplitShard {
                upstream_idx: 0,
                shard_idx: 0,
                split_factor: 1
            }
        );
        assert_eq!(
            shards[1],
            SplitShard {
                upstream_idx: 1,
                shard_idx: 0,
                split_factor: 1
            }
        );
        assert_eq!(
            shards[2],
            SplitShard {
                upstream_idx: 2,
                shard_idx: 0,
                split_factor: 3
            }
        );
        assert_eq!(
            shards[3],
            SplitShard {
                upstream_idx: 2,
                shard_idx: 1,
                split_factor: 3
            }
        );
        assert_eq!(
            shards[4],
            SplitShard {
                upstream_idx: 2,
                shard_idx: 2,
                split_factor: 3
            }
        );
        assert_eq!(
            shards[5],
            SplitShard {
                upstream_idx: 3,
                shard_idx: 0,
                split_factor: 1
            }
        );
    }

    #[test]
    fn factors_to_shards_all_passthrough() {
        let shards = factors_to_shards(&[1, 1, 1]);
        assert_eq!(shards.len(), 3);
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(s.upstream_idx, i as u32);
            assert_eq!(s.shard_idx, 0);
            assert_eq!(s.split_factor, 1);
        }
    }
}
