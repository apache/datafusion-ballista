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

//! Partition-size thresholds and paired mapper ranges for skew joins.

use crate::state::aqe::coalesce::split_size_list_by_target_size;
use ballista_core::execution_plans::SkewJoinShard;

/// Compute the threshold once per side. A partition must strictly exceed it.
/// Match Spark's integer median, with a minimum of one byte.
pub fn skew_threshold(sizes: &[u64], factor: f64, threshold_bytes: u64) -> u64 {
    let mut sorted = sizes.to_vec();
    sorted.sort_unstable();
    let lower_middle = sorted[(sorted.len() - 1) / 2];
    let upper_middle = sorted[sorted.len() / 2];
    let median = lower_middle.midpoint(upper_middle).max(1);
    threshold_bytes.max((median as f64 * factor) as u64)
}

/// Pack mapper outputs into contiguous ranges, using the shared coalesce packer.
pub fn map_ranges_for_upstream(
    per_mapper_bytes: &[u64],
    advisory_bytes: u64,
    small_partition_factor: f64,
) -> Vec<(u32, u32)> {
    let starts = split_size_list_by_target_size(
        per_mapper_bytes,
        advisory_bytes,
        small_partition_factor,
        1.0,
    );
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

pub fn pair_shards(
    left_ranges_per_upstream: &[Vec<(u32, u32)>],
    right_ranges_per_upstream: &[Vec<(u32, u32)>],
) -> (Vec<SkewJoinShard>, Vec<SkewJoinShard>) {
    let mut left_shards = Vec::new();
    let mut right_shards = Vec::new();
    for (i, (l_ranges, r_ranges)) in left_ranges_per_upstream
        .iter()
        .zip(right_ranges_per_upstream.iter())
        .enumerate()
    {
        let i = i as u32;
        for &(left_start, left_end) in l_ranges {
            for &(right_start, right_end) in r_ranges {
                left_shards.push(SkewJoinShard {
                    upstream_idx: i,
                    start_map_idx: left_start,
                    end_map_idx: left_end,
                });
                right_shards.push(SkewJoinShard {
                    upstream_idx: i,
                    start_map_idx: right_start,
                    end_map_idx: right_end,
                });
            }
        }
    }
    (left_shards, right_shards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_split_buckets_without_crossing_upstream_partitions() {
        let left = vec![vec![(0, 2), (2, 4)], vec![(0, 1)]];
        let right = vec![vec![(0, 1), (1, 3)], vec![(0, 2)]];
        let (left, right) = pair_shards(&left, &right);
        let pairs: Vec<_> = left
            .iter()
            .zip(&right)
            .map(|(l, r)| {
                (
                    l.upstream_idx,
                    l.start_map_idx,
                    l.end_map_idx,
                    r.upstream_idx,
                    r.start_map_idx,
                    r.end_map_idx,
                )
            })
            .collect();
        assert_eq!(
            pairs,
            vec![
                (0, 0, 2, 0, 0, 1),
                (0, 0, 2, 0, 1, 3),
                (0, 2, 4, 0, 0, 1),
                (0, 2, 4, 0, 1, 3),
                (1, 0, 1, 1, 0, 2),
            ]
        );
    }
}
