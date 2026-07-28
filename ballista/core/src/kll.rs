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

//! KLL quantile sketch — bounded-memory data structure that answers
//! rank/quantile queries over a stream of `Ord` items with provable error.
//!
//! # Why this sketch
//!
//! Ballista's scheduler picks global partition boundaries from runtime
//! statistics gathered by executors (see
//! [`crate::execution_plans::RuntimeStatsExec`]). To generalize those
//! boundaries beyond a single numeric column, the sketch must accept
//! multi-column, potentially nullable sort keys with per-column ASC/DESC
//! direction — the full `ORDER BY` grammar. This sketch is generic over
//! `T: Ord`, which composes with `arrow::row::OwnedRow` to cover that
//! case: the row encoding carries column count, null policy, and per-column
//! direction, leaving the sketch itself dtype-agnostic.
//!
//! # Data layout
//!
//! The sketch is a stack of *compactors*, one buffer per level:
//!
//! ```text
//! level 2:  [ · · · · ]   each item has weight 4
//! level 1:  [ · · · · ]   each item has weight 2
//! level 0:  [ · · · · ]   each item has weight 1 (new items land here)
//! ```
//!
//! An item at level `h` represents `2^h` items from the input stream.
//! New items always enter level 0 with weight 1.
//!
//! # Compaction
//!
//! When a level-`h` compactor fills to capacity `k`, it:
//!
//! 1. Sorts its contents.
//! 2. Flips a fair coin.
//! 3. Keeps either the odd-indexed items (heads) or even-indexed items
//!    (tails), discarding the other half.
//! 4. Promotes the surviving `k/2` items to level `h+1`, creating that
//!    level if needed.
//! 5. Empties itself.
//!
//! The coin flip is what makes the sketch unbiased: the expected error
//! introduced per compaction is zero.
//!
//! # Rank
//!
//! ```text
//! rank(x)  =  Σ over levels h of  2^h · | { y ∈ level_h : y < x } |
//! ```
//!
//! # Parameters
//!
//! `k` is the top-level compactor capacity. Larger `k` uses more memory
//! and yields smaller error, roughly `O(1/k · sqrt(log(n/k)))`.
//!
//! Per-level capacity shrinks geometrically down the stack — the top
//! level always has capacity `k`, and each level below it has capacity
//! `ceil(k · (2/3)^depth)` floored at `MIN_LEVEL_WIDTH`. Because the
//! geometric sum converges, total retained items is bounded by `~3k`
//! (plus floor slop) regardless of stream size — the space is **constant
//! in n**, not `O(k · log(n/k))`. Matches Apache DataSketches'
//! `level_capacity` in `kll_helper.hpp`.
//!
//! # Ownership
//!
//! Callers hand fully-owned items to `KllSketch::insert`. For the Arrow
//! use case this means materializing each input `Row<'_>` into an
//! `OwnedRow` up front — one small heap allocation per input row, most of
//! which are eventually discarded by cascading compactions. A future
//! optimization can defer materialization: hold borrowed `Row<'_>` in
//! level 0 within a single batch, compact intra-batch, and only own the
//! survivors that promote to level 1. That would trade the current
//! per-item API for a batch-oriented one (e.g. `absorb_rows(&Rows)`).
//!
//! # Level order
//!
//! Levels carry no "sorted" invariant between operations. `rank` filters,
//! `quantile` collects-then-sorts, and every compaction sorts before
//! halving — so `merge` can simply extend same-height compactors without
//! order concerns. That trades ~`log(k)` extra comparisons per amortized
//! insert for a simpler shape. Apache DataSketches maintains per-level
//! sortedness (with an `is_level_zero_sorted_` flag) to skip re-sorting at
//! compact time and to merge two sketches via linear-time merge-sort
//! rather than concatenate-and-resort — a legitimate follow-up if
//! benchmarks demand it.
//!
//! # Reference
//!
//! Karnin, Lang, Liberty. *Optimal Quantile Approximation in Streams.*
//! FOCS 2016. <https://arxiv.org/abs/1603.05346>

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Floor on per-level compactor capacity. Levels whose shrinking-`k`
/// formula would drop below this stay at exactly this width — the paper's
/// error bound depends on levels never getting arbitrarily small. `8`
/// matches Apache DataSketches' `DEFAULT_M`.
const MIN_LEVEL_WIDTH: usize = 8;

/// KLL quantile sketch, generic over `Ord + Clone` items.
///
/// `Clone` is required so the true stream minimum and maximum can be
/// tracked outside the compactor stack — compaction can otherwise coin-
/// flip either extreme out, and the scheduler wants exact boundaries.
pub struct KllSketch<T: Ord + Clone> {
    /// One buffer per level; `levels[h]` is the level-`h` compactor.
    levels: Vec<Vec<T>>,
    /// Nominal compactor capacity — the top level's capacity. Lower
    /// levels shrink geometrically toward `MIN_LEVEL_WIDTH`.
    k: usize,
    /// PRNG advanced once per compaction coin flip.
    rng: StdRng,
    /// True stream minimum, maintained outside the compactor stack so
    /// `quantile(0.0)` stays exact regardless of coin-flip history.
    min: Option<T>,
    /// True stream maximum, tracked the same way for `quantile(1.0)`.
    max: Option<T>,
}

/// Capacity of the level at `height` given the current stack of `num_levels`.
///
/// Follows the KLL paper's geometric shape as implemented in Apache
/// DataSketches: `depth = num_levels - height - 1`, and capacity is
/// `ceil(k · (2/3)^depth)` floored at `MIN_LEVEL_WIDTH`. The top level
/// (`depth = 0`) always has capacity `k`; each level below it is `2/3` the
/// size of the one above.
fn level_capacity(k: usize, num_levels: usize, height: usize) -> usize {
    let depth = (num_levels - height - 1) as u32;
    // ceil(k · (2/3)^depth) via integer fixed-point:
    //   tmp    = floor(2k · 2^depth / 3^depth)
    //   result = (tmp + 1) / 2                 (ceiling of half)
    let two_k = 2u64 * (k as u64);
    let numer = two_k
        .checked_shl(depth)
        .expect("KLL level depth exceeds u64 range");
    let denom = 3u64.pow(depth);
    let raw = ((numer / denom + 1) >> 1) as usize;
    raw.max(MIN_LEVEL_WIDTH)
}

impl<T: Ord + Clone> KllSketch<T> {
    /// Construct an empty sketch with top-level compactor capacity `k`,
    /// seeded from OS entropy.
    pub fn new(k: usize) -> Self {
        Self::with_seed(k, rand::random::<u64>())
    }

    /// Construct an empty sketch with top-level compactor capacity `k`
    /// and a fixed PRNG seed. Intended for deterministic tests.
    pub fn with_seed(k: usize, seed: u64) -> Self {
        Self {
            levels: vec![Vec::new()],
            k,
            rng: StdRng::seed_from_u64(seed),
            min: None,
            max: None,
        }
    }

    /// Consume `other` and fold its content into `self`.
    ///
    /// Same-height compactors concatenate: items promoted to level `h` in
    /// either sketch have weight `2^h`, so they combine at level `h` with
    /// their weights preserved. The extend can leave several levels over
    /// their (shrinking) capacities at once — the compaction driver walks
    /// the full stack to normalize.
    ///
    /// Both sketches must have the same `k`. Mismatch is a caller bug and
    /// is caught with `debug_assert!` — matching DataSketches semantics
    /// would require tracking a min-of-observed-k, which is future work.
    pub fn merge(&mut self, other: Self) {
        debug_assert_eq!(self.k, other.k, "KllSketch::merge: sketches must share k");
        // Fold extremes across both sketches. `Option::cmp` uses
        // `None < Some(_)`, which is not what we want here.
        self.min = match (self.min.take(), other.min) {
            (Some(a), Some(b)) => Some(if a <= b { a } else { b }),
            (a @ Some(_), None) => a,
            (None, b) => b,
        };
        self.max = match (self.max.take(), other.max) {
            (Some(a), Some(b)) => Some(if a >= b { a } else { b }),
            (a @ Some(_), None) => a,
            (None, b) => b,
        };
        // Extend same-height compactors, growing the stack as needed.
        for (h, level) in other.levels.into_iter().enumerate() {
            if h == self.levels.len() {
                self.levels.push(Vec::new());
            }
            self.levels[h].extend(level);
        }
        self.compact_all();
    }

    /// Add one item to the sketch.
    pub fn insert(&mut self, x: T) {
        // Track true stream extremes outside the compactor stack; a clone
        // fires only when `x` is a new min or max, which is rare after warmup.
        if self.min.as_ref().is_none_or(|m| x < *m) {
            self.min = Some(x.clone());
        }
        if self.max.as_ref().is_none_or(|m| x > *m) {
            self.max = Some(x.clone());
        }
        self.levels[0].push(x);
        self.compact_all();
    }

    /// Return the estimated number of items strictly less than `x`.
    ///
    /// Sum over levels `h` of `2^h · | { y ∈ levels[h] : y < x } |`.
    pub fn rank(&self, x: &T) -> u64 {
        self.levels
            .iter()
            .enumerate()
            .map(|(h, level)| {
                let weight = 1u64 << h;
                let count = level.iter().filter(|y| *y < x).count() as u64;
                weight * count
            })
            .sum()
    }

    /// Return the item at the `q`-quantile of the sketched stream, where
    /// `q ∈ [0, 1]` is clamped to that range. Returns `None` if the sketch
    /// is empty.
    ///
    /// Semantics: the smallest retained item `x` whose cumulative weight
    /// (from the smallest item up to and including `x`) is at least
    /// `q · total_weight`. Non-interpolated — the returned reference is
    /// always to an item actually present in the sketch, which is the
    /// shape the scheduler wants for partition-boundary picking.
    pub fn quantile(&self, q: f64) -> Option<&T> {
        let q = q.clamp(0.0, 1.0);
        // Extreme quantiles bypass the compactor entirely so coin-flip
        // history can't shift the stream's true min/max.
        if q == 0.0 {
            return self.min.as_ref();
        }
        if q == 1.0 {
            return self.max.as_ref();
        }
        let total_weight: u64 = self
            .levels
            .iter()
            .enumerate()
            .map(|(h, level)| (1u64 << h) * level.len() as u64)
            .sum();
        if total_weight == 0 {
            return None;
        }
        let mut pairs: Vec<(&T, u64)> = self
            .levels
            .iter()
            .enumerate()
            .flat_map(|(h, level)| {
                let weight = 1u64 << h;
                level.iter().map(move |item| (item, weight))
            })
            .collect();
        pairs.sort_by_key(|(item, _)| *item);

        let target = (q * total_weight as f64) as u64;
        let mut cumulative = 0u64;
        for (item, weight) in &pairs {
            cumulative += weight;
            if cumulative >= target {
                return Some(*item);
            }
        }
        // total_weight > 0 and q ≤ 1 ⇒ cumulative reaches total_weight ≥ target
        // on the final iteration, so the loop always returns.
        unreachable!("quantile: threshold not reached despite non-empty sketch")
    }

    /// Compact until every level fits within its (dynamically shrinking)
    /// capacity. Adding a new top level shrinks the effective capacity of
    /// every existing level, so a merge or a promotion may leave more
    /// than one level over capacity at once — hence the fixed-point loop.
    fn compact_all(&mut self) {
        loop {
            let num_levels = self.levels.len();
            let target = (0..num_levels)
                .find(|&h| self.levels[h].len() >= level_capacity(self.k, num_levels, h));
            match target {
                None => return,
                Some(h) => self.compact_level(h),
            }
        }
    }

    /// Sort level `h`, coin-flip, promote every other item to level `h+1`.
    ///
    /// On odd-length levels the smallest item stays behind at level `h`
    /// with its original weight — the algorithm requires an even-length
    /// buffer for coin-flip halving, and keeping the odd one at the
    /// current weight is what preserves the total-weight invariant across
    /// compaction. Matches Apache DataSketches' `general_compress`
    /// odd-pop handling.
    fn compact_level(&mut self, h: usize) {
        let level_len = self.levels[h].len();
        if level_len < 2 {
            return;
        }
        let mut buf = std::mem::take(&mut self.levels[h]);
        buf.sort_unstable();
        let leftover = if level_len % 2 == 1 {
            Some(buf.remove(0))
        } else {
            None
        };
        let start = self.rng.random::<bool>() as usize;
        let promoted: Vec<T> = buf
            .into_iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == start)
            .map(|(_, x)| x)
            .collect();
        if h + 1 == self.levels.len() {
            self.levels.push(Vec::new());
        }
        self.levels[h + 1].extend(promoted);
        if let Some(x) = leftover {
            self.levels[h].push(x);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_is_exact_below_capacity() {
        let mut sketch: KllSketch<u32> = KllSketch::with_seed(1024, 42);
        for x in [7, 3, 10, 1, 5, 8, 2, 6, 9, 4] {
            sketch.insert(x);
        }
        // 4 items strictly less than 5: {1, 2, 3, 4}
        assert_eq!(sketch.rank(&5), 4);
    }

    #[test]
    fn rank_is_exact_below_capacity_with_owned_rows() {
        use datafusion::arrow::array::{ArrayRef, UInt32Array};
        use datafusion::arrow::datatypes::DataType;
        use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
        use std::sync::Arc;

        let converter =
            RowConverter::new(vec![SortField::new(DataType::UInt32)]).unwrap();

        let stream: ArrayRef =
            Arc::new(UInt32Array::from(vec![7u32, 3, 10, 1, 5, 8, 2, 6, 9, 4]));
        let rows = converter.convert_columns(&[stream]).unwrap();

        let mut sketch: KllSketch<OwnedRow> = KllSketch::with_seed(1024, 42);
        for row in rows.iter() {
            sketch.insert(row.owned());
        }

        let probe: ArrayRef = Arc::new(UInt32Array::from(vec![5u32]));
        let probe_row = converter.convert_columns(&[probe]).unwrap().row(0).owned();

        // Same assertion as the u32 test: 4 items strictly less than 5.
        assert_eq!(sketch.rank(&probe_row), 4);
    }

    #[test]
    fn rank_counts_nulls_below_non_null_probe() {
        use datafusion::arrow::array::{ArrayRef, UInt32Array};
        use datafusion::arrow::datatypes::DataType;
        use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
        use std::sync::Arc;

        // SortField::new default = ASC NULLS FIRST, so encoded NULL < encoded Some(_).
        let converter =
            RowConverter::new(vec![SortField::new(DataType::UInt32)]).unwrap();

        let stream: ArrayRef = Arc::new(UInt32Array::from(vec![
            Some(7u32),
            None,
            Some(3),
            Some(10),
            Some(1),
            None,
            Some(5),
            Some(8),
            Some(2),
            Some(6),
            Some(9),
            Some(4),
        ]));
        let rows = converter.convert_columns(&[stream]).unwrap();

        let mut sketch: KllSketch<OwnedRow> = KllSketch::with_seed(1024, 42);
        for row in rows.iter() {
            sketch.insert(row.owned());
        }

        let probe: ArrayRef = Arc::new(UInt32Array::from(vec![Some(5u32)]));
        let probe_row = converter.convert_columns(&[probe]).unwrap().row(0).owned();

        // 2 NULLs + {1, 2, 3, 4} = 6 items strictly less than Some(5).
        assert_eq!(sketch.rank(&probe_row), 6);
    }

    #[test]
    fn rank_over_two_column_keys() {
        use datafusion::arrow::array::{ArrayRef, UInt32Array};
        use datafusion::arrow::datatypes::DataType;
        use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
        use std::sync::Arc;

        // Two ASC NULLS FIRST columns — lex (a, b).
        let converter = RowConverter::new(vec![
            SortField::new(DataType::UInt32),
            SortField::new(DataType::UInt32),
        ])
        .unwrap();

        // Stream of (a, b) pairs: (1,10) (1,20) (2,5) (2,15) (3,1) (3,30).
        let col_a: ArrayRef = Arc::new(UInt32Array::from(vec![1u32, 1, 2, 2, 3, 3]));
        let col_b: ArrayRef = Arc::new(UInt32Array::from(vec![10u32, 20, 5, 15, 1, 30]));
        let rows = converter.convert_columns(&[col_a, col_b]).unwrap();

        let mut sketch: KllSketch<OwnedRow> = KllSketch::with_seed(1024, 42);
        for row in rows.iter() {
            sketch.insert(row.owned());
        }

        let probe_a: ArrayRef = Arc::new(UInt32Array::from(vec![2u32]));
        let probe_b: ArrayRef = Arc::new(UInt32Array::from(vec![10u32]));
        let probe_row = converter
            .convert_columns(&[probe_a, probe_b])
            .unwrap()
            .row(0)
            .owned();

        // Items strictly less than (2, 10) under lex (a ASC, b ASC):
        //   (1, 10), (1, 20)   — smaller a dominates
        //   (2,  5)            — same a, smaller b
        // Excluded: (2, 15) same a but larger b; (3, *) larger a.
        assert_eq!(sketch.rank(&probe_row), 3);
    }

    #[test]
    fn compaction_preserves_total_mass_and_below_min() {
        // k=4 with 32 items forces the level-0 buffer to compact eight
        // times and cascades up through several levels.
        let mut sketch: KllSketch<u32> = KllSketch::with_seed(4, 42);
        for x in 1u32..=32 {
            sketch.insert(x);
        }
        // Every retained item is in [1, 32]. Probing above the max must
        // sum every retained item's weight to exactly n = 32, regardless
        // of which coin flips happened along the way.
        assert_eq!(sketch.rank(&33), 32);
        // Probing at the min: no retained item is strictly less than 1.
        assert_eq!(sketch.rank(&1), 0);
    }

    #[test]
    fn quantile_on_empty_returns_none() {
        let sketch: KllSketch<u32> = KllSketch::with_seed(1024, 42);
        assert_eq!(sketch.quantile(0.5), None);
    }

    #[test]
    fn merge_grows_stack_when_other_is_taller() {
        // Small sketch → shallow stack; large sketch → deep stack.
        // Merging the large into the small forces every iteration past
        // small.levels.len() to hit the `push(Vec::new())` branch.
        let mut small: KllSketch<u32> = KllSketch::with_seed(8, 42);
        for x in 1u32..=4 {
            small.insert(x);
        }
        let mut large: KllSketch<u32> = KllSketch::with_seed(8, 7);
        for x in 5u32..=2000 {
            large.insert(x);
        }
        let small_h = small.levels.len();
        let large_h = large.levels.len();
        assert!(
            small_h < large_h,
            "test setup expects small stack shorter: got small={small_h}, large={large_h}"
        );

        small.merge(large);

        // The stack must have grown to at least the taller sketch's height —
        // otherwise items promoted above small's original top would be lost.
        assert!(
            small.levels.len() >= large_h,
            "post-merge stack {} shorter than pre-merge large.levels.len {large_h}",
            small.levels.len()
        );
        // Mass and extreme invariants: nothing lost, extremes span both streams.
        assert_eq!(small.rank(&2001), 2000);
        assert_eq!(small.rank(&1), 0);
        assert_eq!(small.quantile(0.0), Some(&1));
        assert_eq!(small.quantile(1.0), Some(&2000));
    }

    #[test]
    fn merge_preserves_upper_levels_when_other_is_shorter() {
        // Reverse orientation: large sketch absorbs a small one. The merge
        // loop terminates before reaching self's upper levels, so those
        // levels must survive intact — a gross drop would show up as lost
        // mass at the high end.
        let mut large: KllSketch<u32> = KllSketch::with_seed(8, 42);
        for x in 1u32..=2000 {
            large.insert(x);
        }
        let mut small: KllSketch<u32> = KllSketch::with_seed(8, 7);
        for x in 2001u32..=2004 {
            small.insert(x);
        }
        let large_h = large.levels.len();
        let small_h = small.levels.len();
        assert!(
            small_h < large_h,
            "test setup expects small stack shorter: got small={small_h}, large={large_h}"
        );

        large.merge(small);

        // Stack cannot shrink — the upper levels of `large` are untouched
        // by the extend loop, and compact_all can only push higher, not lower.
        assert!(
            large.levels.len() >= large_h,
            "post-merge stack {} shorter than pre-merge large.levels.len {large_h}",
            large.levels.len()
        );
        assert_eq!(large.rank(&2005), 2004);
        assert_eq!(large.rank(&1), 0);
        assert_eq!(large.quantile(0.0), Some(&1));
        assert_eq!(large.quantile(1.0), Some(&2004));
    }

    #[test]
    fn merge_preserves_mass_and_extremes() {
        // Two disjoint streams into two sketches; merge one into the other
        // and verify the standard invariants across the combined sketch.
        let mut a: KllSketch<u32> = KllSketch::with_seed(8, 42);
        for x in 1u32..=32 {
            a.insert(x);
        }
        let mut b: KllSketch<u32> = KllSketch::with_seed(8, 7);
        for x in 33u32..=64 {
            b.insert(x);
        }
        a.merge(b);
        // Combined mass = 64; nothing is < 1 or ≥ 65.
        assert_eq!(a.rank(&65), 64);
        assert_eq!(a.rank(&1), 0);
        // Min/max span both sketches.
        assert_eq!(a.quantile(0.0), Some(&1));
        assert_eq!(a.quantile(1.0), Some(&64));
    }

    #[test]
    fn min_and_max_stay_exact_after_compactions() {
        // Compactions can coin-flip the true extremes out of the compactor
        // stack, so quantile(0)/quantile(1) drift from the stream's true
        // min/max unless the sketch tracks them separately.
        let n = 10_000u32;
        let mut sketch: KllSketch<u32> = KllSketch::with_seed(64, 42);
        for x in 1..=n {
            sketch.insert(x);
        }
        assert_eq!(sketch.quantile(0.0), Some(&1));
        assert_eq!(sketch.quantile(1.0), Some(&n));
    }

    #[test]
    fn quantile_range_and_rank_roundtrip_under_compactions() {
        // n >> k forces cascading compactions; items now sit at multiple
        // levels with weights 1, 2, 4, ... — the multi-level branch of
        // quantile's cumulative-weight walk.
        let n = 10_000u32;
        let mut sketch: KllSketch<u32> = KllSketch::with_seed(64, 42);
        for x in 1..=n {
            sketch.insert(x);
        }
        // Every returned quantile is a retained item, which was inserted,
        // so it must lie in [1, n].
        for &q in &[0.0, 0.25, 0.5, 0.75, 1.0] {
            let item = sketch.quantile(q).expect("non-empty sketch");
            assert!(
                *item >= 1 && *item <= n,
                "quantile({q}) = {item} outside [1, {n}]"
            );
        }
        // rank(quantile(q)) should track q · n within KLL's error bound.
        // For k = 64 the normalized error ε ≈ 1.854/√k ≈ 0.23, so ±0.25·n
        // is a comfortable slack for a deterministic seed.
        let slack = (n as f64 * 0.25) as u64;
        for &q in &[0.25, 0.5, 0.75] {
            let item = sketch.quantile(q).expect("non-empty");
            let observed = sketch.rank(item);
            let expected = (q * n as f64) as u64;
            assert!(
                observed.abs_diff(expected) <= slack,
                "quantile({q}) = {item} → rank = {observed}, expected ~{expected} ±{slack}"
            );
        }
    }

    #[test]
    fn quantile_is_exact_below_capacity() {
        let mut sketch: KllSketch<u32> = KllSketch::with_seed(1024, 42);
        // n < k = 1024 → no compactions; every item stays at level 0 weight 1.
        for x in 1u32..=100 {
            sketch.insert(x);
        }
        // Smallest item where cumulative weight ≥ q · n.
        assert_eq!(sketch.quantile(0.0), Some(&1));
        assert_eq!(sketch.quantile(0.5), Some(&50));
        assert_eq!(sketch.quantile(1.0), Some(&100));
    }

    #[test]
    fn shrinking_capacity_bounds_retained_items() {
        // Insert n >> k so many cascades occur. Under shrinking-k, total
        // retained items stays near 3k regardless of n; under fixed-k it
        // would grow with num_levels.
        let k = 64;
        let mut sketch: KllSketch<u32> = KllSketch::with_seed(k, 42);
        for x in 1u32..=10_000 {
            sketch.insert(x);
        }
        let retained: usize = sketch.levels.iter().map(|l| l.len()).sum();
        let num_levels = sketch.levels.len();
        let naive_bound = k * num_levels;
        let shrinking_bound: usize = (0..num_levels)
            .map(|h| level_capacity(k, num_levels, h))
            .sum();
        assert!(
            shrinking_bound < naive_bound,
            "shrinking bound {shrinking_bound} should be tighter than \
             naive fixed-k bound {naive_bound} at num_levels={num_levels}"
        );
        assert!(
            retained <= shrinking_bound,
            "retained {retained} exceeds shrinking-capacity total \
             {shrinking_bound} across {num_levels} levels"
        );
        // Mass invariants still hold across all the cascades.
        assert_eq!(sketch.rank(&10_001), 10_000);
        assert_eq!(sketch.rank(&1), 0);
    }
}
