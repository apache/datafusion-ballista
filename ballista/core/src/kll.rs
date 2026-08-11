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
//! which are eventually discarded by cascading compactions. See
//! **Deferred optimizations** below.
//!
//! # Level order
//!
//! Levels carry no "sorted" invariant between operations. `rank` filters,
//! `quantile` collects-then-sorts, and every compaction sorts before
//! halving — so `merge` can simply extend same-height compactors without
//! order concerns. That trades ~`log(k)` extra comparisons per amortized
//! insert for a simpler shape. See **Deferred optimizations** below for
//! the per-level-sortedness follow-up that DataSketches implements.
//!
//! # Deferred optimizations
//!
//! Ingest measured on uniform 1M-row streams (see
//! `benchmarks/benches/quantile_sketch.rs`):
//!
//! - `T = OrderedFloat<f64>` via `absorb_slice`: **1.8× TDigest** (down
//!   from 3.7× on per-row `insert` with no batch API).
//! - `T = OwnedRow` via `absorb`: **5.7× TDigest** (down from 6.8× on
//!   per-row `insert`).
//!
//! Items (1) and (3) below have landed. One deferred win remains, and
//! it's the one that matters for the OwnedRow path:
//!
//! 1. ~~**Amortize `compact_all` across a batch.**~~ **Landed.** `absorb`
//!    caches level 0's capacity and skips the `compact_all` O(L) scan
//!    when no level is full. `absorb_slice` stacks a batch min/max and
//!    `extend_from_slice` on top for `T: Copy`.
//!
//! 2. **Defer ownership at level 0.** For `T = OwnedRow` the caller pays
//!    one heap allocation per input row via `row.owned()`, even though
//!    ~half of level-0 items are coin-flipped out at the first compaction
//!    and never promoted. Holding borrowed `Row<'_>` at level 0 within a
//!    batch and materializing `OwnedRow` only on promotion to level 1
//!    saves the alloc/free on the discarded half. The batch-oriented
//!    `absorb` API is what makes the borrow lifetime work out — level 0
//!    lives for the duration of `absorb`, compacts before the caller's
//!    `Rows` buffer goes out of scope. Only helps `T = OwnedRow`.
//!
//! 3. ~~**Per-level sortedness invariant.**~~ **Landed.** `KllSketch`
//!    carries a `sorted: Vec<bool>` parallel to `levels`. `compact_level`
//!    skips the sort when the level is already ordered, and promoted
//!    items merge-extend the next level in linear time when it too was
//!    sorted. `KllSketch::merge` (across sketches) does not yet fold the
//!    two sortedness flags — cross-sketch merge-extend is a further
//!    deferred win.
//!
//! 4. **Sorted-input ingest via a caller-side plan-shape change.** The
//!    `absorb_sorted_slice` API exists but is not yet called from
//!    production. When wired into an ORRE plan shape where a `SortExec`
//!    is placed upstream of `RuntimeStatsExec` (with a `DamExec` between
//!    stats and the router), level 0 stays permanently sorted and
//!    compaction skips *every* sort — not just those at levels 1+. That
//!    kills the largest remaining ingest cost on the `T: Copy` path.
//!    The sketch side is free; the win requires the planner-side move.
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
    /// `sorted[h] == true` iff `levels[h]` is in ascending order. Same
    /// length as `levels`, kept in lockstep with every mutation.
    /// Compaction reads this to skip the re-sort when a level was left
    /// sorted by an earlier promotion; promotions extend the destination
    /// level via linear-time merge instead of concatenate-then-sort when
    /// the destination is already sorted. Matches DataSketches'
    /// `is_level_zero_sorted_` bookkeeping, generalized per-level.
    sorted: Vec<bool>,
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
            // An empty level is trivially sorted.
            sorted: vec![true],
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
        // Cross-sketch extend does not preserve any per-level sortedness
        // — the concatenation of two sorted runs is not sorted, and we
        // don't yet fold the sortedness flags of two sketches into a
        // merge-extend. `compact_all` will sort on demand.
        for (h, level) in other.levels.into_iter().enumerate() {
            if h == self.levels.len() {
                self.levels.push(Vec::new());
                self.sorted.push(true);
            }
            if !level.is_empty() {
                self.levels[h].extend(level);
                self.sorted[h] = false;
            }
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
        // Arbitrary insertion breaks any existing sort order at level 0.
        self.sorted[0] = false;
        self.compact_all();
    }

    /// Add many items in one call, amortizing the per-item overhead of
    /// `insert`.
    ///
    /// `insert` invokes `compact_all` after every row, and each call scans
    /// every level asking "is anyone full?" That's an `O(L)` scan per row
    /// even when the answer is universally "no." `absorb` caches level 0's
    /// capacity once, checks a single length per row, and only fires
    /// `compact_all` when level 0 actually reaches capacity.
    ///
    /// Owned-value ingest — takes items by value, no `T: Clone` needed on
    /// the incoming stream. Prefer this for heap-heavy `T` (e.g.
    /// `OwnedRow`) where cloning would double the allocation cost. For
    /// `T: Copy` prefer `absorb_slice` — it uses `extend_from_slice` +
    /// batch min/max for a memcpy-friendly hot loop.
    ///
    /// Output is bit-identical to `for x in xs { self.insert(x) }`: the
    /// compaction trigger points, coin flips, and min/max tracking are all
    /// preserved.
    ///
    /// TODO: retire once **Deferred optimizations** item (2) lands. The
    /// borrowed-row ingest path there takes an arrow `Rows` directly and
    /// holds `Row<'_>` at level 0 — a specialized API, not a generalization
    /// of this one. This function exists as a modest speedup for the
    /// interim OwnedRow path (~14% over `insert`-in-loop on 1M rows) and
    /// has no other production caller.
    pub fn absorb<I: IntoIterator<Item = T>>(&mut self, xs: I) {
        // Cache level 0's capacity; recompute whenever compaction may have
        // grown the stack (which shrinks per-level capacities).
        let mut level_0_cap = level_capacity(self.k, self.levels.len(), 0);
        for x in xs {
            if self.min.as_ref().is_none_or(|m| x < *m) {
                self.min = Some(x.clone());
            }
            if self.max.as_ref().is_none_or(|m| x > *m) {
                self.max = Some(x.clone());
            }
            self.levels[0].push(x);
            self.sorted[0] = false;
            if self.levels[0].len() >= level_0_cap {
                self.compact_all();
                level_0_cap = level_capacity(self.k, self.levels.len(), 0);
            }
        }
    }

    /// Slice-ingest variant that trades one clone per item for two extra
    /// wins on top of `absorb`'s compact_all amortization:
    ///
    /// 1. Batch min/max in a single tight scan of `xs`, updating `self.min`
    ///    / `self.max` at most once per call instead of at most once per
    ///    row.
    /// 2. `Vec::extend_from_slice` for chunk-fills of level 0 — one memcpy
    ///    per chunk for `T: Copy`, potentially SIMD-vectorized by LLVM.
    ///
    /// Prefer this for `T: Copy` (e.g. `OrderedFloat<f64>`, `i64`) where
    /// the clone is free. Avoid for heap-heavy `T` (e.g. `OwnedRow`) —
    /// there the extend_from_slice clone allocates on top of whatever the
    /// caller already paid to construct the value; `absorb` moves instead.
    ///
    /// Output is bit-identical to `for x in xs { self.insert(x.clone()) }`.
    pub fn absorb_slice(&mut self, xs: &[T]) {
        if xs.is_empty() {
            return;
        }
        // One scan across xs for min/max; a single Option update per batch
        // regardless of how many progressive extremes appear in xs.
        let (batch_min, batch_max) = {
            let mut min = &xs[0];
            let mut max = &xs[0];
            for x in &xs[1..] {
                if x < min {
                    min = x;
                }
                if x > max {
                    max = x;
                }
            }
            (min, max)
        };
        if self.min.as_ref().is_none_or(|m| batch_min < m) {
            self.min = Some(batch_min.clone());
        }
        if self.max.as_ref().is_none_or(|m| batch_max > m) {
            self.max = Some(batch_max.clone());
        }
        // Chunk-fill level 0 via memcpy-style extend; compact when it fills.
        let mut i = 0;
        while i < xs.len() {
            let cap = level_capacity(self.k, self.levels.len(), 0);
            let room = cap.saturating_sub(self.levels[0].len());
            let take = (xs.len() - i).min(room);
            self.levels[0].extend_from_slice(&xs[i..i + take]);
            self.sorted[0] = false;
            i += take;
            if self.levels[0].len() >= cap {
                self.compact_all();
            }
        }
    }

    /// Slice-ingest variant that expects the input to already be sorted
    /// ascending — the caller is downstream of a `SortExec` or otherwise
    /// producing sorted output. Keeps level 0 permanently sorted by
    /// merge-extending each chunk into it, so `compact_level(0)` never
    /// pays a `sort_unstable`. That's the largest remaining cost on the
    /// `T: Copy` ingest path once the per-level sortedness invariant is
    /// in place.
    ///
    /// The min/max scan is also skipped — for sorted input the extremes
    /// are `xs[0]` and `xs[last]`.
    ///
    /// Sortedness is verified unconditionally (O(n) comparisons against
    /// the O(n log n) sort skipped when it holds). Unsorted input falls
    /// through to `absorb_slice`, which sorts at compaction time. The
    /// motivating plan shape is `Scan -> Sort -> RuntimeStats -> DamExec
    /// -> ORRE`, but a future optimizer change that moves or drops the
    /// upstream `SortExec` must not silently corrupt quantiles — the
    /// fallback keeps this method safe under any caller.
    pub fn absorb_sorted_slice(&mut self, xs: &[T]) {
        if xs.is_empty() {
            return;
        }
        if !xs.is_sorted() {
            self.absorb_slice(xs);
            return;
        }
        // Sorted input: extremes are the first and last elements. No scan.
        let batch_min = &xs[0];
        let batch_max = &xs[xs.len() - 1];
        if self.min.as_ref().is_none_or(|m| batch_min < m) {
            self.min = Some(batch_min.clone());
        }
        if self.max.as_ref().is_none_or(|m| batch_max > m) {
            self.max = Some(batch_max.clone());
        }
        // If a prior `insert` or `absorb_slice` left level 0 unsorted,
        // `merge_sorted` below would interleave sorted `xs` into
        // unsorted state and the result wouldn't be sorted. Pay the sort
        // once here so the loop's merge-extends stay honest and the doc's
        // "level 0 stays sorted" claim actually holds.
        if !self.sorted[0] {
            self.levels[0].sort_unstable();
            self.sorted[0] = true;
        }
        // Chunk-merge sorted `xs` into (sorted) level 0. Level 0 stays
        // sorted through the whole ingest — `sorted[0]` never flips false.
        let mut i = 0;
        while i < xs.len() {
            let cap = level_capacity(self.k, self.levels.len(), 0);
            let room = cap.saturating_sub(self.levels[0].len());
            let take = (xs.len() - i).min(room);
            let incoming = xs[i..i + take].to_vec();
            let existing = std::mem::take(&mut self.levels[0]);
            self.levels[0] = merge_sorted(existing, incoming);
            i += take;
            if self.levels[0].len() >= cap {
                self.compact_all();
            }
        }
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

    /// Sort level `h` (or reuse an existing sort), coin-flip, promote every
    /// other item to level `h+1`.
    ///
    /// On odd-length levels the smallest item stays behind at level `h`
    /// with its original weight — the algorithm requires an even-length
    /// buffer for coin-flip halving, and keeping the odd one at the
    /// current weight is what preserves the total-weight invariant across
    /// compaction. Matches Apache DataSketches' `general_compress`
    /// odd-pop handling.
    ///
    /// Sortedness bookkeeping: promoted items form a sorted subsequence
    /// (every other element of a sorted array). If the destination level
    /// was already sorted (which it is by default when previously touched
    /// only by this function), we merge-extend in linear time instead of
    /// concatenate-then-sort. The saving compounds up the stack — for
    /// large streams, levels 1+ are always sorted going into compaction,
    /// so the `sort_unstable` at level 0 is the only one paid.
    fn compact_level(&mut self, h: usize) {
        let level_len = self.levels[h].len();
        if level_len < 2 {
            return;
        }
        let mut buf = std::mem::take(&mut self.levels[h]);
        if !self.sorted[h] {
            buf.sort_unstable();
        }
        // levels[h] is now empty and thus trivially sorted.
        self.sorted[h] = true;
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
            self.sorted.push(true);
        }
        // If the destination is already sorted, linear-time merge preserves
        // its sortedness. Otherwise fall back to append; the next compaction
        // at h+1 will pay the sort.
        if self.sorted[h + 1] {
            let existing = std::mem::take(&mut self.levels[h + 1]);
            self.levels[h + 1] = merge_sorted(existing, promoted);
        } else {
            self.levels[h + 1].extend(promoted);
        }
        if let Some(x) = leftover {
            // Single-item level is trivially sorted.
            self.levels[h].push(x);
        }
    }
}

/// Merge two sorted vectors into one sorted vector in linear time.
/// Preserves ties in source order (`a` before `b`) — matters only for
/// non-unique `T`, where downstream halving still picks half the items
/// but may pick different specific copies than `sort_unstable(a ++ b)`.
fn merge_sorted<T: Ord>(a: Vec<T>, b: Vec<T>) -> Vec<T> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut ai = a.into_iter().peekable();
    let mut bi = b.into_iter().peekable();
    loop {
        match (ai.peek(), bi.peek()) {
            (Some(av), Some(bv)) => {
                if av <= bv {
                    out.push(ai.next().unwrap());
                } else {
                    out.push(bi.next().unwrap());
                }
            }
            (Some(_), None) => {
                out.extend(ai);
                break;
            }
            (None, Some(_)) => {
                out.extend(bi);
                break;
            }
            (None, None) => break,
        }
    }
    out
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

    /// Apache DataSketches' single-sided normalized rank error, fit to the
    /// P99 max error across thousands of empirical trials. Source:
    /// `datasketches-cpp/kll/include/kll_sketch_impl.hpp:619`.
    fn normalized_rank_error_ss(k: usize) -> f64 {
        2.296 / (k as f64).powf(0.9723)
    }

    /// Insert `stream` into a KLL sketch at capacity `k`, probe rank at 9
    /// interior quantiles picked from a sorted copy of the stream, and
    /// return the worst normalized rank error observed.
    fn worst_rank_error(stream: &[u32], k: usize, sketch_seed: u64) -> f64 {
        let mut sketch: KllSketch<u32> = KllSketch::with_seed(k, sketch_seed);
        for x in stream {
            sketch.insert(*x);
        }
        let mut sorted = stream.to_vec();
        sorted.sort_unstable();
        let n = stream.len();
        (1..10)
            .map(|i| {
                let idx = (i as f64 / 10.0 * n as f64) as usize;
                let probe = sorted[idx.min(n - 1)];
                // True rank: count of items strictly less than probe.
                // partition_point on the sorted stream gives it in O(log n).
                let true_rank_frac =
                    sorted.partition_point(|x| *x < probe) as f64 / n as f64;
                let est_rank_frac = sketch.rank(&probe) as f64 / n as f64;
                (est_rank_frac - true_rank_frac).abs()
            })
            .fold(0.0_f64, f64::max)
    }

    /// Shuffled `1..=n` — every distinct value appears exactly once.
    fn uniform_stream(n: u32, rng: &mut StdRng) -> Vec<u32> {
        use rand::seq::SliceRandom;
        let mut v: Vec<u32> = (1..=n).collect();
        v.shuffle(rng);
        v
    }

    /// 90% of items in the bottom 1% of the value range, remainder spread
    /// over the rest. Cuts near the low quantiles land inside the dense
    /// cluster; cuts near the high quantiles land in the sparse tail.
    fn clustered_stream(n: u32, rng: &mut StdRng) -> Vec<u32> {
        use rand::seq::SliceRandom;
        let hot_max = (n / 100).max(2);
        let hot = n * 9 / 10;
        let cold = n - hot;
        let mut v = Vec::with_capacity(n as usize);
        for _ in 0..hot {
            v.push(rng.random_range(1u32..=hot_max));
        }
        for _ in 0..cold {
            v.push(rng.random_range(hot_max + 1..=n));
        }
        v.shuffle(rng);
        v
    }

    /// Only 100 distinct values, each ~n/100 times. Exercises the sort +
    /// coin-flip halving under degenerate orderings and confirms that
    /// ties don't shift rank estimates.
    fn tied_stream(n: u32, rng: &mut StdRng) -> Vec<u32> {
        (0..n).map(|_| rng.random_range(1u32..=100)).collect()
    }

    /// Sweep `trials` seeds building a fresh stream per trial and assert
    /// the DataSketches bound holds in ≥95% of them.
    fn assert_ds_bound_holds(
        distribution: &str,
        stream_fn: impl Fn(u32, &mut StdRng) -> Vec<u32>,
    ) {
        let k = 200;
        let n = 10_000u32;
        let bound = normalized_rank_error_ss(k);
        let trials = 100u64;
        let failures = (0..trials)
            .filter(|&seed| {
                let mut shuffle_rng = StdRng::seed_from_u64(seed);
                let stream = stream_fn(n, &mut shuffle_rng);
                // Sketch RNG derived from a different sub-seed so the
                // stream construction and the compaction coin flips aren't
                // drawn from correlated streams.
                let sketch_seed = seed.wrapping_add(0x9E37_79B9);
                worst_rank_error(&stream, k, sketch_seed) > bound
            })
            .count();
        let fail_rate = failures as f64 / trials as f64;
        // The DataSketches eps is a P99 empirical fit, so ≤1% of trials
        // should exceed it in a correct implementation. 5% slack absorbs
        // tail luck without letting a real distribution shift slip
        // through — an optimization bug that biases the error
        // distribution up by a few percent pushes the fail rate well
        // past 5%.
        assert!(
            fail_rate <= 0.05,
            "{distribution}: rank error exceeded DataSketches bound \
             {bound:.4} in {failures}/{trials} trials ({:.1}%); expected ≤ 5%",
            fail_rate * 100.0
        );
    }

    #[test]
    fn absorb_apis_match_per_row_insert() {
        // Both `absorb` and `absorb_slice` only skip work that per-row
        // `insert` provably would have skipped too — the compaction
        // trigger points, coin flips, and final min/max are all preserved.
        // Three sketches seeded identically and fed the same stream via
        // the three APIs must agree on every rank probe.
        use rand::seq::SliceRandom;
        let seed = 12345u64;
        let mut shuffle_rng = StdRng::seed_from_u64(seed);
        let n = 10_000u32;
        let mut stream: Vec<u32> = (1..=n).collect();
        stream.shuffle(&mut shuffle_rng);

        let mut via_insert: KllSketch<u32> = KllSketch::with_seed(200, seed);
        for x in &stream {
            via_insert.insert(*x);
        }

        let mut via_absorb: KllSketch<u32> = KllSketch::with_seed(200, seed);
        via_absorb.absorb(stream.iter().copied());

        let mut via_absorb_slice: KllSketch<u32> = KllSketch::with_seed(200, seed);
        via_absorb_slice.absorb_slice(&stream);

        // Probe every 100 to keep the test cheap; the space is dense
        // enough that any drift would surface within these probes.
        for probe in (1..=n).step_by(100) {
            let r_insert = via_insert.rank(&probe);
            let r_absorb = via_absorb.rank(&probe);
            let r_absorb_slice = via_absorb_slice.rank(&probe);
            assert_eq!(
                r_insert, r_absorb,
                "rank({probe}) diverged: insert={r_insert} absorb={r_absorb}"
            );
            assert_eq!(
                r_insert, r_absorb_slice,
                "rank({probe}) diverged: insert={r_insert} absorb_slice={r_absorb_slice}"
            );
        }
        assert_eq!(via_insert.quantile(0.0), via_absorb.quantile(0.0));
        assert_eq!(via_insert.quantile(0.0), via_absorb_slice.quantile(0.0));
        assert_eq!(via_insert.quantile(0.5), via_absorb.quantile(0.5));
        assert_eq!(via_insert.quantile(0.5), via_absorb_slice.quantile(0.5));
        assert_eq!(via_insert.quantile(1.0), via_absorb.quantile(1.0));
        assert_eq!(via_insert.quantile(1.0), via_absorb_slice.quantile(1.0));
    }

    #[test]
    fn absorb_sorted_slice_matches_absorb_slice_on_sorted_input() {
        // absorb_sorted_slice's trick is to skip the compact-time sort by
        // keeping level 0 sorted via merge-extend. On sorted input its
        // output is bit-identical to absorb_slice because sort_unstable on
        // an already-sorted array of distinct items is the identity, so
        // both paths produce the same halved subsequence at each
        // compaction.
        let seed = 42u64;
        let n = 10_000u32;
        let sorted_stream: Vec<u32> = (1..=n).collect();

        let mut via_absorb_slice: KllSketch<u32> = KllSketch::with_seed(200, seed);
        via_absorb_slice.absorb_slice(&sorted_stream);

        let mut via_sorted: KllSketch<u32> = KllSketch::with_seed(200, seed);
        via_sorted.absorb_sorted_slice(&sorted_stream);

        for probe in (1..=n).step_by(100) {
            assert_eq!(
                via_absorb_slice.rank(&probe),
                via_sorted.rank(&probe),
                "rank({probe}) diverged"
            );
        }
        assert_eq!(via_absorb_slice.quantile(0.0), via_sorted.quantile(0.0));
        assert_eq!(via_absorb_slice.quantile(0.5), via_sorted.quantile(0.5));
        assert_eq!(via_absorb_slice.quantile(1.0), via_sorted.quantile(1.0));
    }

    #[test]
    fn absorb_sorted_slice_restores_level_0_sortedness_after_prior_insert() {
        // Prior `insert` calls leave sorted[0] = false. absorb_sorted_slice
        // must sort level 0 before merge-extending, otherwise it would
        // interleave sorted input into an unsorted buffer and the doc's
        // "level 0 stays sorted" claim would be a lie.
        //
        // Small enough n to avoid tripping level-0 compaction, so we can
        // inspect the sorted[] flag and level 0 directly.
        let mut sketch: KllSketch<u32> = KllSketch::with_seed(1024, 42);
        for x in [7u32, 3, 10, 1, 5] {
            sketch.insert(x);
        }
        assert!(
            !sketch.sorted[0],
            "test setup: insert should leave sorted[0] = false"
        );
        sketch.absorb_sorted_slice(&[2u32, 4, 6, 8, 9]);
        assert!(
            sketch.sorted[0],
            "absorb_sorted_slice should sort-and-set level 0 before merge-extending"
        );
        assert!(
            sketch.levels[0].is_sorted(),
            "level 0 contents must actually be sorted: {:?}",
            sketch.levels[0]
        );
        // Sanity on the final counts and extremes across the mixed ingest.
        assert_eq!(sketch.rank(&11), 10);
        assert_eq!(sketch.quantile(0.0), Some(&1));
        assert_eq!(sketch.quantile(1.0), Some(&10));
    }

    #[test]
    fn absorb_sorted_slice_falls_back_on_unsorted_input() {
        // Fed unsorted data, absorb_sorted_slice must not silently corrupt
        // the sketch — it verifies sortedness and falls through to
        // absorb_slice. Guards against a future plan-shape change dropping
        // the upstream SortExec: the sketch would still answer correctly,
        // just without the sort-skip speedup.
        use rand::seq::SliceRandom;
        let seed = 12345u64;
        let mut shuffle_rng = StdRng::seed_from_u64(seed);
        let n = 10_000u32;
        let mut stream: Vec<u32> = (1..=n).collect();
        stream.shuffle(&mut shuffle_rng);

        let mut via_absorb_slice: KllSketch<u32> = KllSketch::with_seed(200, seed);
        via_absorb_slice.absorb_slice(&stream);

        let mut via_sorted: KllSketch<u32> = KllSketch::with_seed(200, seed);
        via_sorted.absorb_sorted_slice(&stream);

        for probe in (1..=n).step_by(100) {
            assert_eq!(
                via_absorb_slice.rank(&probe),
                via_sorted.rank(&probe),
                "rank({probe}) diverged"
            );
        }
        assert_eq!(via_absorb_slice.quantile(0.0), via_sorted.quantile(0.0));
        assert_eq!(via_absorb_slice.quantile(0.5), via_sorted.quantile(0.5));
        assert_eq!(via_absorb_slice.quantile(1.0), via_sorted.quantile(1.0));
    }

    #[test]
    fn rank_error_bound_uniform_distribution() {
        assert_ds_bound_holds("uniform", uniform_stream);
    }

    #[test]
    fn rank_error_bound_clustered_distribution() {
        assert_ds_bound_holds("clustered", clustered_stream);
    }

    #[test]
    fn rank_error_bound_heavy_ties() {
        assert_ds_bound_holds("heavy-ties", tied_stream);
    }

    #[test]
    fn merge_matches_single_sketch_within_bound() {
        // Insert the same shuffled `1..=n` stream two ways:
        //   Path A: straight into one sketch.
        //   Path B: split into 4 shards, one sketch each, merged pairwise.
        // Both carry O(ε) rank error individually, so any single quantile
        // probe differs by at most ~2ε in expectation. Bounds any merge
        // bug that would shift ranks by more than the sum of the two
        // sketches' inherent errors.
        use rand::seq::SliceRandom;
        let k = 200;
        let n = 10_000u32;
        let bound = 2.0 * normalized_rank_error_ss(k);
        let trials = 100u64;
        let mut failures = 0;
        for seed in 0..trials {
            let mut shuffle_rng = StdRng::seed_from_u64(seed);
            let mut stream: Vec<u32> = (1..=n).collect();
            stream.shuffle(&mut shuffle_rng);

            let mut single: KllSketch<u32> =
                KllSketch::with_seed(k, seed.wrapping_add(0x9E37_79B9));
            for x in &stream {
                single.insert(*x);
            }

            let shard_size = n as usize / 4;
            let mut shards: Vec<KllSketch<u32>> = (0..4u64)
                .map(|i| KllSketch::with_seed(k, seed.wrapping_add(0xB504_F334 + i)))
                .collect();
            for (i, x) in stream.iter().enumerate() {
                shards[(i / shard_size).min(3)].insert(*x);
            }
            let mut merged = shards.remove(0);
            for s in shards {
                merged.merge(s);
            }

            let worst_diff = (1..10)
                .map(|i| {
                    let q = i as f64 / 10.0;
                    let probe = ((q * n as f64) as u32).max(1);
                    let ra = single.rank(&probe) as f64 / n as f64;
                    let rb = merged.rank(&probe) as f64 / n as f64;
                    (ra - rb).abs()
                })
                .fold(0.0_f64, f64::max);

            if worst_diff > bound {
                failures += 1;
            }
        }
        let fail_rate = failures as f64 / trials as f64;
        assert!(
            fail_rate <= 0.05,
            "merge vs single quantile diff exceeded {bound:.4} in \
             {failures}/{trials} trials ({:.1}%); expected ≤ 5%",
            fail_rate * 100.0
        );
    }
}
