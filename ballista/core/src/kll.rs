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
//! `k` is the compactor capacity. Larger `k` uses more memory and yields
//! smaller error, roughly `O(1/k · sqrt(log(n/k)))`. This implementation
//! uses a fixed `k` at every level; the paper's refinement of
//! geometrically-shrinking level sizes is not implemented here.
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
//! # Reference
//!
//! Karnin, Lang, Liberty. *Optimal Quantile Approximation in Streams.*
//! FOCS 2016. <https://arxiv.org/abs/1603.05346>

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// KLL quantile sketch, generic over `Ord` items.
pub struct KllSketch<T: Ord> {
    /// One buffer per level; `levels[h]` is the level-`h` compactor.
    levels: Vec<Vec<T>>,
    /// Max items each compactor holds before it must compact.
    k: usize,
    /// PRNG advanced once per compaction coin flip.
    rng: StdRng,
}

impl<T: Ord> KllSketch<T> {
    /// Construct an empty sketch with compactor capacity `k`, seeded from
    /// OS entropy.
    pub fn new(k: usize) -> Self {
        Self::with_seed(k, rand::random::<u64>())
    }

    /// Construct an empty sketch with compactor capacity `k` and a fixed
    /// PRNG seed. Intended for deterministic tests.
    pub fn with_seed(k: usize, seed: u64) -> Self {
        Self {
            levels: vec![Vec::new()],
            k,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Add one item to the sketch.
    pub fn insert(&mut self, x: T) {
        self.levels[0].push(x);
        self.compact_from(0);
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

    /// Walk up from level `h`, compacting every level that has filled to `k`.
    fn compact_from(&mut self, mut h: usize) {
        loop {
            if h >= self.levels.len() || self.levels[h].len() < self.k {
                return;
            }
            let mut buf = std::mem::take(&mut self.levels[h]);
            buf.sort_unstable();
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
            h += 1;
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
}
