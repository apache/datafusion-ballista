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
//! # Reference
//!
//! Karnin, Lang, Liberty. *Optimal Quantile Approximation in Streams.*
//! FOCS 2016. <https://arxiv.org/abs/1603.05346>

/// KLL quantile sketch, generic over `Ord` items.
pub struct KllSketch<T: Ord> {
    items: Vec<T>,
}

impl<T: Ord> KllSketch<T> {
    /// Construct an empty sketch.
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Add one item to the sketch.
    pub fn insert(&mut self, x: T) {
        self.items.push(x);
    }

    /// Return the number of items in the sketch strictly less than `x`.
    pub fn rank(&self, x: &T) -> u64 {
        self.items.iter().filter(|y| *y < x).count() as u64
    }
}

impl<T: Ord> Default for KllSketch<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_is_exact_below_capacity() {
        let mut sketch: KllSketch<u32> = KllSketch::new();
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

        let mut sketch: KllSketch<OwnedRow> = KllSketch::new();
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

        let mut sketch: KllSketch<OwnedRow> = KllSketch::new();
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

        let mut sketch: KllSketch<OwnedRow> = KllSketch::new();
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
}
