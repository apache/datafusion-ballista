/// KLL quantile sketch (Karnin-Lang-Liberty), generic over `Ord` items.
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
