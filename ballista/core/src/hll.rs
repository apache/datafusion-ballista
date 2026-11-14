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

//! HyperLogLog distinct-count sketch for shuffle output columns, backed by the
//! Apache DataSketches HLL sketch.
//!
//! Shuffle writers update one sketch per sketched column, executors ship it to
//! the scheduler in `TaskColumnStats.sketches`, and the scheduler merges every
//! task's sketch for a column into a stage-level distinct count with a
//! [`HyperLogLogUnion`].
//!
//! Values are hashed with DataFusion's `HLL_RANDOM_STATE`, the hash state
//! `approx_distinct` uses, and the hashes are added to the sketch.
//!
//! # Precision
//!
//! A sketch's precision `p` is the number of hash bits that pick a register.
//! It has `2^p` registers, serialized at 4 bits each once it holds many
//! values (and much smaller before that), and a standard error of about
//! `1.04 / 2^(p/2)`, so each 4x in size halves the error:
//!
//! | precision | size    | standard error |
//! | --------- | ------- | -------------- |
//! | 10        | 512 B   | 3.3%           |
//! | 12        | 2 KiB   | 1.6%           |
//! | 14        | 8 KiB   | 0.8%           |
//!
//! Sketches of different precisions merge, at the lowest of them.
//!
//! The default, [`DEFAULT_PRECISION`](crate::hll::DEFAULT_PRECISION), is 12
//! rather than DataFusion's 14. The estimate feeds join cardinalities, where
//! a relative error in the distinct count carries straight through to the
//! estimated row count, so an error of a couple of percent is small next to
//! the row-count errors that having a distinct count at all corrects. The
//! cost of a sketch is its size, once per key column per task: the scheduler
//! keeps every successful task's sketches. Deployments with many thousands of
//! tasks per stage can lower the precision with
//! `ballista.shuffle.collect_distinct_counts_precision`.

use datafusion::arrow::array::Array;
use datafusion::common::Result;
use datafusion::common::hash_utils::{HLL_RANDOM_STATE, create_hashes};
use datasketches::hll::{HllSketch, HllType, HllUnion};

/// The smallest supported precision, 16 registers.
pub const MIN_PRECISION: u8 = 4;
/// The largest supported precision, 64 Ki registers.
pub const MAX_PRECISION: u8 = 16;
/// The precision shuffle writers use unless configured otherwise.
pub const DEFAULT_PRECISION: u8 = 12;

/// A HyperLogLog sketch with `2^precision` registers.
#[derive(Clone, Debug, PartialEq)]
pub struct HyperLogLog(HllSketch);

impl HyperLogLog {
    /// An empty sketch with `2^precision` registers.
    ///
    /// # Panics
    ///
    /// If `precision` is outside [`MIN_PRECISION`] to [`MAX_PRECISION`].
    pub fn new(precision: u8) -> Self {
        assert_precision(precision);
        Self(HllSketch::new(precision, HllType::Hll8).expect("precision is in range"))
    }

    /// The number of hash bits that select a register.
    pub fn precision(&self) -> u8 {
        self.0.lg_config_k()
    }

    /// Restores a sketch from [`Self::to_bytes`]. Returns `None` if `bytes`
    /// is not a sketch of a supported precision.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let sketch = HllSketch::deserialize(bytes).ok()?;
        (MIN_PRECISION..=MAX_PRECISION)
            .contains(&sketch.lg_config_k())
            .then_some(Self(sketch))
    }

    /// The sketch's serialized form.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.serialize()
    }

    /// Adds every non-null value of `array`. `hashes` is scratch space,
    /// reused across calls to avoid an allocation per batch.
    pub fn update(&mut self, array: &dyn Array, hashes: &mut Vec<u64>) -> Result<()> {
        hashes.clear();
        hashes.resize(array.len(), 0);
        create_hashes([array], &HLL_RANDOM_STATE, hashes)?;
        match array.logical_nulls() {
            None => hashes.iter().for_each(|&hash| self.0.update(hash)),
            Some(nulls) => {
                for (row, &hash) in hashes.iter().enumerate() {
                    if nulls.is_valid(row) {
                        self.0.update(hash);
                    }
                }
            }
        }
        Ok(())
    }

    /// The estimated number of distinct values added.
    pub fn count(&self) -> usize {
        self.0.estimate().round() as usize
    }
}

/// Merges many [`HyperLogLog`] sketches into one.
#[derive(Clone, Debug)]
pub struct HyperLogLogUnion(HllUnion);

impl HyperLogLogUnion {
    /// An empty union of at most `2^precision` registers. Sketches of a
    /// higher precision are merged at this one.
    ///
    /// # Panics
    ///
    /// If `precision` is outside [`MIN_PRECISION`] to [`MAX_PRECISION`].
    pub fn new(precision: u8) -> Self {
        assert_precision(precision);
        Self(HllUnion::new(precision).expect("precision is in range"))
    }

    /// The highest precision the union keeps.
    pub fn precision(&self) -> u8 {
        self.0.lg_max_k()
    }

    /// Folds `sketch` into the union.
    pub fn update(&mut self, sketch: &HyperLogLog) {
        self.0.update(&sketch.0);
    }

    /// The estimated number of distinct values across the merged sketches.
    pub fn count(&self) -> usize {
        self.0.estimate().round() as usize
    }

    /// The union as a sketch, in its most compact form.
    pub fn to_sketch(&self) -> HyperLogLog {
        HyperLogLog(self.0.to_sketch(HllType::Hll4))
    }
}

fn assert_precision(precision: u8) {
    assert!(
        (MIN_PRECISION..=MAX_PRECISION).contains(&precision),
        "HyperLogLog precision {precision} is outside \
         {MIN_PRECISION}..={MAX_PRECISION}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};

    fn sketch_at(precision: u8, values: impl IntoIterator<Item = i64>) -> HyperLogLog {
        let array = Int64Array::from_iter_values(values);
        let mut hll = HyperLogLog::new(precision);
        hll.update(&array, &mut Vec::new()).unwrap();
        hll
    }

    fn sketch(values: impl IntoIterator<Item = i64>) -> HyperLogLog {
        sketch_at(DEFAULT_PRECISION, values)
    }

    fn union_of(precision: u8, sketches: &[HyperLogLog]) -> HyperLogLogUnion {
        let mut union = HyperLogLogUnion::new(precision);
        sketches.iter().for_each(|sketch| union.update(sketch));
        union
    }

    // Six standard errors, so the tests are deterministic in practice.
    fn assert_close(precision: u8, got: usize, expected: usize) {
        let registers = (1u64 << precision) as f64;
        let margin = 6.0 * 1.04 / registers.sqrt();
        let error = (got as f64 - expected as f64).abs() / expected as f64;
        assert!(
            error <= margin,
            "precision {precision}: estimated {got}, expected about {expected}"
        );
    }

    #[test]
    fn empty_counts_zero() {
        for precision in MIN_PRECISION..=MAX_PRECISION {
            assert_eq!(HyperLogLog::new(precision).count(), 0);
        }
    }

    #[test]
    fn counts_distinct_values_not_rows() {
        assert_eq!(sketch([7, 7, 7]).count(), 1);
        for n in [100, 10_000, 1_000_000] {
            assert_close(
                DEFAULT_PRECISION,
                sketch((0..n).chain(0..n)).count(),
                n as usize,
            );
        }
    }

    #[test]
    fn counts_within_the_standard_error_at_every_precision() {
        for precision in [8, 10, 12, 14, 16] {
            for n in [1_000, 200_000] {
                assert_close(precision, sketch_at(precision, 0..n).count(), n as usize);
            }
        }
    }

    #[test]
    fn few_distinct_values_serialize_small() {
        let few = sketch([1, 2, 3]).to_bytes().len();
        let many = union_of(DEFAULT_PRECISION, &[sketch(0..1_000_000)])
            .to_sketch()
            .to_bytes()
            .len();
        assert!(few < 100, "{few} bytes");
        assert!(many <= (1 << DEFAULT_PRECISION) / 2 + 100, "{many} bytes");
    }

    #[test]
    fn skips_nulls() {
        let array = StringArray::from(vec![Some("a"), None, Some("b"), None]);
        let mut hll = HyperLogLog::new(DEFAULT_PRECISION);
        hll.update(&array, &mut Vec::new()).unwrap();
        assert_eq!(hll.count(), 2);
    }

    #[test]
    fn union_counts_the_union() {
        for precision in [10, DEFAULT_PRECISION] {
            let union = union_of(
                precision,
                &[
                    sketch_at(precision, 0..60_000),
                    sketch_at(precision, 40_000..100_000),
                ],
            );
            assert_close(precision, union.count(), 100_000);
            assert_close(precision, union.to_sketch().count(), 100_000);
        }
    }

    #[test]
    fn union_merges_different_precisions_at_the_lowest() {
        let union = union_of(
            DEFAULT_PRECISION,
            &[sketch_at(12, 0..60_000), sketch_at(10, 40_000..100_000)],
        );
        assert_eq!(union.to_sketch().precision(), 10);
        assert_close(10, union.count(), 100_000);
    }

    #[test]
    fn round_trips_through_bytes() {
        for precision in [MIN_PRECISION, 10, DEFAULT_PRECISION, MAX_PRECISION] {
            let hll = sketch_at(precision, 0..1_000);
            let restored = HyperLogLog::from_bytes(&hll.to_bytes()).unwrap();
            assert_eq!(restored.precision(), precision);
            assert_eq!(restored.count(), hll.count());
        }
    }

    #[test]
    fn from_bytes_rejects_what_is_not_a_sketch() {
        assert_eq!(HyperLogLog::from_bytes(&[]), None);
        assert_eq!(HyperLogLog::from_bytes(&[1, 2, 3]), None);
        assert_eq!(HyperLogLog::from_bytes(&vec![0; 4096]), None);
    }

    #[test]
    #[should_panic(expected = "outside")]
    fn new_rejects_a_precision_below_the_minimum() {
        HyperLogLog::new(MIN_PRECISION - 1);
    }

    #[test]
    #[should_panic(expected = "outside")]
    fn new_rejects_a_precision_above_the_maximum() {
        HyperLogLog::new(MAX_PRECISION + 1);
    }
}
