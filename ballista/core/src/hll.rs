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

//! HyperLogLog distinct-count sketch for shuffle output columns.
//!
//! Shuffle writers update one sketch per sketched column, executors ship the
//! registers to the scheduler in `TaskColumnStats.sketches`, and the scheduler
//! merges every task's sketch for a column into a stage-level distinct count.
//!
//! Values are hashed with DataFusion's `HLL_RANDOM_STATE`, the hash state
//! `approx_distinct` uses, and the count uses the same estimator (Ertl,
//! "New cardinality estimation algorithms for HyperLogLog sketches", 2017).
//!
//! # Precision
//!
//! A sketch's precision `p` is the number of hash bits that pick a register.
//! It has `2^p` one-byte registers, which is also its serialized size, and a
//! standard error of about `1.04 / 2^(p/2)`, so each 4x in size halves the
//! error:
//!
//! | precision | size   | standard error |
//! | --------- | ------ | -------------- |
//! | 10        | 1 KiB  | 3.3%           |
//! | 12        | 4 KiB  | 1.6%           |
//! | 14        | 16 KiB | 0.8%           |
//!
//! The sketch is not tied to one precision on the wire: the register count
//! tells [`HyperLogLog::from_bytes`](crate::hll::HyperLogLog::from_bytes) which
//! one it holds, and only sketches of the same precision merge.
//!
//! The default, [`DEFAULT_PRECISION`](crate::hll::DEFAULT_PRECISION), is 12
//! rather than DataFusion's 14. The estimate feeds join cardinalities, where
//! a relative error in the distinct count carries straight through to the
//! estimated row count, so an error of a couple of percent is small next to
//! the row-count errors that having a distinct count at all corrects. The
//! cost of a sketch is its size, once per key column per task: the scheduler
//! keeps every successful task's sketches. Precision 10 would save 3 KiB of
//! that, about 60 MB for a stage of 10,000 tasks with two key columns.
//! Deployments that big can lower the precision with
//! `ballista.shuffle.collect_distinct_counts_precision`.

use datafusion::arrow::array::Array;
use datafusion::common::Result;
use datafusion::common::hash_utils::{HLL_RANDOM_STATE, create_hashes};

/// The smallest supported precision, 16 registers.
pub const MIN_PRECISION: u8 = 4;
/// The largest supported precision, 64 Ki registers.
pub const MAX_PRECISION: u8 = 16;
/// The precision shuffle writers use unless configured otherwise.
pub const DEFAULT_PRECISION: u8 = 12;

/// A HyperLogLog sketch with `2^precision` one-byte registers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HyperLogLog {
    /// Number of hash bits that select a register.
    precision: u8,
    registers: Box<[u8]>,
}

impl HyperLogLog {
    /// An empty sketch with `2^precision` registers.
    ///
    /// # Panics
    ///
    /// If `precision` is outside [`MIN_PRECISION`] to [`MAX_PRECISION`].
    pub fn new(precision: u8) -> Self {
        assert!(
            (MIN_PRECISION..=MAX_PRECISION).contains(&precision),
            "HyperLogLog precision {precision} is outside \
             {MIN_PRECISION}..={MAX_PRECISION}"
        );
        Self {
            precision,
            registers: vec![0; 1 << precision].into_boxed_slice(),
        }
    }

    /// The number of hash bits that select a register.
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Restores a sketch from [`Self::as_bytes`]. Returns `None` if `bytes`
    /// is not a valid register array: its length must be `2^p` for a
    /// supported precision `p`, and no register can exceed the largest rank
    /// a hash can produce.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if !bytes.len().is_power_of_two() {
            return None;
        }
        let precision = u8::try_from(bytes.len().trailing_zeros()).ok()?;
        if !(MIN_PRECISION..=MAX_PRECISION).contains(&precision) {
            return None;
        }
        if bytes.iter().any(|&r| r > max_rank(precision)) {
            return None;
        }
        Some(Self {
            precision,
            registers: bytes.into(),
        })
    }

    /// The registers, which are the sketch's serialized form.
    pub fn as_bytes(&self) -> &[u8] {
        &self.registers
    }

    /// Adds every non-null value of `array`. `hashes` is scratch space,
    /// reused across calls to avoid an allocation per batch.
    pub fn update(&mut self, array: &dyn Array, hashes: &mut Vec<u64>) -> Result<()> {
        hashes.clear();
        hashes.resize(array.len(), 0);
        create_hashes([array], &HLL_RANDOM_STATE, hashes)?;
        match array.logical_nulls() {
            None => hashes.iter().for_each(|&hash| self.add_hashed(hash)),
            Some(nulls) => {
                for (row, &hash) in hashes.iter().enumerate() {
                    if nulls.is_valid(row) {
                        self.add_hashed(hash);
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn add_hashed(&mut self, hash: u64) {
        let index = (hash & ((1 << self.precision) - 1)) as usize;
        let rho = ((hash >> self.precision) | (1 << (64 - self.precision)))
            .trailing_zeros() as u8
            + 1;
        self.registers[index] = self.registers[index].max(rho);
    }

    /// Folds `other` into this sketch, so it counts the union of both inputs.
    /// Registers of different precisions don't line up, so if the two differ
    /// this returns `false` and leaves the sketch unchanged.
    #[must_use = "a sketch of a different precision is not merged"]
    pub fn merge(&mut self, other: &HyperLogLog) -> bool {
        if self.precision != other.precision {
            return false;
        }
        for (mine, theirs) in self.registers.iter_mut().zip(other.registers.iter()) {
            *mine = (*mine).max(*theirs);
        }
        true
    }

    /// The estimated number of distinct values added.
    pub fn count(&self) -> usize {
        let q = 64 - self.precision as usize;
        let mut histogram = [0u32; 64 - MIN_PRECISION as usize + 2];
        for &r in self.registers.iter() {
            histogram[r as usize] += 1;
        }
        let m = self.registers.len() as f64;
        let mut z = m * tau((m - histogram[q + 1] as f64) / m);
        for &h in histogram[1..=q].iter().rev() {
            z += h as f64;
            z *= 0.5;
        }
        z += m * sigma(histogram[0] as f64 / m);
        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }
}

/// The largest value a register can hold at `precision`: the rank of a hash
/// whose bits after the register index are all zero.
fn max_rank(precision: u8) -> u8 {
    64 - precision + 1
}

fn sigma(x: f64) -> f64 {
    if x == 1. {
        return f64::INFINITY;
    }
    let (mut x, mut y, mut z) = (x, 1.0, x);
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            return z;
        }
    }
}

fn tau(x: f64) -> f64 {
    if x == 0. || x == 1. {
        return 0.;
    }
    let (mut x, mut y, mut z) = (x, 1.0, 1.0 - x);
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z {
            return z / 3.0;
        }
    }
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
    fn size_is_two_to_the_precision() {
        for precision in MIN_PRECISION..=MAX_PRECISION {
            let hll = HyperLogLog::new(precision);
            assert_eq!(hll.precision(), precision);
            assert_eq!(hll.as_bytes().len(), 1 << precision);
        }
    }

    #[test]
    fn skips_nulls() {
        let array = StringArray::from(vec![Some("a"), None, Some("b"), None]);
        let mut hll = HyperLogLog::new(DEFAULT_PRECISION);
        hll.update(&array, &mut Vec::new()).unwrap();
        assert_eq!(hll.count(), 2);
    }

    #[test]
    fn merge_counts_the_union() {
        for precision in [10, DEFAULT_PRECISION] {
            let mut left = sketch_at(precision, 0..60_000);
            assert!(left.merge(&sketch_at(precision, 40_000..100_000)));
            assert_close(precision, left.count(), 100_000);
        }
    }

    #[test]
    fn merge_refuses_a_different_precision() {
        let mut left = sketch_at(12, 0..1_000);
        let before = left.clone();
        assert!(!left.merge(&sketch_at(10, 0..5_000)));
        assert_eq!(left, before);
    }

    #[test]
    fn round_trips_through_bytes() {
        for precision in [MIN_PRECISION, 10, DEFAULT_PRECISION, MAX_PRECISION] {
            let hll = sketch_at(precision, 0..1_000);
            assert_eq!(HyperLogLog::from_bytes(hll.as_bytes()), Some(hll));
        }
    }

    #[test]
    fn from_bytes_rejects_what_is_not_a_sketch() {
        // Not a supported register count: empty, not a power of two, below
        // the smallest precision, above the largest.
        for len in [0, 1, 8, 15, 3_000, 1 << 17] {
            assert_eq!(HyperLogLog::from_bytes(&vec![0; len]), None, "len {len}");
        }
        // A register above the largest rank its precision allows.
        for precision in [MIN_PRECISION, DEFAULT_PRECISION, MAX_PRECISION] {
            let mut bytes = vec![0; 1 << precision];
            bytes[0] = max_rank(precision);
            assert!(HyperLogLog::from_bytes(&bytes).is_some());
            bytes[0] += 1;
            assert_eq!(HyperLogLog::from_bytes(&bytes), None);
        }
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
