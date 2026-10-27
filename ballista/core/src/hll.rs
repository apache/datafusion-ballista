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
//! Precision is 12 rather than DataFusion's 14: 4 KiB of registers per
//! column per task instead of 16 KiB, for a standard error of about 1.6%,
//! which is plenty for join cardinality estimation.

use datafusion::arrow::array::Array;
use datafusion::common::Result;
use datafusion::common::hash_utils::{HLL_RANDOM_STATE, create_hashes};

/// Number of hash bits that select a register.
const PRECISION: usize = 12;
/// Hash bits left over for the leading-zero count.
const Q: usize = 64 - PRECISION;
/// Number of registers, and so the serialized size in bytes.
pub const NUM_REGISTERS: usize = 1 << PRECISION;
const REGISTER_MASK: u64 = NUM_REGISTERS as u64 - 1;

/// A HyperLogLog sketch with 2^12 one-byte registers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HyperLogLog {
    registers: Box<[u8; NUM_REGISTERS]>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    /// An empty sketch.
    pub fn new() -> Self {
        Self {
            registers: Box::new([0; NUM_REGISTERS]),
        }
    }

    /// Restores a sketch from [`Self::as_bytes`]. Returns `None` if `bytes`
    /// is not a valid register array for this precision.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let registers: [u8; NUM_REGISTERS] = bytes.try_into().ok()?;
        if registers.iter().any(|&r| r as usize > Q + 1) {
            return None;
        }
        Some(Self {
            registers: Box::new(registers),
        })
    }

    /// The registers, which are the sketch's serialized form.
    pub fn as_bytes(&self) -> &[u8] {
        self.registers.as_slice()
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
        let index = (hash & REGISTER_MASK) as usize;
        let rho = ((hash >> PRECISION) | (1 << Q)).trailing_zeros() as u8 + 1;
        self.registers[index] = self.registers[index].max(rho);
    }

    /// Folds `other` into this sketch, so it counts the union of both inputs.
    pub fn merge(&mut self, other: &HyperLogLog) {
        for (mine, theirs) in self.registers.iter_mut().zip(other.registers.iter()) {
            *mine = (*mine).max(*theirs);
        }
    }

    /// The estimated number of distinct values added.
    pub fn count(&self) -> usize {
        let mut histogram = [0u32; Q + 2];
        for &r in self.registers.iter() {
            histogram[r as usize] += 1;
        }
        let m = NUM_REGISTERS as f64;
        let mut z = m * tau((m - histogram[Q + 1] as f64) / m);
        for &h in histogram[1..=Q].iter().rev() {
            z += h as f64;
            z *= 0.5;
        }
        z += m * sigma(histogram[0] as f64 / m);
        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }
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

    fn sketch(values: impl IntoIterator<Item = i64>) -> HyperLogLog {
        let array = Int64Array::from_iter_values(values);
        let mut hll = HyperLogLog::new();
        hll.update(&array, &mut Vec::new()).unwrap();
        hll
    }

    // Six standard errors, so the tests are deterministic in practice.
    fn assert_close(got: usize, expected: usize) {
        let margin = 6.0 * 1.04 / (NUM_REGISTERS as f64).sqrt();
        let error = (got as f64 - expected as f64).abs() / expected as f64;
        assert!(
            error <= margin,
            "estimated {got}, expected about {expected}"
        );
    }

    #[test]
    fn empty_counts_zero() {
        assert_eq!(HyperLogLog::new().count(), 0);
    }

    #[test]
    fn counts_distinct_values_not_rows() {
        assert_eq!(sketch([7, 7, 7]).count(), 1);
        for n in [100, 10_000, 1_000_000] {
            assert_close(sketch((0..n).chain(0..n)).count(), n as usize);
        }
    }

    #[test]
    fn skips_nulls() {
        let array = StringArray::from(vec![Some("a"), None, Some("b"), None]);
        let mut hll = HyperLogLog::new();
        hll.update(&array, &mut Vec::new()).unwrap();
        assert_eq!(hll.count(), 2);
    }

    #[test]
    fn merge_counts_the_union() {
        let mut left = sketch(0..60_000);
        left.merge(&sketch(40_000..100_000));
        assert_close(left.count(), 100_000);
    }

    #[test]
    fn round_trips_through_bytes() {
        let hll = sketch(0..1_000);
        assert_eq!(HyperLogLog::from_bytes(hll.as_bytes()), Some(hll));
        assert_eq!(HyperLogLog::from_bytes(&[0; 16]), None);
        assert_eq!(HyperLogLog::from_bytes(&[Q as u8 + 2; NUM_REGISTERS]), None);
    }
}
