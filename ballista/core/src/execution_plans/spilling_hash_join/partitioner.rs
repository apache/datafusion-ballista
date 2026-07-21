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

// RowPartitioner hashes the join key columns of a `RecordBatch` and splits
// its rows into a fixed number of sub-partitions ("buckets"). It is used for
// both the build side and the probe side of `SpillingHashJoinExec` so that
// rows with equal join keys always land in the same sub-partition, whether
// they are processed in memory or after being spilled and re-read.
//
// The random state used for hashing is fixed and distinct from DataFusion's
// default so that bucket assignment here is independent of any hash used
// upstream (e.g. shuffle partitioning).

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::common::Result;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use datafusion::physical_expr::PhysicalExprRef;

/// A fixed, non-default seed for [`RandomState`] so that `RowPartitioner`
/// bucketing is independent of the hash used for shuffle partitioning
/// upstream. `RandomState::with_seed(0)` is defined to match
/// `RandomState::default()`, so any non-zero constant here is sufficient to
/// diverge from it; the exact value carries no other meaning.
const ROW_PARTITIONER_SEED: u64 = 0x5350_4a5f_484a_3121;

/// Derives the hashing seed for drain-phase recursion `depth`.
/// `seed_for_depth(0)` returns the base seed, so level-0 bucketing is
/// byte-identical to [`RowPartitioner::new`]; deeper levels perturb the seed so
/// re-partitioning a bucket that did not fit splits its keys differently than
/// the level that produced it. The multiplier is an arbitrary odd constant
/// (the golden-ratio mix) chosen only to spread successive depths apart.
pub fn seed_for_depth(depth: usize) -> u64 {
    ROW_PARTITIONER_SEED ^ (depth as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Splits the rows of a `RecordBatch` into `num_sub` sub-partitions by
/// hashing a set of join key expressions, so that rows with equal keys
/// always land in the same sub-partition.
pub struct RowPartitioner {
    keys: Vec<PhysicalExprRef>,
    num_sub: usize,
    random_state: RandomState,
}

/// One non-empty bucket produced by [`RowPartitioner::partition`]: the
/// bucket index, the sub-batch of rows routed to it, and the per-row hash
/// values for those rows (in the same order as the sub-batch's rows) so
/// callers can reuse them without re-hashing.
pub struct PartitionedBatch {
    /// Index of the sub-partition this batch was routed to, in
    /// `0..num_sub`.
    pub bucket: usize,
    /// The subset of rows from the input batch routed to `bucket`.
    pub batch: RecordBatch,
    /// The hash value of each row in `batch`, in the same row order.
    pub hashes: Vec<u64>,
}

impl RowPartitioner {
    /// Creates a new `RowPartitioner` that hashes `keys` and routes rows
    /// into `num_sub` sub-partitions using a fixed, non-default random
    /// state.
    pub fn new(keys: Vec<PhysicalExprRef>, num_sub: usize) -> Self {
        Self::with_seed(keys, num_sub, ROW_PARTITIONER_SEED)
    }

    /// Creates a `RowPartitioner` hashing with an explicit `seed`. Used by the
    /// drain-phase recursion to split an oversized bucket differently than the
    /// level that produced it (see [`seed_for_depth`]); `new` delegates here
    /// with the fixed base seed.
    pub fn with_seed(keys: Vec<PhysicalExprRef>, num_sub: usize, seed: u64) -> Self {
        Self {
            keys,
            num_sub,
            random_state: RandomState::with_seed(seed),
        }
    }

    /// Hashes the key columns of `batch` and splits its rows into
    /// sub-partitions. Bucket assignment is `(hash >> 32) % num_sub`. Only
    /// non-empty buckets are returned, each carrying the hash values for the
    /// rows it contains.
    pub fn partition(&self, batch: &RecordBatch) -> Result<Vec<PartitionedBatch>> {
        let num_rows = batch.num_rows();

        let key_arrays: Vec<ArrayRef> = self
            .keys
            .iter()
            .map(|expr| expr.evaluate(batch)?.into_array(num_rows))
            .collect::<Result<_>>()?;

        let mut hashes = vec![0u64; num_rows];
        create_hashes(&key_arrays, &self.random_state, &mut hashes)?;

        let mut indices: Vec<Vec<u32>> = vec![Vec::new(); self.num_sub];
        let mut bucket_hashes: Vec<Vec<u64>> = vec![Vec::new(); self.num_sub];
        for (row, &hash) in hashes.iter().enumerate() {
            let bucket = ((hash >> 32) as usize) % self.num_sub;
            indices[bucket].push(row as u32);
            bucket_hashes[bucket].push(hash);
        }

        let schema = batch.schema();
        let mut out = Vec::new();
        for (bucket, row_indices) in indices.into_iter().enumerate() {
            if row_indices.is_empty() {
                continue;
            }
            let idx = UInt32Array::from(row_indices);
            let taken_cols = batch
                .columns()
                .iter()
                .map(|col| take(col, &idx, None))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let sub_batch = RecordBatch::try_new(Arc::clone(&schema), taken_cols)?;
            out.push(PartitionedBatch {
                bucket,
                batch: sub_batch,
                hashes: std::mem::take(&mut bucket_hashes[bucket]),
            });
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;

    fn batch_with_keys(keys: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(keys))]).unwrap()
    }

    fn partitioner(num_sub: usize) -> RowPartitioner {
        let keys: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("k", 0))];
        RowPartitioner::new(keys, num_sub)
    }

    #[test]
    fn partitions_cover_all_rows_and_are_stable() {
        let num_sub = 4;
        let p = partitioner(num_sub);

        let batch_a = batch_with_keys(vec![1, 2, 3, 1, 2]);
        let batch_b = batch_with_keys(vec![2, 1]);

        let out_a = p.partition(&batch_a).unwrap();
        let out_b = p.partition(&batch_b).unwrap();

        // All rows accounted for, per batch.
        let rows_a: usize = out_a.iter().map(|pb| pb.batch.num_rows()).sum();
        let rows_b: usize = out_b.iter().map(|pb| pb.batch.num_rows()).sum();
        assert_eq!(rows_a, batch_a.num_rows());
        assert_eq!(rows_b, batch_b.num_rows());

        // No empty buckets returned, and every bucket index is in range.
        for pb in out_a.iter().chain(out_b.iter()) {
            assert!(pb.bucket < num_sub);
            assert!(pb.batch.num_rows() > 0);
            assert_eq!(pb.batch.num_rows(), pb.hashes.len());
        }

        // Stability: key value 1 must land in the same bucket in both batches.
        let bucket_for_key = |out: &[PartitionedBatch], key: i32| -> usize {
            for pb in out {
                let col = pb
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                if col.iter().any(|v| v == Some(key)) {
                    return pb.bucket;
                }
            }
            panic!("key {key} not found in any bucket");
        };

        assert_eq!(bucket_for_key(&out_a, 1), bucket_for_key(&out_b, 1));
        assert_eq!(bucket_for_key(&out_a, 2), bucket_for_key(&out_b, 2));
    }

    #[test]
    fn stable_across_recreated_partitioner() {
        // Later tasks recompute hashes on spilled data with a brand-new
        // `RowPartitioner`, so the fixed seed must make bucket assignment
        // (and the hash values themselves) reproducible across instances,
        // not just across calls on the same instance.
        let num_sub = 4;
        let batch = batch_with_keys(vec![1, 2, 3, 1, 2]);

        let out_1 = partitioner(num_sub).partition(&batch).unwrap();
        let out_2 = partitioner(num_sub).partition(&batch).unwrap();

        assert_eq!(out_1.len(), out_2.len());
        for (a, b) in out_1.iter().zip(out_2.iter()) {
            assert_eq!(a.bucket, b.bucket);
            assert_eq!(a.hashes, b.hashes);
        }
    }
}
