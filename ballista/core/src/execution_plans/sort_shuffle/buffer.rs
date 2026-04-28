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

//! In-memory buffering for sort-based shuffle.
//!
//! Holds whole input record batches plus per-output-partition row indices
//! (`(batch_idx, row_idx)` pairs). Rows are not copied at insertion time;
//! materialization is deferred to spill or final-write time and performed
//! via `arrow::compute::interleave_record_batch`.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;

/// Holds whole input `RecordBatch`es and per-partition `(batch_idx, row_idx)`
/// index lists. Rows are not copied at insertion time — only the indices are
/// recorded. Materialization happens through `PartitionedBatchIterator` at
/// spill or final-write time, by way of `interleave_record_batch`.
#[derive(Debug)]
pub struct BufferedBatches {
    schema: SchemaRef,
    /// All input batches, in arrival order. Indexed by `batch_idx` in
    /// `indices`. Never sliced.
    batches: Vec<RecordBatch>,
    /// One entry per output partition. Each `(u32, u32)` is `(batch_idx,
    /// row_idx)`, referring to a row inside `batches[batch_idx]`.
    indices: Vec<Vec<(u32, u32)>>,
    /// Total rows currently referenced by `indices`. Test diagnostic only.
    num_buffered_rows: usize,
}

impl BufferedBatches {
    /// Creates a new buffer for the given partition count and schema.
    pub fn new(num_partitions: usize, schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            indices: (0..num_partitions).map(|_| Vec::new()).collect(),
            num_buffered_rows: 0,
        }
    }

    /// Returns the configured number of output partitions.
    pub fn num_partitions(&self) -> usize {
        self.indices.len()
    }

    /// Returns true if no batches have been pushed yet.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Returns the total number of rows currently referenced by indices.
    #[allow(dead_code)]
    pub fn num_buffered_rows(&self) -> usize {
        self.num_buffered_rows
    }

    /// Returns the total heap-allocated size, in bytes, of the per-partition
    /// `(batch_idx, row_idx)` index `Vec`s. Uses capacity (not length) so the
    /// figure tracks actual heap allocation as `Vec`s grow.
    pub fn indices_allocated_size(&self) -> usize {
        self.indices
            .iter()
            .map(|v| v.capacity() * std::mem::size_of::<(u32, u32)>())
            .sum()
    }

    /// Returns the row indices for output partition `partition_id`.
    pub fn indices_for(&self, partition_id: usize) -> &[(u32, u32)] {
        &self.indices[partition_id]
    }

    /// Pushes a whole input `batch` and records, for each output partition
    /// `p`, the row indices from `per_partition_rows[p]` as `(batch_idx, r)`
    /// pairs in that partition's index list.
    ///
    /// `per_partition_rows.len()` must equal `num_partitions()`.
    pub fn push_batch(&mut self, batch: RecordBatch, per_partition_rows: &[Vec<u32>]) {
        debug_assert_eq!(per_partition_rows.len(), self.indices.len());
        debug_assert!(
            *batch.schema() == *self.schema,
            "BufferedBatches::push_batch schema mismatch"
        );
        let batch_idx = self.batches.len() as u32;
        for (p, rows) in per_partition_rows.iter().enumerate() {
            let dst = &mut self.indices[p];
            dst.reserve(rows.len());
            for &r in rows {
                dst.push((batch_idx, r));
                self.num_buffered_rows += 1;
            }
        }
        self.batches.push(batch);
    }

    /// Drains all state, returning the buffered batches and per-partition
    /// index lists. After this call the buffer is empty.
    pub fn take(&mut self) -> (Vec<RecordBatch>, Vec<Vec<(u32, u32)>>) {
        self.num_buffered_rows = 0;
        let batches = std::mem::take(&mut self.batches);
        // Preserve the partition count by replacing each inner vec with empty
        let indices: Vec<Vec<(u32, u32)>> =
            self.indices.iter_mut().map(std::mem::take).collect();
        (batches, indices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn create_test_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn buffered_batches_pushes_and_partitions_indices() {
        let schema = create_test_schema();
        let mut bb = BufferedBatches::new(3, schema.clone());
        assert!(bb.is_empty());
        assert_eq!(bb.num_partitions(), 3);

        let batch_a = create_test_batch(&schema, vec![10, 20, 30, 40]);
        let batch_b = create_test_batch(&schema, vec![50, 60]);

        // Partition 0 gets rows {0, 2} from batch 0; partition 1 gets {1, 3} from
        // batch 0; partition 2 gets {0, 1} from batch 1.
        let per_partition_a: Vec<Vec<u32>> = vec![vec![0, 2], vec![1, 3], vec![]];
        let per_partition_b: Vec<Vec<u32>> = vec![vec![], vec![], vec![0, 1]];

        bb.push_batch(batch_a, &per_partition_a);
        bb.push_batch(batch_b, &per_partition_b);

        assert!(!bb.is_empty());
        // Total rows referenced by indices: 2 + 2 + 2 = 6
        assert_eq!(bb.num_buffered_rows(), 6);

        let p0 = bb.indices_for(0);
        assert_eq!(p0, &[(0u32, 0u32), (0, 2)]);
        let p1 = bb.indices_for(1);
        assert_eq!(p1, &[(0u32, 1u32), (0, 3)]);
        let p2 = bb.indices_for(2);
        assert_eq!(p2, &[(1u32, 0u32), (1, 1)]);
    }

    #[test]
    fn buffered_batches_take_drains_state() {
        let schema = create_test_schema();
        let mut bb = BufferedBatches::new(2, schema.clone());
        bb.push_batch(create_test_batch(&schema, vec![1, 2]), &[vec![0], vec![1]]);

        let (batches, indices) = bb.take();
        assert_eq!(batches.len(), 1);
        assert_eq!(indices.len(), 2);
        assert_eq!(indices[0], vec![(0, 0)]);
        assert_eq!(indices[1], vec![(0, 1)]);

        assert!(bb.is_empty());
        assert_eq!(bb.num_buffered_rows(), 0);
    }
}
