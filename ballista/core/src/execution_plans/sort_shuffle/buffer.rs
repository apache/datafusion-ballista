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
//! Holds whole input record batches plus each output partition's
//! `(batch_idx, row_idx)` pairs. Rows are not copied when they arrive; output
//! batches are gathered with `interleave_record_batch` when the buffer is
//! drained, at spill or final-write time.

use super::partitioned_batch_iterator::PartitionedBatchIterator;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

/// Rows buffered for every output partition of one input partition.
#[derive(Debug)]
pub struct BufferedBatches {
    schema: SchemaRef,
    batch_size: usize,
    /// Input batches in arrival order, indexed by `batch_idx`.
    batches: Vec<RecordBatch>,
    /// Total `get_array_memory_size` of `batches`.
    batches_bytes: usize,
    /// One list of `(batch_idx, row_idx)` pairs per output partition.
    indices: Vec<Vec<(u32, u32)>>,
    /// Total allocated capacity of `indices`, in bytes.
    indices_bytes: usize,
}

impl BufferedBatches {
    /// Creates a buffer for `num_partitions` output partitions that drains
    /// into batches of at most `batch_size` rows.
    pub fn new(num_partitions: usize, schema: SchemaRef, batch_size: usize) -> Self {
        Self {
            schema,
            batch_size,
            batches: Vec::new(),
            batches_bytes: 0,
            indices: vec![Vec::new(); num_partitions],
            indices_bytes: 0,
        }
    }

    /// Returns the configured number of output partitions.
    pub fn num_partitions(&self) -> usize {
        self.indices.len()
    }

    /// Returns true if no rows are buffered.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Returns the bytes held: the buffered batches plus the allocated
    /// capacity of the index lists.
    pub fn memory_size(&self) -> usize {
        self.batches_bytes + self.indices_bytes
    }

    /// Buffers `batch`, recording the rows listed in `per_partition_rows[p]`
    /// for output partition `p`.
    ///
    /// `per_partition_rows.len()` must equal `num_partitions()`.
    pub fn push_batch(&mut self, batch: RecordBatch, per_partition_rows: &[Vec<u32>]) {
        debug_assert_eq!(per_partition_rows.len(), self.indices.len());
        debug_assert!(
            *batch.schema() == *self.schema,
            "BufferedBatches::push_batch schema mismatch"
        );
        let batch_idx = self.batches.len() as u32;
        for (partition_indices, rows) in self.indices.iter_mut().zip(per_partition_rows) {
            let capacity_before = partition_indices.capacity();
            partition_indices.extend(rows.iter().map(|&r| (batch_idx, r)));
            self.indices_bytes += (partition_indices.capacity() - capacity_before)
                * size_of::<(u32, u32)>();
        }
        self.batches_bytes += batch.get_array_memory_size();
        self.batches.push(batch);
    }

    /// Empties the buffer, passing each output batch of at most `batch_size`
    /// rows to `f` with its partition, in partition order and then arrival
    /// order. Output batches are gathered one at a time, as `f` consumes them.
    pub fn drain(
        &mut self,
        mut f: impl FnMut(usize, RecordBatch) -> Result<()>,
    ) -> Result<()> {
        self.batches_bytes = 0;
        self.indices_bytes = 0;
        let batches = std::mem::take(&mut self.batches);
        let indices: Vec<_> = self.indices.iter_mut().map(std::mem::take).collect();
        for (partition, partition_indices) in indices.iter().enumerate() {
            let iter = PartitionedBatchIterator::new(
                &batches,
                partition_indices,
                self.batch_size,
            );
            for batch in iter {
                f(partition, batch?)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn drain(bb: &mut BufferedBatches) -> Vec<(usize, RecordBatch)> {
        let mut out = vec![];
        bb.drain(|p, b| {
            out.push((p, b));
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn drains_each_partition_in_arrival_order_and_batch_size() {
        let mut bb = BufferedBatches::new(3, schema(), 2);
        assert!(bb.is_empty());
        assert_eq!(bb.num_partitions(), 3);

        // Partition 0 gets rows {0, 2} of the first batch and all of the
        // second; partition 1 gets {3, 1} of the first.
        bb.push_batch(
            batch(vec![10, 20, 30, 40]),
            &[vec![0, 2], vec![3, 1], vec![]],
        );
        bb.push_batch(batch(vec![50, 60]), &[vec![0, 1], vec![], vec![]]);
        assert!(!bb.is_empty());

        assert_eq!(
            drain(&mut bb),
            vec![
                (0, batch(vec![10, 30])),
                (0, batch(vec![50, 60])),
                (1, batch(vec![40, 20])),
            ]
        );
        assert!(bb.is_empty());
        assert_eq!(drain(&mut bb), vec![]);
    }

    #[test]
    fn memory_size_counts_batches_and_index_capacity() {
        let mut bb = BufferedBatches::new(2, schema(), 8);
        assert_eq!(bb.memory_size(), 0);

        let input = batch(vec![1, 2, 3]);
        let batch_bytes = input.get_array_memory_size();
        bb.push_batch(input, &[vec![0, 2], vec![1]]);
        let index_bytes: usize = bb
            .indices
            .iter()
            .map(|v| v.capacity() * size_of::<(u32, u32)>())
            .sum();
        assert_eq!(bb.memory_size(), batch_bytes + index_bytes);

        drain(&mut bb);
        assert_eq!(bb.memory_size(), 0);
    }
}
