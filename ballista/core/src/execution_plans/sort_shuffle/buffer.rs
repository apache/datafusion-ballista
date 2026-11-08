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
use datafusion::common::utils::memory::RecordBatchMemoryCounter;
use datafusion::error::Result;

/// Rows buffered for every output partition of one input partition.
#[derive(Debug)]
pub struct BufferedBatches {
    schema: SchemaRef,
    batch_size: usize,
    /// Input batches in arrival order, indexed by `batch_idx`.
    batches: Vec<RecordBatch>,
    /// Counts each buffer of `batches` once, so zero-copy slices of one array
    /// (as aggregates emit) are not charged once per slice. Never refunds,
    /// which holds because `batches` only grows until `drain`.
    batches_counter: RecordBatchMemoryCounter,
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
            batches_counter: RecordBatchMemoryCounter::new(),
            indices: vec![Vec::new(); num_partitions],
            indices_bytes: 0,
        }
    }

    /// Returns the configured number of output partitions.
    pub fn num_partitions(&self) -> usize {
        self.indices.len()
    }

    /// Returns true if no batches are buffered.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Returns the bytes held: the buffered batches plus the allocated
    /// capacity of the index lists.
    pub fn memory_size(&self) -> usize {
        self.batches_counter.memory_usage() + self.indices_bytes
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
        self.batches_counter.count_batch(&batch);
        self.batches.push(batch);
    }

    /// Empties the buffer, calling `f` once per non-empty partition, in
    /// partition order, with an iterator over its output batches of at most
    /// `batch_size` rows, in arrival order. Batches are gathered one at a time,
    /// as `f` consumes them. The buffer is emptied first, so if `f` or a gather
    /// fails, the remaining rows are dropped.
    pub fn drain(
        &mut self,
        mut f: impl FnMut(usize, &mut dyn Iterator<Item = Result<RecordBatch>>) -> Result<()>,
    ) -> Result<()> {
        self.batches_counter = RecordBatchMemoryCounter::new();
        self.indices_bytes = 0;
        let batches = std::mem::take(&mut self.batches);
        let indices: Vec<_> = self.indices.iter_mut().map(std::mem::take).collect();
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        for (partition, partition_indices) in indices.iter().enumerate() {
            if partition_indices.is_empty() {
                continue;
            }
            let mut iter = PartitionedBatchIterator::new(
                &batch_refs,
                partition_indices,
                self.batch_size,
            );
            f(partition, &mut iter)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::test_util::{int32_batch, int32_schema};
    use datafusion::common::utils::memory::get_record_batch_memory_size;

    /// Each partition `drain` visits, with its batches.
    fn drain(bb: &mut BufferedBatches) -> Vec<(usize, Vec<RecordBatch>)> {
        let mut out = vec![];
        bb.drain(|p, batches| {
            out.push((p, batches.collect::<Result<_>>()?));
            Ok(())
        })
        .unwrap();
        out
    }

    fn index_bytes(bb: &BufferedBatches) -> usize {
        bb.indices
            .iter()
            .map(|v| v.capacity() * size_of::<(u32, u32)>())
            .sum()
    }

    #[test]
    fn drains_each_partition_in_arrival_order_and_batch_size() {
        // With 3 rows a batch, partition 0's first output batch takes rows
        // from both inputs, so chunking by `batch_size` is told apart from
        // one output batch per input batch.
        let mut bb = BufferedBatches::new(3, int32_schema(), 3);
        assert!(bb.is_empty());
        assert_eq!(bb.num_partitions(), 3);

        // Partition 0 gets rows {0, 2} of the first batch and all of the
        // second; partition 1 gets {3, 1} of the first; partition 2, none.
        bb.push_batch(
            int32_batch(&[10, 20, 30, 40]),
            &[vec![0, 2], vec![3, 1], vec![]],
        );
        bb.push_batch(int32_batch(&[50, 60]), &[vec![0, 1], vec![], vec![]]);
        assert!(!bb.is_empty());

        assert_eq!(
            drain(&mut bb),
            vec![
                (0, vec![int32_batch(&[10, 30, 50]), int32_batch(&[60])]),
                (1, vec![int32_batch(&[40, 20])]),
            ]
        );
        assert!(bb.is_empty());
        assert_eq!(drain(&mut bb), vec![]);
    }

    #[test]
    fn memory_size_counts_buffers_shared_by_slices_once() {
        let whole = int32_batch(&(0..8192).collect::<Vec<_>>());
        let whole_bytes = get_record_batch_memory_size(&whole);
        let mut bb = BufferedBatches::new(1, int32_schema(), 8);
        for i in 0..8 {
            bb.push_batch(whole.slice(i * 1024, 1024), &[vec![0]]);
        }
        assert_eq!(
            bb.memory_size(),
            whole_bytes + index_bytes(&bb),
            "eight slices of one array hold that array's buffers once"
        );
        drain(&mut bb);
        assert_eq!(bb.memory_size(), 0);
        bb.push_batch(whole.slice(0, 1024), &[vec![0]]);
        assert_eq!(
            bb.memory_size(),
            whole_bytes + index_bytes(&bb),
            "a drained buffer counts the shared buffers again"
        );
    }

    #[test]
    fn memory_size_counts_batches_and_index_capacity() {
        let mut bb = BufferedBatches::new(2, int32_schema(), 8);
        assert_eq!(bb.memory_size(), 0);

        let first = int32_batch(&[1, 2, 3]);
        let mut batch_bytes = get_record_batch_memory_size(&first);
        bb.push_batch(first, &[vec![0, 2], vec![1]]);
        assert_eq!(bb.memory_size(), batch_bytes + index_bytes(&bb));

        // More rows for partition 0 than its list has room for, so the
        // running total must follow a reallocation.
        let capacity_before = bb.indices[0].capacity();
        let second = int32_batch(&[0, 1, 2, 3, 4, 5, 6, 7]);
        batch_bytes += get_record_batch_memory_size(&second);
        bb.push_batch(second, &[(0..8).collect(), vec![]]);
        assert!(bb.indices[0].capacity() > capacity_before);
        assert_eq!(bb.memory_size(), batch_bytes + index_bytes(&bb));

        drain(&mut bb);
        assert_eq!(bb.memory_size(), 0);
    }
}
