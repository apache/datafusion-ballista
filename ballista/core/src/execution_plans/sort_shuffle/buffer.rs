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

//! In-memory partition buffer for sort-based shuffle.
//!
//! Each output partition has a buffer that accumulates record batches
//! until the buffer is full or needs to be spilled to disk.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::coalesce::LimitedBatchCoalescer;

/// Buffer for accumulating record batches for a single output partition.
///
/// When the buffer exceeds its maximum size, it signals that it should be
/// spilled to disk.
#[derive(Debug)]
pub struct PartitionBuffer {
    /// Partition ID this buffer is for
    partition_id: usize,
    /// Buffered record batches
    batches: Vec<RecordBatch>,
    /// Current memory usage in bytes
    memory_used: usize,
    /// Number of rows in the buffer
    num_rows: usize,
    /// Schema for this partition's data
    schema: SchemaRef,
}

impl PartitionBuffer {
    /// Creates a new partition buffer.
    pub fn new(partition_id: usize, schema: SchemaRef) -> Self {
        Self {
            partition_id,
            batches: Vec::new(),
            memory_used: 0,
            num_rows: 0,
            schema,
        }
    }

    /// Returns the partition ID for this buffer.
    pub fn partition_id(&self) -> usize {
        self.partition_id
    }

    /// Returns the schema for this buffer's data.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the current memory usage in bytes.
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    /// Returns the number of rows in the buffer.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns the number of batches in the buffer.
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Appends a record batch to the buffer.
    ///
    /// Returns the new total memory usage after appending.
    pub fn append(&mut self, batch: RecordBatch) -> usize {
        let batch_size = batch.get_array_memory_size();
        self.num_rows += batch.num_rows();
        self.memory_used += batch_size;
        self.batches.push(batch);
        self.memory_used
    }

    /// Drains all batches from the buffer, resetting it to empty.
    ///
    /// Returns the drained batches.
    pub fn drain(&mut self) -> Vec<RecordBatch> {
        self.memory_used = 0;
        self.num_rows = 0;
        std::mem::take(&mut self.batches)
    }

    /// Takes all batches from the buffer without resetting memory tracking.
    ///
    /// This is useful when the caller wants to handle the batches but the
    /// buffer will be discarded anyway.
    pub fn take_batches(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.batches)
    }

    /// Drains batches from the buffer, coalescing small batches into
    /// larger ones up to `target_batch_size` rows each.
    pub fn drain_coalesced(&mut self, target_batch_size: usize) -> Vec<RecordBatch> {
        self.memory_used = 0;
        self.num_rows = 0;
        let batches = std::mem::take(&mut self.batches);
        coalesce_batches(batches, &self.schema, target_batch_size)
    }

    /// Takes all batches, coalescing small batches into larger ones
    /// up to `target_batch_size` rows each.
    pub fn take_batches_coalesced(
        &mut self,
        target_batch_size: usize,
    ) -> Vec<RecordBatch> {
        let batches = std::mem::take(&mut self.batches);
        coalesce_batches(batches, &self.schema, target_batch_size)
    }
}

/// Holds whole input `RecordBatch`es and per-partition `(batch_idx, row_idx)`
/// index lists. Rows are not copied at insertion time — only the indices are
/// recorded. Materialization happens through `PartitionedBatchIterator` at
/// spill or final-write time, by way of `interleave_record_batch`.
#[derive(Debug)]
#[allow(dead_code)]
pub struct BufferedBatches {
    schema: SchemaRef,
    /// All input batches, in arrival order. Indexed by `batch_idx` in
    /// `indices`. Never sliced.
    batches: Vec<RecordBatch>,
    /// One entry per output partition. Each `(u32, u32)` is `(batch_idx,
    /// row_idx)`, referring to a row inside `batches[batch_idx]`.
    indices: Vec<Vec<(u32, u32)>>,
    /// Total rows currently referenced by `indices`. Drives metrics.
    num_buffered_rows: usize,
}

impl BufferedBatches {
    /// Creates a new buffer for the given partition count and schema.
    #[allow(dead_code)]
    pub fn new(num_partitions: usize, schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            indices: (0..num_partitions).map(|_| Vec::new()).collect(),
            num_buffered_rows: 0,
        }
    }

    /// Returns the schema this buffer is bound to.
    #[allow(dead_code)]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the configured number of output partitions.
    #[allow(dead_code)]
    pub fn num_partitions(&self) -> usize {
        self.indices.len()
    }

    /// Returns true if no batches have been pushed yet.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Returns the total number of rows currently referenced by indices.
    #[allow(dead_code)]
    pub fn num_buffered_rows(&self) -> usize {
        self.num_buffered_rows
    }

    /// Returns the buffered batches.
    #[allow(dead_code)]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Returns the row indices for output partition `partition_id`.
    #[allow(dead_code)]
    pub fn indices_for(&self, partition_id: usize) -> &[(u32, u32)] {
        &self.indices[partition_id]
    }

    /// Pushes a whole input `batch` and records, for each output partition
    /// `p`, the row indices from `per_partition_rows[p]` as `(batch_idx, r)`
    /// pairs in that partition's index list.
    ///
    /// `per_partition_rows.len()` must equal `num_partitions()`.
    #[allow(dead_code)]
    pub fn push_batch(&mut self, batch: RecordBatch, per_partition_rows: &[Vec<u32>]) {
        debug_assert_eq!(per_partition_rows.len(), self.indices.len());
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
    #[allow(dead_code)]
    pub fn take(&mut self) -> (Vec<RecordBatch>, Vec<Vec<(u32, u32)>>) {
        self.num_buffered_rows = 0;
        let batches = std::mem::take(&mut self.batches);
        // Preserve the partition count by replacing each inner vec with empty
        let indices: Vec<Vec<(u32, u32)>> =
            self.indices.iter_mut().map(std::mem::take).collect();
        (batches, indices)
    }
}

/// Coalesces small batches into larger ones up to `target_batch_size`
/// rows each using DataFusion's `LimitedBatchCoalescer`.
fn coalesce_batches(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
    target_batch_size: usize,
) -> Vec<RecordBatch> {
    if batches.len() <= 1 {
        return batches;
    }

    let mut coalescer =
        LimitedBatchCoalescer::new(schema.clone(), target_batch_size, None);
    let mut result = Vec::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // push_batch can only fail on schema mismatch, which won't
        // happen here since all batches share the same schema
        let _ = coalescer.push_batch(batch);
        while let Some(completed) = coalescer.next_completed_batch() {
            result.push(completed);
        }
    }

    // Flush remaining buffered rows
    let _ = coalescer.finish();
    while let Some(completed) = coalescer.next_completed_batch() {
        result.push(completed);
    }

    result
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
    fn test_new_buffer() {
        let schema = create_test_schema();
        let buffer = PartitionBuffer::new(0, schema);

        assert_eq!(buffer.partition_id(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.memory_used(), 0);
        assert_eq!(buffer.num_rows(), 0);
        assert_eq!(buffer.num_batches(), 0);
    }

    #[test]
    fn test_append() {
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(0, schema.clone());

        let batch = create_test_batch(&schema, vec![1, 2, 3]);
        buffer.append(batch);

        assert!(!buffer.is_empty());
        assert!(buffer.memory_used() > 0);
        assert_eq!(buffer.num_rows(), 3);
        assert_eq!(buffer.num_batches(), 1);
    }

    #[test]
    fn test_drain() {
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(0, schema.clone());

        buffer.append(create_test_batch(&schema, vec![1, 2, 3]));
        buffer.append(create_test_batch(&schema, vec![4, 5]));

        assert_eq!(buffer.num_batches(), 2);
        assert_eq!(buffer.num_rows(), 5);

        let batches = buffer.drain();

        assert_eq!(batches.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.memory_used(), 0);
        assert_eq!(buffer.num_rows(), 0);
    }

    #[test]
    fn buffered_batches_pushes_and_partitions_indices() {
        use super::BufferedBatches;

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
        use super::BufferedBatches;

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
