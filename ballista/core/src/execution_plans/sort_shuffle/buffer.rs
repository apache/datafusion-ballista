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
}
