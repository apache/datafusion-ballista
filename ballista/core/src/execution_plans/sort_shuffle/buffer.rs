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

//! In-memory partition buffer and scratch space for sort-based shuffle.
//!
//! Provides:
//! - `ScratchSpace`: Reusable per-batch scratch buffer for computing partition
//!   assignments using a prefix-sum algorithm (modeled on Apache DataFusion Comet).
//! - `InputBatchStore`: Centralized storage for input record batches.
//! - `materialize_partition`: Materializes partition data from indices using
//!   `BatchCoalescer::push_batch_with_indices`.

use std::sync::Arc;

use datafusion::arrow::array::UInt64Builder;
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::common::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;

/// Reusable per-batch scratch buffer for computing partition assignments.
///
/// Uses a prefix-sum algorithm to efficiently map rows to partitions:
/// 1. Evaluate hash expressions and compute partition IDs
/// 2. Count rows per partition
/// 3. Compute cumulative offsets (prefix sum)
/// 4. Place row indices into contiguous slices per partition
///
/// After `compute_partition_assignments`, use `partition_indices(id)` to get
/// the row indices for a specific partition.
pub struct ScratchSpace {
    hash_buffer: Vec<u64>,
    partition_ids: Vec<usize>,
    partition_row_indices: Vec<u32>,
    /// Length = num_partitions + 1. After computation,
    /// partition k's rows are `partition_row_indices[starts[k]..starts[k+1]]`.
    partition_starts: Vec<usize>,
}

impl ScratchSpace {
    /// Creates a new scratch space for the given number of partitions.
    pub fn new(num_partitions: usize) -> Self {
        Self {
            hash_buffer: Vec::new(),
            partition_ids: Vec::new(),
            partition_row_indices: Vec::new(),
            partition_starts: vec![0; num_partitions + 1],
        }
    }

    /// Compute partition assignments for a batch using hash partitioning.
    ///
    /// Uses the same hashing logic as DataFusion's `BatchPartitioner` to ensure
    /// identical partition assignments.
    pub fn compute_partition_assignments(
        &mut self,
        exprs: &[Arc<dyn PhysicalExpr>],
        batch: &RecordBatch,
        num_partitions: usize,
    ) -> Result<()> {
        let num_rows = batch.num_rows();

        let arrays = evaluate_expressions_to_arrays(exprs, batch)?;

        self.hash_buffer.resize(num_rows, 0);
        create_hashes(
            &arrays,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut self.hash_buffer,
        )?;

        self.partition_ids.resize(num_rows, 0);
        for (i, hash) in self.hash_buffer.iter().enumerate() {
            self.partition_ids[i] = (*hash % num_partitions as u64) as usize;
        }

        self.map_partition_ids_to_starts_and_indices(num_partitions, num_rows);
        Ok(())
    }

    /// Prefix-sum algorithm to organize row indices by partition.
    fn map_partition_ids_to_starts_and_indices(
        &mut self,
        num_partitions: usize,
        num_rows: usize,
    ) {
        self.partition_starts.truncate(0);
        self.partition_starts.resize(num_partitions + 1, 0);
        self.partition_row_indices.resize(num_rows, 0);
        for &pid in &self.partition_ids {
            self.partition_starts[pid] += 1;
        }

        // Cumulative sum converts counts to end offsets
        let mut sum = 0;
        for start in self.partition_starts.iter_mut() {
            sum += *start;
            *start = sum;
        }

        // Reverse iteration converts end offsets to start offsets
        for row_idx in (0..num_rows).rev() {
            let pid = self.partition_ids[row_idx];
            self.partition_starts[pid] -= 1;
            self.partition_row_indices[self.partition_starts[pid]] = row_idx as u32;
        }
    }

    /// Returns the row indices assigned to `partition_id` after calling
    /// `compute_partition_assignments`.
    pub fn partition_indices(&self, partition_id: usize) -> &[u32] {
        let start = self.partition_starts[partition_id];
        let end = self.partition_starts[partition_id + 1];
        &self.partition_row_indices[start..end]
    }
}

/// Materializes a partition's data from `(batch_idx, row_idx)` pairs into
/// coalesced `RecordBatch`es using `BatchCoalescer::push_batch_with_indices`.
///
/// Uses `scratch_builder` as a reusable builder to avoid allocations.
pub fn materialize_partition(
    partition_indices: &[(u32, u32)],
    input_batches: &InputBatchStore,
    target_batch_size: usize,
    scratch_builder: &mut UInt64Builder,
) -> Result<Vec<RecordBatch>> {
    if partition_indices.is_empty() {
        return Ok(Vec::new());
    }

    let mut coalescer =
        BatchCoalescer::new(input_batches.schema().clone(), target_batch_size);
    let mut result = Vec::new();

    let mut start = 0;
    while start < partition_indices.len() {
        let current_batch_idx = partition_indices[start].0;
        let mut end = start + 1;
        while end < partition_indices.len()
            && partition_indices[end].0 == current_batch_idx
        {
            end += 1;
        }

        let batch = input_batches.get_batch(current_batch_idx);

        for (_, r) in &partition_indices[start..end] {
            scratch_builder.append_value(*r as u64);
        }
        let idx_array = scratch_builder.finish();

        coalescer.push_batch_with_indices(batch.clone(), &idx_array)?;
        while let Some(completed) = coalescer.next_completed_batch() {
            result.push(completed);
        }

        start = end;
    }

    coalescer.finish_buffered_batch()?;
    while let Some(completed) = coalescer.next_completed_batch() {
        result.push(completed);
    }
    Ok(result)
}

/// Stores all input batches across all partitions
pub struct InputBatchStore {
    /// Vector of all incoming record batches
    batches: Vec<RecordBatch>,
    /// Schema of all batches
    schema: SchemaRef,
    /// Total memory of all record batches
    total_memory: usize,
}

impl InputBatchStore {
    /// Creates new instance of InputBatchStore
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            batches: Vec::new(),
            schema,
            total_memory: 0,
        }
    }

    /// Appends RecordBatch to the store, returning the batch index.
    pub fn push_batch(&mut self, batch: RecordBatch) -> usize {
        let batch_idx = self.batches.len();
        self.total_memory += batch.get_array_memory_size();
        self.batches.push(batch);
        batch_idx
    }

    /// Return the batch at the given index
    ///
    /// # Panics
    /// Can panic if idx >= batches.len()
    pub fn get_batch(&self, idx: u32) -> &RecordBatch {
        &self.batches[idx as usize]
    }

    /// Returns total memory of all record batches
    pub fn total_memory(&self) -> usize {
        self.total_memory
    }

    /// Drains record batches and memory stats. The same SchemaRef remains.
    pub fn clear(&mut self) {
        self.batches.clear();
        self.total_memory = 0;
    }

    /// Returns the schema of the stored batches.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns a reference to all stored batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::expressions::Column;
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn create_test_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_scratch_space_partition_assignments() {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema, vec![1, 2, 3, 4, 5, 6]);
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("a", 0))];

        let mut scratch = ScratchSpace::new(3);
        scratch
            .compute_partition_assignments(&exprs, &batch, 3)
            .unwrap();

        // Every row should be assigned to exactly one partition
        let mut total_rows = 0;
        for pid in 0..3 {
            total_rows += scratch.partition_indices(pid).len();
        }
        assert_eq!(total_rows, 6);

        // Row indices should be within range
        for pid in 0..3 {
            for &row_idx in scratch.partition_indices(pid) {
                assert!(row_idx < 6);
            }
        }
    }

    #[test]
    fn test_scratch_space_reuse() {
        let schema = create_test_schema();
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("a", 0))];
        let mut scratch = ScratchSpace::new(2);

        // First batch
        let batch1 = create_test_batch(&schema, vec![1, 2, 3]);
        scratch
            .compute_partition_assignments(&exprs, &batch1, 2)
            .unwrap();
        let total1: usize = (0..2).map(|p| scratch.partition_indices(p).len()).sum();
        assert_eq!(total1, 3);

        // Second batch (reuses scratch space)
        let batch2 = create_test_batch(&schema, vec![10, 20]);
        scratch
            .compute_partition_assignments(&exprs, &batch2, 2)
            .unwrap();
        let total2: usize = (0..2).map(|p| scratch.partition_indices(p).len()).sum();
        assert_eq!(total2, 2);
    }

    #[test]
    fn test_materialize_partition_empty() {
        let schema = create_test_schema();
        let store = InputBatchStore::new(schema);
        let mut scratch = UInt64Builder::new();
        let result = materialize_partition(&[], &store, 8192, &mut scratch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_materialize_partition_single_batch() {
        let schema = create_test_schema();
        let mut store = InputBatchStore::new(schema.clone());
        store.push_batch(create_test_batch(&schema, vec![10, 20, 30, 40, 50]));

        // Select rows 0, 2, 4 from batch 0
        let indices = vec![(0u32, 0u32), (0, 2), (0, 4)];
        let mut scratch = UInt64Builder::new();
        let result = materialize_partition(&indices, &store, 8192, &mut scratch).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify values
        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 10);
        assert_eq!(col.value(1), 30);
        assert_eq!(col.value(2), 50);
    }

    #[test]
    fn test_materialize_partition_multiple_batches() {
        let schema = create_test_schema();
        let mut store = InputBatchStore::new(schema.clone());
        store.push_batch(create_test_batch(&schema, vec![10, 20, 30]));
        store.push_batch(create_test_batch(&schema, vec![40, 50, 60]));

        // Select row 1 from batch 0, rows 0 and 2 from batch 1
        let indices = vec![(0u32, 1u32), (1, 0), (1, 2)];
        let mut scratch = UInt64Builder::new();
        let result = materialize_partition(&indices, &store, 8192, &mut scratch).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_materialize_partition_respects_batch_size() {
        let schema = create_test_schema();
        let mut store = InputBatchStore::new(schema.clone());

        // Create a large input batch with 20,000 rows
        let large_batch = create_test_batch(&schema, (0..20000).collect());
        store.push_batch(large_batch);

        // Select all rows from this partition
        let indices: Vec<(u32, u32)> = (0..20000).map(|i| (0u32, i as u32)).collect();

        // Use target_batch_size of 8192
        let mut scratch = UInt64Builder::new();
        let result = materialize_partition(&indices, &store, 8192, &mut scratch).unwrap();

        // Should produce multiple batches
        assert!(result.len() >= 2, "Expected multiple output batches");

        // Verify total rows
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 20000);

        // Most batches should be close to target_batch_size (8192)
        // Last batch may be smaller
        for (i, batch) in result.iter().enumerate() {
            if i < result.len() - 1 {
                // All but last batch should be close to target size
                assert!(
                    batch.num_rows() >= 7000 && batch.num_rows() <= 9000,
                    "Batch {} has {} rows, expected ~8192",
                    i,
                    batch.num_rows()
                );
            }
        }
    }

    #[test]
    fn test_input_batch_store_memory_tracking() {
        let schema = create_test_schema();
        let mut store = InputBatchStore::new(schema.clone());

        assert_eq!(store.total_memory(), 0);

        let batch1 = create_test_batch(&schema, vec![1, 2, 3]);
        let memory1 = batch1.get_array_memory_size();
        store.push_batch(batch1);
        assert_eq!(store.total_memory(), memory1);

        let batch2 = create_test_batch(&schema, vec![4, 5, 6, 7]);
        let memory2 = batch2.get_array_memory_size();
        store.push_batch(batch2);
        assert_eq!(store.total_memory(), memory1 + memory2);

        store.clear();
        assert_eq!(store.total_memory(), 0);
        assert_eq!(store.batches().len(), 0);
    }

    #[test]
    fn test_materialize_partition_coalesces_small_batches() {
        let schema = create_test_schema();
        let mut store = InputBatchStore::new(schema.clone());

        // Create many small input batches (100 batches with 100 rows each)
        for i in 0..100 {
            let start = i * 100;
            let batch = create_test_batch(&schema, (start..start + 100).collect());
            store.push_batch(batch);
        }

        // Select all rows from all batches
        let mut indices = Vec::new();
        for batch_idx in 0..100u32 {
            for row_idx in 0..100u32 {
                indices.push((batch_idx, row_idx));
            }
        }

        // Use target_batch_size of 8192
        let mut scratch = UInt64Builder::new();
        let result = materialize_partition(&indices, &store, 8192, &mut scratch).unwrap();

        // Should coalesce into far fewer output batches than input batches
        assert!(
            result.len() < 10,
            "Expected coalescing: got {} output batches from 100 input batches",
            result.len()
        );

        // Verify total rows
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10000);

        // Most batches should be well-sized
        for (i, batch) in result.iter().enumerate() {
            if i < result.len() - 1 {
                assert!(
                    batch.num_rows() >= 7000,
                    "Batch {} should be well-sized, got {} rows",
                    i,
                    batch.num_rows()
                );
            }
        }
    }
}
