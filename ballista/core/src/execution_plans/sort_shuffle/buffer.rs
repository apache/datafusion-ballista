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

/// Return type of [`BufferedBatches::take`]: `(batches, per-partition indices)`.
pub type BufferedTake = (Vec<RecordBatch>, Vec<Vec<(u32, u32)>>);

/// Row-to-partition assignment for one input batch, in compressed-sparse-row
/// form: partition `p` owns `rows[offsets[p]..offsets[p + 1]]`.
///
/// A single flat `rows` buffer replaces the `Vec<Vec<u32>>` a per-partition
/// layout would need. That matters because the writer computes an assignment
/// for every input batch: the nested form allocates once per output partition
/// per batch, so its cost grows with the partition count even when most
/// partitions receive a handful of rows. The CSR form allocates twice per
/// batch and is reused across batches, so the cost tracks the row count
/// instead.
#[derive(Debug, Default)]
pub struct PartitionAssignment {
    rows: Vec<u32>,
    offsets: Vec<u32>,
    /// Scratch for the counting sort's per-partition write cursors, retained
    /// across batches so the sort itself allocates nothing.
    cursor: Vec<u32>,
}

impl PartitionAssignment {
    /// Number of output partitions this assignment covers.
    pub fn num_partitions(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Row indices assigned to `partition`, in ascending row order.
    pub fn rows_for(&self, partition: usize) -> &[u32] {
        let start = self.offsets[partition] as usize;
        let end = self.offsets[partition + 1] as usize;
        &self.rows[start..end]
    }

    /// Rebuilds the assignment from `partition_of_row`, a per-row partition id
    /// of length `num_rows`, via a counting sort over `num_partitions`.
    pub fn rebuild(&mut self, partition_of_row: &[u32], num_partitions: usize) {
        let num_rows = partition_of_row.len();

        self.offsets.clear();
        self.offsets.resize(num_partitions + 1, 0);
        for &p in partition_of_row {
            self.offsets[p as usize + 1] += 1;
        }
        for p in 0..num_partitions {
            self.offsets[p + 1] += self.offsets[p];
        }

        self.cursor.clear();
        self.cursor
            .extend_from_slice(&self.offsets[..num_partitions]);

        self.rows.clear();
        self.rows.resize(num_rows, 0);
        for (row, &p) in partition_of_row.iter().enumerate() {
            let slot = &mut self.cursor[p as usize];
            self.rows[*slot as usize] = row as u32;
            *slot += 1;
        }
    }
}

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
    ///
    /// This walks every partition, so the writer's hot loop uses the growth
    /// figure [`Self::push_batch`] returns instead of calling this per batch.
    #[allow(dead_code)]
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
    /// `p`, the rows `assignment` gave it as `(batch_idx, r)` pairs in that
    /// partition's index list.
    ///
    /// Returns how many bytes of index heap the push added, so the caller can
    /// mirror the growth into its memory reservation without re-walking every
    /// partition. `assignment.num_partitions()` must equal `num_partitions()`.
    pub fn push_batch(
        &mut self,
        batch: RecordBatch,
        assignment: &PartitionAssignment,
    ) -> usize {
        debug_assert_eq!(assignment.num_partitions(), self.indices.len());
        debug_assert!(
            *batch.schema() == *self.schema,
            "BufferedBatches::push_batch schema mismatch"
        );
        let batch_idx = self.batches.len() as u32;
        let mut growth = 0usize;
        // Only partitions that actually received rows can grow, so this skips
        // the empty ones rather than touching all `num_partitions()` of them.
        for p in 0..assignment.num_partitions() {
            let rows = assignment.rows_for(p);
            if rows.is_empty() {
                continue;
            }
            let dst = &mut self.indices[p];
            let before = dst.capacity();
            dst.reserve(rows.len());
            for &r in rows {
                dst.push((batch_idx, r));
            }
            self.num_buffered_rows += rows.len();
            growth += (dst.capacity() - before) * std::mem::size_of::<(u32, u32)>();
        }
        self.batches.push(batch);
        growth
    }

    /// Drains all state, returning the buffered batches and per-partition
    /// index lists. After this call the buffer is empty.
    pub fn take(&mut self) -> BufferedTake {
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

    /// Builds an assignment from a per-row partition id list.
    fn assignment(
        partition_of_row: &[u32],
        num_partitions: usize,
    ) -> PartitionAssignment {
        let mut a = PartitionAssignment::default();
        a.rebuild(partition_of_row, num_partitions);
        a
    }

    #[test]
    fn partition_assignment_groups_rows_by_partition() {
        // rows 0,2 -> p0; rows 1,3 -> p1; nothing -> p2
        let a = assignment(&[0, 1, 0, 1], 3);
        assert_eq!(a.num_partitions(), 3);
        assert_eq!(a.rows_for(0), &[0, 2]);
        assert_eq!(a.rows_for(1), &[1, 3]);
        assert!(a.rows_for(2).is_empty());
    }

    #[test]
    fn partition_assignment_rebuild_clears_previous_state() {
        let mut a = assignment(&[0, 0, 0, 0], 2);
        assert_eq!(a.rows_for(0), &[0, 1, 2, 3]);
        a.rebuild(&[1, 1], 2);
        assert!(a.rows_for(0).is_empty());
        assert_eq!(a.rows_for(1), &[0, 1]);
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
        let per_partition_a = assignment(&[0, 1, 0, 1], 3);
        let per_partition_b = assignment(&[2, 2], 3);

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
        bb.push_batch(
            create_test_batch(&schema, vec![1, 2]),
            &assignment(&[0, 1], 2),
        );

        let (batches, indices) = bb.take();
        assert_eq!(batches.len(), 1);
        assert_eq!(indices.len(), 2);
        assert_eq!(indices[0], vec![(0, 0)]);
        assert_eq!(indices[1], vec![(0, 1)]);

        assert!(bb.is_empty());
        assert_eq!(bb.num_buffered_rows(), 0);
    }
}
