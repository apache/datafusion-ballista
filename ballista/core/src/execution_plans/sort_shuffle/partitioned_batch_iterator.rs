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

//! Iterator that materializes per-partition rows into well-sized
//! `RecordBatch`es using `arrow::compute::interleave_record_batch`.

use datafusion::arrow::array::{Array, ArrayRef, BinaryViewArray, StringViewArray};
use datafusion::arrow::compute::interleave_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use std::sync::Arc;

/// Compacts `Utf8View` / `BinaryView` columns by running `gc()` on them.
///
/// `interleave_record_batch` preserves the original data buffers for view
/// arrays, so the output references every source data buffer the inputs
/// touched even if it only emits a few rows. Without compaction, downstream
/// IPC writes serialize all those source buffers (potentially hundreds of MB)
/// rather than just the bytes the views point at.
fn compact_view_columns(batch: RecordBatch) -> Result<RecordBatch> {
    let mut changed = false;
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| -> ArrayRef {
            if let Some(a) = c.as_any().downcast_ref::<StringViewArray>() {
                changed = true;
                Arc::new(a.gc())
            } else if let Some(a) = c.as_any().downcast_ref::<BinaryViewArray>() {
                changed = true;
                Arc::new(a.gc())
            } else {
                c.clone()
            }
        })
        .collect();
    if !changed {
        return Ok(batch);
    }
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| {
        DataFusionError::ArrowError(Box::new(e), Some(DataFusionError::get_back_trace()))
    })
}

/// Iterator over per-partition output `RecordBatch`es.
///
/// Walks a slice of `(batch_idx, row_idx)` pairs in `indices`, and for each
/// chunk of up to `batch_size` pairs gathers the referenced rows from
/// `batches` via `interleave_record_batch`, yielding one `RecordBatch` per
/// chunk.
pub(crate) struct PartitionedBatchIterator<'a> {
    batch_refs: Vec<&'a RecordBatch>,
    indices: &'a [(u32, u32)],
    batch_size: usize,
    pos: usize,
    /// `interleave_record_batch` takes `&[(usize, usize)]`, so we materialize
    /// a small reusable buffer per `next()` to avoid allocating on every call.
    scratch: Vec<(usize, usize)>,
}

impl<'a> PartitionedBatchIterator<'a> {
    /// Create a new iterator.
    ///
    /// * `batches` — the source record batches to pull rows from.
    /// * `indices` — `(batch_idx, row_idx)` pairs that select which rows to
    ///   emit, in order.
    /// * `batch_size` — maximum number of rows per output `RecordBatch`.
    pub(crate) fn new(
        batches: &'a [RecordBatch],
        indices: &'a [(u32, u32)],
        batch_size: usize,
    ) -> Self {
        let batch_refs = batches.iter().collect();
        Self {
            batch_refs,
            indices,
            batch_size,
            pos: 0,
            scratch: Vec::with_capacity(batch_size.min(indices.len())),
        }
    }
}

impl<'a> Iterator for PartitionedBatchIterator<'a> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.indices.len() {
            return None;
        }
        let end = (self.pos + self.batch_size).min(self.indices.len());
        self.scratch.clear();
        self.scratch.extend(
            self.indices[self.pos..end]
                .iter()
                .map(|&(b, r)| (b as usize, r as usize)),
        );
        self.pos = end;

        match interleave_record_batch(&self.batch_refs, &self.scratch) {
            Ok(batch) => Some(compact_view_columns(batch)),
            Err(e) => Some(Err(DataFusionError::ArrowError(
                Box::new(e),
                Some(DataFusionError::get_back_trace()),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values.to_vec()))])
            .unwrap()
    }

    #[test]
    fn yields_chunks_of_at_most_batch_size_rows() {
        use super::PartitionedBatchIterator;

        let batches = vec![batch(&[1, 2, 3, 4]), batch(&[5, 6, 7])];
        let indices: Vec<(u32, u32)> =
            vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1), (1, 2)];

        let mut iter = PartitionedBatchIterator::new(&batches, &indices, 4);

        let first = iter.next().unwrap().unwrap();
        assert_eq!(first.num_rows(), 4);
        let arr = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.values(), &[1, 2, 3, 4]);

        let second = iter.next().unwrap().unwrap();
        assert_eq!(second.num_rows(), 3);
        let arr = second
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.values(), &[5, 6, 7]);

        assert!(iter.next().is_none());
    }

    #[test]
    fn empty_indices_yields_nothing() {
        use super::PartitionedBatchIterator;

        let batches = vec![batch(&[1, 2, 3])];
        let indices: Vec<(u32, u32)> = vec![];
        let mut iter = PartitionedBatchIterator::new(&batches, &indices, 4);
        assert!(iter.next().is_none());
    }

    #[test]
    fn interleaves_rows_from_multiple_input_batches() {
        use super::PartitionedBatchIterator;

        let batches = vec![batch(&[10, 20, 30]), batch(&[40, 50, 60])];
        // Pull alternating rows from the two input batches
        let indices: Vec<(u32, u32)> =
            vec![(0, 0), (1, 0), (0, 1), (1, 1), (0, 2), (1, 2)];

        let mut iter = PartitionedBatchIterator::new(&batches, &indices, 8192);
        let only = iter.next().unwrap().unwrap();
        assert_eq!(only.num_rows(), 6);
        let arr = only
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.values(), &[10, 40, 20, 50, 30, 60]);
        assert!(iter.next().is_none());
    }
}
