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

//! Reader for sort-based shuffle output files.
//!
//! Reads partition data from the consolidated data file using the index
//! file to locate partition boundaries. Uses Arrow IPC FileReader for
//! efficient random access to specific batches.

use crate::error::{BallistaError, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use std::fs::File;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::index::ShuffleIndex;

/// Checks if a shuffle output uses the sort-based format by looking for
/// the index file.
pub fn is_sort_shuffle_output(data_path: &Path) -> bool {
    let index_path = data_path.with_extension("arrow.index");
    index_path.exists()
}

/// Gets the index file path for a data file.
pub fn get_index_path(data_path: &Path) -> std::path::PathBuf {
    data_path.with_extension("arrow.index")
}

/// A stream that reads batches from a sort shuffle partition lazily.
///
/// Wraps an Arrow FileReader and yields batches one at a time without
/// loading them all into memory upfront.
struct SortShufflePartitionStream {
    reader: FileReader<File>,
    schema: SchemaRef,
    remaining: usize,
}

impl SortShufflePartitionStream {
    fn new(reader: FileReader<File>, schema: SchemaRef, num_batches: usize) -> Self {
        Self {
            reader,
            schema,
            remaining: num_batches,
        }
    }
}

impl futures::Stream for SortShufflePartitionStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }

        match self.reader.next() {
            Some(Ok(batch)) => {
                self.remaining -= 1;
                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(e)) => {
                self.remaining = 0;
                Poll::Ready(Some(Err(DataFusionError::ArrowError(Box::new(e), None))))
            }
            None => {
                self.remaining = 0;
                Poll::Ready(None)
            }
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for SortShufflePartitionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Returns a stream of record batches for a specific partition from a sort shuffle data file.
///
/// Unlike `read_sort_shuffle_partition`, this returns a lazy stream that reads batches
/// on-demand rather than loading all batches into memory upfront.
///
/// # Arguments
/// * `data_path` - Path to the consolidated data file (Arrow IPC File format)
/// * `index_path` - Path to the index file
/// * `partition_id` - The partition to read
///
/// # Returns
/// A stream of record batches for the requested partition.
pub fn stream_sort_shuffle_partition(
    data_path: &Path,
    index_path: &Path,
    partition_id: usize,
) -> Result<SendableRecordBatchStream> {
    // Load the index
    let index = ShuffleIndex::read_from_file(index_path)?;

    if partition_id >= index.partition_count() {
        return Err(BallistaError::General(format!(
            "Partition {partition_id} not found in index (max: {})",
            index.partition_count()
        )));
    }

    // Open the data file to get the schema
    let file = File::open(data_path).map_err(BallistaError::IoError)?;
    let reader = FileReader::try_new(file, None)?;
    let schema = reader.schema();

    // Check if partition has data
    if !index.partition_has_data(partition_id) {
        // Return empty stream with the schema
        let empty_stream = futures::stream::empty();
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            empty_stream,
        )));
    }

    // Get the batch range for this partition
    let (start_batch, end_batch) = index.get_partition_range(partition_id);
    let start_batch = start_batch as usize;
    let end_batch = end_batch as usize;
    let num_batches = end_batch - start_batch;

    // Re-open and position the reader at the start batch
    let file = File::open(data_path).map_err(BallistaError::IoError)?;
    let mut reader = FileReader::try_new(file, None)?;
    reader.set_index(start_batch)?;

    Ok(Box::pin(SortShufflePartitionStream::new(
        reader,
        schema,
        num_batches,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_is_sort_shuffle_output() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().join("data.arrow");
        let index_path = temp_dir.path().join("data.arrow.index");

        // No index file
        std::fs::write(&data_path, b"test").unwrap();
        assert!(!is_sort_shuffle_output(&data_path));

        // With index file
        std::fs::write(&index_path, b"test").unwrap();
        assert!(is_sort_shuffle_output(&data_path));
    }
}
