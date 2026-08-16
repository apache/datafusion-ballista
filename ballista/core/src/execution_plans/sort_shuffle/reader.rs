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
//! file to locate each partition's byte range. Within a partition's range
//! the bytes are zero or more concatenated Arrow IPC streams; the leading
//! bytes of the data file hold a schema-header stream so the schema is
//! always recoverable.

use crate::error::{BallistaError, Result};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::physical_plan::SendableRecordBatchStream;
use std::fs::File;
use std::path::Path;

use super::index::ShuffleIndex;
use super::memory_store::InMemoryShuffle;
use super::multi_stream_reader::MultiStreamPartitionStream;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::memory::MemoryStream;
use std::io::Cursor;
use std::sync::Arc;

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

/// Returns a stream of record batches for `partition_id` from a sort-shuffle
/// data file. Reads the schema from the leading schema-header stream and
/// then yields batches from the partition's byte range, transparently
/// crossing concatenated IPC stream boundaries within that range.
pub fn stream_sort_shuffle_partition(
    data_path: &Path,
    index_path: &Path,
    partition_id: usize,
) -> Result<SendableRecordBatchStream> {
    let index = ShuffleIndex::read_from_file(index_path)?;

    if partition_id >= index.partition_count() {
        return Err(BallistaError::General(format!(
            "Partition {partition_id} not found in index (max: {})",
            index.partition_count()
        )));
    }

    // Read the schema from the leading header stream.
    let header_file = File::open(data_path)?;
    let schema = StreamReader::try_new(header_file, None)?.schema();

    let (start, end) = index.get_partition_range(partition_id);
    if start < 0 || end < start {
        return Err(BallistaError::General(format!(
            "Invalid partition byte range for partition {partition_id}: ({start}, {end})"
        )));
    }

    let stream = MultiStreamPartitionStream::new(
        data_path.to_path_buf(),
        schema,
        start as u64,
        end as u64,
    );

    Ok(Box::pin(stream))
}

/// Returns a stream of record batches for `partition_id` from shuffle output
/// held in [`super::memory_store`] rather than written to disk.
///
/// The stored bytes are the same concatenated Arrow IPC streams the on-disk
/// reader would have addressed through the index, so this decodes exactly
/// what `stream_sort_shuffle_partition` would have produced for the same
/// partition — it just reads them from memory.
///
/// Batches are decoded eagerly. A partition's bytes are already resident, so
/// there is nothing to stream in from elsewhere, and decoding up front keeps
/// the reader free of the sub-stream cursor bookkeeping the file path needs.
pub fn stream_in_memory_partition(
    shuffle: &InMemoryShuffle,
    partition_id: usize,
) -> Result<SendableRecordBatchStream> {
    let bytes = shuffle.partitions.get(partition_id).ok_or_else(|| {
        BallistaError::General(format!(
            "Partition {partition_id} not found in in-memory shuffle (has {})",
            shuffle.partitions.len()
        ))
    })?;

    let schema = shuffle.schema.clone();
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut pos: u64 = 0;
    let len = bytes.len() as u64;

    // Walk the concatenated IPC streams: each `StreamReader` stops at its own
    // end-of-stream marker, so pick up where it left off until the bytes run out.
    while pos < len {
        let mut cursor = Cursor::new(bytes.as_slice());
        cursor.set_position(pos);
        let reader = StreamReader::try_new(cursor, None)?;
        let mut consumed_any = false;
        let mut reader = reader;
        for batch in reader.by_ref() {
            batches.push(batch?);
            consumed_any = true;
        }
        let next = reader.get_mut().position();
        if next <= pos && !consumed_any {
            // A stream that yielded nothing and did not advance would loop
            // forever; treat it as the end of meaningful data.
            break;
        }
        pos = next;
    }

    Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?))
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
