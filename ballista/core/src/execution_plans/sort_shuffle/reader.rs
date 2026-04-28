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
use super::multi_stream_reader::MultiStreamPartitionStream;

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
    let header_file = File::open(data_path).map_err(BallistaError::IoError)?;
    let header_reader = StreamReader::try_new(header_file, None)
        .map_err(|e| BallistaError::General(format!("read schema header: {e}")))?;
    let schema = header_reader.schema();
    drop(header_reader);

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
