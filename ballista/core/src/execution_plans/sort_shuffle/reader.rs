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
//! file to locate partition boundaries.

use crate::error::{BallistaError, Result};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use std::fs::File;
use std::path::Path;

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

/// Reads all batches for a specific partition from a sort shuffle data file.
///
/// This reads the entire data file and filters for batches belonging to the
/// requested partition. For a more efficient implementation with proper byte
/// offsets, we would seek directly to the partition's data.
///
/// # Arguments
/// * `data_path` - Path to the consolidated data file
/// * `index_path` - Path to the index file
/// * `partition_id` - The partition to read
///
/// # Returns
/// Vector of record batches for the requested partition.
pub fn read_sort_shuffle_partition(
    data_path: &Path,
    index_path: &Path,
    partition_id: usize,
) -> Result<Vec<RecordBatch>> {
    // Load the index
    let index = ShuffleIndex::read_from_file(index_path)?;

    if partition_id >= index.partition_count() {
        return Err(BallistaError::General(format!(
            "Partition {partition_id} not found in index (max: {})",
            index.partition_count()
        )));
    }

    // Check if partition has data
    if !index.partition_has_data(partition_id) {
        return Ok(Vec::new());
    }

    // Get the batch range for this partition from the index
    // The index stores cumulative batch counts:
    // - offset[i] = starting batch index for partition i
    // - offset[i+1] (or total_length for last partition) = ending batch index (exclusive)
    let (start_batch, end_batch) = index.get_partition_range(partition_id);
    let start_batch = start_batch as usize;
    let end_batch = end_batch as usize;

    // Read the data file
    let file = File::open(data_path).map_err(BallistaError::IoError)?;
    let reader = StreamReader::try_new(file, None)?;

    let mut batches = Vec::new();

    // Read batches and collect only those belonging to our partition
    for (batch_idx, batch_result) in reader.enumerate() {
        if batch_idx >= end_batch {
            // We've passed the partition's range, stop reading
            break;
        }
        if batch_idx >= start_batch {
            // This batch belongs to our partition
            batches.push(batch_result?);
        }
    }

    Ok(batches)
}

/// Reads all batches from a sort shuffle data file, returning them grouped by partition.
///
/// # Arguments
/// * `data_path` - Path to the consolidated data file
///
/// # Returns
/// Vector of all record batches in the file.
pub fn read_all_batches(data_path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(data_path).map_err(BallistaError::IoError)?;
    let reader = StreamReader::try_new(file, None)?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::CompressionType;
    use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
    use std::io::BufWriter;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_schema() -> datafusion::arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn create_test_batch(
        schema: &datafusion::arrow::datatypes::SchemaRef,
        values: Vec<i32>,
    ) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

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

    #[test]
    fn test_read_all_batches() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let data_path = temp_dir.path().join("data.arrow");

        // Write test data
        let file = File::create(&data_path).unwrap();
        let buffered = BufWriter::new(file);
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer =
            StreamWriter::try_new_with_options(buffered, &schema, options).unwrap();

        writer
            .write(&create_test_batch(&schema, vec![1, 2, 3]))
            .unwrap();
        writer
            .write(&create_test_batch(&schema, vec![4, 5]))
            .unwrap();
        writer.finish().unwrap();

        // Read back
        let batches = read_all_batches(&data_path)?;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[1].num_rows(), 2);

        Ok(())
    }
}
