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

//! Spill manager for sort-based shuffle.
//!
//! Handles writing partition buffers to disk when memory pressure is high,
//! and reading them back during the finalization phase.

use crate::error::{BallistaError, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::ipc::{CompressionType, writer::IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use log::debug;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

/// Manages spill files for sort-based shuffle.
///
/// When partition buffers exceed memory limits, they are spilled to disk
/// as Arrow IPC files. During finalization, these spill files are read
/// back and merged into the consolidated output file.
#[derive(Debug)]
pub struct SpillManager {
    /// Base directory for spill files
    spill_dir: PathBuf,
    /// Spill files per output partition: partition_id -> Vec<spill_file_path>
    spill_files: HashMap<usize, Vec<PathBuf>>,
    /// Counter for generating unique spill file names
    spill_counter: usize,
    /// Compression codec for spill files
    compression: CompressionType,
    /// Total number of spills performed
    total_spills: usize,
    /// Total bytes spilled
    total_bytes_spilled: u64,
}

impl SpillManager {
    /// Creates a new spill manager.
    ///
    /// # Arguments
    /// * `work_dir` - Base work directory
    /// * `job_id` - Job identifier
    /// * `stage_id` - Stage identifier
    /// * `input_partition` - Input partition number
    /// * `compression` - Compression codec for spill files
    pub fn new(
        work_dir: &str,
        job_id: &str,
        stage_id: usize,
        input_partition: usize,
        compression: CompressionType,
    ) -> Result<Self> {
        let mut spill_dir = PathBuf::from(work_dir);
        spill_dir.push(job_id);
        spill_dir.push(format!("{stage_id}"));
        spill_dir.push(format!("{input_partition}"));
        spill_dir.push("spill");

        // Create spill directory
        std::fs::create_dir_all(&spill_dir).map_err(BallistaError::IoError)?;

        Ok(Self {
            spill_dir,
            spill_files: HashMap::new(),
            spill_counter: 0,
            compression,
            total_spills: 0,
            total_bytes_spilled: 0,
        })
    }

    /// Spills batches for a partition to disk.
    ///
    /// Returns the number of bytes written.
    pub fn spill(
        &mut self,
        partition_id: usize,
        batches: Vec<RecordBatch>,
        schema: &SchemaRef,
    ) -> Result<u64> {
        if batches.is_empty() {
            return Ok(0);
        }

        let spill_path = self.next_spill_path(partition_id);
        debug!(
            "Spilling {} batches for partition {} to {:?}",
            batches.len(),
            partition_id,
            spill_path
        );

        let file = File::create(&spill_path).map_err(BallistaError::IoError)?;
        let buffered = BufWriter::new(file);

        let options =
            IpcWriteOptions::default().try_with_compression(Some(self.compression))?;

        let mut writer = StreamWriter::try_new_with_options(buffered, schema, options)?;

        for batch in &batches {
            writer.write(batch)?;
        }

        writer.finish()?;

        let bytes_written = std::fs::metadata(&spill_path)
            .map_err(BallistaError::IoError)?
            .len();

        // Track the spill file
        self.spill_files
            .entry(partition_id)
            .or_default()
            .push(spill_path);

        self.total_spills += 1;
        self.total_bytes_spilled += bytes_written;

        Ok(bytes_written)
    }

    /// Returns the spill files for a partition.
    pub fn get_spill_files(&self, partition_id: usize) -> &[PathBuf] {
        self.spill_files
            .get(&partition_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Returns true if the partition has spill files.
    pub fn has_spill_files(&self, partition_id: usize) -> bool {
        self.spill_files
            .get(&partition_id)
            .is_some_and(|v| !v.is_empty())
    }

    /// Reads all spill files for a partition and returns the batches.
    pub fn read_spill_files(&self, partition_id: usize) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for spill_path in self.get_spill_files(partition_id) {
            let file = File::open(spill_path).map_err(BallistaError::IoError)?;
            let reader = StreamReader::try_new(file, None)?;

            for batch_result in reader {
                all_batches.push(batch_result?);
            }
        }

        Ok(all_batches)
    }

    /// Cleans up all spill files.
    pub fn cleanup(&self) -> Result<()> {
        if self.spill_dir.exists() {
            std::fs::remove_dir_all(&self.spill_dir).map_err(BallistaError::IoError)?;
        }
        Ok(())
    }

    /// Returns the total number of spills performed.
    pub fn total_spills(&self) -> usize {
        self.total_spills
    }

    /// Returns the total bytes spilled to disk.
    pub fn total_bytes_spilled(&self) -> u64 {
        self.total_bytes_spilled
    }

    /// Generates the next spill file path for a partition.
    fn next_spill_path(&mut self, partition_id: usize) -> PathBuf {
        let path = self.spill_dir.join(format!(
            "part-{partition_id}-spill-{}.arrow",
            self.spill_counter
        ));
        self.spill_counter += 1;
        path
    }
}

impl Drop for SpillManager {
    fn drop(&mut self) {
        // Best-effort cleanup on drop
        if let Err(e) = self.cleanup() {
            debug!("Failed to cleanup spill files: {e:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn create_test_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_spill_and_read() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            CompressionType::LZ4_FRAME,
        )?;

        // Spill some batches
        let batches = vec![
            create_test_batch(&schema, vec![1, 2, 3]),
            create_test_batch(&schema, vec![4, 5]),
        ];
        let bytes = manager.spill(0, batches, &schema)?;
        assert!(bytes > 0);

        // Verify spill tracking
        assert!(manager.has_spill_files(0));
        assert!(!manager.has_spill_files(1));
        assert_eq!(manager.total_spills(), 1);

        // Read back
        let read_batches = manager.read_spill_files(0)?;
        assert_eq!(read_batches.len(), 2);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(read_batches[1].num_rows(), 2);

        Ok(())
    }

    #[test]
    fn test_multiple_spills() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            CompressionType::LZ4_FRAME,
        )?;

        // Multiple spills for same partition
        manager.spill(0, vec![create_test_batch(&schema, vec![1, 2])], &schema)?;
        manager.spill(0, vec![create_test_batch(&schema, vec![3, 4])], &schema)?;

        assert_eq!(manager.get_spill_files(0).len(), 2);
        assert_eq!(manager.total_spills(), 2);

        // Read all back
        let batches = manager.read_spill_files(0)?;
        assert_eq!(batches.len(), 2);

        Ok(())
    }

    #[test]
    fn test_cleanup() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            CompressionType::LZ4_FRAME,
        )?;

        manager.spill(0, vec![create_test_batch(&schema, vec![1, 2])], &schema)?;

        let spill_dir = manager.spill_dir.clone();
        assert!(spill_dir.exists());

        manager.cleanup()?;
        assert!(!spill_dir.exists());

        Ok(())
    }
}
