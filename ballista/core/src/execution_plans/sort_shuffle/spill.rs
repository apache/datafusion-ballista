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
//! Handles writing partition buffers to disk when memory pressure is high.
//! At finalization, the spill bytes are concatenated verbatim into the
//! consolidated output file alongside the in-memory remainder.

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
use std::path::{Path, PathBuf};

/// Manages spill files for sort-based shuffle.
///
/// When partition buffers exceed memory limits, they are spilled to disk
/// as Arrow IPC streams. Each output partition has at most one spill file
/// that is appended to across multiple spill calls. During finalization,
/// the spill file bytes are concatenated directly into the consolidated
/// output file (no decode/re-encode round-trip).
pub struct SpillManager {
    /// Base directory for spill files
    spill_dir: PathBuf,
    /// Schema shared by all spill writers
    schema: SchemaRef,
    /// Spill file path per output partition: partition_id -> spill_file_path
    spill_files: HashMap<usize, PathBuf>,
    /// Active writers per partition, kept open for appending
    active_writers: HashMap<usize, StreamWriter<BufWriter<File>>>,
    /// Compression codec for spill files
    compression: CompressionType,
    /// Total number of spills performed (counts batches, not events)
    total_spills: usize,
    /// Total bytes spilled
    total_bytes_spilled: u64,
    /// Per-partition counters: partition_id -> (batches, rows, bytes)
    partition_counters: HashMap<usize, (u64, u64, u64)>,
}

impl SpillManager {
    /// Creates a new spill manager.
    ///
    /// # Arguments
    /// * `work_dir` - Base work directory
    /// * `job_id` - Job identifier
    /// * `stage_id` - Stage identifier
    /// * `input_partition` - Input partition number
    /// * `schema` - Schema shared by all spill writers
    /// * `compression` - Compression codec for spill files
    pub fn new(
        work_dir: &str,
        job_id: &str,
        stage_id: usize,
        input_partition: usize,
        schema: SchemaRef,
        compression: CompressionType,
    ) -> Result<Self> {
        let mut spill_dir = PathBuf::from(work_dir);
        spill_dir.push(job_id);
        spill_dir.push(format!("{stage_id}"));
        spill_dir.push(format!("{input_partition}"));
        spill_dir.push("spill");

        std::fs::create_dir_all(&spill_dir).map_err(BallistaError::IoError)?;

        Ok(Self {
            spill_dir,
            schema,
            spill_files: HashMap::new(),
            active_writers: HashMap::new(),
            compression,
            total_spills: 0,
            total_bytes_spilled: 0,
            partition_counters: HashMap::new(),
        })
    }

    /// Spills a single `batch` for `partition_id` to disk. The first call for
    /// a given `partition_id` creates the spill file; subsequent calls append.
    ///
    /// Returns the number of bytes written (estimated from the batch's array
    /// memory size).
    pub fn spill(&mut self, partition_id: usize, batch: &RecordBatch) -> Result<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        if !self.active_writers.contains_key(&partition_id) {
            let spill_path = self.spill_file_path(partition_id);
            debug!(
                "Creating spill file for partition {} at {:?}",
                partition_id, spill_path
            );
            let file = File::create(&spill_path).map_err(BallistaError::IoError)?;
            let buffered = BufWriter::new(file);

            let options = IpcWriteOptions::default()
                .try_with_compression(Some(self.compression))?;

            let writer =
                StreamWriter::try_new_with_options(buffered, &self.schema, options)?;

            self.active_writers.insert(partition_id, writer);
            self.spill_files.insert(partition_id, spill_path);
        }

        let writer = self.active_writers.get_mut(&partition_id).unwrap();
        let bytes_written = batch.get_array_memory_size() as u64;
        writer.write(batch)?;

        let entry = self
            .partition_counters
            .entry(partition_id)
            .or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += batch.num_rows() as u64;
        entry.2 += bytes_written;

        self.total_spills += 1;
        self.total_bytes_spilled += bytes_written;

        Ok(bytes_written)
    }

    /// Returns true if the partition has a spill file.
    pub fn has_spill_files(&self, partition_id: usize) -> bool {
        self.spill_files.contains_key(&partition_id)
    }

    /// Finishes all active writers so spill files can be read.
    /// Must be called before `open_spill_reader`.
    pub fn finish_writers(&mut self) -> Result<()> {
        for (_, mut writer) in self.active_writers.drain() {
            writer.finish()?;
        }
        Ok(())
    }

    /// Opens the spill file for a partition and returns a streaming
    /// reader. `finish_writers` must be called before this method.
    pub fn open_spill_reader(
        &self,
        partition_id: usize,
    ) -> Result<Option<StreamReader<File>>> {
        match self.spill_files.get(&partition_id) {
            Some(spill_path) => {
                let file = File::open(spill_path).map_err(BallistaError::IoError)?;
                let reader = StreamReader::try_new(file, None)?;
                Ok(Some(reader))
            }
            None => Ok(None),
        }
    }

    /// Cleans up all spill files.
    pub fn cleanup(&mut self) -> Result<()> {
        // Finish any active writers first
        for (_, mut writer) in self.active_writers.drain() {
            let _ = writer.finish();
        }
        if self.spill_dir.exists() {
            std::fs::remove_dir_all(&self.spill_dir).map_err(BallistaError::IoError)?;
        }
        Ok(())
    }

    /// Returns the total number of batches spilled across all partitions.
    pub fn total_spills(&self) -> usize {
        self.total_spills
    }

    /// Returns the total bytes spilled to disk.
    pub fn total_bytes_spilled(&self) -> u64 {
        self.total_bytes_spilled
    }

    /// Returns `(batches, rows, bytes)` spilled for the given partition, or
    /// `(0, 0, 0)` if the partition never spilled.
    ///
    /// The `bytes` value is the Arrow in-memory buffer size of each batch
    /// at the time of the spill call (`RecordBatch::get_array_memory_size`).
    /// It is **not** the compressed on-disk size.
    pub fn partition_stats(&self, partition_id: usize) -> (u64, u64, u64) {
        self.partition_counters
            .get(&partition_id)
            .copied()
            .unwrap_or((0, 0, 0))
    }

    /// Returns the path to a partition's spill file, or `None` if no batches
    /// were ever spilled for that partition. `finish_writers` must be called
    /// before this path is read by another process.
    pub fn spill_path(&self, partition_id: usize) -> Option<&Path> {
        self.spill_files.get(&partition_id).map(PathBuf::as_path)
    }

    /// Returns the spill file path for a partition.
    fn spill_file_path(&self, partition_id: usize) -> PathBuf {
        self.spill_dir.join(format!("part-{partition_id}.arrow"))
    }
}

impl std::fmt::Debug for SpillManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillManager")
            .field("spill_dir", &self.spill_dir)
            .field("spill_files", &self.spill_files)
            .field("compression", &self.compression)
            .field("total_spills", &self.total_spills)
            .field("total_bytes_spilled", &self.total_bytes_spilled)
            .finish()
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
            schema.clone(),
            CompressionType::LZ4_FRAME,
        )?;

        let b1 = create_test_batch(&schema, vec![1, 2, 3]);
        let b2 = create_test_batch(&schema, vec![4, 5]);
        assert!(manager.spill(0, &b1)? > 0);
        assert!(manager.spill(0, &b2)? > 0);

        assert!(manager.has_spill_files(0));
        assert!(!manager.has_spill_files(1));
        assert_eq!(manager.total_spills(), 2);

        manager.finish_writers()?;

        let reader = manager.open_spill_reader(0)?.unwrap();
        let read_batches: Vec<_> =
            reader.into_iter().collect::<std::result::Result<_, _>>()?;
        assert_eq!(read_batches.len(), 2);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(read_batches[1].num_rows(), 2);

        Ok(())
    }

    #[test]
    fn test_multiple_partitions() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            schema.clone(),
            CompressionType::LZ4_FRAME,
        )?;

        manager.spill(0, &create_test_batch(&schema, vec![1, 2]))?;
        manager.spill(1, &create_test_batch(&schema, vec![3, 4]))?;

        assert!(manager.has_spill_files(0));
        assert!(manager.has_spill_files(1));
        assert_eq!(manager.total_spills(), 2);

        manager.finish_writers()?;

        let r0 = manager
            .open_spill_reader(0)?
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let r1 = manager
            .open_spill_reader(1)?
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(r0.len(), 1);
        assert_eq!(r1.len(), 1);

        Ok(())
    }

    #[test]
    fn test_per_partition_stats() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            schema.clone(),
            CompressionType::LZ4_FRAME,
        )?;

        manager.spill(0, &create_test_batch(&schema, vec![1, 2, 3]))?;
        manager.spill(0, &create_test_batch(&schema, vec![4, 5]))?;
        manager.spill(1, &create_test_batch(&schema, vec![6]))?;

        let (b0, r0, bytes0) = manager.partition_stats(0);
        let (b1, r1, bytes1) = manager.partition_stats(1);
        let (b2, r2, bytes2) = manager.partition_stats(2);

        assert_eq!((b0, r0), (2, 5));
        assert_eq!((b1, r1), (1, 1));
        assert_eq!((b2, r2), (0, 0));

        assert!(
            bytes0 > 0,
            "spilled partition should have non-zero bytes counter"
        );
        assert!(
            bytes1 > 0,
            "spilled partition should have non-zero bytes counter"
        );
        assert_eq!(
            bytes2, 0,
            "never-spilled partition should have zero bytes counter"
        );

        assert!(manager.spill_path(0).is_some());
        assert!(manager.spill_path(1).is_some());
        assert!(manager.spill_path(2).is_none());

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
            schema.clone(),
            CompressionType::LZ4_FRAME,
        )?;

        manager.spill(0, &create_test_batch(&schema, vec![1, 2]))?;

        let spill_dir = manager.spill_dir.clone();
        assert!(spill_dir.exists());

        manager.cleanup()?;
        assert!(!spill_dir.exists());

        Ok(())
    }
}
