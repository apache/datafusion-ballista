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
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use log::debug;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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

    /// Returns a stream that lazily reads all spill files for a partition.
    ///
    /// This is more memory-efficient than `read_spill_files` as it doesn't
    /// load all batches into memory at once.
    pub fn stream_spill_files(
        &self,
        partition_id: usize,
        schema: SchemaRef,
    ) -> Result<SendableRecordBatchStream> {
        let paths: VecDeque<PathBuf> =
            self.get_spill_files(partition_id).iter().cloned().collect();

        let stream = SpillFileStream::try_new(paths, schema)?;
        Ok(Box::pin(stream))
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

/// Opens a spill file with buffered I/O and optional validation skipping.
fn open_spill_file(path: &Path) -> Result<StreamReader<BufReader<File>>> {
    let file = File::open(path).map_err(BallistaError::IoError)?;
    let file = BufReader::new(file);
    // Safety: with_skip_validation requires unsafe, user assures data is valid
    let reader = unsafe {
        StreamReader::try_new(file, None)?
            .with_skip_validation(cfg!(feature = "arrow-ipc-optimizations"))
    };
    Ok(reader)
}

/// A stream that lazily reads record batches from multiple spill files.
struct SpillFileStream {
    schema: SchemaRef,
    remaining_paths: VecDeque<PathBuf>,
    current_reader: Option<StreamReader<BufReader<File>>>,
}

impl SpillFileStream {
    /// Creates a new SpillFileStream.
    ///
    /// Opens the first file to get the schema and initial reader.
    /// Returns an error if the first file cannot be opened.
    fn try_new(mut paths: VecDeque<PathBuf>, schema: SchemaRef) -> Result<Self> {
        let current_reader = if let Some(path) = paths.pop_front() {
            Some(open_spill_file(&path)?)
        } else {
            None
        };

        Ok(Self {
            schema,
            remaining_paths: paths,
            current_reader,
        })
    }

    /// Opens the next file in the queue.
    ///
    /// Returns Ok(true) if a new file was opened, Ok(false) if no more files.
    fn open_next_file(&mut self) -> Result<bool> {
        if let Some(path) = self.remaining_paths.pop_front() {
            self.current_reader = Some(open_spill_file(&path)?);
            Ok(true)
        } else {
            self.current_reader = None;
            Ok(false)
        }
    }
}

impl Stream for SpillFileStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ref mut reader) = self.current_reader {
                if let Some(batch) = reader.next() {
                    return Poll::Ready(Some(batch.map_err(|e| e.into())));
                }
                // Current file exhausted, try next
                match self.open_next_file() {
                    Ok(true) => continue,
                    Ok(false) => return Poll::Ready(None),
                    Err(e) => {
                        return Poll::Ready(Some(Err(
                            datafusion::error::DataFusionError::External(Box::new(e)),
                        )));
                    }
                }
            }
            return Poll::Ready(None);
        }
    }
}

impl RecordBatchStream for SpillFileStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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

    #[tokio::test]
    async fn test_stream_spill_files() -> Result<()> {
        use futures::StreamExt;

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            CompressionType::LZ4_FRAME,
        )?;

        // Create multiple spills
        manager.spill(0, vec![create_test_batch(&schema, vec![1, 2, 3])], &schema)?;
        manager.spill(0, vec![create_test_batch(&schema, vec![4, 5])], &schema)?;

        // Stream back
        let mut stream = manager.stream_spill_files(0, schema)?;
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            batches.push(batch_result.unwrap());
        }

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[1].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_spill_files_empty() -> Result<()> {
        use futures::StreamExt;

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            CompressionType::LZ4_FRAME,
        )?;

        // No spills for partition 0
        assert!(!manager.has_spill_files(0));

        // Stream should be empty
        let mut stream = manager.stream_spill_files(0, schema)?;
        let batch = stream.next().await;
        assert!(batch.is_none());

        Ok(())
    }
}
