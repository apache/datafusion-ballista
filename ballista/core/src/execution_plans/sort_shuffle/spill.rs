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
//! Handles writing partition buffers to disk when the writer decides to flush
//! them, either because the runtime `MemoryPool` rejected a reservation grow or
//! because the per-task buffer budget was reached.
//! At finalization, the spill bytes are concatenated verbatim into the
//! consolidated output file alongside the in-memory remainder.
//!
//! Each spill event appends one complete Arrow IPC stream to each spilled
//! partition's file and closes its writer, so a partition's spill file holds
//! one stream per event and no writer outlives the event. Keeping a writer
//! open per partition would hold a file descriptor and a compression context
//! (about 0.3-0.7 MB each for zstd, outside the memory pool) for every output
//! partition until the task finishes.

use crate::JobId;
use crate::error::{BallistaError, Result};
use crate::utils::create_write_options;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::record_batch::RecordBatch;
use log::debug;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Batch, row and byte counts for rows written as IPC.
///
/// `num_bytes` is the Arrow in-memory size of each batch
/// (`RecordBatch::get_array_memory_size`), not the encoded size.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct EncodedStats {
    pub(super) num_batches: u64,
    pub(super) num_rows: u64,
    pub(super) num_bytes: u64,
}

impl std::ops::AddAssign for EncodedStats {
    fn add_assign(&mut self, other: Self) {
        self.num_batches += other.num_batches;
        self.num_rows += other.num_rows;
        self.num_bytes += other.num_bytes;
    }
}

/// Writes `batches` to `sink` as one complete Arrow IPC stream and returns
/// the sink with the counts of what was written.
///
/// Shared by spilling (a spill file) and the final in-memory encode (a
/// `Vec<u8>`), so both produce byte-identical streams for the same rows.
pub(super) fn write_ipc_stream<W: Write>(
    sink: W,
    schema: &SchemaRef,
    options: &IpcWriteOptions,
    batches: &mut dyn Iterator<Item = datafusion::error::Result<RecordBatch>>,
) -> datafusion::error::Result<(W, EncodedStats)> {
    let mut writer = StreamWriter::try_new_with_options(sink, schema, options.clone())?;
    let mut stats = EncodedStats::default();
    for batch in batches {
        let batch = batch?;
        stats.num_batches += 1;
        stats.num_rows += batch.num_rows() as u64;
        stats.num_bytes += batch.get_array_memory_size() as u64;
        writer.write(&batch)?;
    }
    Ok((writer.into_inner()?, stats))
}

/// Manages spill files for sort-based shuffle.
///
/// When partition buffers exceed memory limits, they are spilled to disk
/// as Arrow IPC streams. Each output partition has at most one spill file,
/// and each spill call appends one complete stream to it. During
/// finalization, the spill file bytes are concatenated directly into the
/// consolidated output file (no decode/re-encode round-trip); readers
/// already cross concatenated streams.
pub struct SpillManager {
    /// Base directory for spill files
    spill_dir: PathBuf,
    /// Schema shared by all spill writers
    schema: SchemaRef,
    /// IPC write options (including the compression codec) for spill files
    write_options: IpcWriteOptions,
    /// Total number of batches written across all spill files. One call to
    /// `spill_all_partitions` typically increments this multiple times (once
    /// per partition that had buffered rows).
    total_spilled_batches: u64,
    /// Total bytes spilled
    total_bytes_spilled: u64,
    /// Counts per spilled output partition. A partition has an entry exactly
    /// when it has a spill file.
    partition_counters: HashMap<usize, EncodedStats>,
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
        job_id: &JobId,
        stage_id: usize,
        task_id: usize,
        input_partition: usize,
        schema: SchemaRef,
        compression: Option<CompressionType>,
    ) -> Result<Self> {
        // Spills live beside the task's data file, one directory per input
        // partition, so a task owns exactly one directory under the stage and
        // `cleanup` leaves only `data.arrow` and its index behind.
        let mut spill_dir = PathBuf::from(work_dir);
        spill_dir.push(job_id.as_str());
        spill_dir.push(format!("{stage_id}"));
        spill_dir.push(format!("{task_id}"));
        spill_dir.push(format!("spill-{input_partition}"));

        std::fs::create_dir_all(&spill_dir).map_err(BallistaError::IoError)?;

        Ok(Self {
            spill_dir,
            schema,
            write_options: create_write_options(compression)?,
            total_spilled_batches: 0,
            total_bytes_spilled: 0,
            partition_counters: HashMap::new(),
        })
    }

    /// Spills `batches` for `partition_id` to disk as one complete IPC
    /// stream appended to the partition's spill file, which the first call
    /// for a given `partition_id` creates. The writer is finished and dropped
    /// before returning.
    ///
    /// Returns `(batches, bytes)` written, with bytes estimated from each
    /// batch's array memory size.
    pub fn spill_batches(
        &mut self,
        partition_id: usize,
        batches: &mut dyn Iterator<Item = datafusion::error::Result<RecordBatch>>,
    ) -> Result<(u64, u64)> {
        let file = self.open_spill_file(partition_id)?;
        let (sink, stats) = write_ipc_stream(
            BufWriter::new(file),
            &self.schema,
            &self.write_options,
            batches,
        )?;
        sink.into_inner()
            .map_err(|e| BallistaError::IoError(e.into_error()))?;
        self.count_spill(partition_id, stats);

        Ok((stats.num_batches, stats.num_bytes))
    }

    /// Appends `stream` to `partition_id`'s spill file, as [`Self::spill_batches`]
    /// would for the rows it holds. `stream` must be one complete IPC stream
    /// written by [`write_ipc_stream`] with this manager's schema and write
    /// options, and `stats` its counts.
    pub(super) fn spill_stream(
        &mut self,
        partition_id: usize,
        stream: &[u8],
        stats: EncodedStats,
    ) -> Result<()> {
        self.open_spill_file(partition_id)?
            .write_all(stream)
            .map_err(BallistaError::IoError)?;
        self.count_spill(partition_id, stats);
        Ok(())
    }

    /// Opens `partition_id`'s spill file for appending, creating it on the
    /// partition's first spill.
    fn open_spill_file(&self, partition_id: usize) -> Result<File> {
        let path = self.spill_file_path(partition_id);
        debug!("Spilling partition {partition_id} to {path:?}");
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(BallistaError::IoError)
    }

    /// Adds one spilled stream's counts to `partition_id` and the totals.
    fn count_spill(&mut self, partition_id: usize, stats: EncodedStats) {
        *self.partition_counters.entry(partition_id).or_default() += stats;
        self.total_spilled_batches += stats.num_batches;
        self.total_bytes_spilled += stats.num_bytes;
    }

    /// Returns true if the partition has a spill file.
    pub fn has_spill_files(&self, partition_id: usize) -> bool {
        self.partition_counters.contains_key(&partition_id)
    }

    /// Cleans up all spill files.
    pub fn cleanup(&mut self) -> Result<()> {
        if self.spill_dir.exists() {
            std::fs::remove_dir_all(&self.spill_dir).map_err(BallistaError::IoError)?;
        }
        Ok(())
    }

    /// Returns the total number of batches written to spill files across all
    /// partitions. Note this counts batches, not spill *events*: a single
    /// spill event in the writer typically produces one batch per
    /// non-empty output partition. Spill-event accounting lives at the writer
    /// layer because the spill manager only sees batch-level calls.
    pub fn total_spilled_batches(&self) -> u64 {
        self.total_spilled_batches
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
        let stats = self
            .partition_counters
            .get(&partition_id)
            .copied()
            .unwrap_or_default();
        (stats.num_batches, stats.num_rows, stats.num_bytes)
    }

    /// Returns the path to a partition's spill file, or `None` if the
    /// partition never spilled.
    pub fn spill_path(&self, partition_id: usize) -> Option<PathBuf> {
        self.has_spill_files(partition_id)
            .then(|| self.spill_file_path(partition_id))
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
            .field("write_options", &self.write_options)
            .field("partition_counters", &self.partition_counters)
            .field("total_spilled_batches", &self.total_spilled_batches)
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
    use crate::execution_plans::test_util::{int32_batch, int32_schema};
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::io::{Seek, SeekFrom};
    use tempfile::TempDir;

    /// Reads every batch of a partition's spill file, across the one stream
    /// each spill event appended.
    fn read_spill(
        manager: &SpillManager,
        partition_id: usize,
    ) -> Result<Vec<RecordBatch>> {
        let path = manager.spill_path(partition_id).unwrap();
        let len = std::fs::metadata(&path)?.len();
        let mut batches = Vec::new();
        let mut offset = 0;
        while offset < len {
            let mut file = std::fs::File::open(&path)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut reader = StreamReader::try_new(file, None)?;
            for batch in reader.by_ref() {
                batches.push(batch?);
            }
            offset = reader.get_mut().stream_position()?;
        }
        Ok(batches)
    }

    /// Spills a single batch as its own spill event, returning bytes spilled.
    fn spill_one(
        manager: &mut SpillManager,
        partition_id: usize,
        batch: RecordBatch,
    ) -> Result<u64> {
        let (_, bytes) =
            manager.spill_batches(partition_id, &mut std::iter::once(Ok(batch)))?;
        Ok(bytes)
    }

    #[test]
    fn spill_stream_writes_what_spill_batches_would() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let compression = Some(CompressionType::ZSTD);
        let manager = |input_partition| {
            SpillManager::new(
                temp_dir.path().to_str().unwrap(),
                &"job1".into(),
                1,
                0,
                input_partition,
                int32_schema(),
                compression,
            )
        };
        let batches = || {
            [int32_batch(&[1, 2, 3]), int32_batch(&[4, 5])]
                .into_iter()
                .map(Ok)
        };

        // Two spills each, so the second appends to the first.
        let mut spilled = manager(0)?;
        let mut streamed = manager(1)?;
        for _ in 0..2 {
            spilled.spill_batches(0, &mut batches())?;
            let (stream, stats) = write_ipc_stream(
                Vec::new(),
                &int32_schema(),
                &create_write_options(compression)?,
                &mut batches(),
            )?;
            streamed.spill_stream(0, &stream, stats)?;
        }

        let written = |manager: &SpillManager| -> Result<_> {
            Ok((
                std::fs::read(manager.spill_path(0).unwrap())?,
                manager.partition_stats(0),
                manager.total_spilled_batches(),
                manager.total_bytes_spilled(),
            ))
        };
        assert_eq!(written(&streamed)?, written(&spilled)?);
        Ok(())
    }

    #[test]
    fn test_spill_and_read() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = int32_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        let b1 = int32_batch(&[1, 2, 3]);
        let b2 = int32_batch(&[4, 5]);
        assert!(spill_one(&mut manager, 0, b1)? > 0);
        assert!(spill_one(&mut manager, 0, b2)? > 0);

        assert!(manager.has_spill_files(0));
        assert!(!manager.has_spill_files(1));
        assert_eq!(manager.total_spilled_batches(), 2);

        let read_batches = read_spill(&manager, 0)?;
        assert_eq!(read_batches.len(), 2);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(read_batches[1].num_rows(), 2);

        Ok(())
    }

    #[test]
    fn test_multiple_partitions() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = int32_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        spill_one(&mut manager, 0, int32_batch(&[1, 2]))?;
        spill_one(&mut manager, 1, int32_batch(&[3, 4]))?;

        assert!(manager.has_spill_files(0));
        assert!(manager.has_spill_files(1));
        assert_eq!(manager.total_spilled_batches(), 2);

        let r0 = read_spill(&manager, 0)?;
        let r1 = read_spill(&manager, 1)?;
        assert_eq!(r0.len(), 1);
        assert_eq!(r1.len(), 1);

        Ok(())
    }

    #[test]
    fn test_per_partition_stats() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = int32_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        spill_one(&mut manager, 0, int32_batch(&[1, 2, 3]))?;
        spill_one(&mut manager, 0, int32_batch(&[4, 5]))?;
        spill_one(&mut manager, 1, int32_batch(&[6]))?;

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
        let schema = int32_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        spill_one(&mut manager, 0, int32_batch(&[1, 2]))?;

        let spill_dir = manager.spill_dir.clone();
        assert!(spill_dir.exists());

        manager.cleanup()?;
        assert!(!spill_dir.exists());

        Ok(())
    }
}
