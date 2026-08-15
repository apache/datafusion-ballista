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
//!
//! Each flush produces one **spill run**: a single file holding every spilled
//! partition's batches back to back, in ascending partition order, each
//! partition's batches forming one self-contained Arrow IPC stream. The run
//! records the byte range it gave each partition. At finalization the writer
//! concatenates, for each partition, that partition's range from every run
//! verbatim into the consolidated output file (no decode/re-encode
//! round-trip).
//!
//! One file per flush — rather than one file per output partition — keeps the
//! open file descriptors and the buffered-writer memory at O(1) instead of
//! O(num_output_partitions), which is what lets a task shuffle into thousands
//! of partitions without exhausting its fd limit.

use crate::JobId;
use crate::error::{BallistaError, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::ipc::{CompressionType, writer::IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use log::debug;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::{Path, PathBuf};

/// Byte range `[start, end)` a run gave to one output partition.
type ByteRange = (u64, u64);

/// A completed spill run: one file, with the byte range each partition
/// occupies inside it.
#[derive(Debug)]
struct SpillRun {
    path: PathBuf,
    ranges: HashMap<usize, ByteRange>,
}

/// The partition currently being appended to a run: its IPC stream writer,
/// the partition id, and the byte offset its stream started at.
struct ActivePartition {
    writer: Box<StreamWriter<BufWriter<File>>>,
    partition: usize,
    start: u64,
}

/// The run currently being written.
///
/// Exactly one of `idle` and `active` is `Some`: `idle` holds the file between
/// partitions, `active` holds it wrapped in the IPC stream writer for the
/// partition being appended. Switching partitions finishes the active stream
/// (which writes its EOS marker) and records the range, so every partition's
/// slice of the run is an independently readable IPC stream.
struct CurrentRun {
    path: PathBuf,
    idle: Option<BufWriter<File>>,
    active: Option<ActivePartition>,
    ranges: HashMap<usize, ByteRange>,
}

/// Manages spill files for sort-based shuffle.
///
/// See the module docs for the on-disk layout. Callers drive one run per
/// flush: [`SpillManager::spill`] appends a batch for a partition, and
/// [`SpillManager::finish_run`] closes the run. Within a run, all batches for
/// one partition must be written consecutively; visiting a partition again
/// after moving on simply opens a second range, which stays correct but wastes
/// a schema header, so callers should iterate partitions in order.
pub struct SpillManager {
    /// Base directory for spill files
    spill_dir: PathBuf,
    /// Schema shared by all spill writers
    schema: SchemaRef,
    /// Compression codec for spill files
    compression: Option<CompressionType>,
    /// Runs completed so far, in flush order.
    runs: Vec<SpillRun>,
    /// Run currently open, if a flush is in progress.
    current: Option<CurrentRun>,
    /// Total number of batches written across all runs. One flush typically
    /// increments this multiple times (once per partition that had buffered
    /// rows).
    total_spilled_batches: u64,
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
        job_id: &JobId,
        stage_id: usize,
        input_partition: usize,
        schema: SchemaRef,
        compression: Option<CompressionType>,
    ) -> Result<Self> {
        let mut spill_dir = PathBuf::from(work_dir);
        spill_dir.push(job_id.as_str());
        spill_dir.push(format!("{stage_id}"));
        spill_dir.push(format!("{input_partition}"));
        spill_dir.push("spill");

        std::fs::create_dir_all(&spill_dir).map_err(BallistaError::IoError)?;

        Ok(Self {
            spill_dir,
            schema,
            compression,
            runs: Vec::new(),
            current: None,
            total_spilled_batches: 0,
            total_bytes_spilled: 0,
            partition_counters: HashMap::new(),
        })
    }

    /// Spills a single `batch` for `partition_id` into the current run,
    /// starting a run if none is open.
    ///
    /// Returns the number of bytes written (estimated from the batch's array
    /// memory size).
    pub fn spill(&mut self, partition_id: usize, batch: &RecordBatch) -> Result<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        if self.current.is_none() {
            self.begin_run()?;
        }
        self.switch_partition(partition_id)?;

        let bytes_written = batch.get_array_memory_size() as u64;
        let active = self
            .current
            .as_mut()
            .and_then(|c| c.active.as_mut())
            .ok_or_else(|| {
                BallistaError::General(
                    "spill run has no active partition writer".to_owned(),
                )
            })?;
        active.writer.write(batch)?;

        let entry = self
            .partition_counters
            .entry(partition_id)
            .or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += batch.num_rows() as u64;
        entry.2 += bytes_written;

        self.total_spilled_batches += 1;
        self.total_bytes_spilled += bytes_written;

        Ok(bytes_written)
    }

    /// Opens a new run file.
    fn begin_run(&mut self) -> Result<()> {
        let path = self
            .spill_dir
            .join(format!("run-{}.arrow", self.runs.len()));
        debug!("Creating spill run at {path:?}");
        let file = File::create(&path).map_err(BallistaError::IoError)?;
        self.current = Some(CurrentRun {
            path,
            idle: Some(BufWriter::new(file)),
            active: None,
            ranges: HashMap::new(),
        });
        Ok(())
    }

    /// Makes `partition_id` the run's active partition, closing the previous
    /// partition's IPC stream and recording its byte range first.
    fn switch_partition(&mut self, partition_id: usize) -> Result<()> {
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| BallistaError::General("no open spill run".to_owned()))?;

        if let Some(active) = &current.active
            && active.partition == partition_id
        {
            return Ok(());
        }

        Self::close_active(current)?;
        let mut out = current
            .idle
            .take()
            .ok_or_else(|| BallistaError::General("spill run file missing".to_owned()))?;
        out.flush().map_err(BallistaError::IoError)?;
        let start = out
            .get_mut()
            .stream_position()
            .map_err(BallistaError::IoError)?;
        let options =
            IpcWriteOptions::default().try_with_compression(self.compression)?;
        let writer = StreamWriter::try_new_with_options(out, &self.schema, options)?;
        current.active = Some(ActivePartition {
            writer: Box::new(writer),
            partition: partition_id,
            start,
        });
        Ok(())
    }

    /// Finishes the active partition stream (if any), records its byte range,
    /// and returns the run's file to its idle slot.
    fn close_active(current: &mut CurrentRun) -> Result<()> {
        let Some(ActivePartition {
            writer,
            partition,
            start,
        }) = current.active.take()
        else {
            return Ok(());
        };
        // `into_inner` finishes the stream (EOS marker) and flushes.
        let mut out = writer.into_inner()?;
        out.flush().map_err(BallistaError::IoError)?;
        let end = out
            .get_mut()
            .stream_position()
            .map_err(BallistaError::IoError)?;
        match current.ranges.entry(partition) {
            // A partition visited twice in one run extends its range; the
            // bytes stay contiguous only if nothing else was written in
            // between, which ordered iteration guarantees.
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.get_mut().1 = end;
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert((start, end));
            }
        }
        current.idle = Some(out);
        Ok(())
    }

    /// Closes the current run, if one is open, so its bytes can be read back.
    /// Safe to call when no run is open.
    pub fn finish_run(&mut self) -> Result<()> {
        let Some(mut current) = self.current.take() else {
            return Ok(());
        };
        Self::close_active(&mut current)?;
        if let Some(mut out) = current.idle.take() {
            out.flush().map_err(BallistaError::IoError)?;
        }
        self.runs.push(SpillRun {
            path: current.path,
            ranges: current.ranges,
        });
        Ok(())
    }

    /// Finishes any open run so spill files can be read.
    /// Must be called before [`SpillManager::partition_segments`].
    pub fn finish_writers(&mut self) -> Result<()> {
        self.finish_run()
    }

    /// Returns true if any run holds bytes for the partition.
    pub fn has_spill_files(&self, partition_id: usize) -> bool {
        self.runs
            .iter()
            .any(|run| run.ranges.contains_key(&partition_id))
    }

    /// Returns `(path, start, end)` for every run segment belonging to
    /// `partition_id`, in flush order. Concatenating these segments in order
    /// reproduces everything spilled for that partition.
    ///
    /// [`SpillManager::finish_writers`] must be called first.
    pub fn partition_segments(
        &self,
        partition_id: usize,
    ) -> impl Iterator<Item = (&Path, u64, u64)> {
        self.runs.iter().filter_map(move |run| {
            run.ranges
                .get(&partition_id)
                .map(|&(start, end)| (run.path.as_path(), start, end))
        })
    }

    /// Cleans up all spill files.
    pub fn cleanup(&mut self) -> Result<()> {
        // Drop any half-written run first so its file handle is released.
        self.current = None;
        self.runs.clear();
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
        self.partition_counters
            .get(&partition_id)
            .copied()
            .unwrap_or((0, 0, 0))
    }
}

impl std::fmt::Debug for SpillManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillManager")
            .field("spill_dir", &self.spill_dir)
            .field("runs", &self.runs.len())
            .field("compression", &self.compression)
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
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn create_test_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

    /// Reads back everything spilled for `partition_id` by concatenating its
    /// run segments, the same way `finalize_output` does.
    fn read_partition(
        manager: &SpillManager,
        partition_id: usize,
    ) -> Result<Vec<RecordBatch>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut bytes = Vec::new();
        for (path, start, end) in manager.partition_segments(partition_id) {
            let mut f = File::open(path).map_err(BallistaError::IoError)?;
            f.seek(SeekFrom::Start(start))
                .map_err(BallistaError::IoError)?;
            let mut seg = vec![0u8; (end - start) as usize];
            f.read_exact(&mut seg).map_err(BallistaError::IoError)?;
            bytes.extend_from_slice(&seg);
        }
        if bytes.is_empty() {
            return Ok(vec![]);
        }
        // The concatenation is a sequence of complete IPC streams; walk them.
        let mut out = Vec::new();
        let mut cursor = std::io::Cursor::new(bytes);
        loop {
            let pos = cursor.position();
            let len = cursor.get_ref().len() as u64;
            if pos >= len {
                break;
            }
            let reader = StreamReader::try_new(&mut cursor, None)?;
            for b in reader {
                out.push(b?);
            }
            if cursor.position() == pos {
                break;
            }
        }
        Ok(out)
    }

    #[test]
    fn test_spill_and_read() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        let b1 = create_test_batch(&schema, vec![1, 2, 3]);
        let b2 = create_test_batch(&schema, vec![4, 5]);
        assert!(manager.spill(0, &b1)? > 0);
        assert!(manager.spill(0, &b2)? > 0);

        assert_eq!(manager.total_spilled_batches(), 2);

        manager.finish_writers()?;
        assert!(manager.has_spill_files(0));
        assert!(!manager.has_spill_files(1));

        let read_batches = read_partition(&manager, 0)?;
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
            &"job1".into(),
            1,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        manager.spill(0, &create_test_batch(&schema, vec![1, 2]))?;
        manager.spill(1, &create_test_batch(&schema, vec![3, 4]))?;
        manager.finish_writers()?;

        assert!(manager.has_spill_files(0));
        assert!(manager.has_spill_files(1));
        assert_eq!(manager.total_spilled_batches(), 2);

        let r0 = read_partition(&manager, 0)?;
        let r1 = read_partition(&manager, 1)?;
        assert_eq!(r0.len(), 1);
        assert_eq!(r1.len(), 1);
        assert_eq!(
            r0[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );
        assert_eq!(
            r1[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[3, 4]
        );

        Ok(())
    }

    /// Several flushes must leave each partition's bytes readable in flush
    /// order — this is what `finalize_output` relies on when it concatenates
    /// run segments.
    #[test]
    fn test_multiple_runs_concatenate_in_order() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        for run in 0..3 {
            manager
                .spill(0, &create_test_batch(&schema, vec![run * 10, run * 10 + 1]))?;
            manager.spill(1, &create_test_batch(&schema, vec![run * 100]))?;
            manager.finish_run()?;
        }
        manager.finish_writers()?;

        // One file per flush, not one per partition.
        let files: Vec<_> = std::fs::read_dir(&manager.spill_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(files.len(), 3, "expected one file per flush, got {files:?}");

        let r0 = read_partition(&manager, 0)?;
        assert_eq!(r0.len(), 3);
        let vals: Vec<i32> = r0
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(vals, vec![0, 1, 10, 11, 20, 21]);

        let r1 = read_partition(&manager, 1)?;
        assert_eq!(r1.len(), 3);

        Ok(())
    }

    #[test]
    fn test_per_partition_stats() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        manager.spill(0, &create_test_batch(&schema, vec![1, 2, 3]))?;
        manager.spill(0, &create_test_batch(&schema, vec![4, 5]))?;
        manager.spill(1, &create_test_batch(&schema, vec![6]))?;
        manager.finish_writers()?;

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

        assert_eq!(manager.partition_segments(0).count(), 1);
        assert_eq!(manager.partition_segments(1).count(), 1);
        assert_eq!(manager.partition_segments(2).count(), 0);

        Ok(())
    }

    /// The open-fd count must not scale with the output partition count.
    #[test]
    fn test_open_files_do_not_scale_with_partitions() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        for p in 0..512 {
            manager.spill(p, &create_test_batch(&schema, vec![p as i32]))?;
        }
        let open_during = std::fs::read_dir(&manager.spill_dir).unwrap().count();
        assert_eq!(
            open_during, 1,
            "a flush across 512 partitions must produce a single run file"
        );
        manager.finish_writers()?;
        Ok(())
    }

    #[test]
    fn test_cleanup() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            &"job1".into(),
            1,
            0,
            schema.clone(),
            Some(CompressionType::LZ4_FRAME),
        )?;

        manager.spill(0, &create_test_batch(&schema, vec![1, 2]))?;

        let spill_dir = manager.spill_dir.clone();
        assert!(spill_dir.exists());

        manager.cleanup()?;
        assert!(!spill_dir.exists());

        Ok(())
    }
}
