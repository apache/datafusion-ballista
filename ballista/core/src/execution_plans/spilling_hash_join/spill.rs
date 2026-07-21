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

// Per-bucket spill files for `SpillingHashJoinExec`. A `JoinSpillWriter` owns
// one Arrow-IPC stream file per sub-partition bucket, created lazily on the
// first `append` for that bucket and appended to on every subsequent one. The
// join uses two independent writers per task: one for build-side batches (build
// schema) and one for probe-side batches (probe schema). Only the batches are
// persisted — per-row hashes are recomputed on read-back from the same fixed
// seed used at partition time, so they never need to be stored.
//
// The IPC read/write pattern mirrors `sort_shuffle/spill.rs`. Files are backed
// by `DiskManager`-managed temp files (`RefCountedTempFile`), so they are
// cleaned up when the writer is dropped at the end of the task.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::Result;
use datafusion::execution::disk_manager::{DiskManager, RefCountedTempFile};

/// Writes build- or probe-side batches to per-bucket Arrow-IPC spill files.
///
/// One file is created per bucket on demand (the first `append` for that
/// bucket) and appended to thereafter. All batches written through a given
/// writer share `schema`. The temp files are owned by the writer via
/// `RefCountedTempFile`, so they outlive individual `File` handles and are
/// removed when the writer is dropped.
pub struct JoinSpillWriter {
    schema: SchemaRef,
    disk_manager: Arc<DiskManager>,
    files: HashMap<usize, (RefCountedTempFile, StreamWriter<BufWriter<File>>)>,
}

impl JoinSpillWriter {
    /// Creates a writer that will persist batches with `schema` to temp files
    /// obtained from `disk_manager`.
    pub fn new(schema: SchemaRef, disk_manager: Arc<DiskManager>) -> Self {
        Self {
            schema,
            disk_manager,
            files: HashMap::new(),
        }
    }

    /// Appends `batch` to `bucket`'s spill file, creating the file on the first
    /// call for that bucket. Empty batches are ignored so an all-empty bucket
    /// never creates a file.
    pub fn append(&mut self, bucket: usize, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if !self.files.contains_key(&bucket) {
            let temp = self
                .disk_manager
                .create_tmp_file("SpillingHashJoin spill")?;
            let file = File::create(temp.path())?;
            let writer = StreamWriter::try_new(BufWriter::new(file), &self.schema)?;
            self.files.insert(bucket, (temp, writer));
        }

        let (_, writer) = self.files.get_mut(&bucket).unwrap();
        writer.write(batch)?;
        Ok(())
    }

    /// Flushes and finalizes every open writer so the spill files can be read.
    /// Must be called before `reader`.
    pub fn finish(&mut self) -> Result<()> {
        for (_, writer) in self.files.values_mut() {
            writer.finish()?;
        }
        Ok(())
    }

    /// Opens a streaming reader over `bucket`'s spill file, or `None` if
    /// nothing was ever spilled for that bucket. `finish` must be called first.
    pub fn reader(&self, bucket: usize) -> Result<Option<JoinSpillReader>> {
        match self.files.get(&bucket) {
            Some((temp, _)) => {
                let file = File::open(temp.path())?;
                let inner = StreamReader::try_new(BufReader::new(file), None)?;
                Ok(Some(JoinSpillReader { inner }))
            }
            None => Ok(None),
        }
    }
}

/// A streaming reader over one bucket's spill file, yielding the batches back
/// in the order they were appended.
pub struct JoinSpillReader {
    inner: StreamReader<BufReader<File>>,
}

impl Iterator for JoinSpillReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| r.map_err(Into::into))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::disk_manager::DiskManagerBuilder;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int32Array::from(values))])
            .unwrap()
    }

    #[test]
    fn append_finish_read_roundtrip_per_bucket() {
        let disk = Arc::new(DiskManagerBuilder::default().build().unwrap());
        let schema = schema();
        let mut writer = JoinSpillWriter::new(Arc::clone(&schema), disk);

        // Bucket 0 gets two batches; bucket 3 gets one; bucket 1 stays empty.
        writer.append(0, &batch(&schema, vec![1, 2, 3])).unwrap();
        writer.append(0, &batch(&schema, vec![4, 5])).unwrap();
        writer.append(3, &batch(&schema, vec![6])).unwrap();
        // Empty batch must not create a file.
        writer.append(2, &batch(&schema, vec![])).unwrap();

        writer.finish().unwrap();

        let b0: Vec<RecordBatch> = writer
            .reader(0)
            .unwrap()
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(b0.len(), 2);
        assert_eq!(b0[0].num_rows(), 3);
        assert_eq!(b0[1].num_rows(), 2);

        let b3: Vec<RecordBatch> = writer
            .reader(3)
            .unwrap()
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(b3.len(), 1);
        assert_eq!(b3[0].num_rows(), 1);

        // Buckets that never received a non-empty batch have no reader.
        assert!(writer.reader(1).unwrap().is_none());
        assert!(writer.reader(2).unwrap().is_none());
    }
}
