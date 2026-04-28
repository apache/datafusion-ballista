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

//! Reader that yields `RecordBatch`es from a byte range of a sort-shuffle
//! data file, transparently spanning multiple concatenated Arrow IPC streams.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Reads `RecordBatch`es from `[start_offset, end_offset)` of `data_path`,
/// where the byte range contains zero or more concatenated Arrow IPC streams.
pub(crate) struct MultiStreamPartitionStream {
    data_path: PathBuf,
    schema: SchemaRef,
    end_offset: u64,
    /// Active sub-stream reader, or `None` between sub-streams.
    reader: Option<StreamReader<File>>,
    /// Absolute byte position to seek to for the next sub-stream. `None`
    /// while `reader` is `Some` (the position is held inside the reader).
    next_offset: Option<u64>,
    finished: bool,
}

impl MultiStreamPartitionStream {
    /// Creates a new bounded multi-stream reader. `schema` is the schema of
    /// the partition data; the caller must obtain it from the data file's
    /// leading schema-header stream.
    pub(crate) fn new(
        data_path: PathBuf,
        schema: SchemaRef,
        start_offset: u64,
        end_offset: u64,
    ) -> Self {
        let finished = start_offset >= end_offset;
        Self {
            data_path,
            schema,
            end_offset,
            reader: None,
            next_offset: Some(start_offset),
            finished,
        }
    }

    /// Synchronous core: pulls the next batch, advancing across sub-stream
    /// boundaries as needed.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            if self.finished {
                return Ok(None);
            }

            if let Some(reader) = self.reader.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => return Ok(Some(batch)),
                    Some(Err(e)) => {
                        self.finished = true;
                        return Err(e);
                    }
                    None => {
                        // EOS for this sub-stream: capture position and drop
                        // the reader so we can construct the next one.
                        let pos = reader
                            .get_mut()
                            .stream_position()
                            .map_err(|e| ArrowError::IoError(format!("{e}"), e))?;
                        self.next_offset = Some(pos);
                        self.reader = None;
                    }
                }
            }

            let next = self
                .next_offset
                .expect("invariant: next_offset is Some when reader is None");
            if next >= self.end_offset {
                self.finished = true;
                return Ok(None);
            }

            self.finished = true;
            let mut file = File::open(&self.data_path)
                .map_err(|e| ArrowError::IoError(format!("{e}"), e))?;
            file.seek(SeekFrom::Start(next))
                .map_err(|e| ArrowError::IoError(format!("{e}"), e))?;
            let reader = StreamReader::try_new(file, None)?;
            self.reader = Some(reader);
            self.next_offset = None;
            self.finished = false;
        }
    }
}

impl Stream for MultiStreamPartitionStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.next_batch() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(DataFusionError::ArrowError(
                Box::new(e),
                None,
            )))),
        }
    }
}

impl RecordBatchStream for MultiStreamPartitionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
    use futures::StreamExt;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])
            .unwrap()
    }

    /// Writes an IPC stream containing `batches` to `path`, appending if
    /// the file already exists. Returns the number of bytes written.
    fn append_stream(
        path: &std::path::Path,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> u64 {
        // Determine the current file size before opening in append mode.
        // We cannot rely on stream_position() after open with O_APPEND because
        // on some platforms the fd starts at position 0 even when the file
        // already has content, so `end - start` would count from 0 rather than
        // from the true start of this append.
        let start = if path.exists() {
            std::fs::metadata(path).unwrap().len()
        } else {
            0
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        let opts = IpcWriteOptions::default();
        {
            let mut writer = StreamWriter::try_new_with_options(&mut file, schema, opts)
                .unwrap();
            for b in batches {
                writer.write(b).unwrap();
            }
            writer.finish().unwrap();
        }
        file.flush().unwrap();
        let end = std::fs::metadata(path).unwrap().len();
        end - start
    }

    #[tokio::test]
    async fn yields_batches_from_single_stream() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.arrow");
        let s = schema();
        let len = append_stream(&path, &s, &[batch(&s, vec![1, 2]), batch(&s, vec![3])]);

        let mut stream = MultiStreamPartitionStream::new(path, s.clone(), 0, len);
        let mut rows = vec![];
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend(arr.values().iter().copied());
        }
        assert_eq!(rows, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn yields_batches_across_two_concatenated_streams() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.arrow");
        let s = schema();
        let mut total = 0;
        total += append_stream(&path, &s, &[batch(&s, vec![1, 2])]);
        total += append_stream(&path, &s, &[batch(&s, vec![3]), batch(&s, vec![4, 5])]);

        let mut stream = MultiStreamPartitionStream::new(path, s.clone(), 0, total);
        let mut rows = vec![];
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend(arr.values().iter().copied());
        }
        assert_eq!(rows, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn empty_range_returns_no_batches() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.arrow");
        // Touch the file so it exists but is empty.
        std::fs::write(&path, b"").unwrap();
        let s = schema();

        let mut stream = MultiStreamPartitionStream::new(path, s.clone(), 0, 0);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn respects_end_offset_when_more_bytes_follow() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.arrow");
        let s = schema();
        let len_first = append_stream(&path, &s, &[batch(&s, vec![1, 2])]);
        // Append a second stream whose bytes must NOT be read.
        append_stream(&path, &s, &[batch(&s, vec![99])]);

        let mut stream = MultiStreamPartitionStream::new(path, s.clone(), 0, len_first);
        let mut rows = vec![];
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend(arr.values().iter().copied());
        }
        assert_eq!(rows, vec![1, 2]);
    }

    #[tokio::test]
    async fn starts_at_non_zero_offset() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.arrow");
        let s = schema();
        // Two streams in sequence; the test reads only the second.
        let len_first = append_stream(&path, &s, &[batch(&s, vec![1, 2, 3])]);
        let len_second = append_stream(&path, &s, &[batch(&s, vec![10, 20])]);

        let mut stream = MultiStreamPartitionStream::new(
            path,
            s.clone(),
            len_first,
            len_first + len_second,
        );
        let mut rows = vec![];
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend(arr.values().iter().copied());
        }
        assert_eq!(rows, vec![10, 20]);
    }
}
