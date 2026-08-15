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
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Upper bound on the read-ahead buffer placed in front of each sub-stream.
/// `StreamReader` does no buffering of its own and issues a separate read for
/// each message's length prefix, metadata and body, so an unbuffered `File`
/// costs several syscalls per record batch — which bites hardest exactly when
/// batches are small, the case a spilling writer produces.
const READ_BUFFER_CAPACITY: usize = 256 * 1024;

/// Reads `RecordBatch`es from `[start_offset, end_offset)` of `data_path`,
/// where the byte range contains zero or more concatenated Arrow IPC streams.
pub(crate) struct MultiStreamPartitionStream {
    data_path: PathBuf,
    schema: SchemaRef,
    end_offset: u64,
    /// Handle kept open across sub-streams. `StreamReader` takes its reader by
    /// value and offers no way to get it back, so each sub-stream needs its own
    /// handle; duplicating this one is cheaper than re-resolving the path, and
    /// a partition's range holds one sub-stream per spill run.
    handle: Option<File>,
    state: State,
}

enum State {
    /// Next sub-stream begins at this absolute byte offset.
    Pending(u64),
    /// Currently draining a sub-stream. The reader may buffer past the
    /// sub-stream's end; `BufReader::stream_position` discounts whatever is
    /// still sitting in the buffer, so the offset it reports is the logical
    /// end of the sub-stream, not the file position the kernel is at.
    Reading(Box<StreamReader<BufReader<File>>>),
    /// Range exhausted or an error has terminated the stream.
    Done,
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
        let state = if start_offset >= end_offset {
            State::Done
        } else {
            State::Pending(start_offset)
        };
        Self {
            data_path,
            schema,
            end_offset,
            handle: None,
            state,
        }
    }

    /// A handle positioned at `offset`, reusing the cached one when present.
    fn open_at(&mut self, offset: u64) -> Result<File, std::io::Error> {
        let base = match self.handle.take() {
            Some(f) => f,
            None => File::open(&self.data_path)?,
        };
        let mut file = base.try_clone()?;
        self.handle = Some(base);
        file.seek(SeekFrom::Start(offset))?;
        Ok(file)
    }

    /// Synchronous core: pulls the next batch, advancing across sub-stream
    /// boundaries as needed. Any error path leaves `state == Done`, so a
    /// repeated poll after an error returns `Ok(None)` rather than retrying
    /// the failed offset.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            match std::mem::replace(&mut self.state, State::Done) {
                State::Done => return Ok(None),
                State::Reading(mut reader) => match reader.next() {
                    Some(Ok(batch)) => {
                        self.state = State::Reading(reader);
                        return Ok(Some(batch));
                    }
                    Some(Err(e)) => return Err(e),
                    None => {
                        let pos = reader.get_mut().stream_position()?;
                        if pos < self.end_offset {
                            self.state = State::Pending(pos);
                        }
                    }
                },
                State::Pending(next) => {
                    if next >= self.end_offset {
                        return Ok(None);
                    }
                    let file = self.open_at(next)?;
                    // Never buffer more than the range still holds: a shuffle
                    // into many partitions leaves each one small, and a fixed
                    // 256 KiB buffer per partition read would dwarf the data.
                    let remaining = (self.end_offset - next) as usize;
                    let buffered = BufReader::with_capacity(
                        remaining.clamp(1, READ_BUFFER_CAPACITY),
                        file,
                    );
                    // Safety: setting `skip_validation` requires `unsafe`; these
                    // bytes were produced by this cluster's own shuffle writer,
                    // the same trust assumption the hash-shuffle reader makes.
                    let reader = unsafe {
                        StreamReader::try_new(buffered, None)?.with_skip_validation(cfg!(
                            feature = "arrow-ipc-optimizations"
                        ))
                    };
                    self.state = State::Reading(Box::new(reader));
                }
            }
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
            Err(e) => {
                Poll::Ready(Some(Err(DataFusionError::ArrowError(Box::new(e), None))))
            }
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
            let mut writer =
                StreamWriter::try_new_with_options(&mut file, schema, opts).unwrap();
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
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
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
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
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
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
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
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            rows.extend(arr.values().iter().copied());
        }
        assert_eq!(rows, vec![10, 20]);
    }
}
