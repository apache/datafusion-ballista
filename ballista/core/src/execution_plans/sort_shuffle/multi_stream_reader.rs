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

use crate::shuffle_ipc::ShuffleIpcReader;
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
    state: State,
}

enum State {
    /// Next sub-stream begins at this absolute byte offset.
    Pending(u64),
    /// Currently draining a sub-stream.
    Reading(Box<StreamReader<ShuffleIpcReader<File>>>),
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
            state,
        }
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
                        // The adapter never reads past the end-of-stream
                        // marker, so the file sits at the next stream.
                        let pos = reader.get_mut().get_mut().stream_position()?;
                        if pos < self.end_offset {
                            self.state = State::Pending(pos);
                        }
                    }
                },
                State::Pending(next) => {
                    if next >= self.end_offset {
                        return Ok(None);
                    }
                    let mut file = File::open(&self.data_path)?;
                    file.seek(SeekFrom::Start(next))?;
                    // Safety: setting `skip_validation` requires `unsafe`, user assures data is valid
                    let reader = unsafe {
                        StreamReader::try_new(ShuffleIpcReader::new(file), None)?
                            .with_skip_validation(cfg!(
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
    use crate::execution_plans::test_util::{
        int32_batch, int32_schema, ipc_stream_bytes,
    };
    use datafusion::arrow::array::Int32Array;
    use futures::StreamExt;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::TempDir;

    /// Appends an IPC stream containing `batches` to `path`, creating the file
    /// if needed. Returns the number of bytes written.
    fn append_stream(path: &std::path::Path, batches: &[RecordBatch]) -> u64 {
        let bytes = ipc_stream_bytes(batches);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        file.write_all(&bytes).unwrap();
        bytes.len() as u64
    }

    #[tokio::test]
    async fn yields_batches_from_single_stream() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.arrow");
        let s = int32_schema();
        let len = append_stream(&path, &[int32_batch(&[1, 2]), int32_batch(&[3])]);

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
        let s = int32_schema();
        let mut total = 0;
        total += append_stream(&path, &[int32_batch(&[1, 2])]);
        total += append_stream(&path, &[int32_batch(&[3]), int32_batch(&[4, 5])]);

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
        let s = int32_schema();

        let mut stream = MultiStreamPartitionStream::new(path, s.clone(), 0, 0);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn respects_end_offset_when_more_bytes_follow() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("data.arrow");
        let s = int32_schema();
        let len_first = append_stream(&path, &[int32_batch(&[1, 2])]);
        // Append a second stream whose bytes must NOT be read.
        append_stream(&path, &[int32_batch(&[99])]);

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
        let s = int32_schema();
        // Two streams in sequence; the test reads only the second.
        let len_first = append_stream(&path, &[int32_batch(&[1, 2, 3])]);
        let len_second = append_stream(&path, &[int32_batch(&[10, 20])]);

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
