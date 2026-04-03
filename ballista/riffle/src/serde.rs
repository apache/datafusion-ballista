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

//! Arrow IPC serialization helpers for Riffle shuffle data.
//!
//! Riffle treats shuffle data as opaque bytes. We use Arrow IPC format
//! to serialize RecordBatches before pushing to Riffle, and deserialize
//! when reading back.

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;

use crate::error::Result;

/// Serializes record batches to Arrow IPC bytes with LZ4 compression.
pub fn record_batches_to_ipc_bytes(
    batches: &[RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let options =
        IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?;
    let mut writer = StreamWriter::try_new_with_options(&mut buf, schema, options)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buf)
}

/// Deserializes Arrow IPC bytes back to record batches.
pub fn ipc_bytes_to_record_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)?;
    let batches: std::result::Result<Vec<_>, _> = reader.collect();
    Ok(batches?)
}

/// Deserializes multi-block shuffle data from Riffle into record batches.
///
/// When multiple mappers write to the same partition, Riffle stores each
/// mapper's data as a separate block. The concatenated data contains multiple
/// independent IPC streams — one per block. This function uses the segment
/// metadata to split the data and deserialize each block independently.
///
/// If no segments are provided (e.g., single-mapper case), falls back to
/// treating the entire blob as one IPC stream.
pub fn shuffle_read_to_record_batches(
    data: &[u8],
    segments: &[crate::client::BlockSegment],
) -> Result<Vec<RecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    // If no segments, treat the whole blob as a single IPC stream
    if segments.is_empty() {
        return ipc_bytes_to_record_batches(data);
    }

    let mut all_batches = Vec::new();
    for segment in segments {
        let start = segment.offset as usize;
        let end = start + segment.length as usize;
        if end > data.len() {
            return Err(crate::error::RiffleError::General(format!(
                "Block segment out of bounds: offset={}, length={}, data_len={}",
                segment.offset, segment.length, data.len()
            )));
        }
        let block_bytes = &data[start..end];
        let batches = ipc_bytes_to_record_batches(block_bytes)?;
        all_batches.extend(batches);
    }
    Ok(all_batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::BlockSegment;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn test_batch(schema: &arrow::datatypes::SchemaRef, values: Vec<i32>) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_roundtrip_single_batch() {
        let schema = test_schema();
        let batch = test_batch(&schema, vec![1, 2, 3, 4, 5]);

        let bytes = record_batches_to_ipc_bytes(&[batch.clone()], &schema).unwrap();
        let result = ipc_bytes_to_record_batches(&bytes).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], batch);
    }

    #[test]
    fn test_roundtrip_multiple_batches() {
        let schema = test_schema();
        let batch1 = test_batch(&schema, vec![1, 2, 3]);
        let batch2 = test_batch(&schema, vec![4, 5]);

        let bytes =
            record_batches_to_ipc_bytes(&[batch1.clone(), batch2.clone()], &schema).unwrap();
        let result = ipc_bytes_to_record_batches(&bytes).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], batch1);
        assert_eq!(result[1], batch2);
    }

    #[test]
    fn test_roundtrip_empty() {
        let schema = test_schema();

        let bytes = record_batches_to_ipc_bytes(&[], &schema).unwrap();
        let result = ipc_bytes_to_record_batches(&bytes).unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_multi_block_deserialization() {
        let schema = test_schema();

        // Simulate two mappers writing to the same partition
        let mapper0_batches = vec![test_batch(&schema, vec![1, 2, 3])];
        let mapper1_batches = vec![test_batch(&schema, vec![4, 5])];

        let block0 = record_batches_to_ipc_bytes(&mapper0_batches, &schema).unwrap();
        let block1 = record_batches_to_ipc_bytes(&mapper1_batches, &schema).unwrap();

        // Concatenate like Riffle would
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(&block0);
        concatenated.extend_from_slice(&block1);

        // Create segments like Riffle returns
        let segments = vec![
            BlockSegment {
                block_id: 0,
                offset: 0,
                length: block0.len() as i32,
                uncompress_length: block0.len() as i32,
                crc: 0,
                task_attempt_id: 0,
            },
            BlockSegment {
                block_id: 1,
                offset: block0.len() as i64,
                length: block1.len() as i32,
                uncompress_length: block1.len() as i32,
                crc: 0,
                task_attempt_id: 1,
            },
        ];

        let result = shuffle_read_to_record_batches(&concatenated, &segments).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 3); // mapper 0
        assert_eq!(result[1].num_rows(), 2); // mapper 1

        // Verify actual values
        let col0 = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col0.values(), &[1, 2, 3]);

        let col1 = result[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col1.values(), &[4, 5]);
    }

    #[test]
    fn test_multi_block_empty_segments_fallback() {
        let schema = test_schema();
        let batches = vec![test_batch(&schema, vec![1, 2, 3])];

        let bytes = record_batches_to_ipc_bytes(&batches, &schema).unwrap();

        // No segments → fall back to single IPC stream
        let result = shuffle_read_to_record_batches(&bytes, &[]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }
}
