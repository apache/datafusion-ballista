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

//! Arrow IPC streams for shuffle data, with offsets stored as lengths.
//!
//! Arrow's offsets buffers are running sums, which never repeat, so zstd
//! compresses them poorly even when every value has the same length. With
//! zstd, shuffle writers can instead encode each record batch uncompressed,
//! rewrite every offsets buffer of the message body as byte planes of value
//! lengths, and compress the whole body as a single zstd frame. The readers
//! rebuild the offsets before Arrow decodes the batch.
//!
//! # Format
//!
//! The stream keeps Arrow IPC framing: a schema message, record-batch
//! messages and an end-of-stream marker, each with the usual continuation
//! marker and metadata length. A rewritten record-batch message differs from
//! a standard one in three ways:
//!
//! * its `BodyCompression` codec is
//!   [`OFFSETS_AS_LENGTHS_ZSTD`](crate::shuffle_ipc::OFFSETS_AS_LENGTHS_ZSTD), a value the
//!   Arrow format does not define,
//! * its buffer offsets and lengths describe the *uncompressed* body, and
//! * its body is the uncompressed body length and the compressed length as
//!   little-endian `u64`s, then one zstd frame of the rewritten body,
//!   zero-padded to 8 bytes.
//!
//! Readers that do not know the codec fail with arrow-ipc's "compression type
//! not supported" error rather than misreading the data. Schema messages,
//! dictionary messages and record batches of other codecs are untouched, so
//! a stream may mix both kinds and
//! [`ShuffleIpcReader`](crate::shuffle_ipc::ShuffleIpcReader) passes standard
//! messages through as they are.
//!
//! Only schemas that have at least one offsets buffer, and whose types all
//! have a buffer layout this module maps, are rewritten. Views, dictionaries, unions and run-end encoded columns keep
//! arrow-ipc's own compression.

mod offsets;
mod reader;
mod writer;

pub use reader::{ShuffleBytesTranslator, ShuffleIpcReader};
pub use writer::{ShuffleEncoding, ShuffleStreamWriter};

use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::{
    self, BodyCompression, BodyCompressionArgs, BodyCompressionMethod, CompressionType,
    KeyValue, KeyValueArgs, Message, MessageArgs, MessageHeader, RecordBatchArgs,
};
use flatbuffers::FlatBufferBuilder;

/// The `BodyCompression` codec of a record-batch message whose body is one
/// zstd frame of the rewritten body. Arrow defines only `LZ4_FRAME` (0) and
/// `ZSTD` (1).
pub const OFFSETS_AS_LENGTHS_ZSTD: CompressionType = CompressionType(0x40);

/// Arrow IPC's continuation marker, which precedes a message's metadata length.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// How shuffle writers compress the Arrow IPC streams they write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShuffleCompression {
    /// The codec, or `None` for uncompressed streams.
    pub codec: Option<CompressionType>,
    /// Whether to store offsets as lengths in byte planes before compressing.
    /// Only takes effect with zstd.
    pub offsets_as_lengths: bool,
}

impl ShuffleCompression {
    /// Plain arrow-ipc compression with `codec`, without offsets as lengths.
    pub fn plain(codec: Option<CompressionType>) -> Self {
        Self {
            codec,
            offsets_as_lengths: false,
        }
    }

    /// Whether streams written with this setting rewrite offsets as lengths
    /// for schemas that support it.
    fn uses_lengths(&self) -> bool {
        self.offsets_as_lengths && self.codec == Some(CompressionType::ZSTD)
    }
}

/// Builds the metadata of a record-batch message that copies everything from
/// `message` and `batch` except the body compression and the body length.
fn rebuild_batch_message(
    message: &Message<'_>,
    batch: &ipc::RecordBatch<'_>,
    codec: Option<CompressionType>,
    body_length: usize,
) -> Result<Vec<u8>, ArrowError> {
    let mut fbb = FlatBufferBuilder::new();
    let nodes = batch
        .nodes()
        .map(|nodes| fbb.create_vector(&nodes.iter().copied().collect::<Vec<_>>()));
    let buffers = batch
        .buffers()
        .map(|buffers| fbb.create_vector(&buffers.iter().copied().collect::<Vec<_>>()));
    let variadic = batch
        .variadicBufferCounts()
        .map(|counts| fbb.create_vector(&counts.iter().collect::<Vec<_>>()));
    let compression = codec.map(|codec| {
        BodyCompression::create(
            &mut fbb,
            &BodyCompressionArgs {
                codec,
                method: BodyCompressionMethod::BUFFER,
            },
        )
    });
    let header = ipc::RecordBatch::create(
        &mut fbb,
        &RecordBatchArgs {
            length: batch.length(),
            nodes,
            buffers,
            compression,
            variadicBufferCounts: variadic,
        },
    );
    let custom_metadata = message.custom_metadata().map(|entries| {
        let entries: Vec<_> = entries
            .iter()
            .map(|entry| {
                let key = entry.key().map(|key| fbb.create_string(key));
                let value = entry.value().map(|value| fbb.create_string(value));
                KeyValue::create(&mut fbb, &KeyValueArgs { key, value })
            })
            .collect();
        fbb.create_vector(&entries)
    });
    let body_length = i64::try_from(body_length).map_err(|_| {
        ArrowError::IpcError(format!("Shuffle message body of {body_length} bytes"))
    })?;
    let root = Message::create(
        &mut fbb,
        &MessageArgs {
            version: message.version(),
            header_type: MessageHeader::RecordBatch,
            header: Some(header.as_union_value()),
            bodyLength: body_length,
            custom_metadata,
        },
    );
    fbb.finish(root, None);
    Ok(fbb.finished_data().to_vec())
}

/// The continuation marker, the metadata length and the metadata of a
/// message, zero-padded so that the body that follows starts on an 8-byte
/// boundary.
fn frame_metadata(metadata: &[u8]) -> Vec<u8> {
    let padded = (metadata.len() + 8).next_multiple_of(8) - 8;
    let mut framed = Vec::with_capacity(8 + padded);
    framed.extend_from_slice(&CONTINUATION_MARKER);
    framed.extend_from_slice(&(padded as i32).to_le_bytes());
    framed.extend_from_slice(metadata);
    framed.resize(8 + padded, 0);
    framed
}

#[cfg(test)]
mod tests;
