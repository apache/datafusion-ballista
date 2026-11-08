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

//! The shuffle stream writer.

use super::offsets::{self, BufferRole};
use super::{
    CONTINUATION_MARKER, OFFSETS_AS_LENGTHS_ZSTD, ShuffleCompression, frame_metadata,
    rebuild_batch_message,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::root_as_message;
use datafusion::arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
    write_message,
};
use datafusion::arrow::record_batch::RecordBatch;
use log::warn;
use std::io::Write;
use std::sync::Arc;

/// zstd level for rewritten bodies, the same as arrow-ipc's default.
const ZSTD_LEVEL: i32 = 3;

/// How [`ShuffleStreamWriter`]s encode the batches of one schema, decided once
/// so that writers opened for many partitions of the same schema share it.
#[derive(Clone, Debug)]
pub struct ShuffleEncoding {
    /// Options for arrow-ipc's own compression, used when offsets stay as is.
    plain: IpcWriteOptions,
    /// The schema's buffer roles, when its batches store offsets as lengths.
    roles: Option<Arc<[BufferRole]>>,
}

impl ShuffleEncoding {
    /// Decides how to encode batches of `schema` under `compression`.
    ///
    /// Offsets are stored as lengths only with zstd, only for schemas with an
    /// offsets buffer and a buffer layout this module maps, and only when
    /// arrow-ipc lays out an empty batch of the schema the way the mapping
    /// says. Otherwise batches use arrow-ipc's own compression.
    pub fn try_new(
        schema: &Schema,
        compression: ShuffleCompression,
    ) -> Result<Self, ArrowError> {
        let plain = IpcWriteOptions::default().try_with_compression(compression.codec)?;
        let roles = if compression.uses_lengths() {
            offsets::buffer_roles(schema)
                .filter(|roles| offsets::has_offsets(roles))
                .filter(|roles| layout_matches(schema, roles))
                .map(Arc::from)
        } else {
            None
        };
        Ok(Self { plain, roles })
    }

    /// Whether batches are written with offsets stored as lengths.
    pub fn uses_lengths(&self) -> bool {
        self.roles.is_some()
    }
}

/// Whether arrow-ipc writes an empty batch of `schema` with the buffers that
/// `roles` describe: one entry of the offset width for each offsets buffer and
/// nothing for the rest. A type's buffer count does not depend on its data, so
/// this guards against arrow-ipc changing its layout.
fn layout_matches(schema: &Schema, roles: &[BufferRole]) -> bool {
    let check = || -> Result<bool, ArrowError> {
        let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        let (_, encoded) = IpcDataGenerator::default().encode(
            &batch,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
            &mut IpcWriteContext::default(),
        )?;
        let message = root_as_message(&encoded.ipc_message)
            .map_err(|e| ArrowError::IpcError(e.to_string()))?;
        let Some(batch) = message.header_as_record_batch() else {
            return Ok(false);
        };
        let ranges = offsets::buffer_ranges(&batch)?;
        Ok(ranges.len() == roles.len()
            && ranges.iter().zip(roles).all(|((_, len), role)| {
                *len == match role {
                    BufferRole::Other => 0,
                    BufferRole::Offsets32 => 4,
                    BufferRole::Offsets64 => 8,
                }
            }))
    };
    let matches = check().unwrap_or(false);
    if !matches {
        warn!(
            "arrow-ipc does not lay out shuffle batches of schema {schema} as expected; \
             writing their offsets as is"
        );
    }
    matches
}

/// Writes an Arrow IPC stream of shuffle data, storing offsets as lengths when
/// its [`ShuffleEncoding`] says so and using arrow-ipc's [`StreamWriter`]
/// otherwise. Read the stream back through [`super::ShuffleIpcReader`] or
/// [`super::ShuffleBytesTranslator`].
pub enum ShuffleStreamWriter<W: Write> {
    /// arrow-ipc's own writer, offsets as is.
    Plain(Box<StreamWriter<W>>),
    /// Offsets stored as lengths, body compressed as one zstd frame.
    Lengths(Box<LengthsWriter<W>>),
}

impl<W: Write> ShuffleStreamWriter<W> {
    /// Starts a stream of `schema` on `writer`, writing its schema message.
    pub fn try_new(
        writer: W,
        schema: &Schema,
        encoding: &ShuffleEncoding,
    ) -> Result<Self, ArrowError> {
        Ok(match &encoding.roles {
            Some(roles) => Self::Lengths(Box::new(LengthsWriter::try_new(
                writer,
                schema,
                Arc::clone(roles),
            )?)),
            None => Self::Plain(Box::new(StreamWriter::try_new_with_options(
                writer,
                schema,
                encoding.plain.clone(),
            )?)),
        })
    }

    /// Writes one record batch.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        match self {
            Self::Plain(writer) => writer.write(batch),
            Self::Lengths(writer) => writer.write(batch),
        }
    }

    /// Writes the end-of-stream marker.
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        match self {
            Self::Plain(writer) => writer.finish(),
            Self::Lengths(writer) => writer.finish(),
        }
    }

    /// Finishes the stream if it is not finished yet and returns the
    /// underlying writer.
    pub fn into_inner(self) -> Result<W, ArrowError> {
        match self {
            Self::Plain(writer) => writer.into_inner(),
            Self::Lengths(writer) => writer.into_inner(),
        }
    }
}

/// The [`ShuffleStreamWriter`] that stores offsets as lengths.
pub struct LengthsWriter<W: Write> {
    writer: W,
    roles: Arc<[BufferRole]>,
    generator: IpcDataGenerator,
    tracker: DictionaryTracker,
    options: IpcWriteOptions,
    context: IpcWriteContext,
    compressor: zstd::bulk::Compressor<'static>,
    finished: bool,
}

impl<W: Write> LengthsWriter<W> {
    fn try_new(
        mut writer: W,
        schema: &Schema,
        roles: Arc<[BufferRole]>,
    ) -> Result<Self, ArrowError> {
        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let options = IpcWriteOptions::default();
        let encoded = generator.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut tracker,
            &options,
        );
        write_message(&mut writer, encoded, &options)?;
        let compressor = zstd::bulk::Compressor::new(ZSTD_LEVEL)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        Ok(Self {
            writer,
            roles,
            generator,
            tracker,
            options,
            context: IpcWriteContext::default(),
            compressor,
            finished: false,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IpcError(
                "Cannot write to a finished shuffle stream".to_string(),
            ));
        }
        let (dictionaries, encoded) = self.generator.encode(
            batch,
            &mut self.tracker,
            &self.options,
            &mut self.context,
        )?;
        if !dictionaries.is_empty() {
            return Err(ArrowError::IpcError(
                "Shuffle stream with offsets as lengths cannot hold dictionaries"
                    .to_string(),
            ));
        }
        let mut body = encoded.arrow_data;
        let message = root_as_message(&encoded.ipc_message)
            .map_err(|e| ArrowError::IpcError(e.to_string()))?;
        let header = message.header_as_record_batch().ok_or_else(|| {
            ArrowError::IpcError("Encoded batch is not a record batch".to_string())
        })?;
        offsets::offsets_to_lengths(
            &mut body,
            &offsets::buffer_ranges(&header)?,
            &self.roles,
        )?;
        let compressed = self
            .compressor
            .compress(&body)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        let unpadded = 16 + compressed.len();
        let body_length = unpadded.next_multiple_of(8);
        let metadata = rebuild_batch_message(
            &message,
            &header,
            Some(OFFSETS_AS_LENGTHS_ZSTD),
            body_length,
        )?;
        self.writer.write_all(&frame_metadata(&metadata))?;
        self.writer.write_all(&(body.len() as u64).to_le_bytes())?;
        self.writer
            .write_all(&(compressed.len() as u64).to_le_bytes())?;
        self.writer.write_all(&compressed)?;
        self.writer.write_all(&[0; 8][..body_length - unpadded])?;
        Ok(())
    }

    /// The underlying writer.
    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    fn finish(&mut self) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IpcError(
                "Cannot finish a finished shuffle stream".to_string(),
            ));
        }
        self.writer.write_all(&CONTINUATION_MARKER)?;
        self.writer.write_all(&0i32.to_le_bytes())?;
        // Like arrow-ipc's `StreamWriter`, so that callers can read the
        // file's length as soon as the stream is finished.
        self.writer.flush()?;
        self.finished = true;
        Ok(())
    }

    fn into_inner(mut self) -> Result<W, ArrowError> {
        if !self.finished {
            self.finish()?;
        }
        Ok(self.writer)
    }
}
