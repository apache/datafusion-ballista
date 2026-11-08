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

//! Turns shuffle streams back into standard Arrow IPC streams.
//!
//! Both readers here parse message framing only. Every rewritten record-batch
//! message comes out as a standard uncompressed one, and every other message
//! comes out byte for byte, so arrow-ipc's `StreamReader` and `StreamDecoder`
//! decode the result.

use super::offsets::{self, BufferRole};
use super::{
    CONTINUATION_MARKER, OFFSETS_AS_LENGTHS_ZSTD, frame_metadata, rebuild_batch_message,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::{MessageHeader, convert::fb_to_schema, root_as_message};
use prost::bytes::{Buf, Bytes, BytesMut};
use std::io::{self, Read};

/// What the message after a prefix and its metadata turns into.
enum Translated {
    /// Keep the prefix and metadata, then `body` bytes as they are.
    Passthrough { body: u64 },
    /// Read `body` bytes, then replace the whole message with
    /// [`MessageTranslator::translate_body`]'s output.
    Rewrite { body: usize },
}

/// The state both readers share: the buffer roles of the current stream's
/// schema and a zstd decompression context.
#[derive(Default)]
struct MessageTranslator {
    /// Roles of the most recent schema message, `None` when its layout is not
    /// mapped or no schema has been seen.
    roles: Option<Vec<BufferRole>>,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

impl MessageTranslator {
    /// Inspects a message's metadata. A schema message sets the roles that
    /// later record batches are rebuilt with.
    fn inspect(&mut self, metadata: &[u8]) -> Result<Translated, ArrowError> {
        let message = root_as_message(metadata).map_err(|e| {
            ArrowError::ParseError(format!("Unable to get root as message: {e:?}"))
        })?;
        let body = u64::try_from(message.bodyLength()).map_err(|_| {
            ArrowError::IpcError(format!(
                "Shuffle message with body length {}",
                message.bodyLength()
            ))
        })?;
        match message.header_type() {
            MessageHeader::Schema => {
                let schema = message.header_as_schema().ok_or_else(|| {
                    ArrowError::IpcError(
                        "Unable to read IPC message as schema".to_string(),
                    )
                })?;
                self.roles = offsets::buffer_roles(&fb_to_schema(schema));
            }
            MessageHeader::RecordBatch => {
                let rewritten = message
                    .header_as_record_batch()
                    .and_then(|batch| batch.compression())
                    .is_some_and(|c| c.codec() == OFFSETS_AS_LENGTHS_ZSTD);
                if rewritten {
                    let body = usize::try_from(body).map_err(|_| {
                        ArrowError::IpcError(format!(
                            "Shuffle message body of {body} bytes"
                        ))
                    })?;
                    return Ok(Translated::Rewrite { body });
                }
            }
            _ => {}
        }
        Ok(Translated::Passthrough { body })
    }

    /// Rebuilds a rewritten record batch from its metadata and body as the
    /// framed metadata and body of a standard uncompressed message.
    fn translate_body(
        &mut self,
        metadata: &[u8],
        body: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
        let message = root_as_message(metadata).map_err(|e| {
            ArrowError::ParseError(format!("Unable to get root as message: {e:?}"))
        })?;
        let batch = message.header_as_record_batch().ok_or_else(|| {
            ArrowError::IpcError("Unable to read IPC message as record batch".to_string())
        })?;
        let roles = self.roles.as_deref().ok_or_else(|| {
            ArrowError::IpcError(
                "Shuffle batch stores offsets as lengths but its schema has no mapped layout"
                    .to_string(),
            )
        })?;
        let malformed = || {
            ArrowError::IpcError(format!(
                "Malformed shuffle batch body of {} bytes",
                body.len()
            ))
        };
        let (header, rest) = body.split_first_chunk::<16>().ok_or_else(malformed)?;
        let field = |bytes: &[u8]| {
            usize::try_from(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
                .map_err(|_| malformed())
        };
        let len = field(&header[..8])?;
        let frame = rest.get(..field(&header[8..])?).ok_or_else(malformed)?;
        // The writer pads every buffer to 64 bytes, so the body ends less than
        // 64 bytes after its last buffer. Checking that bounds the allocation
        // a corrupt length could ask for.
        let ranges = offsets::buffer_ranges(&batch)?;
        let end = ranges
            .iter()
            .map(|(offset, len)| offset.saturating_add(*len))
            .max()
            .unwrap_or(0);
        if len < end || len - end >= 64 {
            return Err(ArrowError::IpcError(format!(
                "Shuffle batch body of {len} bytes does not fit buffers ending at {end}"
            )));
        }
        let decompressor = match &mut self.decompressor {
            Some(decompressor) => decompressor,
            None => self.decompressor.insert(
                zstd::bulk::Decompressor::new()
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?,
            ),
        };
        let mut decoded = decompressor
            .decompress(frame, len)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        if decoded.len() != len {
            return Err(ArrowError::IpcError(format!(
                "Shuffle batch body decompressed to {} bytes where {len} were expected",
                decoded.len()
            )));
        }
        offsets::lengths_to_offsets(&mut decoded, &ranges, roles)?;
        let metadata = rebuild_batch_message(&message, &batch, None, decoded.len())?;
        Ok((frame_metadata(&metadata), decoded))
    }
}

/// Splits a message prefix into its length and the metadata length it gives,
/// or `None` while fewer than `prefix` bytes are known. Accepts both the
/// continuation-marker framing and the legacy one without it.
fn parse_prefix(bytes: &[u8]) -> Option<Result<(usize, usize), ArrowError>> {
    let first: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
    let (prefix, len) = if first == CONTINUATION_MARKER {
        (8, i32::from_le_bytes(bytes.get(4..8)?.try_into().ok()?))
    } else {
        (4, i32::from_le_bytes(first))
    };
    Some(usize::try_from(len).map(|len| (prefix, len)).map_err(|_| {
        ArrowError::IpcError(format!("Shuffle message metadata length {len}"))
    }))
}

/// A [`Read`] adapter that turns a shuffle stream into a standard Arrow IPC
/// stream for arrow-ipc's `StreamReader`.
///
/// It reads its input one message at a time and never past the message being
/// returned, so once a `StreamReader` over it reaches the end-of-stream
/// marker, the input is positioned just past that marker.
pub struct ShuffleIpcReader<R: Read> {
    inner: R,
    translator: MessageTranslator,
    /// Translated bytes not yet returned: framed metadata, then a body.
    pending: [Vec<u8>; 2],
    /// Position within `pending`.
    pending_index: usize,
    pending_offset: usize,
    /// Body bytes of a passed-through message still to copy from `inner`.
    passthrough: u64,
}

impl<R: Read> ShuffleIpcReader<R> {
    /// Wraps `inner`, which starts at the beginning of a message.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            translator: MessageTranslator::default(),
            pending: [Vec::new(), Vec::new()],
            pending_index: 2,
            pending_offset: 0,
            passthrough: 0,
        }
    }

    /// The wrapped reader.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// The wrapped reader, mutably. Reading from it directly desynchronizes
    /// this adapter.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Returns the wrapped reader.
    pub fn into_inner(self) -> R {
        self.inner
    }

    fn set_pending(&mut self, header: Vec<u8>, body: Vec<u8>) {
        self.pending = [header, body];
        self.pending_index = 0;
        self.pending_offset = 0;
    }

    /// Reads the next message's prefix and metadata from `inner` and queues
    /// what it becomes. Returns `false` at the end of the input.
    fn next_message(&mut self) -> io::Result<bool> {
        let mut prefix = [0u8; 8];
        let read = read_full(&mut self.inner, &mut prefix[..4])?;
        if read == 0 {
            return Ok(false);
        }
        if read < 4 {
            return Err(truncated(io::ErrorKind::UnexpectedEof.into()));
        }
        let mut prefix_len = 4;
        if prefix[..4] == CONTINUATION_MARKER {
            self.inner.read_exact(&mut prefix[4..]).map_err(truncated)?;
            prefix_len = 8;
        }
        let (_, metadata_len) = parse_prefix(&prefix[..prefix_len])
            .expect("prefix is complete")
            .map_err(io::Error::other)?;
        let mut header = Vec::with_capacity(prefix_len + metadata_len);
        header.extend_from_slice(&prefix[..prefix_len]);
        if metadata_len == 0 {
            // End-of-stream marker.
            self.set_pending(header, Vec::new());
            return Ok(true);
        }
        header.resize(prefix_len + metadata_len, 0);
        self.inner
            .read_exact(&mut header[prefix_len..])
            .map_err(truncated)?;
        match self
            .translator
            .inspect(&header[prefix_len..])
            .map_err(io::Error::other)?
        {
            Translated::Passthrough { body } => {
                self.set_pending(header, Vec::new());
                self.passthrough = body;
            }
            Translated::Rewrite { body } => {
                let mut bytes = vec![0; body];
                self.inner.read_exact(&mut bytes).map_err(truncated)?;
                let (header, body) = self
                    .translator
                    .translate_body(&header[prefix_len..], &bytes)
                    .map_err(io::Error::other)?;
                self.set_pending(header, body);
            }
        }
        Ok(true)
    }
}

/// Reports input that ends inside a message as invalid data. arrow-ipc's
/// `StreamReader` takes an `UnexpectedEof` while it reads a message prefix
/// for a clean end of stream, and this adapter reads whole messages while
/// `StreamReader` asks for their prefix, so passing the kind through would
/// drop a truncated message silently.
fn truncated(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Shuffle stream ends inside a message",
        )
    } else {
        error
    }
}

/// Reads until `buf` is full or the input ends, returning the bytes read.
fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut read = 0;
    while read < buf.len() {
        match reader.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(read)
}

impl<R: Read> Read for ShuffleIpcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            while self.pending_index < 2 {
                let pending = &self.pending[self.pending_index][self.pending_offset..];
                if pending.is_empty() {
                    self.pending_index += 1;
                    self.pending_offset = 0;
                    continue;
                }
                let n = pending.len().min(buf.len());
                buf[..n].copy_from_slice(&pending[..n]);
                self.pending_offset += n;
                return Ok(n);
            }
            if self.passthrough > 0 {
                let limit = usize::try_from(self.passthrough)
                    .unwrap_or(usize::MAX)
                    .min(buf.len());
                let n = self.inner.read(&mut buf[..limit])?;
                if n == 0 {
                    return Err(truncated(io::ErrorKind::UnexpectedEof.into()));
                }
                self.passthrough -= n as u64;
                return Ok(n);
            }
            if !self.next_message()? {
                return Ok(0);
            }
        }
    }
}

/// The push counterpart of [`ShuffleIpcReader`], for shuffle bytes that
/// arrive in blocks, such as over block transport, on their way to
/// arrow-ipc's `StreamDecoder`.
///
/// Bytes of passed-through messages come out as slices of the input blocks,
/// without copying. A rewritten record batch is held back until its whole
/// body has arrived.
#[derive(Default)]
pub struct ShuffleBytesTranslator {
    translator: MessageTranslator,
    /// Input not yet translated: the start of a message.
    pending: BytesMut,
    /// How many bytes `pending` must hold before it is worth parsing again.
    needed: usize,
    /// Body bytes of a passed-through message still to come.
    passthrough: u64,
}

impl ShuffleBytesTranslator {
    /// Creates a translator positioned at the start of a stream.
    pub fn new() -> Self {
        Self::default()
    }

    /// Translates one more block of input, appending the translated bytes to
    /// `out` as zero or more blocks.
    pub fn push(&mut self, block: Bytes, out: &mut Vec<Bytes>) -> Result<(), ArrowError> {
        let mut input = if self.pending.is_empty() {
            block
        } else {
            self.pending.extend_from_slice(&block);
            if self.pending.len() < self.needed {
                return Ok(());
            }
            self.pending.split().freeze()
        };
        loop {
            if self.passthrough > 0 {
                let n = usize::try_from(self.passthrough)
                    .unwrap_or(usize::MAX)
                    .min(input.len());
                if n > 0 {
                    out.push(input.split_to(n));
                    self.passthrough -= n as u64;
                }
                if self.passthrough > 0 {
                    return Ok(());
                }
            }
            if input.is_empty() {
                return Ok(());
            }
            let Some(prefix) = parse_prefix(&input) else {
                self.hold(&input, 8);
                return Ok(());
            };
            let (prefix, metadata_len) = prefix?;
            if metadata_len == 0 {
                out.push(input.split_to(prefix));
                continue;
            }
            let header_len = prefix + metadata_len;
            if input.len() < header_len {
                self.hold(&input, header_len);
                return Ok(());
            }
            match self.translator.inspect(&input[prefix..header_len])? {
                Translated::Passthrough { body } => {
                    out.push(input.split_to(header_len));
                    self.passthrough = body;
                }
                Translated::Rewrite { body: body_len } => {
                    if input.len() - header_len < body_len {
                        self.hold(&input, header_len + body_len);
                        return Ok(());
                    }
                    let (header, body) = self.translator.translate_body(
                        &input[prefix..header_len],
                        &input[header_len..header_len + body_len],
                    )?;
                    input.advance(header_len + body_len);
                    out.push(Bytes::from(header));
                    out.push(Bytes::from(body));
                }
            }
        }
    }

    /// Keeps the start of a message that is not complete yet, until `needed`
    /// bytes of it have arrived.
    fn hold(&mut self, input: &[u8], needed: usize) {
        self.pending.extend_from_slice(input);
        self.needed = needed;
    }

    /// Whether every byte pushed so far has been translated, which holds
    /// between messages.
    pub fn is_idle(&self) -> bool {
        self.pending.is_empty() && self.passthrough == 0
    }
}

#[cfg(test)]
pub(super) struct MessageTranslatorForTests;

#[cfg(test)]
impl MessageTranslatorForTests {
    /// Translates the first record batch of `bytes` without having seen its
    /// stream's schema.
    pub(super) fn translate_first_batch_without_roles(
        bytes: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
        let mut translator = MessageTranslator::default();
        let mut position = 0;
        loop {
            let (prefix, len) = parse_prefix(&bytes[position..]).unwrap()?;
            let metadata = &bytes[position + prefix..position + prefix + len];
            let message = root_as_message(metadata).unwrap();
            let body = message.bodyLength() as usize;
            let start = position + prefix + len;
            if message.header_type() == MessageHeader::RecordBatch {
                return translator.translate_body(metadata, &bytes[start..start + body]);
            }
            position = start + body;
        }
    }
}
