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

//! Arrow IPC **file** format write and open for the range shuffle.
//!
//! The passthrough shuffle writes the IPC **stream** format, which has no
//! index: finding the byte range that holds a given value means walking every
//! message from the head. The file format ends with a footer listing one
//! `Block { offset, metadata_length, body_length }` per record batch, so a
//! reader that knows which batches it wants can seek straight to them.
//!
//! The seeking itself sits above this module: `reader` turns a consumer's
//! value range into batch ordinals and opens the file at them, and `remote`
//! turns those ordinals into byte ranges to fetch. What this module provides is
//! the substrate both stand on — a footer to seek against, and a reader for it.
//!
//! # Where the offsets come from
//!
//! The footer, read back after the file is closed — not byte accounting at
//! the writer's boundary. One `FileWriter::write` call emits a batch's
//! dictionary blocks *and* its record block, so a byte counter wrapped around
//! that call cannot say where one ends and the next begins: it would hand out
//! an offset pointing at a dictionary and a length spanning both. Data with no
//! dictionary columns hides that completely, since the two coincide.
//!
//! The footer already separates them, exactly, as computed by the writer that
//! emitted them. It costs one tail read per shuffle file and removes a whole
//! class of silent misaddressing.
//!
//! [`RangeShuffleWriterExec`]: super::RangeShuffleWriterExec
//! [`RangeShuffleReaderExec`]: crate::execution_plans::RangeShuffleReaderExec

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom};
use std::path::Path;

use datafusion::arrow::ipc::reader::{FileReader, read_footer_length};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::ipc::{Block, CompressionType, root_as_footer};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{RecordBatchStream, metrics};
use futures::StreamExt;
use log::{debug, error};

use crate::error::{BallistaError, Result};
use crate::serde::scheduler::PartitionStats;
use crate::utils::create_write_options;

/// Leading bytes of an Arrow IPC file: the `ARROW1` magic plus two padding
/// bytes. An IPC stream opens with a continuation marker instead, so the two
/// formats are distinguishable from the head of the file.
const ARROW_FILE_MAGIC: &[u8; 6] = b"ARROW1";

/// Where one IPC message sits in the file, copied verbatim from the footer's
/// `Block`.
///
/// `offset` is absolute from the file start and `[offset, offset + len)` is
/// the byte range a reader fetches to decode this message on its own — which
/// is what a value index turns a cut range into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageBlock {
    /// Byte offset of the IPC message, absolute from the file start.
    pub offset: u64,
    /// Bytes the message occupies: metadata plus body.
    pub len: u64,
}

/// A finished IPC file's message layout, as its footer records it.
///
/// The two kinds are kept apart because they are consumed differently: a
/// consumer selects the record batches covering its value range, but must
/// also take the dictionaries those batches reference, which sit at their own
/// offsets earlier in the file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileLayout {
    /// Dictionary batches, in file order.
    pub dictionaries: Vec<MessageBlock>,
    /// Record batches, in file order — positionally the batches as written.
    pub record_batches: Vec<MessageBlock>,
}

/// True when `path` holds an Arrow IPC **file**, false for an IPC stream.
///
/// The on-disk layout is authoritative — same principle as
/// `is_sort_shuffle_output`. Deciding by magic rather than by a flag on
/// `PartitionLocation` keeps the format out of the wire protocol, so a
/// consumer, a Flight server, and a debugging human all reach the same
/// answer from the file alone.
///
/// An unreadable or too-short file reads as "not a file format", leaving the
/// error to whichever reader opens it next and can report it in context.
pub fn is_ipc_file(path: &Path) -> bool {
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; ARROW_FILE_MAGIC.len()];
    match file.read_exact(&mut magic) {
        Ok(()) => &magic == ARROW_FILE_MAGIC,
        Err(_) => false,
    }
}

/// Read a finished IPC file's footer and return where every message sits.
///
/// The trailer's last 10 bytes give the footer's length, the footer gives the
/// blocks. Called once per shuffle file after the writer closes it, so the
/// index records what the writer actually emitted rather than a reconstruction
/// of it.
pub fn read_file_layout(path: &Path) -> Result<FileLayout> {
    let mut file = File::open(path).map_err(|e| {
        BallistaError::General(format!(
            "Failed to open range shuffle file {path:?}: {e:?}"
        ))
    })?;

    let mut trailer = [0u8; 10];
    file.seek(SeekFrom::End(-10)).map_err(|e| {
        BallistaError::General(format!("Failed to seek {path:?} trailer: {e:?}"))
    })?;
    file.read_exact(&mut trailer).map_err(|e| {
        BallistaError::General(format!("Failed to read {path:?} trailer: {e:?}"))
    })?;
    let footer_len = read_footer_length(trailer).map_err(|e| {
        BallistaError::General(format!("{path:?} has no readable IPC footer: {e:?}"))
    })?;

    let mut footer_buf = vec![0u8; footer_len];
    file.seek(SeekFrom::End(-10 - footer_len as i64))
        .map_err(|e| {
            BallistaError::General(format!("Failed to seek {path:?} footer: {e:?}"))
        })?;
    file.read_exact(&mut footer_buf).map_err(|e| {
        BallistaError::General(format!("Failed to read {path:?} footer: {e:?}"))
    })?;

    let footer = root_as_footer(&footer_buf).map_err(|e| {
        BallistaError::General(format!("Failed to parse {path:?} footer: {e:?}"))
    })?;

    // Taken by bound rather than by the footer vector's concrete type, which
    // would pull `flatbuffers` in as a direct dependency just to name it.
    fn blocks<'a>(
        source: Option<impl IntoIterator<Item = &'a Block>>,
    ) -> Vec<MessageBlock> {
        source
            .map(|blocks| {
                blocks
                    .into_iter()
                    .map(|block| MessageBlock {
                        offset: block.offset() as u64,
                        len: block.metaDataLength() as u64 + block.bodyLength() as u64,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    Ok(FileLayout {
        dictionaries: blocks(footer.dictionaries()),
        record_batches: blocks(footer.recordBatches()),
    })
}

/// Open an Arrow IPC file for a whole-file read.
pub fn open_ipc_file(path: &Path) -> Result<FileReader<BufReader<File>>> {
    let file = File::open(path).map_err(|e| {
        BallistaError::General(format!(
            "Failed to open range shuffle file {path:?}: {e:?}"
        ))
    })?;
    let file = BufReader::with_capacity(256 * 1024, file);
    // Safety: setting `skip_validation` requires `unsafe`, user assures data is valid
    let reader = unsafe {
        FileReader::try_new(file, None)
            .map_err(|e| {
                BallistaError::General(format!(
                    "Failed to create arrow FileReader at {path:?}: {e:?}"
                ))
            })?
            .with_skip_validation(cfg!(feature = "arrow-ipc-optimizations"))
    };
    Ok(reader)
}

/// Stream data to disk in Arrow IPC file format, returning where each batch
/// landed alongside the usual stats.
///
/// Structured like [`crate::utils::write_stream_to_disk`]: batches cross a
/// bounded channel into a `spawn_blocking` task that owns every synchronous
/// file operation, so no tokio worker blocks on I/O.
pub async fn write_stream_to_ipc_file<S>(
    stream: &mut S,
    path: &Path,
    disk_write_metric: &metrics::Time,
    channel_capacity: usize,
    compression_type: Option<CompressionType>,
) -> Result<(PartitionStats, FileLayout)>
where
    S: RecordBatchStream + Unpin + ?Sized,
{
    let schema = stream.schema();
    let path_owned = path.to_owned();
    let write_metric = disk_write_metric.clone();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(channel_capacity);

    let handle =
        tokio::task::spawn_blocking(move || -> Result<(u64, usize, FileLayout)> {
            let file = File::create(&path_owned).map_err(|e| {
                error!("Failed to create partition file at {:?}: {e:?}", path_owned);
                BallistaError::IoError(e)
            })?;

            let options = create_write_options(compression_type)?;
            let mut writer = FileWriter::try_new_with_options(
                BufWriter::new(file),
                schema.as_ref(),
                options,
            )?;

            let mut batches_written = 0;
            while let Some(batch) = rx.blocking_recv() {
                let timer = write_metric.timer();
                writer.write(&batch)?;
                batches_written += 1;
                timer.done();
            }
            let timer = write_metric.timer();
            writer.finish()?;
            timer.done();

            // Only readable once the footer is on disk, which `finish` above
            // is what guarantees.
            let layout = read_file_layout(&path_owned)?;
            Ok((
                std::fs::metadata(&path_owned).map(|m| m.len()).unwrap_or(0),
                batches_written,
                layout,
            ))
        });

    let mut num_rows = 0;
    let mut num_batches = 0;

    let stream_err = loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                num_batches += 1;
                num_rows += batch.num_rows();
                if tx.send(batch).await.is_err() {
                    break None;
                }
            }
            Some(Err(e)) => break Some(e),
            None => break None,
        }
    };
    drop(tx);

    let write_result = handle
        .await
        .map_err(|e| BallistaError::General(format!("Disk writer task failed: {e}")))?;

    if let Some(e) = stream_err {
        if let Err(write_err) = &write_result {
            error!("Disk writer also failed: {write_err}");
        }
        return Err(e.into());
    }
    let (num_bytes, batches_written, layout) = write_result?;

    // Record blocks are addressed positionally by the index that follows, so
    // one block per batch written is the invariant that makes that mapping
    // valid. A mismatch means the writer re-chunked and every offset would
    // name the wrong batch.
    if layout.record_batches.len() != batches_written {
        return Err(BallistaError::General(format!(
            "range shuffle {path:?} footer records {} record batches but {} were \
             written — offsets cannot be mapped to batches",
            layout.record_batches.len(),
            batches_written,
        )));
    }

    debug!(
        "range shuffle wrote {path:?}: {} record blocks, {} dictionary blocks: {layout:?}",
        layout.record_batches.len(),
        layout.dictionaries.len(),
    );

    Ok((
        PartitionStats::new(Some(num_rows as u64), Some(num_batches), Some(num_bytes)),
        layout,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{DictionaryArray, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::arrow::ipc::writer::StreamWriter;
    use datafusion::arrow::ipc::{Message, root_as_message};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let payload: Vec<String> = values.iter().map(|v| format!("row-{v}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(values.to_vec())),
                Arc::new(StringArray::from(payload)),
            ],
        )
        .unwrap()
    }

    /// Decode the IPC message occupying `[start, end)`, the way a reader
    /// issuing a range request over just those bytes would have to.
    fn read_message_at(bytes: &[u8], start: usize, end: usize) -> Message<'_> {
        // An IPC message opens with a continuation marker and a little-endian
        // metadata length, then the flatbuffer header.
        let slice = &bytes[start..end];
        assert_eq!(
            &slice[0..4],
            &[0xff, 0xff, 0xff, 0xff],
            "continuation marker"
        );
        let meta_len = i32::from_le_bytes(slice[4..8].try_into().unwrap()) as usize;
        root_as_message(&slice[8..8 + meta_len]).unwrap()
    }

    async fn write_batches(
        path: &Path,
        batches: Vec<RecordBatch>,
    ) -> (PartitionStats, FileLayout) {
        let schema = batches[0].schema();
        let metrics = ExecutionPlanMetricsSet::new();
        let write_time = MetricBuilder::new(&metrics).subset_time("write_time", 0);
        let mut stream = RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        );
        write_stream_to_ipc_file(&mut stream, path, &write_time, 8, None)
            .await
            .unwrap()
    }

    /// The offsets are what a value index will hand to a range read, so each
    /// one has to address a message that decodes on its own. Slicing the file
    /// at `[offset, offset + len)` and decoding is the check that catches an
    /// offset naming the wrong thing; arithmetic self-consistency would not.
    #[tokio::test]
    async fn record_offsets_address_decodable_batches() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-0.arrow");
        let batches = vec![batch(&[1, 2, 3]), batch(&[4, 5]), batch(&[6, 7, 8, 9])];

        let (_, layout) = write_batches(&path, batches.clone()).await;

        assert_eq!(layout.record_batches.len(), 3, "one block per batch");
        assert!(
            layout.dictionaries.is_empty(),
            "no dictionary columns in these batches",
        );

        let bytes = std::fs::read(&path).unwrap();
        for (block, expected) in layout.record_batches.iter().zip(&batches) {
            let start = block.offset as usize;
            let message = read_message_at(&bytes, start, start + block.len as usize);
            assert_eq!(
                message.header_as_record_batch().map(|rb| rb.length()),
                Some(expected.num_rows() as i64),
                "block at {start} must hold this batch's record message",
            );
        }
    }

    /// Dictionary-encoded data is where byte accounting silently broke: one
    /// `write` call emits the dictionary and the record batch together, so a
    /// counter wrapped around that call hands out an offset pointing at the
    /// dictionary. The footer keeps the two apart, and only data with a
    /// dictionary column can tell the difference.
    #[tokio::test]
    async fn separates_dictionary_blocks_from_record_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dict.arrow");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let dict: DictionaryArray<Int32Type> =
            vec!["a", "b", "a", "c"].into_iter().collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();

        let (_, layout) = write_batches(&path, vec![batch.clone()]).await;

        assert_eq!(layout.record_batches.len(), 1);
        assert_eq!(
            layout.dictionaries.len(),
            1,
            "the dictionary must be recorded as its own block",
        );

        let dictionary = layout.dictionaries[0];
        let record = layout.record_batches[0];
        // The distinction byte counting could not make: the record block
        // starts past the dictionary rather than at it.
        assert!(
            record.offset >= dictionary.offset + dictionary.len,
            "record block must start after the dictionary ends, got {record:?} \
             against {dictionary:?}",
        );

        let bytes = std::fs::read(&path).unwrap();
        let start = record.offset as usize;
        let message = read_message_at(&bytes, start, start + record.len as usize);
        assert_eq!(
            message.header_as_record_batch().map(|rb| rb.length()),
            Some(batch.num_rows() as i64),
        );
        let start = dictionary.offset as usize;
        let message = read_message_at(&bytes, start, start + dictionary.len as usize);
        assert!(
            message.header_as_dictionary_batch().is_some(),
            "the dictionary block must hold a dictionary message",
        );
    }

    /// The layout has to describe the same file the whole-file reader sees, or
    /// a seeking consumer and a streaming one disagree about the contents.
    #[tokio::test]
    async fn layout_matches_what_the_reader_sees() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-0.arrow");
        let batches = vec![batch(&[1, 2]), batch(&[3]), batch(&[4, 5, 6])];

        let (_, layout) = write_batches(&path, batches.clone()).await;

        let reader = open_ipc_file(&path).unwrap();
        assert_eq!(reader.num_batches(), layout.record_batches.len());
        let read_back: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(read_back, batches, "round trip must preserve the batches");
    }

    /// Format detection decides which reader opens a shuffle file, so it has
    /// to separate the two IPC formats and not merely detect "some arrow".
    #[test]
    fn detects_file_format_against_stream_format() {
        let dir = tempdir().unwrap();
        let as_file = dir.path().join("file.arrow");
        let as_stream = dir.path().join("stream.arrow");
        let batches = vec![batch(&[1, 2, 3])];

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(write_batches(&as_file, batches.clone()));

        let out = File::create(&as_stream).unwrap();
        let mut writer = StreamWriter::try_new(out, &batches[0].schema()).unwrap();
        writer.write(&batches[0]).unwrap();
        writer.finish().unwrap();

        assert!(is_ipc_file(&as_file), "IPC file must be detected");
        assert!(!is_ipc_file(&as_stream), "IPC stream must not be");
        assert!(
            !is_ipc_file(&dir.path().join("absent.arrow")),
            "a missing file is not an IPC file",
        );
    }
}
