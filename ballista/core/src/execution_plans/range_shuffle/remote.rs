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

//! Reading a remote range shuffle file down to the bytes a range covers.
//!
//! Two fetches per source: the index, then the bytes it points at. The
//! consumer does the searching, so the executor never reads an index or
//! compares a value — which is what makes the same two steps work against
//! object storage, where there is no executor to ask.
//!
//! The extra round trip is paid once per source and, batched across sources,
//! once per read. What it buys is the difference between a whole file and the
//! batches that can hold a row the consumer wants.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message,
};
use datafusion::common::ScalarValue;

use crate::error::{BallistaError, Result};
use crate::serde::scheduler::ByteRange;

use super::index::{byte_len_column, byte_offset_column, is_dict_column};

/// Encode `schema` as an IPC schema message, the header a stream decoder needs
/// before any batch.
///
/// Synthesized rather than sliced out of the data file. The file's own schema
/// message sits after the magic padded to the writer's alignment — 64 bytes in
/// arrow-rs, 8 in some other writers — so slicing it means encoding another
/// implementation's padding rule into a byte offset. The consumer already knows
/// the schema it asked for, so it can write the header itself and fetch only
/// batches.
pub fn schema_message(schema: &Schema) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let encoded =
        generator.schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options);

    let mut bytes = Vec::new();
    write_message(&mut bytes, encoded, &options).map_err(|e| {
        BallistaError::General(format!("range shuffle: cannot encode schema: {e}"))
    })?;
    Ok(bytes)
}

/// The byte ranges holding the batches covering `[lo, hi)`, and the
/// dictionaries they need.
///
/// Two parts, in file order:
///
/// - every dictionary block before the selected batches, since a batch
///   referencing a dictionary cannot be decoded without it and the index
///   cannot say which batch needs which
/// - the selected record batches, coalesced, so a contiguous range costs one
///   request
///
/// No schema range: see [`schema_message`]. `None` when nothing was selected —
/// the consumer's range misses this file entirely and there is nothing to
/// fetch.
pub fn byte_ranges_for(
    index: &RecordBatch,
    selected: &[usize],
) -> Result<Option<Vec<ByteRange>>> {
    if selected.is_empty() {
        return Ok(None);
    }

    let is_dict = is_dict_column(index)?;
    let offsets = byte_offset_column(index)?;
    let lengths = byte_len_column(index)?;

    // `selected` counts record batches; the index counts every message. Walk
    // once, mapping one onto the other.
    let mut record_ordinal = 0;
    let mut dictionaries = Vec::new();
    let mut records = Vec::new();
    let last_selected = selected.last().copied().unwrap_or(0);

    for row in 0..index.num_rows() {
        let range = ByteRange {
            offset: offsets.value(row),
            length: lengths.value(row),
        };

        if is_dict.value(row) {
            // Conservative: every dictionary preceding the last batch we want.
            // Replacement and delta dictionaries both stay correct, at the cost
            // of dictionaries a narrower rule could have skipped.
            if records.is_empty() || record_ordinal <= last_selected {
                dictionaries.push(range);
            }
            continue;
        }
        if selected.contains(&record_ordinal) {
            records.push(range);
        }
        record_ordinal += 1;
    }

    if records.is_empty() {
        return Ok(None);
    }

    let mut ranges = Vec::with_capacity(dictionaries.len() + records.len());
    ranges.extend(dictionaries);
    ranges.extend(coalesce(records));
    Ok(Some(ranges))
}

/// Merge ranges that touch, so a contiguous run of batches costs one request
/// rather than one per batch. A value range selects a contiguous run, so this
/// is the common case and the one where per-request latency would otherwise
/// undo the saving.
fn coalesce(ranges: Vec<ByteRange>) -> Vec<ByteRange> {
    let mut merged: Vec<ByteRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(last) if last.offset + last.length == range.offset => {
                last.length += range.length;
            }
            _ => merged.push(range),
        }
    }
    merged
}

/// Whether an index says anything in this file can fall inside `[lo, hi)`.
///
/// Used to skip a source outright rather than fetch bytes from it.
pub fn covers(
    index: &RecordBatch,
    lo: Option<&ScalarValue>,
    hi: Option<&ScalarValue>,
    descending: bool,
) -> Result<bool> {
    Ok(
        super::index::select_record_batches(index, lo, hi, descending)?
            .is_none_or(|selected| !selected.is_empty()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::range_shuffle::index::{
        BYTE_OFFSET_COLUMN, fixed_fields,
    };
    use datafusion::arrow::array::{BooleanArray, Float64Array, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Build an index whose rows are `(is_dict, offset, len)` in file order.
    ///
    /// The key column is deliberately named `byte_offset`, colliding with a
    /// fixed column: Arrow permits duplicate field names and resolves lookups
    /// to the first match, so this fixture reads correctly only while the
    /// fixed columns are addressed by position.
    fn index(rows: &[(bool, u64, u64)]) -> RecordBatch {
        let mut fields = vec![Field::new(BYTE_OFFSET_COLUMN, DataType::Float64, true)];
        fields.extend(fixed_fields());
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.0); rows.len()])),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(vec![Some(1u64); rows.len()])),
            ],
        )
        .unwrap()
    }

    /// A contiguous run of batches has to cost one range, not one per batch,
    /// or per-request latency eats the bandwidth saving.
    #[test]
    fn coalesces_a_contiguous_run_into_one_range() {
        // Blocks at 100, 200, 300, 400, each 100 bytes.
        let index = index(&[
            (false, 100, 100),
            (false, 200, 100),
            (false, 300, 100),
            (false, 400, 100),
        ]);

        let ranges = byte_ranges_for(&index, &[1, 2]).unwrap().unwrap();

        assert_eq!(
            ranges,
            vec![ByteRange {
                offset: 200,
                length: 200
            }],
            "two adjacent batches are one request, not two",
        );
    }

    /// A batch referencing a dictionary cannot be decoded without it, and the
    /// index cannot say which batch needs which — so preceding dictionaries
    /// come along.
    #[test]
    fn includes_dictionaries_preceding_the_selection() {
        let index = index(&[
            (true, 100, 50),   // dictionary
            (false, 150, 100), // batch 0
            (false, 250, 100), // batch 1
        ]);

        let ranges = byte_ranges_for(&index, &[1]).unwrap().unwrap();

        assert!(
            ranges.contains(&ByteRange {
                offset: 100,
                length: 50
            }),
            "the dictionary must be fetched with the batch: {ranges:?}",
        );
        assert!(
            ranges.contains(&ByteRange {
                offset: 250,
                length: 100
            }),
            "the selected batch must be fetched: {ranges:?}",
        );
    }

    /// Selecting nothing means this file holds nothing the consumer wants, so
    /// there is no request to make — not a request for zero bytes.
    #[test]
    fn selecting_nothing_fetches_nothing() {
        let index = index(&[(false, 100, 100)]);
        assert_eq!(byte_ranges_for(&index, &[]).unwrap(), None);
    }

    /// The ranges are handed to an executor that resolves nothing, so what
    /// comes back is exactly these bytes concatenated — and it has to decode as
    /// an IPC stream. Slicing a real file and decoding the result is the only
    /// check that covers the schema prefix, the offsets, and the framing
    /// together.
    #[tokio::test]
    async fn sliced_ranges_decode_as_an_ipc_stream() {
        use crate::execution_plans::range_shuffle::{
            index_path, read_index_file, select_record_batches, write_index_file,
        };
        use crate::execution_plans::range_shuffle::{
            index_schema, write_stream_to_ipc_file,
        };
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::buffer::Buffer;
        use datafusion::arrow::ipc::reader::StreamDecoder;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::metrics::{
            ExecutionPlanMetricsSet, MetricBuilder,
        };
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("data-0.arrow");

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let batches: Vec<RecordBatch> = [vec![0, 1], vec![2, 3], vec![4, 5]]
            .iter()
            .map(|keys| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(keys.clone()))],
                )
                .unwrap()
            })
            .collect();

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("k", 0),
        ))])
        .unwrap();

        let metrics = ExecutionPlanMetricsSet::new();
        let write_time = MetricBuilder::new(&metrics).subset_time("write_time", 0);
        let mut keyed = crate::execution_plans::range_shuffle::KeyCollector::new(
            Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(batches.clone().into_iter().map(Ok)),
            )),
            ordering.clone(),
        );
        let (_, layout) =
            write_stream_to_ipc_file(&mut keyed, &path, &write_time, 8, None)
                .await
                .unwrap();
        let keys = keyed.into_keys();
        let index_file = index_path(&path);
        write_index_file(
            &index_file,
            index_schema(&ordering, schema.as_ref()).unwrap(),
            &layout,
            &keys,
        )
        .unwrap();

        // Ask for the middle batch only, the way a consumer covering [2, 4)
        // would.
        let index = read_index_file(&index_file).unwrap();
        let selected = select_record_batches(
            &index,
            Some(&ScalarValue::Int32(Some(2))),
            Some(&ScalarValue::Int32(Some(4))),
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected, vec![1], "only the middle batch holds [2, 4)");

        let ranges = byte_ranges_for(&index, &selected).unwrap().unwrap();

        // What the client assembles: the header it writes itself, then the
        // bytes the executor concatenates.
        let file = std::fs::read(&path).unwrap();
        let mut body = schema_message(schema.as_ref()).unwrap();
        for range in &ranges {
            let start = range.offset as usize;
            body.extend_from_slice(&file[start..start + range.length as usize]);
        }

        let mut decoder = StreamDecoder::new();
        let mut buffer = Buffer::from(body);
        let mut decoded = Vec::new();
        while let Some(batch) = decoder.decode(&mut buffer).unwrap() {
            decoded.push(batch);
        }
        assert_eq!(
            decoded,
            vec![batches[1].clone()],
            "the sliced bytes must decode to exactly the selected batch",
        );
    }
}
