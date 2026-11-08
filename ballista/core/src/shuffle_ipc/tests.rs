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

use super::reader::MessageTranslatorForTests;
use super::*;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, DictionaryArray, FixedSizeListBuilder, Int32Array, Int64Array,
    LargeListBuilder, LargeStringArray, ListBuilder, MapBuilder, StringArray,
    StringBuilder, StringViewArray, StructArray,
};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::{StreamDecoder, StreamReader};
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::record_batch::RecordBatch;
use std::io::{Cursor, Read};
use std::sync::Arc;

const LENGTHS: ShuffleCompression = ShuffleCompression {
    codec: Some(CompressionType::ZSTD),
    offsets_as_lengths: true,
};

/// Every compression setting a shuffle writer can be configured with.
fn all_compressions() -> Vec<ShuffleCompression> {
    let mut all = Vec::new();
    for codec in [
        None,
        Some(CompressionType::LZ4_FRAME),
        Some(CompressionType::ZSTD),
    ] {
        for offsets_as_lengths in [false, true] {
            all.push(ShuffleCompression {
                codec,
                offsets_as_lengths,
            });
        }
    }
    all
}

/// Batches with offsets in string, binary, list, large list, fixed-size list,
/// map and struct columns, with nulls and empty values, plus a sliced batch
/// whose offsets do not start at zero and an empty batch.
fn nested_batches() -> (SchemaRef, Vec<RecordBatch>) {
    let rows = 300;
    let strings = |i: usize| -> Option<String> {
        match i % 7 {
            0 => None,
            1 => Some(String::new()),
            _ => Some(format!("value-{}", i * 37 % 101)),
        }
    };
    let utf8 = Arc::new(StringArray::from_iter((0..rows).map(strings))) as ArrayRef;
    let large = Arc::new(LargeStringArray::from_iter((0..rows).map(strings))) as ArrayRef;
    let binary = Arc::new(BinaryArray::from_iter(
        (0..rows).map(|i| strings(i).map(String::into_bytes)),
    )) as ArrayRef;
    let ints = Arc::new(Int64Array::from_iter(
        (0..rows).map(|i| (i % 5 != 0).then_some(i as i64)),
    )) as ArrayRef;

    let mut list = ListBuilder::new(StringBuilder::new());
    let mut large_list = LargeListBuilder::new(StringBuilder::new());
    let mut fixed = FixedSizeListBuilder::new(StringBuilder::new(), 2);
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Array::builder(0));
    for i in 0..rows {
        for _ in 0..i % 4 {
            list.values().append_option(strings(i));
            large_list.values().append_option(strings(i + 1));
            map.keys().append_value(format!("k{}", i % 3));
            map.values().append_value(i as i32);
        }
        list.append(i % 9 != 0);
        large_list.append(i % 11 != 0);
        fixed.values().append_option(strings(i));
        fixed.values().append_option(strings(i + 2));
        fixed.append(i % 6 != 0);
        map.append(i % 8 != 0).unwrap();
    }
    let list = Arc::new(list.finish()) as ArrayRef;
    let large_list = Arc::new(large_list.finish()) as ArrayRef;
    let fixed = Arc::new(fixed.finish()) as ArrayRef;
    let map = Arc::new(map.finish()) as ArrayRef;
    let strukt = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::clone(&utf8),
        ),
        (
            Arc::new(Field::new("n", DataType::Int64, true)),
            Arc::clone(&ints),
        ),
    ])) as ArrayRef;

    let columns = vec![
        utf8, large, binary, ints, list, large_list, fixed, map, strukt,
    ];
    let schema = Arc::new(Schema::new(
        [
            "utf8",
            "large",
            "binary",
            "ints",
            "list",
            "large_list",
            "fixed",
            "map",
            "struct",
        ]
        .iter()
        .zip(&columns)
        .map(|(name, column)| Field::new(*name, column.data_type().clone(), true))
        .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let batches = vec![
        batch.slice(0, 100),
        batch.slice(137, 150),
        RecordBatch::new_empty(Arc::clone(&schema)),
        batch,
    ];
    (schema, batches)
}

fn write_stream(
    schema: &Schema,
    batches: &[RecordBatch],
    compression: ShuffleCompression,
) -> Vec<u8> {
    let encoding = ShuffleEncoding::try_new(schema, compression).unwrap();
    let mut writer = ShuffleStreamWriter::try_new(Vec::new(), schema, &encoding).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.into_inner().unwrap()
}

fn read_stream(bytes: &[u8]) -> Vec<RecordBatch> {
    StreamReader::try_new(ShuffleIpcReader::new(Cursor::new(bytes)), None)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Decodes `bytes` through the push translator, fed in blocks of `block` bytes.
fn decode_in_blocks(bytes: &[u8], block: usize) -> Vec<RecordBatch> {
    let mut translator = ShuffleBytesTranslator::new();
    let mut decoder = StreamDecoder::new();
    let mut batches = Vec::new();
    let mut translated = Vec::new();
    for chunk in bytes.chunks(block) {
        translator
            .push(Bytes::copy_from_slice(chunk), &mut translated)
            .unwrap();
        for piece in translated.drain(..) {
            let mut buffer = Buffer::from(piece);
            while let Some(batch) = decoder.decode(&mut buffer).unwrap() {
                batches.push(batch);
            }
            assert!(buffer.is_empty());
        }
    }
    decoder.finish().unwrap();
    assert!(translator.is_idle());
    batches
}

use prost::bytes::Bytes;

#[test]
fn round_trips_under_every_compression() {
    let (schema, batches) = nested_batches();
    for compression in all_compressions() {
        let bytes = write_stream(&schema, &batches, compression);
        assert_eq!(read_stream(&bytes), batches, "{compression:?}");
        for block in [1, 7, 64, 4096, bytes.len()] {
            assert_eq!(
                decode_in_blocks(&bytes, block),
                batches,
                "{compression:?} in blocks of {block}"
            );
        }
    }
}

#[test]
fn lengths_only_with_zstd_and_when_asked() {
    let (schema, _) = nested_batches();
    for compression in all_compressions() {
        let encoding = ShuffleEncoding::try_new(&schema, compression).unwrap();
        assert_eq!(
            encoding.uses_lengths(),
            compression == LENGTHS,
            "{compression:?}"
        );
    }
}

#[test]
fn lengths_make_string_data_smaller() {
    let (schema, batches) = nested_batches();
    let plain = write_stream(&schema, &batches, ShuffleCompression::plain(LENGTHS.codec));
    let lengths = write_stream(&schema, &batches, LENGTHS);
    assert!(
        lengths.len() < plain.len(),
        "lengths {} bytes, plain {} bytes",
        lengths.len(),
        plain.len()
    );
}

#[test]
fn plain_streams_match_arrow_byte_for_byte() {
    let (schema, batches) = nested_batches();
    let codecs = [
        None,
        Some(CompressionType::LZ4_FRAME),
        Some(CompressionType::ZSTD),
    ];
    for codec in codecs {
        let options = IpcWriteOptions::default()
            .try_with_compression(codec)
            .unwrap();
        let mut arrow =
            StreamWriter::try_new_with_options(Vec::new(), &schema, options).unwrap();
        for batch in &batches {
            arrow.write(batch).unwrap();
        }
        let arrow = arrow.into_inner().unwrap();
        assert_eq!(
            write_stream(&schema, &batches, ShuffleCompression::plain(codec)),
            arrow
        );
        // and the reader passes them through untouched
        let mut through = Vec::new();
        ShuffleIpcReader::new(Cursor::new(&arrow))
            .read_to_end(&mut through)
            .unwrap();
        assert_eq!(through, arrow);
    }
}

#[test]
fn unmapped_or_offset_free_schemas_keep_arrow_compression() {
    let view: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "bb", "ccc"]));
    let dictionary: ArrayRef = Arc::new(
        vec!["x", "y", "x"]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
    );
    let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    for columns in [
        vec![Arc::clone(&strings), view],
        vec![Arc::clone(&strings), dictionary],
        vec![ints],
    ] {
        let batch = RecordBatch::try_from_iter(
            columns
                .into_iter()
                .enumerate()
                .map(|(i, column)| (format!("c{i}"), column)),
        )
        .unwrap();
        let schema = batch.schema();
        let encoding = ShuffleEncoding::try_new(&schema, LENGTHS).unwrap();
        assert!(!encoding.uses_lengths(), "{schema}");
        let bytes = write_stream(&schema, std::slice::from_ref(&batch), LENGTHS);
        assert_eq!(read_stream(&bytes), vec![batch.clone()]);
        assert_eq!(decode_in_blocks(&bytes, 5), vec![batch]);
    }
}

#[test]
fn arrow_without_the_translation_rejects_lengths() {
    let (schema, batches) = nested_batches();
    let bytes = write_stream(&schema, &batches, LENGTHS);
    let error = StreamReader::try_new(Cursor::new(&bytes), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();
    assert!(error.to_string().contains("not supported"), "{error}");

    let mut decoder = StreamDecoder::new();
    let mut buffer = Buffer::from(bytes);
    let error = loop {
        match decoder.decode(&mut buffer) {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("decoded a stream that stores offsets as lengths"),
            Err(error) => break error,
        }
    };
    assert!(error.to_string().contains("not supported"), "{error}");
}

#[test]
fn reader_stops_at_the_end_of_each_concatenated_stream() {
    let (schema, batches) = nested_batches();
    let first = write_stream(&schema, &batches[..2], LENGTHS);
    let second = write_stream(&schema, &batches[2..], ShuffleCompression::plain(None));
    let mut bytes = first.clone();
    bytes.extend_from_slice(&second);

    let mut reader =
        StreamReader::try_new(ShuffleIpcReader::new(Cursor::new(&bytes)), None).unwrap();
    let read: Vec<_> = reader.by_ref().collect::<Result<_, _>>().unwrap();
    assert_eq!(read, batches[..2]);
    let position = reader.get_mut().get_mut().position() as usize;
    assert_eq!(position, first.len());
    assert_eq!(read_stream(&bytes[position..]), batches[2..]);

    // the push translator, as block transport uses it, crosses streams too
    let mut translator = ShuffleBytesTranslator::new();
    let mut translated = Vec::new();
    translator
        .push(Bytes::from(bytes), &mut translated)
        .unwrap();
    let mut decoded = Vec::new();
    let mut decoder = StreamDecoder::new();
    for piece in translated {
        let mut buffer = Buffer::from(piece);
        loop {
            match decoder.decode(&mut buffer) {
                Ok(Some(batch)) => decoded.push(batch),
                Ok(None) => break,
                Err(e) if e.to_string().contains("Unexpected EOS") => {
                    decoder = StreamDecoder::new();
                }
                Err(e) => panic!("{e}"),
            }
        }
    }
    assert_eq!(decoded, batches);
}

#[test]
fn rewritten_batch_without_a_mapped_schema_is_an_error() {
    let (schema, batches) = nested_batches();
    let bytes = write_stream(&schema, &batches[..1], LENGTHS);
    let error = MessageTranslatorForTests::translate_first_batch_without_roles(&bytes)
        .unwrap_err();
    assert!(error.to_string().contains("no mapped layout"), "{error}");
}

#[test]
fn truncated_rewritten_batch_is_an_error() {
    let (schema, batches) = nested_batches();
    let bytes = write_stream(&schema, &batches[..1], LENGTHS);
    // cut inside the record batch's body
    let truncated = &bytes[..bytes.len() - 32];
    let result =
        StreamReader::try_new(ShuffleIpcReader::new(Cursor::new(truncated)), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>();
    assert!(result.is_err());
}

#[test]
fn finish_flushes_like_arrow() {
    let (schema, batches) = nested_batches();
    for compression in all_compressions() {
        let encoding = ShuffleEncoding::try_new(&schema, compression).unwrap();
        let mut writer = ShuffleStreamWriter::try_new(
            std::io::BufWriter::new(Vec::new()),
            &schema,
            &encoding,
        )
        .unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
        let written = match &writer {
            ShuffleStreamWriter::Plain(writer) => writer.get_ref().get_ref().len(),
            ShuffleStreamWriter::Lengths(writer) => writer.get_ref().get_ref().len(),
        };
        let all = writer.into_inner().unwrap().into_inner().unwrap();
        assert_eq!(written, all.len(), "{compression:?}");
    }
}
