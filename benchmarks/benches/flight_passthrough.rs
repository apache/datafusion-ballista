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

//! Criterion benchmark for the shuffle Flight serve path.
//!
//! `serve_shuffle_partition` measures the server-side work done to answer a
//! shuffle fetch for one partition. On this branch it relays the partition's
//! raw IPC messages (metadata + body) straight to `FlightData`, without
//! decoding to `RecordBatch` or re-encoding. The baseline branch keeps the
//! same benchmark name but decodes then re-encodes, so `critcmp before after`
//! is a single row.

use std::hint::black_box;
use std::sync::Arc;

use arrow_flight::FlightData;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::root_as_message;
use datafusion::arrow::ipc::writer::{EncodedData, IpcWriteOptions, StreamWriter};
use datafusion::arrow::record_batch::RecordBatch;
use rand::RngExt;

const BATCH_SIZE: usize = 8192;
const NUM_BATCHES: usize = 16;
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

fn build_schema() -> SchemaRef {
    let mut fields = Vec::new();
    for i in 0..6 {
        fields.push(Field::new(format!("i32_{i}"), DataType::Int32, true));
    }
    for i in 0..3 {
        fields.push(Field::new(format!("i64_{i}"), DataType::Int64, true));
    }
    for i in 0..3 {
        fields.push(Field::new(format!("f64_{i}"), DataType::Float64, true));
    }
    for i in 0..2 {
        fields.push(Field::new(format!("b_{i}"), DataType::Boolean, true));
    }
    for i in 0..4 {
        fields.push(Field::new(format!("s_{i}"), DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn build_batch(schema: &SchemaRef) -> RecordBatch {
    let mut rng = rand::rng();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    for _ in 0..6 {
        let vals: Vec<i32> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Int32Array::from(vals)));
    }
    for _ in 0..3 {
        let vals: Vec<i64> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Int64Array::from(vals)));
    }
    for _ in 0..3 {
        let vals: Vec<f64> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Float64Array::from(vals)));
    }
    for _ in 0..2 {
        let vals: Vec<bool> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(BooleanArray::from(vals)));
    }
    for _ in 0..4 {
        let vals: Vec<String> = (0..BATCH_SIZE)
            .map(|_| format!("s{}", rng.random_range(0..100_000)))
            .collect();
        columns.push(Arc::new(StringArray::from(vals)));
    }
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

fn lz4_options() -> IpcWriteOptions {
    IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .unwrap()
}

/// One shuffle partition's bytes: an LZ4-compressed IPC stream, built once.
fn build_fixture(schema: &SchemaRef) -> Vec<u8> {
    let batch = build_batch(schema);
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, schema, lz4_options()).unwrap();
        for _ in 0..NUM_BATCHES {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }
    buf
}

/// Walk the encapsulated IPC stream and relay each message's metadata+body as
/// `FlightData`, without decoding to `RecordBatch` or re-encoding.
fn relay_messages(bytes: &[u8]) -> Vec<FlightData> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= bytes.len() {
        let mut marker = [0u8; 4];
        marker.copy_from_slice(&bytes[pos..pos + 4]);
        pos += 4;
        let size = if marker == CONTINUATION_MARKER {
            let mut s = [0u8; 4];
            s.copy_from_slice(&bytes[pos..pos + 4]);
            pos += 4;
            u32::from_le_bytes(s) as usize
        } else {
            u32::from_le_bytes(marker) as usize
        };
        if size == 0 {
            break;
        }
        let meta = &bytes[pos..pos + size];
        pos += size;
        let body_len = root_as_message(meta).unwrap().bodyLength() as usize;
        let body = &bytes[pos..pos + body_len];
        pos += body_len;
        out.push(FlightData::from(EncodedData {
            ipc_message: meta.to_vec(),
            arrow_data: body.to_vec(),
        }));
    }
    out
}

fn bench_serve(c: &mut Criterion) {
    let schema = build_schema();
    let fixture = build_fixture(&schema);

    c.bench_function("serve_shuffle_partition", |b| {
        b.iter(|| {
            let out = relay_messages(fixture.as_slice());
            black_box(out);
        })
    });
}

criterion_group!(benches, bench_serve);
criterion_main!(benches);
