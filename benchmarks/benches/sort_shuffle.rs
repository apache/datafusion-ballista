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

//! Criterion benchmarks for sort-based shuffle writer.

use std::sync::Arc;

use ballista_core::execution_plans::SortShuffleWriterExec;
use ballista_core::execution_plans::sort_shuffle::SortShuffleConfig;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use rand::Rng;
use tempfile::TempDir;

const BATCH_SIZE: usize = 8192;
const NUM_OUTPUT_PARTITIONS: usize = 200;
// 100 columns: mix of primitive types
const NUM_INT_COLS: usize = 12; // i8, i16, i32, i64 x3 each
const NUM_UINT_COLS: usize = 12; // u8, u16, u32, u64 x3 each
const NUM_FLOAT_COLS: usize = 6; // f32, f64 x3 each
const NUM_BOOL_COLS: usize = 5;
const NUM_STRING_COLS: usize = 5;
// Remaining to reach 100
const NUM_EXTRA_INT32_COLS: usize =
    100 - NUM_INT_COLS - NUM_UINT_COLS - NUM_FLOAT_COLS - NUM_BOOL_COLS - NUM_STRING_COLS;

fn build_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(100);
    let mut idx = 0;

    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::Int8, true));
        idx += 1;
    }
    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::Int16, true));
        idx += 1;
    }
    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::Int32, true));
        idx += 1;
    }
    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::Int64, true));
        idx += 1;
    }

    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::UInt8, true));
        idx += 1;
    }
    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::UInt16, true));
        idx += 1;
    }
    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::UInt32, true));
        idx += 1;
    }
    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::UInt64, true));
        idx += 1;
    }

    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::Float32, true));
        idx += 1;
    }
    for _ in 0..3 {
        fields.push(Field::new(format!("c{idx}"), DataType::Float64, true));
        idx += 1;
    }

    for _ in 0..NUM_BOOL_COLS {
        fields.push(Field::new(format!("c{idx}"), DataType::Boolean, true));
        idx += 1;
    }

    for _ in 0..NUM_STRING_COLS {
        fields.push(Field::new(format!("c{idx}"), DataType::Utf8, true));
        idx += 1;
    }

    for _ in 0..NUM_EXTRA_INT32_COLS {
        fields.push(Field::new(format!("c{idx}"), DataType::Int32, true));
        idx += 1;
    }

    Arc::new(Schema::new(fields))
}

fn build_batch(schema: &SchemaRef) -> RecordBatch {
    let mut rng = rand::rng();
    let mut columns: Vec<Arc<dyn datafusion::arrow::array::Array>> =
        Vec::with_capacity(100);

    // i8 x3
    for _ in 0..3 {
        let vals: Vec<i8> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Int8Array::from(vals)));
    }
    // i16 x3
    for _ in 0..3 {
        let vals: Vec<i16> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Int16Array::from(vals)));
    }
    // i32 x3
    for _ in 0..3 {
        let vals: Vec<i32> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Int32Array::from(vals)));
    }
    // i64 x3
    for _ in 0..3 {
        let vals: Vec<i64> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Int64Array::from(vals)));
    }

    // u8 x3
    for _ in 0..3 {
        let vals: Vec<u8> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(UInt8Array::from(vals)));
    }
    // u16 x3
    for _ in 0..3 {
        let vals: Vec<u16> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(UInt16Array::from(vals)));
    }
    // u32 x3
    for _ in 0..3 {
        let vals: Vec<u32> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(UInt32Array::from(vals)));
    }
    // u64 x3
    for _ in 0..3 {
        let vals: Vec<u64> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(UInt64Array::from(vals)));
    }

    // f32 x3
    for _ in 0..3 {
        let vals: Vec<f32> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Float32Array::from(vals)));
    }
    // f64 x3
    for _ in 0..3 {
        let vals: Vec<f64> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Float64Array::from(vals)));
    }

    // bool
    for _ in 0..NUM_BOOL_COLS {
        let vals: Vec<bool> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(BooleanArray::from(vals)));
    }

    // string (short random strings)
    for _ in 0..NUM_STRING_COLS {
        let vals: Vec<String> = (0..BATCH_SIZE)
            .map(|_| format!("s{}", rng.random_range(0..100_000)))
            .collect();
        columns.push(Arc::new(StringArray::from(vals)));
    }

    // remaining i32 columns
    for _ in 0..NUM_EXTRA_INT32_COLS {
        let vals: Vec<i32> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
        columns.push(Arc::new(Int32Array::from(vals)));
    }

    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

fn create_input(num_batches: usize) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    let schema = build_schema();
    let batch = build_batch(&schema);
    let partition: Vec<RecordBatch> = (0..num_batches).map(|_| batch.clone()).collect();
    let partitions = vec![partition];

    let memory_source =
        Arc::new(MemorySourceConfig::try_new(&partitions, schema, None).unwrap());
    Arc::new(CoalescePartitionsExec::new(Arc::new(DataSourceExec::new(
        memory_source,
    ))))
}

fn run_sort_shuffle(
    input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    work_dir: &str,
    memory_limit: usize,
) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();

    let config = SortShuffleConfig::new(
        true,
        1024 * 1024, // 1MB buffer
        memory_limit,
        0.8,
        CompressionType::LZ4_FRAME,
        8192,
    );

    let writer = SortShuffleWriterExec::try_new(
        "bench_job".to_string(),
        1,
        input,
        work_dir.to_string(),
        Partitioning::Hash(vec![Arc::new(Column::new("c0", 0))], NUM_OUTPUT_PARTITIONS),
        config,
    )
    .unwrap();

    rt.block_on(async {
        let mut stream = writer.execute(0, task_ctx).unwrap();
        while let Some(_batch) = stream.try_next().await.unwrap() {}
    });
}

fn bench_no_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_shuffle_no_spill");
    group.sample_size(10);

    // 10 batches of 8192 rows = 81920 rows, 256MB limit
    let input = create_input(10);
    let work_dir = TempDir::new().unwrap();

    group.bench_function("10_batches_200_partitions", |b| {
        b.iter(|| {
            run_sort_shuffle(
                input.clone(),
                work_dir.path().to_str().unwrap(),
                256 * 1024 * 1024,
            );
        });
    });

    // 50 batches
    let input = create_input(50);
    group.bench_function("50_batches_200_partitions", |b| {
        b.iter(|| {
            run_sort_shuffle(
                input.clone(),
                work_dir.path().to_str().unwrap(),
                256 * 1024 * 1024,
            );
        });
    });

    group.finish();
}

fn bench_with_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_shuffle_with_spill");
    group.sample_size(10);

    let work_dir = TempDir::new().unwrap();

    // 50 batches with 8MB memory limit to force spilling
    let input = create_input(50);
    group.bench_function("50_batches_200_partitions_8mb_limit", |b| {
        b.iter(|| {
            run_sort_shuffle(
                input.clone(),
                work_dir.path().to_str().unwrap(),
                8 * 1024 * 1024,
            );
        });
    });

    // 50 batches with 2MB memory limit to force heavy spilling
    let input = create_input(50);
    group.bench_function("50_batches_200_partitions_2mb_limit", |b| {
        b.iter(|| {
            run_sort_shuffle(
                input.clone(),
                work_dir.path().to_str().unwrap(),
                2 * 1024 * 1024,
            );
        });
    });

    group.finish();
}

criterion_group!(benches, bench_no_spill, bench_with_spill);
criterion_main!(benches);
