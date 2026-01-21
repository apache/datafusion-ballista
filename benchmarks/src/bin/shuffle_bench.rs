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

//! Benchmark comparing hash-based and sort-based shuffle implementations.
//!
//! This benchmark generates synthetic data and measures the performance of
//! both shuffle implementations across various configurations:
//! - Different input sizes (number of rows)
//! - Different partition counts
//! - Different batch sizes
//!
//! Usage:
//!   cargo run --release --bin shuffle_bench -- --help
//!   cargo run --release --bin shuffle_bench -- --rows 1000000 --partitions 16

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::prelude::SessionContext;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tempfile::TempDir;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "shuffle_bench",
    about = "Benchmark comparing hash-based and sort-based shuffle implementations"
)]
struct ShuffleBenchOpt {
    /// Number of rows to generate
    #[structopt(short = "r", long = "rows", default_value = "1000000")]
    rows: usize,

    /// Number of output partitions
    #[structopt(short = "p", long = "partitions", default_value = "16")]
    partitions: usize,

    /// Number of input partitions
    #[structopt(short = "i", long = "input-partitions", default_value = "4")]
    input_partitions: usize,

    /// Batch size
    #[structopt(short = "b", long = "batch-size", default_value = "8192")]
    batch_size: usize,

    /// Number of iterations
    #[structopt(short = "n", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Memory limit for sort shuffle (in MB)
    #[structopt(short = "m", long = "memory-limit", default_value = "256")]
    memory_limit_mb: usize,

    /// Buffer size for sort shuffle (in MB)
    #[structopt(long = "buffer-size", default_value = "1")]
    buffer_size_mb: usize,

    /// Only run hash shuffle
    #[structopt(long = "hash-only")]
    hash_only: bool,

    /// Only run sort shuffle
    #[structopt(long = "sort-only")]
    sort_only: bool,
}

fn create_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("partition_key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn generate_test_batch(
    schema: &SchemaRef,
    batch_size: usize,
    partition_count: usize,
    offset: usize,
) -> RecordBatch {
    let ids: Vec<i64> = (offset..offset + batch_size).map(|i| i as i64).collect();
    let partition_keys: Vec<i64> =
        ids.iter().map(|id| *id % partition_count as i64).collect();
    let values: Vec<String> = ids.iter().map(|id| format!("value_{}", id)).collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(partition_keys)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .unwrap()
}

fn create_test_data(
    schema: &SchemaRef,
    rows: usize,
    batch_size: usize,
    input_partitions: usize,
    partition_count: usize,
) -> Vec<Vec<RecordBatch>> {
    let rows_per_partition = rows / input_partitions;
    let batches_per_partition = rows_per_partition.div_ceil(batch_size);

    let mut partitions = Vec::with_capacity(input_partitions);
    for p in 0..input_partitions {
        let mut batches = Vec::with_capacity(batches_per_partition);
        for b in 0..batches_per_partition {
            let offset = p * rows_per_partition + b * batch_size;
            let current_batch_size =
                std::cmp::min(batch_size, rows_per_partition - b * batch_size);
            if current_batch_size > 0 {
                batches.push(generate_test_batch(
                    schema,
                    current_batch_size,
                    partition_count,
                    offset,
                ));
            }
        }
        partitions.push(batches);
    }
    partitions
}

async fn benchmark_hash_shuffle(
    data: &[Vec<RecordBatch>],
    schema: SchemaRef,
    output_partitions: usize,
    work_dir: &str,
) -> Result<(Duration, usize), Box<dyn std::error::Error>> {
    use ballista_core::execution_plans::ShuffleWriterExec;
    use ballista_core::utils;

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();

    // Create input plan from data
    let memory_source =
        Arc::new(MemorySourceConfig::try_new(data, schema.clone(), None)?);
    let input = Arc::new(DataSourceExec::new(memory_source));

    // Create shuffle writer
    let shuffle_writer = ShuffleWriterExec::try_new(
        "bench_job".to_owned(),
        1,
        input,
        work_dir.to_owned(),
        Some(Partitioning::Hash(
            vec![Arc::new(Column::new("partition_key", 1))],
            output_partitions,
        )),
    )?;

    let start = Instant::now();

    // Execute all input partitions (not output partitions)
    let input_partition_count = data.len();
    let mut total_files = 0;
    for partition in 0..input_partition_count {
        let mut stream = shuffle_writer.execute(partition, task_ctx.clone())?;
        let batches = utils::collect_stream(&mut stream).await?;
        // Count output files from the result
        if let Some(batch) = batches.first() {
            total_files += batch.num_rows();
        }
    }

    let elapsed = start.elapsed();
    Ok((elapsed, total_files))
}

async fn benchmark_sort_shuffle(
    data: &[Vec<RecordBatch>],
    schema: SchemaRef,
    output_partitions: usize,
    work_dir: &str,
    buffer_size: usize,
    memory_limit: usize,
) -> Result<(Duration, usize), Box<dyn std::error::Error>> {
    use ballista_core::execution_plans::sort_shuffle::{
        SortShuffleConfig, SortShuffleWriterExec,
    };
    use ballista_core::utils;

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();

    // Create input plan from data
    let memory_source =
        Arc::new(MemorySourceConfig::try_new(data, schema.clone(), None)?);
    let input = Arc::new(DataSourceExec::new(memory_source));

    // Create sort shuffle config
    let config = SortShuffleConfig::new(
        true,
        buffer_size,
        memory_limit,
        0.8,
        CompressionType::LZ4_FRAME,
    );

    // Create sort shuffle writer
    let shuffle_writer = SortShuffleWriterExec::try_new(
        "bench_job".to_owned(),
        1,
        input,
        work_dir.to_owned(),
        Partitioning::Hash(
            vec![Arc::new(Column::new("partition_key", 1))],
            output_partitions,
        ),
        config,
    )?;

    let start = Instant::now();

    // Execute all input partitions (not output partitions)
    let input_partition_count = data.len();
    let mut total_files = 0;
    for partition in 0..input_partition_count {
        let mut stream = shuffle_writer.execute(partition, task_ctx.clone())?;
        let batches = utils::collect_stream(&mut stream).await?;
        // Count output files from the result
        if let Some(batch) = batches.first() {
            total_files += batch.num_rows();
        }
    }

    let elapsed = start.elapsed();
    Ok((elapsed, total_files))
}

fn count_files_in_dir(dir: &str) -> usize {
    let mut count = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.path().is_file() {
                count += 1;
            } else if entry.path().is_dir() {
                count += count_files_in_dir(entry.path().to_str().unwrap());
            }
        }
    }
    count
}

fn dir_size(dir: &str) -> u64 {
    let mut size = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.path().is_file() {
                if let Ok(meta) = entry.metadata() {
                    size += meta.len();
                }
            } else if entry.path().is_dir() {
                size += dir_size(entry.path().to_str().unwrap());
            }
        }
    }
    size
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = ShuffleBenchOpt::from_args();

    println!("Shuffle Benchmark Configuration:");
    println!("  Rows: {}", opt.rows);
    println!("  Input partitions: {}", opt.input_partitions);
    println!("  Output partitions: {}", opt.partitions);
    println!("  Batch size: {}", opt.batch_size);
    println!("  Iterations: {}", opt.iterations);
    println!("  Sort shuffle memory limit: {} MB", opt.memory_limit_mb);
    println!("  Sort shuffle buffer size: {} MB", opt.buffer_size_mb);
    println!();

    let schema = create_test_schema();

    // Generate test data once
    println!("Generating test data...");
    let data = create_test_data(
        &schema,
        opt.rows,
        opt.batch_size,
        opt.input_partitions,
        opt.partitions,
    );
    let total_batches: usize = data.iter().map(|p| p.len()).sum();
    println!(
        "Generated {} batches across {} input partitions",
        total_batches, opt.input_partitions
    );
    println!();

    let buffer_size = opt.buffer_size_mb * 1024 * 1024;
    let memory_limit = opt.memory_limit_mb * 1024 * 1024;

    // Benchmark hash shuffle
    if !opt.sort_only {
        println!("=== Hash-Based Shuffle ===");
        let mut hash_times: Vec<Duration> = Vec::new();
        let mut hash_file_count = 0;
        let mut hash_total_size = 0u64;

        for i in 0..opt.iterations {
            let temp_dir = TempDir::new()?;
            let work_dir = temp_dir.path().to_str().unwrap();

            let (elapsed, _files) =
                benchmark_hash_shuffle(&data, schema.clone(), opt.partitions, work_dir)
                    .await?;

            hash_file_count = count_files_in_dir(work_dir);
            hash_total_size = dir_size(work_dir);
            hash_times.push(elapsed);

            println!(
                "  Iteration {}: {:?} ({} files, {} KB)",
                i + 1,
                elapsed,
                hash_file_count,
                hash_total_size / 1024
            );
        }

        let avg_time: Duration =
            hash_times.iter().sum::<Duration>() / hash_times.len() as u32;
        let min_time = hash_times.iter().min().unwrap();
        let max_time = hash_times.iter().max().unwrap();

        println!();
        println!("Hash Shuffle Results:");
        println!("  Average time: {:?}", avg_time);
        println!("  Min time: {:?}", min_time);
        println!("  Max time: {:?}", max_time);
        println!("  Files created: {}", hash_file_count);
        println!("  Total size: {} KB", hash_total_size / 1024);
        println!(
            "  Throughput: {:.2} MB/s",
            (opt.rows * 30) as f64 / avg_time.as_secs_f64() / 1024.0 / 1024.0
        );
        println!();
    }

    // Benchmark sort shuffle
    if !opt.hash_only {
        println!("=== Sort-Based Shuffle ===");
        let mut sort_times: Vec<Duration> = Vec::new();
        let mut sort_file_count = 0;
        let mut sort_total_size = 0u64;

        for i in 0..opt.iterations {
            let temp_dir = TempDir::new()?;
            let work_dir = temp_dir.path().to_str().unwrap();

            let (elapsed, _files) = benchmark_sort_shuffle(
                &data,
                schema.clone(),
                opt.partitions,
                work_dir,
                buffer_size,
                memory_limit,
            )
            .await?;

            sort_file_count = count_files_in_dir(work_dir);
            sort_total_size = dir_size(work_dir);
            sort_times.push(elapsed);

            println!(
                "  Iteration {}: {:?} ({} files, {} KB)",
                i + 1,
                elapsed,
                sort_file_count,
                sort_total_size / 1024
            );
        }

        let avg_time: Duration =
            sort_times.iter().sum::<Duration>() / sort_times.len() as u32;
        let min_time = sort_times.iter().min().unwrap();
        let max_time = sort_times.iter().max().unwrap();

        println!();
        println!("Sort Shuffle Results:");
        println!("  Average time: {:?}", avg_time);
        println!("  Min time: {:?}", min_time);
        println!("  Max time: {:?}", max_time);
        println!("  Files created: {}", sort_file_count);
        println!("  Total size: {} KB", sort_total_size / 1024);
        println!(
            "  Throughput: {:.2} MB/s",
            (opt.rows * 30) as f64 / avg_time.as_secs_f64() / 1024.0 / 1024.0
        );
        println!();
    }

    Ok(())
}
