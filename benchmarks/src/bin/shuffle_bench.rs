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

//! Standalone shuffle benchmark for profiling Ballista shuffle write
//! performance outside of a cluster. Streams input from Parquet files and
//! drives either the hash-based or sort-based shuffle writer end-to-end.
//!
//! # Usage
//!
//! ```sh
//! cargo run --release --bin shuffle_bench -- \
//!   --input /data/tpch-sf100/lineitem/ \
//!   --writer sort \
//!   --partitions 200 \
//!   --hash-columns 0,3
//! ```
//!
//! Profile with flamegraph:
//! ```sh
//! cargo flamegraph --release --bin shuffle_bench -- \
//!   --input /data/tpch-sf100/lineitem/ \
//!   --writer sort --partitions 200
//! ```

use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::execution_plans::sort_shuffle::{
    SortShuffleConfig, SortShuffleWriterExec,
};
use ballista_core::utils;
use clap::Parser;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::ipc::CompressionType;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "shuffle_bench",
    about = "Standalone Ballista shuffle benchmark"
)]
struct Args {
    /// Path to input Parquet file or directory of Parquet files.
    #[arg(long)]
    input: PathBuf,

    /// Shuffle writer to drive: `hash` (default) or `sort`.
    #[arg(long, default_value = "hash")]
    writer: String,

    /// Partitioning scheme: `hash`, `single`, or `round-robin`. Currently
    /// both writers only support `hash`; other values are rejected.
    #[arg(long, default_value = "hash")]
    partitioning: String,

    /// Column indices to hash on (comma-separated, e.g. "0,3").
    #[arg(long, default_value = "0")]
    hash_columns: String,

    /// Number of output shuffle partitions.
    #[arg(long, default_value_t = 200)]
    partitions: usize,

    /// DataFusion target batch size (rows).
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Memory pool size in bytes (passed to RuntimeEnvBuilder::with_memory_limit).
    /// When set, the sort writer will spill once usage crosses this limit.
    #[arg(long)]
    memory_limit: Option<usize>,

    /// Limit rows read from Parquet (0 = no limit).
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Number of timed iterations.
    #[arg(long, default_value_t = 1)]
    iterations: usize,

    /// Number of warmup iterations before timing.
    #[arg(long, default_value_t = 0)]
    warmup: usize,

    /// Output work directory for shuffle data.
    #[arg(long, default_value = "/tmp/ballista_shuffle_bench")]
    output_dir: PathBuf,

    /// Concurrent shuffle tasks to simulate executor parallelism.
    #[arg(long, default_value_t = 1)]
    concurrent_tasks: usize,
}

#[derive(Clone, Copy, Debug)]
enum WriterKind {
    Hash,
    Sort,
}

#[derive(Clone, Copy, Debug)]
enum PartitioningKind {
    Hash,
}

fn parse_writer(s: &str) -> Result<WriterKind, String> {
    match s.to_lowercase().as_str() {
        "hash" => Ok(WriterKind::Hash),
        "sort" => Ok(WriterKind::Sort),
        other => Err(format!(
            "unknown writer: {other} (expected 'hash' or 'sort')"
        )),
    }
}

fn parse_partitioning(s: &str) -> Result<PartitioningKind, String> {
    match s.to_lowercase().as_str() {
        "hash" => Ok(PartitioningKind::Hash),
        "single" | "round-robin" => Err(format!(
            "partitioning '{s}' is not supported by Ballista shuffle writers; \
             only 'hash' is currently legal"
        )),
        other => Err(format!("unknown partitioning: {other}")),
    }
}

fn parse_hash_columns(s: &str) -> Vec<usize> {
    s.split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().parse::<usize>().expect("invalid column index"))
        .collect()
}

fn read_parquet_metadata(path: &Path, limit: usize) -> (SchemaRef, u64) {
    let paths = collect_parquet_paths(path);
    let mut schema = None;
    let mut total_rows = 0u64;

    for file_path in &paths {
        let file = fs::File::open(file_path)
            .unwrap_or_else(|e| panic!("Failed to open {}: {}", file_path.display(), e));
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).unwrap_or_else(|e| {
                panic!(
                    "Failed to read Parquet metadata from {}: {}",
                    file_path.display(),
                    e
                )
            });
        if schema.is_none() {
            schema = Some(Arc::clone(builder.schema()));
        }
        total_rows += builder.metadata().file_metadata().num_rows() as u64;
        if limit > 0 && total_rows >= limit as u64 {
            total_rows = total_rows.min(limit as u64);
            break;
        }
    }

    (schema.expect("No parquet files found"), total_rows)
}

fn collect_parquet_paths(path: &Path) -> Vec<PathBuf> {
    if path.is_dir() {
        let mut files: Vec<PathBuf> = fs::read_dir(path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e))
            .filter_map(|entry| {
                let p = entry.ok()?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        files.sort();
        if files.is_empty() {
            panic!("No .parquet files in {}", path.display());
        }
        files
    } else {
        vec![path.to_path_buf()]
    }
}

fn build_partitioning(
    _kind: PartitioningKind,
    num_partitions: usize,
    hash_col_indices: &[usize],
    schema: &SchemaRef,
) -> Partitioning {
    let exprs = hash_col_indices
        .iter()
        .map(|&idx| {
            let field = schema.field(idx);
            Arc::new(Column::new(field.name(), idx))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>
        })
        .collect();
    Partitioning::Hash(exprs, num_partitions)
}

async fn execute_shuffle_write(
    args: &Args,
    writer_kind: WriterKind,
    partitioning_kind: PartitioningKind,
    hash_col_indices: &[usize],
    work_dir: PathBuf,
    task_id: usize,
) -> datafusion::error::Result<MetricsSet> {
    let mut runtime_builder = RuntimeEnvBuilder::new();
    if let Some(mem_limit) = args.memory_limit {
        runtime_builder = runtime_builder.with_memory_limit(mem_limit, 1.0);
    }
    let runtime_env = Arc::new(runtime_builder.build()?);
    let config = SessionConfig::new().with_batch_size(args.batch_size);
    let ctx = SessionContext::new_with_config_rt(config, runtime_env);

    let mut df = ctx
        .read_parquet(args.input.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    if args.limit > 0 {
        df = df.limit(0, Some(args.limit))?;
    }

    let parquet_plan = df.create_physical_plan().await?;
    let input: Arc<dyn ExecutionPlan> = if parquet_plan
        .properties()
        .output_partitioning()
        .partition_count()
        > 1
    {
        Arc::new(CoalescePartitionsExec::new(parquet_plan.clone()))
    } else {
        parquet_plan
    };
    let schema = input.schema();
    let partitioning = build_partitioning(
        partitioning_kind,
        args.partitions,
        hash_col_indices,
        &schema,
    );

    let work_dir_str = work_dir.to_str().unwrap().to_string();
    fs::create_dir_all(&work_dir).expect("create work dir");

    let metrics: MetricsSet = match writer_kind {
        WriterKind::Hash => {
            let exec = ShuffleWriterExec::try_new(
                format!("bench_job_{task_id}"),
                1,
                input,
                work_dir_str,
                Some(partitioning),
            )?;
            let task_ctx = ctx.task_ctx();
            let mut stream = exec.execute(0, task_ctx)?;
            let _ = utils::collect_stream(&mut stream).await;
            exec.metrics().unwrap_or_default()
        }
        WriterKind::Sort => {
            let cfg =
                SortShuffleConfig::new(true, CompressionType::LZ4_FRAME, args.batch_size);
            let exec = SortShuffleWriterExec::try_new(
                format!("bench_job_{task_id}"),
                1,
                input,
                work_dir_str,
                Some(partitioning),
                cfg,
            )?;
            let task_ctx = ctx.task_ctx();
            let mut stream = exec.execute(0, task_ctx)?;
            let _ = utils::collect_stream(&mut stream).await;
            exec.metrics().unwrap_or_default()
        }
    };

    Ok(metrics)
}

fn run_iteration(
    args: &Args,
    writer_kind: WriterKind,
    partitioning_kind: PartitioningKind,
    hash_col_indices: &[usize],
) -> (f64, Option<MetricsSet>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let start = Instant::now();
        if args.concurrent_tasks <= 1 {
            let work_dir = args.output_dir.join("task_0");
            let metrics = execute_shuffle_write(
                args,
                writer_kind,
                partitioning_kind,
                hash_col_indices,
                work_dir.clone(),
                0,
            )
            .await
            .expect("shuffle write failed");
            let elapsed = start.elapsed().as_secs_f64();
            let _ = fs::remove_dir_all(&work_dir);
            (elapsed, Some(metrics))
        } else {
            let mut handles = Vec::with_capacity(args.concurrent_tasks);
            for task_id in 0..args.concurrent_tasks {
                let args = args.clone();
                let hash_col_indices = hash_col_indices.to_vec();
                let work_dir = args.output_dir.join(format!("task_{task_id}"));
                handles.push(tokio::spawn(async move {
                    let m = execute_shuffle_write(
                        &args,
                        writer_kind,
                        partitioning_kind,
                        &hash_col_indices,
                        work_dir.clone(),
                        task_id,
                    )
                    .await
                    .expect("shuffle write failed");
                    let _ = fs::remove_dir_all(&work_dir);
                    m
                }));
            }
            for h in handles {
                let _ = h.await.expect("task panicked");
            }
            (start.elapsed().as_secs_f64(), None)
        }
    })
}

fn print_shuffle_metrics(metrics: &MetricsSet, total_wall_time_secs: f64) {
    let total_ns = (total_wall_time_secs * 1e9) as u64;
    let fmt_time = |nanos: usize| -> String {
        let secs = nanos as f64 / 1e9;
        let pct = if total_ns > 0 {
            (nanos as f64 / total_ns as f64) * 100.0
        } else {
            0.0
        };
        format!("{secs:.3}s ({pct:.1}%)")
    };
    let aggregated = metrics.aggregate_by_name();
    for m in aggregated.iter() {
        let value = m.value();
        let name = value.name();
        let v = value.as_usize();
        if v == 0 {
            continue;
        }
        if matches!(
            value,
            MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_)
        ) {
            continue;
        }
        let is_time = matches!(
            value,
            MetricValue::ElapsedCompute(_) | MetricValue::Time { .. }
        );
        if is_time {
            println!("  {name}: {}", fmt_time(v));
        } else {
            println!("  {name}: {v}");
        }
    }
}

fn describe_schema(schema: &datafusion::arrow::datatypes::Schema) -> String {
    let mut counts: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    for field in schema.fields() {
        let type_name = match field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => "int",
            DataType::Float16 | DataType::Float32 | DataType::Float64 => "float",
            DataType::Utf8 | DataType::LargeUtf8 => "string",
            DataType::Boolean => "bool",
            DataType::Date32 | DataType::Date64 => "date",
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "decimal",
            DataType::Timestamp(_, _) => "timestamp",
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                "binary"
            }
            _ => "other",
        };
        *counts.entry(type_name).or_insert(0) += 1;
    }
    let mut parts: Vec<String> = counts
        .into_iter()
        .map(|(k, v)| format!("{v}x{k}"))
        .collect();
    parts.sort();
    parts.join(", ")
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .init();

    let args = Args::parse();
    let writer_kind = parse_writer(&args.writer).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(2);
    });
    let partitioning_kind = parse_partitioning(&args.partitioning).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(2);
    });
    let hash_col_indices = parse_hash_columns(&args.hash_columns);

    fs::create_dir_all(&args.output_dir).expect("create output dir");

    let (schema, total_rows) = read_parquet_metadata(&args.input, args.limit);

    println!("=== Ballista Shuffle Benchmark ===");
    println!("Writer:         {writer_kind:?}");
    println!("Partitioning:   {partitioning_kind:?}");
    println!("Input:          {}", args.input.display());
    println!(
        "Schema:         {} cols ({})",
        schema.fields().len(),
        describe_schema(&schema)
    );
    println!("Total rows:     {total_rows}");
    println!("Partitions:     {}", args.partitions);
    println!("Batch size:     {}", args.batch_size);
    if let Some(m) = args.memory_limit {
        println!("Memory limit:   {m} bytes");
    }
    if args.concurrent_tasks > 1 {
        println!("Concurrent:     {} tasks", args.concurrent_tasks);
    }
    println!(
        "Iterations:     {} (warmup {})",
        args.iterations, args.warmup
    );
    println!();

    let total_iters = args.warmup + args.iterations;
    let mut times = Vec::with_capacity(args.iterations);
    let mut last_metrics: Option<MetricsSet> = None;

    for i in 0..total_iters {
        let is_warmup = i < args.warmup;
        let label = if is_warmup {
            format!("warmup {}/{}", i + 1, args.warmup)
        } else {
            format!("iter {}/{}", i - args.warmup + 1, args.iterations)
        };
        let (elapsed, metrics) =
            run_iteration(&args, writer_kind, partitioning_kind, &hash_col_indices);
        if !is_warmup {
            times.push(elapsed);
            if metrics.is_some() {
                last_metrics = metrics;
            }
        }
        println!("  [{label}] write: {elapsed:.3}s");
    }

    if !times.is_empty() {
        let avg = times.iter().sum::<f64>() / times.len() as f64;
        let total_writer_rows = total_rows * args.concurrent_tasks as u64;
        println!();
        println!("=== Results ===");
        println!("avg time: {avg:.3}s");
        if times.len() > 1 {
            let min = times.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            println!("min/max:  {min:.3}s / {max:.3}s");
        }
        println!(
            "throughput: {} rows/s (total across {} tasks)",
            (total_writer_rows as f64 / avg) as u64,
            args.concurrent_tasks
        );
        if let Some(metrics) = last_metrics {
            println!();
            println!("Shuffle metrics (last iteration):");
            print_shuffle_metrics(&metrics, avg);
        }
    }

    let _ = fs::remove_dir_all(&args.output_dir);
}
