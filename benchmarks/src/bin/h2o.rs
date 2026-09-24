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

//! H2O `db-benchmark` runner for Ballista.
//!
//! Ports `arrow-datafusion/benchmarks/src/h2o.rs` to Ballista. Supports both
//! single-process DataFusion and distributed Ballista execution over the h2o
//! groupby, join, and window query files.

use ballista::extension::SessionConfigExt;
use ballista::prelude::SessionContextExt;
use ballista_benchmarks::compare_results;
use ballista_core::object_store::{
    session_config_with_s3_support, session_state_with_s3_support,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::{SortColumn, lexsort_to_indices, take};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use std::path::{Path, PathBuf};
use std::time::Instant;
use structopt::StructOpt;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, StructOpt, Clone)]
struct DataFusionBenchmarkOpt {
    /// Query number (1..=max). If not specified, runs every query in the file.
    #[structopt(short, long)]
    query: Option<usize>,

    /// Path to the queries SQL file. Suite is inferred from the file name
    /// (`groupby.sql`, `join.sql`, `window.sql`).
    #[structopt(
        parse(from_os_str),
        short = "r",
        long = "queries-path",
        default_value = "benchmarks/queries/h2o/groupby.sql"
    )]
    queries_path: PathBuf,

    /// Path to the primary data file (used for `groupby.sql` / `window.sql`).
    #[structopt(
        parse(from_os_str),
        short = "p",
        long = "path",
        default_value = "benchmarks/data/h2o/G1_1e7_1e7_100_0.csv"
    )]
    path: PathBuf,

    /// Comma-separated join data files, in order: x, small, medium, large.
    /// Used for `join.sql` and `window.sql` (window uses the `large` file).
    #[structopt(
        short = "j",
        long = "join-paths",
        default_value = "benchmarks/data/h2o/J1_1e7_NA_0.csv,benchmarks/data/h2o/J1_1e7_1e1_0.csv,benchmarks/data/h2o/J1_1e7_1e4_0.csv,benchmarks/data/h2o/J1_1e7_1e7_NA.csv"
    )]
    join_paths: String,

    /// Iterations per query.
    #[structopt(short = "i", long = "iterations", default_value = "1")]
    iterations: usize,

    /// Number of partitions to process in parallel.
    #[structopt(short = "n", long = "partitions", default_value = "2")]
    partitions: usize,

    /// Batch size when reading CSV or Parquet files.
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,

    /// Print plans and query text.
    #[structopt(short, long)]
    debug: bool,

    /// Print the physical plan and exit without running the query.
    #[structopt(long)]
    explain: bool,
}

#[derive(Debug, StructOpt, Clone)]
struct BallistaBenchmarkOpt {
    /// Query number (1..=max). If not specified, runs every query in the file.
    #[structopt(short, long)]
    query: Option<usize>,

    /// Path to the queries SQL file. Suite is inferred from the file name
    /// (`groupby.sql`, `join.sql`, `window.sql`).
    #[structopt(
        parse(from_os_str),
        short = "r",
        long = "queries-path",
        default_value = "benchmarks/queries/h2o/groupby.sql"
    )]
    queries_path: PathBuf,

    /// Path to the primary data file (registered as `x` for `groupby.sql`).
    /// Not required for the join or window suites — they use `--join-paths`.
    /// May be a local path or an object-store URL such as `s3://bucket/prefix`.
    #[structopt(short = "p", long = "path")]
    path: Option<String>,

    /// Comma-separated join data files, in order: x, small, medium, large.
    /// Used for `join.sql` and `window.sql` (window uses the `large` file).
    /// Paths may be local or object-store URLs.
    #[structopt(short = "j", long = "join-paths", default_value = "")]
    join_paths: String,

    /// Iterations per query.
    #[structopt(short = "i", long = "iterations", default_value = "1")]
    iterations: usize,

    /// Number of partitions to process in parallel.
    #[structopt(short = "n", long = "partitions", default_value = "2")]
    partitions: usize,

    /// Batch size when reading CSV or Parquet files.
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,

    /// Ballista scheduler host.
    #[structopt(long = "host")]
    host: String,

    /// Ballista scheduler port.
    #[structopt(long = "port")]
    port: u16,

    /// Configuration overrides in `key=value` format. Repeatable.
    #[structopt(short = "c", long = "config", number_of_values = 1)]
    config_overrides: Vec<String>,

    /// Print plans and query text.
    #[structopt(short, long)]
    debug: bool,

    /// Print the physical plan and exit without running the query.
    #[structopt(long)]
    explain: bool,

    /// Verify Ballista's answer against a local DataFusion oracle running the
    /// same SQL over the same files. Adds one full local execution per query.
    #[structopt(long)]
    verify: bool,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "h2o", about = "H2O db-benchmark for Ballista")]
enum H2oOpt {
    #[structopt(name = "datafusion")]
    DataFusion(DataFusionBenchmarkOpt),
    #[structopt(name = "ballista")]
    Ballista(BallistaBenchmarkOpt),
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    match H2oOpt::from_args() {
        H2oOpt::DataFusion(opt) => run_datafusion(opt).await,
        H2oOpt::Ballista(opt) => run_ballista(opt).await,
    }
}

async fn run_datafusion(opt: DataFusionBenchmarkOpt) -> Result<()> {
    println!("Running h2o (datafusion) with: {opt:?}");
    let queries = AllQueries::from_file(&opt.queries_path)?;
    let query_range = query_range(&queries, opt.query);

    let config = SessionConfig::new()
        .with_target_partitions(opt.partitions)
        .with_batch_size(opt.batch_size);
    let ctx = SessionContext::new_with_config(config);

    let suite = Suite::from_queries_path(&opt.queries_path)?;
    let paths = SuitePaths {
        primary: Some(opt.path.to_string_lossy().into_owned()),
        join_csv: opt.join_paths.clone(),
    };
    register_h2o_tables(&ctx, suite, &paths).await?;

    if opt.explain {
        return explain_queries(&ctx, &queries, query_range).await;
    }

    run_queries(
        &ctx,
        None,
        &queries,
        query_range,
        opt.iterations,
        opt.debug,
        "datafusion",
    )
    .await
}

async fn run_ballista(opt: BallistaBenchmarkOpt) -> Result<()> {
    println!("Running h2o (ballista) with: {opt:?}");
    let queries = AllQueries::from_file(&opt.queries_path)?;
    let query_range = query_range(&queries, opt.query);

    let mut config = session_config_with_s3_support()
        .with_target_partitions(opt.partitions)
        .with_batch_size(opt.batch_size)
        .with_ballista_job_name("h2o benchmark");

    for kv in &opt.config_overrides {
        match kv.split_once('=') {
            Some((key, value)) => {
                if let Err(err) = config.options_mut().set(key.trim(), value.trim()) {
                    println!("Warning: could not set config '{kv}': {err}");
                }
            }
            None => println!(
                "Warning: ignoring invalid config override '{kv}'. Expected key=value"
            ),
        }
    }

    let state = session_state_with_s3_support(config)?;
    let address = format!("df://{}:{}", opt.host, opt.port);
    let ctx = SessionContext::remote_with_state(&address, state).await?;

    let suite = Suite::from_queries_path(&opt.queries_path)?;
    let paths = SuitePaths {
        primary: opt.path.clone(),
        join_csv: opt.join_paths.clone(),
    };
    register_h2o_tables(&ctx, suite, &paths).await?;

    if opt.explain {
        return explain_queries(&ctx, &queries, query_range).await;
    }

    let oracle_ctx = if opt.verify {
        let oracle_config = SessionConfig::new()
            .with_target_partitions(opt.partitions)
            .with_batch_size(opt.batch_size);
        let oracle = SessionContext::new_with_config(oracle_config);
        register_h2o_tables(&oracle, suite, &paths).await?;
        Some(oracle)
    } else {
        None
    };

    run_queries(
        &ctx,
        oracle_ctx.as_ref(),
        &queries,
        query_range,
        opt.iterations,
        opt.debug,
        "ballista",
    )
    .await
}

async fn explain_queries(
    ctx: &SessionContext,
    queries: &AllQueries,
    query_range: std::ops::RangeInclusive<usize>,
) -> Result<()> {
    use datafusion::physical_plan::displayable;
    for query_id in query_range {
        let sql = queries.get(query_id)?;
        println!("Q{query_id}: {sql}");
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        println!(
            "=== Physical plan (Q{query_id}) ===\n{}",
            displayable(plan.as_ref()).indent(true)
        );
    }
    Ok(())
}

async fn run_queries(
    ctx: &SessionContext,
    oracle_ctx: Option<&SessionContext>,
    queries: &AllQueries,
    query_range: std::ops::RangeInclusive<usize>,
    iterations: usize,
    debug: bool,
    label: &str,
) -> Result<()> {
    let mut total_secs = 0.0;
    for query_id in query_range {
        let sql = queries.get(query_id)?;
        println!("Q{query_id}: {sql}");

        let mut per_iter_secs = Vec::with_capacity(iterations);
        let mut row_count = 0usize;
        let mut last_batches = vec![];
        for iteration in 1..=iterations {
            let start = Instant::now();
            let batches = ctx.sql(sql).await?.collect().await?;
            let elapsed = start.elapsed().as_secs_f64();
            row_count = batches.iter().map(|batch| batch.num_rows()).sum();
            per_iter_secs.push(elapsed);
            println!(
                "Query {query_id} iteration {iteration} took {:.3} s and returned {row_count} rows ({label})",
                elapsed
            );
            last_batches = batches;
        }
        let avg = per_iter_secs.iter().sum::<f64>() / per_iter_secs.len() as f64;
        println!("Query {query_id} avg time: {avg:.3} s ({row_count} rows)");
        total_secs += avg;

        if debug {
            let plan = ctx.sql(sql).await?.into_optimized_plan()?;
            println!("=== Optimized logical plan ===\n{plan:?}\n");
        }

        if let Some(oracle) = oracle_ctx {
            let oracle_start = Instant::now();
            let expected = oracle.sql(sql).await?.collect().await?;
            let oracle_secs = oracle_start.elapsed().as_secs_f64();
            println!("Query {query_id} oracle (datafusion) took {oracle_secs:.3} s");

            // h2o queries have no ORDER BY, so Ballista's distributed row
            // order and DataFusion's single-process order can differ.
            // Lexsort each side before comparing.
            let sort_start = Instant::now();
            let expected_sorted = canonicalize(&expected)?;
            let actual_sorted = canonicalize(&last_batches)?;
            let sort_secs = sort_start.elapsed().as_secs_f64();

            let compare_start = Instant::now();
            compare_results(&expected_sorted, &actual_sorted).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Query {query_id} verification failed: {e}"
                ))
            })?;
            let compare_secs = compare_start.elapsed().as_secs_f64();
            println!(
                "Query {query_id} verified vs DataFusion: OK (sort {sort_secs:.3}s, compare {compare_secs:.3}s)"
            );
        }
    }
    println!("Total avg time across queries: {total_secs:.3} s");
    Ok(())
}

fn query_range(
    queries: &AllQueries,
    query: Option<usize>,
) -> std::ops::RangeInclusive<usize> {
    match query {
        Some(query_id) => query_id..=query_id,
        None => queries.min_id()..=queries.max_id(),
    }
}

/// Concatenate `batches` and lex-sort by every column, so distributed and
/// single-process row orderings compare equal.
fn canonicalize(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let schema = batches[0].schema();
    let combined = datafusion::arrow::compute::concat_batches(&schema, batches)
        .map_err(|e| DataFusionError::Execution(format!("canonicalize concat: {e}")))?;
    let sort_cols: Vec<SortColumn> = combined
        .columns()
        .iter()
        .map(|c| SortColumn {
            values: c.clone(),
            options: None,
        })
        .collect();
    let indices = lexsort_to_indices(&sort_cols, None)
        .map_err(|e| DataFusionError::Execution(format!("canonicalize sort: {e}")))?;
    let sorted_cols = combined
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::Execution(format!("canonicalize take: {e}")))?;
    let sorted = RecordBatch::try_new(schema, sorted_cols)
        .map_err(|e| DataFusionError::Execution(format!("canonicalize batch: {e}")))?;
    Ok(vec![sorted])
}

#[derive(Copy, Clone, Debug)]
enum Suite {
    Groupby,
    Join,
    Window,
}

impl Suite {
    fn from_queries_path(path: &Path) -> Result<Self> {
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        match name {
            "groupby.sql" => Ok(Suite::Groupby),
            "join.sql" => Ok(Suite::Join),
            "window.sql" => Ok(Suite::Window),
            other => Err(DataFusionError::Plan(format!(
                "unknown h2o suite {other:?} — expected groupby.sql, join.sql, or window.sql"
            ))),
        }
    }
}

struct SuitePaths {
    primary: Option<String>,
    join_csv: String,
}

async fn register_h2o_tables(
    ctx: &SessionContext,
    suite: Suite,
    paths: &SuitePaths,
) -> Result<()> {
    match suite {
        Suite::Groupby => {
            let primary = paths.primary.as_deref().ok_or_else(|| {
                DataFusionError::Plan(
                    "groupby suite requires --path pointing at the primary data file"
                        .to_string(),
                )
            })?;
            register_table(ctx, "x", primary).await
        }
        Suite::Join => {
            let join_paths: Vec<&str> = paths.join_csv.split(',').collect();
            if join_paths.len() != 4 {
                return Err(DataFusionError::Plan(format!(
                    "join suite needs 4 comma-separated paths, got {}",
                    join_paths.len()
                )));
            }
            for (table, path) in ["x", "small", "medium", "large"]
                .iter()
                .zip(join_paths.iter())
            {
                register_table(ctx, table, path.trim()).await?;
            }
            Ok(())
        }
        Suite::Window => {
            // The window suite uses only the `large` table from the join dataset.
            let large = paths.join_csv.split(',').nth(3).ok_or_else(|| {
                DataFusionError::Plan(
                    "window suite: --join-paths must contain 4 comma-separated paths; \
                     the fourth is registered as `large`"
                        .to_string(),
                )
            })?;
            register_table(ctx, "large", large.trim()).await
        }
    }
}

async fn register_table(ctx: &SessionContext, table: &str, path: &str) -> Result<()> {
    let extension = Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    match extension {
        "csv" => ctx
            .register_csv(table, path, CsvReadOptions::default())
            .await
            .map_err(|err| {
                DataFusionError::Context(
                    format!("registering table {table:?} from {path}"),
                    Box::new(err),
                )
            }),
        "parquet" => ctx
            .register_parquet(table, path, ParquetReadOptions::default())
            .await
            .map_err(|err| {
                DataFusionError::Context(
                    format!("registering table {table:?} from {path}"),
                    Box::new(err),
                )
            }),
        other => Err(DataFusionError::Plan(format!(
            "unsupported extension {other:?} for {path}"
        ))),
    }
}

struct AllQueries {
    queries: Vec<String>,
}

impl AllQueries {
    fn from_file(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|err| {
            DataFusionError::Execution(format!("reading {path:?}: {err}"))
        })?;
        // Queries are separated by blank lines — matches the datafusion h2o
        // runner exactly so query indices line up 1:1 across both binaries.
        let queries = contents.split("\n\n").map(str::to_owned).collect();
        Ok(Self { queries })
    }

    fn get(&self, query_id: usize) -> Result<&str> {
        self.queries
            .get(query_id - 1)
            .map(String::as_str)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "invalid query id {query_id}. Must be between {} and {}",
                    self.min_id(),
                    self.max_id()
                ))
            })
    }

    fn min_id(&self) -> usize {
        1
    }

    fn max_id(&self) -> usize {
        self.queries.len()
    }
}
