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

//! Timing a query suite and writing its JSON summary, shared by the TPC-H and
//! TPC-DS runners so both produce the same summary format.

use crate::execute_query_capturing_answer;
use datafusion::DATAFUSION_VERSION;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub elapsed: f64,
    pub row_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRun {
    /// query number
    pub query: usize,
    /// list of individual run times and row counts
    pub iterations: Vec<QueryResult>,
    /// Set when the query did not run all iterations to completion. The message
    /// captures why; `iterations` then holds only the iterations that finished
    /// before the failure (possibly none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl QueryRun {
    pub fn new(query: usize) -> Self {
        Self {
            query,
            iterations: vec![],
            error: None,
        }
    }

    pub fn add_result(&mut self, elapsed: f64, row_count: usize) {
        self.iterations.push(QueryResult { elapsed, row_count })
    }

    /// Fastest completed iteration in seconds, or `None` if no iteration
    /// finished (e.g. the query failed before completing one).
    pub fn min_elapsed(&self) -> Option<f64> {
        self.iterations
            .iter()
            .map(|r| r.elapsed)
            .min_by(|a, b| a.total_cmp(b))
    }

    /// Row count from the first completed iteration.
    pub fn row_count(&self) -> Option<usize> {
        self.iterations.first().map(|r| r.row_count)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkRun {
    /// Benchmark crate version
    pub benchmark_version: String,
    /// DataFusion crate version
    pub datafusion_version: String,
    /// Number of CPU cores
    pub num_cpus: usize,
    /// Start time
    pub start_time: u64,
    /// CLI arguments
    pub arguments: Vec<String>,
    /// Results for each query
    pub queries: Vec<QueryRun>,
}

impl BenchmarkRun {
    fn new() -> Self {
        Self {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            datafusion_version: DATAFUSION_VERSION.to_owned(),
            num_cpus: std::thread::available_parallelism().unwrap().get(),
            start_time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("current time is later than UNIX_EPOCH")
                .as_secs(),
            arguments: std::env::args().skip(1).collect::<Vec<String>>(),
            queries: vec![],
        }
    }
}

/// Runs `queries` in order, calling `run_query` for each one to push its
/// iteration timings into a fresh `QueryRun`, and returns the summary.
///
/// A query that fails has its error recorded and the suite moves on, so one
/// failure does not hide the timings of the rest. When `output` is set, the
/// summary is written to `<output>/<name>-<start_time>.json` after every
/// query, so a hard kill mid-suite still leaves the results collected so far
/// on disk. Once the whole suite has run, lists the failures and returns an
/// error if there were any.
pub async fn run_suite(
    name: &str,
    queries: &[usize],
    output: Option<&Path>,
    mut run_query: impl AsyncFnMut(usize, &mut QueryRun) -> Result<()>,
) -> Result<BenchmarkRun> {
    let mut benchmark_run = BenchmarkRun::new();
    let mut total_elapsed = 0.0;
    let mut summary_path = None;

    for &query in queries {
        let mut query_run = QueryRun::new(query);
        match run_query(query, &mut query_run).await {
            Ok(()) => total_elapsed += mean_elapsed(&query_run),
            Err(e) => {
                eprintln!("Query {query} failed: {e}");
                query_run.error = Some(e.to_string());
            }
        }
        benchmark_run.queries.push(query_run);
        summary_path = persist_summary(&benchmark_run, name, output)?;
    }

    println!("Total time: {total_elapsed:.3} s");
    if let Some(path) = summary_path {
        println!("Summary written to {}", path.display());
    }

    let failures: Vec<&QueryRun> = benchmark_run
        .queries
        .iter()
        .filter(|q| q.error.is_some())
        .collect();
    if !failures.is_empty() {
        eprintln!("\n{} query failure(s):", failures.len());
        for q in &failures {
            eprintln!("  q{}: {}", q.query, q.error.as_deref().unwrap_or_default());
        }
        return Err(DataFusionError::Execution(format!(
            "{} query failure(s); see the summary for details",
            failures.len()
        )));
    }
    Ok(benchmark_run)
}

pub fn print_iteration(
    query: usize,
    iteration: usize,
    iterations: usize,
    elapsed: f64,
    row_count: usize,
) {
    if iterations == 1 {
        println!("Query {query} took {elapsed:.3} s and returned {row_count} rows");
    } else {
        println!(
            "Query {query} iteration {iteration} took {elapsed:.3} s and returned {row_count} rows"
        );
    }
}

/// Runs `sqls` on `ctx` `iterations` times, pushing each iteration's timing
/// into `query_run`, and returns the answer batches from the last iteration
/// (see `execute_query_capturing_answer`). On error, `query_run` already holds
/// whatever iterations completed before the failure.
pub async fn time_query(
    ctx: &SessionContext,
    query: usize,
    sqls: &[String],
    iterations: usize,
    debug: bool,
    query_run: &mut QueryRun,
) -> Result<Vec<RecordBatch>> {
    let mut result = vec![];
    for i in 0..iterations {
        let start = Instant::now();
        result = execute_query_capturing_answer(ctx, sqls, debug).await?;
        let elapsed = start.elapsed().as_secs_f64();
        if debug {
            pretty::print_batches(&result)?;
        }
        let row_count = result.iter().map(|b| b.num_rows()).sum();
        print_iteration(query, i, iterations, elapsed, row_count);
        query_run.add_result(elapsed, row_count);
    }
    Ok(result)
}

/// A completed query's contribution to the suite total: its mean iteration
/// time, which is printed when there was more than one iteration.
fn mean_elapsed(query_run: &QueryRun) -> f64 {
    let n = query_run.iterations.len();
    if n == 0 {
        return 0.0;
    }
    let mean = query_run.iterations.iter().map(|r| r.elapsed).sum::<f64>() / n as f64;
    if n > 1 {
        println!("Query {} avg time: {mean:.3} s", query_run.query);
    }
    mean
}

/// Write the run summary to `dir` if one was configured; a no-op otherwise.
/// Returns the written path when it wrote one.
fn persist_summary(
    run: &BenchmarkRun,
    name: &str,
    dir: Option<&Path>,
) -> Result<Option<PathBuf>> {
    dir.map(|d| write_summary_json(run, name, d)).transpose()
}

/// Write (or overwrite) the run summary as `<name>-<start_time>.json` in `dir`.
///
/// The write is atomic — a temp file is written then renamed over the target —
/// so callers can persist after every query without risking a truncated file if
/// the process is killed (e.g. an OOM SIGKILL) mid-write. Returns the final path.
fn write_summary_json(
    benchmark_run: &BenchmarkRun,
    name: &str,
    dir: &Path,
) -> Result<PathBuf> {
    let json =
        serde_json::to_string_pretty(&benchmark_run).expect("summary is serializable");
    let final_path = dir.join(format!("{name}-{}.json", benchmark_run.start_time));
    let tmp_path = dir.join(format!("{name}-{}.json.tmp", benchmark_run.start_time));
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    Ok(final_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_run_with(query: usize, times: &[f64], rows: usize) -> QueryRun {
        let mut qr = QueryRun::new(query);
        for &t in times {
            qr.add_result(t, rows);
        }
        qr
    }

    #[test]
    fn min_elapsed_picks_fastest_iteration() {
        let qr = query_run_with(1, &[3.0, 1.5, 2.0], 4);
        assert_eq!(qr.min_elapsed(), Some(1.5));
        assert_eq!(qr.row_count(), Some(4));
    }

    #[test]
    fn min_elapsed_is_none_without_iterations() {
        let qr = QueryRun::new(7);
        assert_eq!(qr.min_elapsed(), None);
        assert_eq!(qr.row_count(), None);
    }

    #[test]
    fn success_run_omits_error_and_roundtrips() {
        let qr = query_run_with(1, &[1.0], 4);
        let json = serde_json::to_string(&qr).unwrap();
        assert!(
            !json.contains("error"),
            "success run should omit error: {json}"
        );
        let back: QueryRun = serde_json::from_str(&json).unwrap();
        assert!(back.error.is_none());
        assert_eq!(back.iterations.len(), 1);
    }

    #[test]
    fn failed_run_keeps_partial_iterations_and_error() {
        let mut qr = query_run_with(5, &[2.0], 3); // one iteration completed
        qr.error = Some("boom".to_string());
        let json = serde_json::to_string(&qr).unwrap();
        let back: QueryRun = serde_json::from_str(&json).unwrap();
        assert_eq!(back.error.as_deref(), Some("boom"));
        assert_eq!(back.min_elapsed(), Some(2.0)); // partial iteration preserved
    }

    // A failing query is recorded and the suite carries on; the summary on
    // disk holds every query and the run as a whole reports the failure.
    #[tokio::test]
    async fn run_suite_records_failures_and_keeps_going() {
        let dir = tempfile::tempdir().unwrap();
        let result = run_suite("tpcds", &[1, 2, 3], Some(dir.path()), {
            async |query: usize, run: &mut QueryRun| {
                run.add_result(query as f64, 10);
                if query == 2 {
                    return Err(DataFusionError::Execution("boom".to_string()));
                }
                run.add_result(query as f64, 10);
                Ok(())
            }
        })
        .await;
        assert!(result.is_err());

        let files: Vec<PathBuf> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1, "{files:?}");
        let file_name = files[0].file_name().unwrap().to_str().unwrap();
        assert!(
            file_name.starts_with("tpcds-") && file_name.ends_with(".json"),
            "{file_name}"
        );

        let run: BenchmarkRun =
            serde_json::from_str(&fs::read_to_string(&files[0]).unwrap()).unwrap();
        let summary: Vec<(usize, usize, Option<&str>)> = run
            .queries
            .iter()
            .map(|q| (q.query, q.iterations.len(), q.error.as_deref()))
            .collect();
        assert_eq!(
            summary,
            [
                (1, 2, None),
                (2, 1, Some("Execution error: boom")),
                (3, 2, None)
            ]
        );
    }

    #[tokio::test]
    async fn run_suite_without_output_writes_nothing() {
        let run = run_suite("tpch", &[4], None, async |_, run: &mut QueryRun| {
            run.add_result(0.5, 1);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(run.queries.len(), 1);
        assert_eq!(run.queries[0].min_elapsed(), Some(0.5));
    }
}
