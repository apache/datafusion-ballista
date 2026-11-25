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

//! Runs DataFusion's sqllogictest (`.slt`) files on plain DataFusion and on
//! standalone Ballista with the static and the adaptive (AQE) planner, and
//! fails on records that fail but aren't in `expected_failures.txt`.
//!
//! The `.slt` files use paths relative to DataFusion's `datafusion/sqllogictest`
//! directory (`../../testing/data/...`, `../core/tests/data/...`), so the
//! harness runs from a DataFusion checkout of the version in `Cargo.lock`, with
//! its `testing` and `parquet-testing` submodules:
//!
//! ```text
//! git clone --depth 1 --branch 56.0.0 https://github.com/apache/datafusion.git /path/to/datafusion
//! git -C /path/to/datafusion submodule update --init --depth 1 testing parquet-testing
//! BALLISTA_SLT_DATAFUSION_DIR=/path/to/datafusion/datafusion/sqllogictest \
//!     cargo test -p ballista-sqllogictest --test sqllogictests -- [filters] [options]
//! ```
//!
//! See "DataFusion sqllogictest suite" in
//! `docs/source/contributors-guide/development.md`.
//!
//! Without `BALLISTA_SLT_DATAFUSION_DIR` the test prints a notice and passes,
//! so `cargo test --workspace` doesn't need the checkout. `--help` lists the
//! options.
//!
//! Each file runs on every engine in turn (they share the file's scratch
//! directory), and files run in parallel up to `--jobs`. Every run of a file
//! on an engine is a child process of this one, so a hang, a crash or a
//! leaked cluster ends with it, and a timeout kills it. Every Ballista run
//! starts its own standalone scheduler and executor from the file's session
//! state.

mod codec;

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use ballista::prelude::SessionContextExt;
use ballista_core::config::{
    BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_STANDALONE_PARALLELISM,
};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::{
    BallistaLogicalExtensionCodec, ComposedLogicalExtensionCodec,
};
use ballista_sqllogictest::expected::ExpectedFailures;
use ballista_sqllogictest::{Engine, Failure, FileResult, Outcome, report};
use clap::Parser;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_sqllogictest::{
    DFColumnType, DataFusion, Filter, TestContext, TestFile, df_value_validator,
    read_dir_recursive, setup_scratch_dir, should_skip_file, should_skip_record,
    test_configurations, value_normalizer,
};
use futures::StreamExt;
use indicatif::ProgressBar;
use sqllogictest::{
    Normalizer, QueryExpect, Record, StatementExpect, parse_file, strict_column_validator,
};

use crate::codec::{InProcessSinkCodec, InProcessTableCodec};

const TEST_DIRECTORY: &str = "test_files/";
const TPCH_PREFIX: &str = "tpch";
/// Needs DataFusion's `parquet_encryption` feature, which the published
/// `datafusion-sqllogictest` crate doesn't enable.
const ENCRYPTED_PARQUET_FILE: &str = "encrypted_parquet.slt";
/// The label DataFusion's own runner gives its engine, so `skipif` / `onlyif`
/// conditions in the files apply here the same way.
const DATAFUSION_LABEL: &str = "Datafusion";
const BALLISTA_LABEL: &str = "Ballista";
/// Planning and decoding deep plans recurses past the default 2 MiB worker
/// stack in debug builds (`union.slt` overflows it).
const STACK_SIZE: usize = 32 * 1024 * 1024;
/// Matches the `TestContext`'s `target_partitions`.
const STANDALONE_PARALLELISM: usize = 4;
/// Prefixes the worker's outcome on stdout.
const OUTCOME_MARKER: &str = "BALLISTA_SLT_OUTCOME ";

/// The DataFusion checkout's root as it appears in plan output (no leading
/// `/`, like an `object_store` path).
static WORKSPACE_ROOT: OnceLock<String> = OnceLock::new();

#[derive(Parser, Debug)]
#[clap(about = "Run DataFusion's sqllogictest files against Ballista")]
struct Options {
    /// Only run files whose path contains one of these, or one record with
    /// `file.slt:line`
    filters: Vec<String>,

    /// DataFusion's `datafusion/sqllogictest` directory, holding `test_files/`
    #[clap(long, env = "BALLISTA_SLT_DATAFUSION_DIR")]
    datafusion_dir: Option<PathBuf>,

    /// Engines to run each file on
    #[clap(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "datafusion,ballista-static,ballista-aqe"
    )]
    engines: Vec<Engine>,

    /// Files to run at once
    #[clap(long, default_value_t = 4)]
    jobs: usize,

    /// Seconds one engine may spend on one file
    #[clap(long, default_value_t = 300)]
    timeout_secs: u64,

    /// The list of records that are known to fail
    #[clap(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/expected_failures.txt"))]
    expected_failures: PathBuf,

    /// Rewrite the expected-failure list from this run instead of failing on
    /// unexpected failures. Only the files and engines that ran are updated.
    #[clap(long)]
    update_expected: bool,

    /// Write the full report, with every failure, to this Markdown file
    #[clap(long)]
    report: Option<PathBuf>,

    /// Also run the `tpch` files
    #[clap(long)]
    include_tpch: bool,

    /// Also check `EXPLAIN` output. Ballista's plans differ from DataFusion's,
    /// so by default plain `EXPLAIN` records are dropped on every engine and
    /// `EXPLAIN ANALYZE` runs without checking its output.
    #[clap(long)]
    include_explain: bool,

    // Accepted and ignored so `cargo test` / nextest arguments don't fail
    // parsing.
    #[clap(long, hide = true)]
    list: bool,
    #[clap(long, hide = true)]
    format: Option<String>,
    #[clap(short = 'Z', hide = true)]
    unstable: Option<String>,
    #[clap(long, hide = true)]
    nocapture: bool,
    #[clap(long, hide = true)]
    test_threads: Option<String>,
    #[clap(long, hide = true)]
    exact: bool,

    /// Internal: run this one file (under `test_files/`) on
    /// `--worker-engine` and print its outcome, for the parent process.
    #[clap(long, hide = true, requires = "worker_engine")]
    worker_file: Option<PathBuf>,
    #[clap(long, hide = true, value_enum)]
    worker_engine: Option<Engine>,
}

fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(STACK_SIZE)
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    // Off by default: Ballista logs every failed job at ERROR, which buries
    // the progress lines. `RUST_LOG` turns it back on.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("off"))
        .init();
    let options = Options::parse();
    if options.list {
        return Ok(());
    }
    let Some(datafusion_dir) = options
        .datafusion_dir
        .as_ref()
        .map(|d| std::path::absolute(d).unwrap_or_else(|_| d.clone()))
    else {
        eprintln!(
            "NOTICE: BALLISTA_SLT_DATAFUSION_DIR is not set, skipping DataFusion's \
             sqllogictest files. Set it to a DataFusion checkout's \
             datafusion/sqllogictest directory to run them."
        );
        return Ok(());
    };
    // The files' relative paths resolve from here, and the scratch
    // directories are created under it.
    std::env::set_current_dir(&datafusion_dir).map_err(|e| {
        DataFusionError::Configuration(format!(
            "cannot enter {}: {e}",
            datafusion_dir.display()
        ))
    })?;
    // `datafusion/sqllogictest` -> the checkout root.
    let root = std::env::current_dir()?
        .join("../..")
        .canonicalize()?
        .to_string_lossy()
        .trim_start_matches('/')
        .to_string();
    WORKSPACE_ROOT.set(root).expect("set once");

    let filters = options
        .filters
        .iter()
        .map(|f| Filter::from_str(f).map_err(DataFusionError::Configuration))
        .collect::<Result<Vec<_>>>()?;
    if let (Some(file), Some(engine)) = (&options.worker_file, options.worker_engine) {
        return run_worker(file, engine, &filters, options.include_explain).await;
    }
    if options.update_expected {
        // The list must describe a default run: one that runs whole files
        // and drops `EXPLAIN` records.
        if options.filters.iter().any(|f| f.contains(':')) {
            return Err(DataFusionError::Configuration(
                "--update-expected needs whole files; drop the `file:line` filters"
                    .to_string(),
            ));
        }
        if options.include_explain {
            return Err(DataFusionError::Configuration(
                "--update-expected can't be combined with --include-explain".to_string(),
            ));
        }
    }

    let expected = ExpectedFailures::load(&options.expected_failures)?;
    let engines = &options.engines;
    let launcher = Launcher {
        datafusion_dir: &datafusion_dir,
        filters: &options.filters,
        include_explain: options.include_explain,
        timeout: Duration::from_secs(options.timeout_secs),
        expected: &expected,
    };

    let files = read_test_files(&options, &filters)?;
    let total = files.len();
    eprintln!(
        "Running {total} files on {} from {}",
        engines
            .iter()
            .map(|e| e.name())
            .collect::<Vec<_>>()
            .join(", "),
        datafusion_dir.display()
    );

    let started = Instant::now();
    let done = AtomicUsize::new(0);
    let mut results = futures::stream::iter(files)
        .map(|file| {
            let (launcher, done) = (&launcher, &done);
            async move {
                let result = launcher.run_file(file, engines).await;
                eprintln!(
                    "[{:>3}/{total}] {:<50} {} ({:.1}s)",
                    done.fetch_add(1, Ordering::Relaxed) + 1,
                    result.file,
                    result
                        .outcomes
                        .iter()
                        .map(|(e, o)| format!("{}={}", e.name(), o.short()))
                        .collect::<Vec<_>>()
                        .join(" "),
                    result.elapsed.as_secs_f64()
                );
                result
            }
        })
        .buffer_unordered(options.jobs.max(1))
        .collect::<Vec<_>>()
        .await;
    results.sort_by(|a, b| a.file.cmp(&b.file));

    let check = expected.check(&results);
    let report = report::render(&results, engines, started.elapsed(), &check);
    println!("{}", report.summary);
    if let Some(path) = &options.report {
        std::fs::write(path, &report.full)?;
        eprintln!("Report written to {}", path.display());
    }

    if options.update_expected {
        let updated = expected.updated(&results);
        std::fs::write(&options.expected_failures, updated.render())?;
        eprintln!("Updated {}", options.expected_failures.display());
        return Ok(());
    }
    if check.unexpected.is_empty() {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "{} unexpected sqllogictest failures, see above. If they're expected, \
             add them to {} or rerun with --update-expected.",
            check.unexpected.len(),
            options.expected_failures.display()
        )))
    }
}

fn read_test_files(options: &Options, filters: &[Filter]) -> Result<Vec<TestFile>> {
    let mut files = read_dir_recursive(TEST_DIRECTORY)?
        .into_iter()
        .map(|p| TestFile::new(p, &[TEST_DIRECTORY]))
        .filter(|f| f.is_slt_file())
        .filter(|f| !f.relative_path_starts_with(TPCH_PREFIX) || options.include_tpch)
        .filter(|f| !f.relative_path_starts_with(ENCRYPTED_PARQUET_FILE))
        .filter(|f| !should_skip_file(&f.relative_path, filters))
        .collect::<Vec<_>>();
    files.sort_unstable();
    Ok(files)
}

/// Runs each file on each engine in a worker process.
struct Launcher<'a> {
    datafusion_dir: &'a Path,
    /// Passed on to the workers, which pick the records of their file.
    filters: &'a [String],
    include_explain: bool,
    timeout: Duration,
    expected: &'a ExpectedFailures,
}

impl Launcher<'_> {
    /// Runs `file` on each engine in turn.
    async fn run_file(&self, file: TestFile, engines: &[Engine]) -> FileResult {
        let started = Instant::now();
        let name = file
            .relative_path
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        let mut outcomes = Vec::with_capacity(engines.len());
        for &engine in engines {
            let outcome = match self.expected.skip_reason(&name, engine) {
                Some(reason) => Outcome::NotRun(reason.to_string()),
                None => self.run_on_engine(&file, engine).await,
            };
            outcomes.push((engine, outcome));
        }
        FileResult {
            file: name,
            outcomes,
            elapsed: started.elapsed(),
        }
    }

    async fn run_on_engine(&self, file: &TestFile, engine: Engine) -> Outcome {
        let exe = match std::env::current_exe() {
            Ok(exe) => exe,
            Err(e) => return Outcome::Error(format!("cannot find own executable: {e}")),
        };
        let mut command = tokio::process::Command::new(exe);
        command
            .current_dir(self.datafusion_dir)
            .env("BALLISTA_SLT_DATAFUSION_DIR", self.datafusion_dir)
            .arg("--worker-file")
            .arg(&file.path)
            .arg("--worker-engine")
            .arg(engine.name())
            .args(self.filters)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        if self.include_explain {
            command.arg("--include-explain");
        }
        // Dropping the future on timeout drops the child, which kills it.
        let output = match tokio::time::timeout(self.timeout, command.output()).await {
            Err(_elapsed) => return Outcome::TimedOut,
            Ok(Err(e)) => return Outcome::Error(format!("cannot start worker: {e}")),
            Ok(Ok(output)) => output,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(json) = stdout.lines().find_map(|l| l.strip_prefix(OUTCOME_MARKER)) {
            return serde_json::from_str(json).unwrap_or_else(|e| {
                Outcome::Error(format!("cannot read worker outcome: {e}"))
            });
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        let tail: Vec<&str> = stderr.lines().rev().take(20).collect();
        Outcome::Error(format!(
            "worker exited with {} and no outcome:\n{}",
            output.status,
            tail.into_iter().rev().collect::<Vec<_>>().join("\n")
        ))
    }
}

/// The worker process: runs one file on one engine and prints the outcome.
async fn run_worker(
    path: &Path,
    engine: Engine,
    filters: &[Filter],
    include_explain: bool,
) -> Result<()> {
    let file = TestFile::new(path.to_path_buf(), &[TEST_DIRECTORY]);
    let outcome = match parse_file::<DFColumnType>(&file.path) {
        Ok(records) => {
            let records = if include_explain {
                records
            } else {
                records.into_iter().filter_map(without_explain).collect()
            };
            run_file_on_engine(&file.path, &file.relative_path, &records, filters, engine)
                .await
        }
        Err(e) => Outcome::Error(format!("cannot parse file: {e}")),
    };
    let json = serde_json::to_string(&outcome)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    println!("{OUTCOME_MARKER}{json}");
    // The standalone cluster's tasks never finish; don't wait for them.
    std::process::exit(0)
}

async fn run_file_on_engine(
    path: &Path,
    relative_path: &Path,
    records: &[Record<DFColumnType>],
    filters: &[Filter],
    engine: Engine,
) -> Outcome {
    let configurations = match test_configurations(path) {
        Ok(configurations) => configurations,
        Err(e) => return Outcome::Error(format!("bad configMatrix: {e}")),
    };

    let mut failures = vec![];
    for configuration in configurations {
        let Some(test_ctx) = TestContext::try_new_for_test_file(relative_path).await
        else {
            return Outcome::Skipped;
        };
        if let Err(e) = setup_scratch_dir(relative_path) {
            return Outcome::Error(format!("cannot set up scratch dir: {e}"));
        }
        if let Err(e) = test_ctx
            .apply_config_overrides(configuration.settings(), relative_path)
            .await
        {
            return Outcome::Error(e.to_string());
        }
        let ctx = match engine.adaptive() {
            None => test_ctx.session_ctx().clone(),
            Some(adaptive) => match ballista_context(&test_ctx, adaptive).await {
                Ok(ctx) => ctx,
                Err(e) => {
                    return Outcome::Error(format!("cannot start Ballista: {e}"));
                }
            },
        };

        let prefix = if configuration.is_empty() {
            String::new()
        } else {
            format!("[configMatrix {:?}] ", configuration.settings())
        };
        for mut failure in run_records(ctx, relative_path, records, filters, engine).await
        {
            failure.message.insert_str(0, &prefix);
            failures.push(failure);
        }
        // `test_ctx` owns the file's temporary directory; keep it until the
        // records have run.
        drop(test_ctx);
    }
    Outcome::Ran { failures }
}

/// A standalone Ballista context with the file's session state: its config,
/// catalogs and functions. The executor is built from the same state.
async fn ballista_context(
    test_ctx: &TestContext,
    adaptive: bool,
) -> Result<SessionContext> {
    let state = test_ctx.session_ctx().state();
    let codec = ComposedLogicalExtensionCodec::new(vec![
        Arc::new(BallistaLogicalExtensionCodec::default()),
        Arc::new(InProcessTableCodec),
    ]);
    let config = state
        .config()
        .clone()
        .upgrade_for_ballista()
        .with_ballista_logical_extension_codec(Arc::new(codec))
        .with_ballista_physical_extension_codec(Arc::new(InProcessSinkCodec::default()))
        .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, adaptive)
        .set_usize(BALLISTA_STANDALONE_PARALLELISM, STANDALONE_PARALLELISM);
    let state = SessionStateBuilder::new_from_existing(state)
        .with_config(config)
        .build();
    SessionContext::standalone_with_state(state).await
}

async fn run_records(
    ctx: SessionContext,
    relative_path: &Path,
    records: &[Record<DFColumnType>],
    filters: &[Filter],
    engine: Engine,
) -> Vec<Failure> {
    let config_change_errors = Arc::new(Mutex::new(Vec::new()));
    let mut runner = sqllogictest::Runner::new(|| {
        let ctx = ctx.clone();
        let relative_path = relative_path.to_path_buf();
        let config_change_errors = Arc::clone(&config_change_errors);
        async move {
            Ok(DataFusion::new(ctx, relative_path, ProgressBar::hidden())
                .with_config_change_errors(config_change_errors))
        }
    });
    runner.add_label(DATAFUSION_LABEL);
    if engine.adaptive().is_some() {
        runner.add_label(BALLISTA_LABEL);
    }
    runner.with_column_validator(strict_column_validator);
    runner.with_normalizer(value_normalizer);
    runner.with_validator(validator);

    let mut failures = vec![];
    for record in records {
        if let Record::Halt { .. } = record {
            break;
        }
        if should_skip_record::<DataFusion>(record, filters) {
            continue;
        }
        if let Err(e) = runner.run_async(record.clone()).await {
            failures.push(Failure {
                line: e.location().line(),
                message: e.to_string(),
            });
        }
    }
    runner.shutdown_async().await;

    for message in config_change_errors.lock().unwrap().drain(..) {
        failures.push(Failure { line: 0, message });
    }
    failures
}

/// DataFusion's `convert_batches` replaces its checkout root with
/// `WORKSPACE_ROOT` in plan output, but finds that root from the crate's
/// build-time location, which for the published crate is the cargo registry.
/// Do the same with the checkout the files run from, for every value, then
/// compare like DataFusion's runner.
fn validator(
    normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    let Some(root) = WORKSPACE_ROOT.get() else {
        return df_value_validator(normalizer, actual, expected);
    };
    let actual = actual
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| v.replace(root.as_str(), "WORKSPACE_ROOT"))
                .collect()
        })
        .collect::<Vec<Vec<String>>>();
    df_value_validator(normalizer, &actual, expected)
}

/// Ballista's plans differ from DataFusion's, so by default `EXPLAIN` records
/// that expect a plan are dropped. `EXPLAIN ANALYZE` runs the statement, and
/// later records can depend on what it did (`dml_delete.slt` deletes rows
/// with it), so those still run, with their output ignored.
fn without_explain(record: Record<DFColumnType>) -> Option<Record<DFColumnType>> {
    // `Some(analyze)` for an `EXPLAIN` statement.
    let explain = |sql: &str| {
        let mut words = sql.split_whitespace().map(str::to_ascii_lowercase);
        (words.next()? == "explain").then(|| words.next().is_some_and(|w| w == "analyze"))
    };
    match record {
        Record::Query {
            ref sql,
            expected: QueryExpect::Results { .. },
            ..
        } if explain(sql) == Some(false) => None,
        Record::Query {
            loc,
            conditions,
            connection,
            sql,
            expected: QueryExpect::Results { .. },
            retry,
            ..
        } if explain(&sql) == Some(true) => Some(Record::Statement {
            loc,
            conditions,
            connection,
            sql,
            expected: StatementExpect::Ok,
            retry,
        }),
        record => Some(record),
    }
}
