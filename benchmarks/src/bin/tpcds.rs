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

use ballista::extension::SessionConfigExt;
use ballista::prelude::SessionContextExt;
use ballista_benchmarks::{
    answer_statement_index, compare_results, execute_query_capturing_answer,
    register_parquet_tables,
};
use ballista_core::object_store::{
    session_config_with_s3_support, session_state_with_s3_support,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::fs;
use std::time::Instant;
use structopt::StructOpt;

/// The 24 TPC-DS tables.
const TABLES: &[&str] = &[
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

/// Queries excluded from the correctness gate, with the reason. The gate runs
/// under the default (static) planner; this list reflects that configuration.
/// Remove an entry as the underlying cause is fixed.
const SKIP: &[(usize, &str)] = &[
    // Distributed execution diverges from single-process DataFusion on the same
    // data (confirmed reproducible; DataFusion is correct under both join modes).
    // See https://github.com/apache/datafusion-ballista/issues/2046
    (
        38,
        "distributed INTERSECT diverges from DataFusion (issue #2046)",
    ),
    (
        87,
        "distributed EXCEPT diverges from DataFusion (issue #2046)",
    ),
    // Non-deterministic: LIMIT/ORDER BY ties without a total order make the
    // result vary run-to-run in both engines, so a row-by-row diff is unstable.
    (31, "non-deterministic (ORDER BY ties; varies run-to-run)"),
    (
        71,
        "non-deterministic (ORDER BY ext_price ties; varies run-to-run)",
    ),
    // tpcgen-cli's TPC-DS schema uses column names that differ from the
    // DataFusion (branch-54) query text, so these fail at plan time. Not a
    // Ballista issue (e.g. cr_return_amount_inc_tax vs cr_return_amt_inc_tax).
    (64, "tpcgen-cli schema column-name mismatch with query text"),
    (81, "tpcgen-cli schema column-name mismatch with query text"),
    (84, "tpcgen-cli schema column-name mismatch with query text"),
    (85, "tpcgen-cli schema column-name mismatch with query text"),
    (93, "tpcgen-cli schema column-name mismatch with query text"),
    // Note: the adaptive planner (AQE on) additionally fails many queries with an
    // `EmptyExec invalid partition` assertion (issue #2047); the gate runs the
    // static planner, so those are not listed here.
];

#[derive(Debug, StructOpt)]
#[structopt(name = "tpcds", about = "Ballista TPC-DS correctness runner")]
struct Opt {
    /// Query number (1-99). If not specified, runs all non-skipped queries.
    #[structopt(short, long)]
    query: Option<usize>,

    /// Show query text and results.
    #[structopt(short, long)]
    debug: bool,

    /// Path to data files (local path or object-store URL).
    #[structopt(required = true, short = "p", long = "path")]
    path: String,

    /// Number of partitions (session target_partitions).
    #[structopt(short = "n", long = "partitions", default_value = "2")]
    partitions: usize,

    /// Batch size.
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,

    /// Ballista scheduler host.
    #[structopt(long = "host")]
    host: String,

    /// Ballista scheduler port.
    #[structopt(long = "port")]
    port: u16,

    /// Config overrides in key=value form (repeatable).
    #[structopt(short = "c", long = "config", number_of_values = 1)]
    config_overrides: Vec<String>,

    /// Verify each Ballista result against single-process DataFusion.
    #[structopt(long = "verify")]
    verify: bool,
}

/// Split a query file into statements, dropping full-line `--` comments and
/// blank statements. TPC-DS files carry a leading TPC copyright comment.
fn split_statements(contents: &str) -> Vec<String> {
    let stripped: String = contents
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    stripped
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn get_query_sql(query: usize) -> Result<Vec<String>> {
    let possibilities = [
        format!("queries-tpcds/q{query}.sql"),
        format!("benchmarks/queries-tpcds/q{query}.sql"),
    ];
    let mut errors = vec![];
    for filename in &possibilities {
        match fs::read_to_string(filename) {
            Ok(contents) => return Ok(split_statements(&contents)),
            Err(e) => errors.push(format!("{filename}: {e}")),
        }
    }
    Err(DataFusionError::Plan(format!(
        "Could not find query {query}: {errors:?}"
    )))
}

/// The queries to run: an explicit `--query`, else 1..=99 minus `skip`.
fn selected_queries(explicit: Option<usize>, skip: &[(usize, &str)]) -> Vec<usize> {
    if let Some(q) = explicit {
        return vec![q];
    }
    let skipped: std::collections::HashSet<usize> =
        skip.iter().map(|(id, _)| *id).collect();
    (1..=99).filter(|q| !skipped.contains(q)).collect()
}

/// Run a single TPC-DS query end to end: load its SQL, stand up a fresh
/// Ballista session, execute it on the cluster, and (if `oracle_ctx` is
/// set) verify the result against single-process DataFusion.
///
/// Every fallible step is tagged with a `<phase>: ` prefix and returned as
/// an `Err` rather than aborting the process, so the caller can record the
/// failure against this query and move on to the next one.
async fn run_one_query(
    opt: &Opt,
    address: &str,
    oracle_ctx: Option<&SessionContext>,
    query: usize,
) -> Result<()> {
    let sqls = get_query_sql(query)
        .map_err(|e| DataFusionError::Execution(format!("load: {e}")))?;

    // Build a fresh Ballista session per query (mirrors tpch.rs).
    let mut config = session_config_with_s3_support()
        .with_target_partitions(opt.partitions)
        .with_ballista_job_name(&format!("TPC-DS q{query}"))
        .with_batch_size(opt.batch_size)
        .with_collect_statistics(true);
    for kv in &opt.config_overrides {
        if let Some((k, v)) = kv.split_once('=') {
            if let Err(e) = config.options_mut().set(k.trim(), v.trim()) {
                println!("Warning: could not set config '{kv}': {e}");
            }
        } else {
            println!("Warning: ignoring invalid config override '{kv}'");
        }
    }
    let state = session_state_with_s3_support(config)
        .map_err(|e| DataFusionError::Execution(format!("session-state: {e}")))?;
    let ctx = SessionContext::remote_with_state(address, state)
        .await
        .map_err(|e| DataFusionError::Execution(format!("connect: {e}")))?;
    register_parquet_tables(&ctx, TABLES, opt.path.as_str(), opt.debug)
        .await
        .map_err(|e| DataFusionError::Execution(format!("register-tables: {e}")))?;

    // Run the query on the cluster, capturing the answer statement's result.
    let answer_idx = answer_statement_index(&sqls);
    let start = Instant::now();
    let mut batches = vec![];
    let mut run_err = None;
    for (idx, sql) in sqls.iter().enumerate() {
        if opt.debug {
            println!("Query {query} SQL:\n{sql}");
        }
        match ctx.sql(sql).await {
            Ok(df) => match df.collect().await {
                Ok(collected) => {
                    if idx == answer_idx {
                        batches = collected;
                    }
                }
                Err(e) => {
                    run_err = Some(format!("collect: {e}"));
                    break;
                }
            },
            Err(e) => {
                run_err = Some(format!("plan: {e}"));
                break;
            }
        }
    }
    let elapsed = start.elapsed().as_secs_f64();

    if let Some(e) = run_err {
        println!("Query {query} FAILED to run in {elapsed:.3}s: {e}");
        return Err(DataFusionError::Execution(e));
    }
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Query {query} took {elapsed:.3}s and returned {row_count} rows");

    if let Some(oracle_ctx) = oracle_ctx {
        let expected = execute_query_capturing_answer(oracle_ctx, &sqls, opt.debug)
            .await
            .map_err(|e| DataFusionError::Execution(format!("oracle: {e}")))?;
        match compare_results(&expected, &batches) {
            Ok(()) => println!("Query {query} verified against DataFusion: OK"),
            Err(e) => {
                println!("Query {query} VERIFY MISMATCH: {e}");
                return Err(DataFusionError::Execution(format!("verify: {e}")));
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    let address = format!("df://{}:{}", opt.host, opt.port);

    // Oracle context (single-process DataFusion), built once when verifying.
    // Not per-query, so a failure here is genuinely fatal to the whole run.
    let oracle_ctx = if opt.verify {
        let cfg = SessionConfig::new()
            .with_target_partitions(opt.partitions)
            .with_batch_size(opt.batch_size);
        let ctx = SessionContext::new_with_config(cfg);
        register_parquet_tables(&ctx, TABLES, opt.path.as_str(), opt.debug).await?;
        Some(ctx)
    } else {
        None
    };

    let mut failures: Vec<(usize, String)> = vec![];

    for query in selected_queries(opt.query, SKIP) {
        if let Err(e) = run_one_query(&opt, &address, oracle_ctx.as_ref(), query).await {
            eprintln!("Query {query} FAILED: {e}");
            failures.push((query, e.to_string()));
        }
    }

    if !failures.is_empty() {
        eprintln!("\n{} query failure(s):", failures.len());
        for (q, e) in &failures {
            eprintln!("  q{q}: {e}");
        }
        return Err(DataFusionError::Execution(format!(
            "{} TPC-DS query failure(s)",
            failures.len()
        )));
    }
    println!("\nAll selected TPC-DS queries passed.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loader_strips_comments_and_splits_statements() {
        let sqls = split_statements("-- c\nselect 1;\n-- d\nselect 2;\n");
        assert_eq!(sqls, vec!["select 1".to_string(), "select 2".to_string()]);
    }

    #[test]
    fn selected_queries_excludes_skiplist() {
        // With no explicit --query, we run 1..=99 minus the skip list. Use a
        // synthetic skip list so this assertion is independent of the real
        // SKIP constant's contents.
        let skip: &[(usize, &str)] = &[(5, "x"), (42, "y")];
        let selected = selected_queries(None, skip);
        assert_eq!(selected.len(), 99 - skip.len());
        for (id, _) in skip {
            assert!(!selected.contains(id), "skip {id} must be excluded");
        }
    }

    #[test]
    fn explicit_query_overrides_skiplist() {
        let skip: &[(usize, &str)] = &[(1, "should be overridden")];
        assert_eq!(selected_queries(Some(1), skip), vec![1]);
    }
}
