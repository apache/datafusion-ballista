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

/// Queries excluded from the correctness gate, with the reason.
/// Remove an entry as the underlying cause is fixed.
const SKIP: &[(usize, &str)] = &[
    // Non-deterministic: LIMIT/ORDER BY ties without a total order make the
    // result vary run-to-run in both engines, so a row-by-row diff is unstable.
    (31, "non-deterministic (ORDER BY ties; varies run-to-run)"),
    (
        71,
        "non-deterministic (ORDER BY ext_price ties; varies run-to-run)",
    ),
];

/// Column renames applied to the query text before planning, as
/// `(query text, tpcgen-cli)` pairs.
///
/// `tpcgen-cli` names three columns differently from the TPC-DS spec, and the
/// DataFusion query text follows the spec, so these queries would otherwise
/// fail at plan time with "column not found". These are pure renames -- the
/// data is present under the other name -- so rewriting the reference is
/// enough. The identical rewrite is applied to the Ballista and oracle runs,
/// so the comparison stays apples-to-apples.
///
/// This is applied at load time rather than by editing the files under
/// `benchmarks/queries-tpcds/`, because `dev/vendor-tpcds-queries.sh`
/// re-downloads all 99 queries and would clobber any local edit.
///
/// Drop a pair once `tpcgen-cli` renames the column to its spec name.
const COLUMN_RENAMES: &[(&str, &str)] = &[
    // income_band: affects q64, q84.
    ("ib_income_band_sk", "ib_income_band_id"),
    // reason: affects q85, q93.
    ("r_reason_desc", "r_reason_description"),
    // catalog_returns: affects q81.
    ("cr_return_amt_inc_tax", "cr_return_amount_inc_tax"),
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

/// Replace whole-word occurrences of `from` with `to`.
///
/// A plain `str::replace` is not safe here: `r_reason_desc` is a prefix of
/// `r_reason_description`, so a substring replace would corrupt an already
/// correct reference. A character is part of a word if it is alphanumeric or
/// `_`, which matches how SQL identifiers are spelled in these queries.
fn replace_word(haystack: &str, from: &str, to: &str) -> String {
    let is_word = |c: char| c.is_alphanumeric() || c == '_';
    let mut out = String::with_capacity(haystack.len());
    let mut rest = haystack;
    while let Some(pos) = rest.find(from) {
        let (before, after) = rest.split_at(pos);
        let tail = &after[from.len()..];
        let boundary_ok = !before.chars().next_back().is_some_and(is_word)
            && !tail.chars().next().is_some_and(is_word);
        out.push_str(before);
        out.push_str(if boundary_ok { to } else { from });
        rest = tail;
    }
    out.push_str(rest);
    out
}

/// Rewrite spec column names to the names `tpcgen-cli` actually emits.
fn apply_column_renames(sql: &str) -> String {
    COLUMN_RENAMES
        .iter()
        .fold(sql.to_string(), |acc, (from, to)| {
            replace_word(&acc, from, to)
        })
}

fn get_query_sql(query: usize) -> Result<Vec<String>> {
    let possibilities = [
        format!("queries-tpcds/q{query}.sql"),
        format!("benchmarks/queries-tpcds/q{query}.sql"),
    ];
    let mut errors = vec![];
    for filename in &possibilities {
        match fs::read_to_string(filename) {
            Ok(contents) => {
                return Ok(split_statements(&apply_column_renames(&contents)));
            }
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

    #[test]
    fn replace_word_only_matches_whole_identifiers() {
        assert_eq!(
            replace_word("a.r_reason_desc, b", "r_reason_desc", "x"),
            "a.x, b"
        );
        // A longer identifier that merely contains the needle is left alone.
        assert_eq!(
            replace_word("r_reason_desc_2", "r_reason_desc", "x"),
            "r_reason_desc_2"
        );
        assert_eq!(
            replace_word("my_r_reason_desc", "r_reason_desc", "x"),
            "my_r_reason_desc"
        );
    }

    #[test]
    fn column_renames_are_idempotent() {
        // `r_reason_desc` is a prefix of its replacement, so a second pass must
        // not extend it again.
        let once = apply_column_renames("select r_reason_desc from reason");
        assert_eq!(once, "select r_reason_description from reason");
        assert_eq!(apply_column_renames(&once), once);
    }

    #[test]
    fn column_renames_cover_every_spec_name() {
        let sql = "ib_income_band_sk, r_reason_desc, cr_return_amt_inc_tax";
        assert_eq!(
            apply_column_renames(sql),
            "ib_income_band_id, r_reason_description, cr_return_amount_inc_tax"
        );
    }

    #[test]
    fn renames_are_applied_when_loading_a_query() {
        // q93 references `r_reason_desc`; after loading, only the tpcgen-cli
        // spelling should remain. Guards against the rewrite being dropped
        // from the load path.
        let Ok(sqls) = get_query_sql(93) else {
            // Query files are not present in every build context.
            return;
        };
        let joined = sqls.join(" ");
        assert!(joined.contains("r_reason_description"));
        assert!(!replace_word(&joined, "r_reason_desc", "?").contains('?'));
    }
}
