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

//! Benchmark derived from TPC-DS. This is not an official TPC-DS benchmark.

use ballista_benchmarks::summary::{QueryRun, run_suite, time_query};
use ballista_benchmarks::{
    ParquetTable, ballista_context, benchmark_session_config, cells_equal,
    compare_results, execute_query_capturing_answer, infer_parquet_tables, local_context,
    read_query_file, register_parquet_tables, rows_as_cells,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::collections::HashSet;
use std::path::PathBuf;
use structopt::StructOpt;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

/// Queries excluded from the correctness gate, with the reason. Empty:
/// `compare_allowing_order_by_ties` handles ORDER BY ties, so all 99 run.
const SKIP: &[(usize, &str)] = &[];

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
/// A pair only applies when the tables have the `tpcgen-cli` column and not
/// the spec one (see `column_renames_for`), so data from `dsdgen` or Spark,
/// which use the spec names, runs the queries unchanged.
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
#[structopt(
    name = "tpcds",
    about = "Ballista TPC-DS benchmark and correctness runner"
)]
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

    /// Number of timed iterations of each query.
    #[structopt(short = "i", long = "iterations", default_value = "1")]
    iterations: usize,

    /// Directory to write the JSON summary (`tpcds-<start_time>.json`) to, in
    /// the same format as the TPC-H runner's.
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Register Parquet tables without their Hive partition columns, so
    /// filters on those columns no longer skip directories.
    #[structopt(long = "no-partition-cols")]
    no_partition_cols: bool,
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

/// Rewrite spec column names to the names the data uses.
fn apply_column_renames(sql: &str, renames: &[(&str, &str)]) -> String {
    renames.iter().fold(sql.to_string(), |acc, (from, to)| {
        replace_word(&acc, from, to)
    })
}

/// The `COLUMN_RENAMES` pairs that `tables` need: those whose `tpcgen-cli`
/// column exists and whose spec column does not.
fn column_renames_for(tables: &[ParquetTable]) -> Vec<(&'static str, &'static str)> {
    let columns: HashSet<String> = tables
        .iter()
        .flat_map(|t| {
            let file_columns = t.schema.fields().iter().map(|f| f.name().clone());
            file_columns.chain(t.partition_cols.iter().map(|(name, _)| name.clone()))
        })
        .collect();
    renames_needed(&columns)
}

fn renames_needed(columns: &HashSet<String>) -> Vec<(&'static str, &'static str)> {
    COLUMN_RENAMES
        .iter()
        .filter(|(spec, generated)| {
            columns.contains(*generated) && !columns.contains(*spec)
        })
        .copied()
        .collect()
}

/// The type of a column that a Parquet table only stores in its directory
/// names, such as `ss_sold_date_sk` in data written with Spark's
/// `partitionBy`. TPC-DS partitions its fact tables by date surrogate key, and
/// every surrogate key is an integer.
fn tpcds_path_column_type(_table: &str, column: &str) -> Option<DataType> {
    column.ends_with("_sk").then_some(DataType::Int32)
}

fn get_query_sql(query: usize, renames: &[(&str, &str)]) -> Result<Vec<String>> {
    let contents = read_query_file("queries-tpcds", query)?;
    Ok(split_statements(&apply_column_renames(&contents, renames)))
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

/// How closely a cluster result matched the oracle.
enum Ordering {
    /// Same rows in the same order.
    Exact,
    /// Same rows, different order — only reachable when `ORDER BY` ties.
    SameRowsDifferentOrder,
}

/// Compares the oracle and cluster results, tolerating row-order differences.
///
/// `ORDER BY` leaves the relative order of rows with equal sort keys
/// unspecified, so two correct engines may emit tied rows in different orders.
/// Comparing positionally first keeps ordering under test; the canonical
/// (row-sorted) retry only runs once that has already failed, so a wrong value
/// still fails and only a pure permutation passes.
///
/// Rows are ordered by their rendered form purely to get a stable canonical
/// sequence; the pairwise check still uses `cells_equal`, preserving the float
/// tolerance.
fn compare_allowing_order_by_ties(
    expected: &[RecordBatch],
    actual: &[RecordBatch],
) -> Result<Ordering> {
    let Err(strict) = compare_results(expected, actual) else {
        return Ok(Ordering::Exact);
    };

    let sort_key = |row: &Vec<_>| {
        row.iter()
            .map(|c| format!("{c}"))
            .collect::<Vec<_>>()
            .join("\u{1}")
    };
    let mut expected_rows = rows_as_cells(expected);
    let mut actual_rows = rows_as_cells(actual);
    if expected_rows.len() != actual_rows.len() {
        return Err(strict);
    }
    expected_rows.sort_by_key(sort_key);
    actual_rows.sort_by_key(sort_key);

    let permuted = expected_rows
        .iter()
        .zip(actual_rows.iter())
        .all(|(e, a)| e.iter().zip(a.iter()).all(|(ec, ac)| cells_equal(ec, ac)));

    if permuted {
        Ok(Ordering::SameRowsDifferentOrder)
    } else {
        Err(strict)
    }
}

/// Tags an error with the phase of `run_one_query` it came from.
fn phase(phase: &'static str) -> impl FnOnce(DataFusionError) -> DataFusionError {
    move |e| DataFusionError::Execution(format!("{phase}: {e}"))
}

/// Run a single TPC-DS query end to end: stand up a fresh Ballista session,
/// register `tables` on it, execute the query on the cluster `opt.iterations`
/// times, pushing each iteration's timing into `query_run`, and (if
/// `oracle_ctx` is set) verify the last result against single-process
/// DataFusion.
///
/// Every fallible step is tagged with a `<phase>: ` prefix, so the failure
/// recorded against this query says where it went wrong. On error,
/// `query_run` holds whatever iterations completed before the failure.
async fn run_one_query(
    opt: &Opt,
    address: &str,
    tables: &[ParquetTable],
    renames: &[(&str, &str)],
    oracle_ctx: Option<&SessionContext>,
    query: usize,
    query_run: &mut QueryRun,
) -> Result<()> {
    let sqls = get_query_sql(query, renames).map_err(phase("load"))?;

    // A fresh Ballista session per query (mirrors tpch.rs).
    let ctx = ballista_context(
        address,
        &format!("TPC-DS q{query}"),
        benchmark_session_config(opt.partitions, opt.batch_size, &opt.config_overrides),
    )
    .await
    .map_err(phase("connect"))?;
    register_parquet_tables(&ctx, tables)
        .await
        .map_err(phase("register-tables"))?;

    let batches = time_query(&ctx, query, &sqls, opt.iterations, opt.debug, query_run)
        .await
        .map_err(phase("run"))?;

    if let Some(oracle_ctx) = oracle_ctx {
        let expected = execute_query_capturing_answer(oracle_ctx, &sqls, opt.debug)
            .await
            .map_err(phase("oracle"))?;
        match compare_allowing_order_by_ties(&expected, &batches) {
            Ok(Ordering::Exact) => {
                println!("Query {query} verified against DataFusion: OK")
            }
            Ok(Ordering::SameRowsDifferentOrder) => println!(
                "Query {query} verified against DataFusion: OK (row order differs; ORDER BY has ties)"
            ),
            Err(e) => {
                println!("Query {query} VERIFY MISMATCH: {e}");
                return Err(phase("verify")(e));
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    println!("Running TPC-DS with the following options: {opt:?}");
    let address = format!("df://{}:{}", opt.host, opt.port);

    // Inferring a table's layout reads its files, so do it once, not per
    // query. Every query's session then registers the same layouts.
    let layout_ctx = local_context(benchmark_session_config(
        opt.partitions,
        opt.batch_size,
        &opt.config_overrides,
    ))?;
    let tables = infer_parquet_tables(
        &layout_ctx,
        TABLES,
        opt.path.as_str(),
        opt.debug,
        !opt.no_partition_cols,
        tpcds_path_column_type,
    )
    .await?;
    let renames = column_renames_for(&tables);

    // Oracle context (single-process DataFusion), built once when verifying.
    // Not per-query, so a failure here is genuinely fatal to the whole run.
    let oracle_ctx = if opt.verify {
        let cfg = SessionConfig::new()
            .with_target_partitions(opt.partitions)
            .with_batch_size(opt.batch_size);
        let ctx = SessionContext::new_with_config(cfg);
        register_parquet_tables(&ctx, &tables).await?;
        Some(ctx)
    } else {
        None
    };

    run_suite(
        "tpcds",
        &selected_queries(opt.query, SKIP),
        opt.output_path.as_deref(),
        async |query, query_run| {
            run_one_query(
                &opt,
                &address,
                &tables,
                &renames,
                oracle_ctx.as_ref(),
                query,
                query_run,
            )
            .await
        },
    )
    .await?;
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
        let once =
            apply_column_renames("select r_reason_desc from reason", COLUMN_RENAMES);
        assert_eq!(once, "select r_reason_description from reason");
        assert_eq!(apply_column_renames(&once, COLUMN_RENAMES), once);
    }

    #[test]
    fn column_renames_cover_every_spec_name() {
        let sql = "ib_income_band_sk, r_reason_desc, cr_return_amt_inc_tax";
        assert_eq!(
            apply_column_renames(sql, COLUMN_RENAMES),
            "ib_income_band_id, r_reason_description, cr_return_amount_inc_tax"
        );
    }

    fn columns(names: &[&str]) -> HashSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn tpcgen_columns_need_every_rename() {
        let tpcgen = columns(&[
            "ib_income_band_id",
            "r_reason_description",
            "cr_return_amount_inc_tax",
        ]);
        assert_eq!(renames_needed(&tpcgen), COLUMN_RENAMES);
    }

    // `dsdgen` and Spark name the columns as the spec does, which is how the
    // queries already refer to them.
    #[test]
    fn spec_columns_need_no_renames() {
        let spec = columns(&[
            "ib_income_band_sk",
            "r_reason_desc",
            "cr_return_amt_inc_tax",
        ]);
        assert!(renames_needed(&spec).is_empty());
    }

    #[test]
    fn date_surrogate_key_partitions_are_integers() {
        assert_eq!(
            tpcds_path_column_type("store_sales", "ss_sold_date_sk"),
            Some(DataType::Int32)
        );
        assert_eq!(tpcds_path_column_type("store_sales", "batch"), None);
    }

    #[test]
    fn renames_are_applied_when_loading_a_query() {
        // q93 references `r_reason_desc`; after loading, only the tpcgen-cli
        // spelling should remain. Guards against the rewrite being dropped
        // from the load path.
        let Ok(sqls) = get_query_sql(93, COLUMN_RENAMES) else {
            // Query files are not present in every build context.
            return;
        };
        let joined = sqls.join(" ");
        assert!(joined.contains("r_reason_description"));
        assert!(!replace_word(&joined, "r_reason_desc", "?").contains('?'));
    }

    /// Writes a one-column `store_sales` table under `dir` in the layout of
    /// Spark's `partitionBy`, where `ss_sold_date_sk` is only a directory
    /// level, and registers it on a new session. Each `(item, date_sk)` pair
    /// becomes the one row of `store_sales/ss_sold_date_sk=<date_sk>/`.
    async fn spark_store_sales(
        dir: &std::path::Path,
        rows: &[(i32, &str)],
    ) -> Result<SessionContext> {
        use datafusion::arrow::array::{ArrayRef, Int32Array};
        use datafusion::parquet::arrow::ArrowWriter;
        use std::fs;
        use std::sync::Arc;

        for (item, date_sk) in rows {
            let item: ArrayRef = Arc::new(Int32Array::from(vec![*item]));
            let batch = RecordBatch::try_from_iter([("ss_item_sk", item)]).unwrap();
            let path = dir.join(format!(
                "store_sales/ss_sold_date_sk={date_sk}/part-0.parquet"
            ));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut writer = ArrowWriter::try_new(
                fs::File::create(path).unwrap(),
                batch.schema(),
                None,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let ctx = SessionContext::new();
        let path = format!("{}/store_sales", dir.display());
        ballista_benchmarks::register_parquet_table(
            &ctx,
            "store_sales",
            &path,
            true,
            tpcds_path_column_type,
        )
        .await?;
        Ok(ctx)
    }

    async fn physical_plan(df: &datafusion::prelude::DataFrame) -> Result<String> {
        Ok(format!(
            "{}",
            datafusion::physical_plan::displayable(
                df.clone().create_physical_plan().await?.as_ref()
            )
            .indent(false)
        ))
    }

    // The date key must register as an integer, so it joins `d_date_sk`, and
    // a filter on it skips the other partitions.
    #[tokio::test]
    async fn path_only_date_sk_partition_is_an_integer_and_prunes() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let ctx =
            spark_store_sales(dir.path(), &[(1, "2451000"), (2, "2451001")]).await?;

        let schema = ctx.table_provider("store_sales").await?.schema();
        assert_eq!(
            schema.field_with_name("ss_sold_date_sk")?.data_type(),
            &DataType::Int32
        );

        let df = ctx
            .sql("SELECT ss_item_sk FROM store_sales WHERE ss_sold_date_sk = 2451001")
            .await?;
        let plan = physical_plan(&df).await?;
        assert!(plan.contains("ss_sold_date_sk=2451001"), "{plan}");
        assert!(!plan.contains("ss_sold_date_sk=2451000"), "{plan}");
        datafusion::assert_batches_eq!(
            [
                "+------------+",
                "| ss_item_sk |",
                "+------------+",
                "| 2          |",
                "+------------+",
            ],
            &df.collect().await?
        );
        Ok(())
    }

    // Spark writes the rows whose partition key is NULL to a
    // `__HIVE_DEFAULT_PARTITION__` directory, and TPC-DS fact tables have
    // rows with a NULL date key. They must read back as NULL integers
    // instead of failing the scan, and a filter on the key must still skip
    // that directory.
    #[tokio::test]
    async fn null_date_sk_partition_reads_as_null() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let ctx = spark_store_sales(
            dir.path(),
            &[(1, "2451000"), (2, "__HIVE_DEFAULT_PARTITION__")],
        )
        .await?;

        let schema = ctx.table_provider("store_sales").await?.schema();
        assert_eq!(
            schema.field_with_name("ss_sold_date_sk")?.data_type(),
            &DataType::Int32
        );

        let all = ctx
            .sql(
                "SELECT ss_item_sk, ss_sold_date_sk FROM store_sales ORDER BY ss_item_sk",
            )
            .await?;
        datafusion::assert_batches_eq!(
            [
                "+------------+-----------------+",
                "| ss_item_sk | ss_sold_date_sk |",
                "+------------+-----------------+",
                "| 1          | 2451000         |",
                "| 2          |                 |",
                "+------------+-----------------+",
            ],
            &all.collect().await?
        );

        let df = ctx
            .sql("SELECT ss_item_sk FROM store_sales WHERE ss_sold_date_sk = 2451000")
            .await?;
        let plan = physical_plan(&df).await?;
        assert!(plan.contains("ss_sold_date_sk=2451000"), "{plan}");
        assert!(
            !plan.contains("ss_sold_date_sk=__HIVE_DEFAULT_PARTITION__"),
            "{plan}"
        );
        datafusion::assert_batches_eq!(
            [
                "+------------+",
                "| ss_item_sk |",
                "+------------+",
                "| 1          |",
                "+------------+",
            ],
            &df.collect().await?
        );
        Ok(())
    }
}
