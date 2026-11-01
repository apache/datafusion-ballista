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

//! Shared helpers for the TPC-H and TPC-DS benchmark/correctness binaries.
//!
//! These are benchmark-agnostic: result comparison against a DataFusion oracle,
//! path resolution, answer-statement selection, Ballista session setup, Parquet
//! table registration, and suite timing (see [`summary`]).

pub mod summary;

use ballista::extension::SessionConfigExt;
use ballista::prelude::SessionContextExt;
use ballista_core::object_store::{
    session_config_with_s3_support, session_state_with_s3_support,
};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::execution::options::ReadOptions;
use datafusion::prelude::{
    Expr, ParquetReadOptions, SessionConfig, SessionContext, cast, ident, lit, nullif,
};
use futures::TryStreamExt;
use futures::future::try_join_all;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// Maximum relative-or-absolute difference tolerated between floating-point
/// cells. Distributed (Ballista) and single-process (DataFusion) execution
/// aggregate in different orders, and float `sum` is non-associative, so ratio
/// queries can differ in their last digits.
const FLOAT_TOLERANCE: f64 = 1e-6;

pub enum Cell {
    Null,
    Float(f64),
    Text(String),
}

impl std::fmt::Display for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cell::Null => f.write_str("NULL"),
            Cell::Float(x) => write!(f, "{x}"),
            Cell::Text(s) => f.write_str(s),
        }
    }
}

pub fn cell_at(column: &ArrayRef, row: usize) -> Cell {
    if column.is_null(row) {
        return Cell::Null;
    }
    match column.data_type() {
        DataType::Float64 => Cell::Float(
            column
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Float32 => Cell::Float(
            column
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row) as f64,
        ),
        _ => Cell::Text(col_str(column, row)),
    }
}

pub fn rows_as_cells(batches: &[RecordBatch]) -> Vec<Vec<Cell>> {
    let mut rows = vec![];
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(batch.columns().iter().map(|c| cell_at(c, row)).collect());
        }
    }
    rows
}

pub fn floats_close(a: f64, b: f64) -> bool {
    if a == b {
        return true;
    }
    (a - b).abs() <= FLOAT_TOLERANCE * a.abs().max(b.abs()).max(1.0)
}

pub fn cells_equal(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (Cell::Null, Cell::Null) => true,
        (Cell::Float(x), Cell::Float(y)) => floats_close(*x, *y),
        (Cell::Text(x), Cell::Text(y)) => x == y,
        _ => false,
    }
}

/// Canonicalizes Arrow string/binary representation variants so that physical
/// differences (e.g. `Utf8` vs `Utf8View`) are not treated as result
/// differences: single-process DataFusion may infer Parquet strings as
/// `Utf8View` while Ballista produces `Utf8`, but both stringify identically.
pub fn canonical_type(dt: &DataType) -> DataType {
    match dt {
        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        DataType::BinaryView | DataType::LargeBinary => DataType::Binary,
        _ => dt.clone(),
    }
}

/// Schema reduced to (name, canonical type) pairs, ignoring nullability and
/// string/binary representation, for result comparison.
pub fn comparable_schema(schema: &Schema) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), canonical_type(f.data_type())))
        .collect()
}

/// Compares two result sets, tolerating tiny floating-point differences.
/// Returns an error describing the first mismatch instead of panicking, so the
/// benchmark binary exits non-zero with a useful message.
pub fn compare_results(expected: &[RecordBatch], actual: &[RecordBatch]) -> Result<()> {
    let expected_rows = rows_as_cells(expected);
    let actual_rows = rows_as_cells(actual);

    if expected_rows.len() != actual_rows.len() {
        return Err(DataFusionError::Execution(format!(
            "result mismatch: expected {} rows, got {} rows",
            expected_rows.len(),
            actual_rows.len()
        )));
    }

    if let (Some(e), Some(a)) = (expected.first(), actual.first()) {
        let e_schema = comparable_schema(&e.schema());
        let a_schema = comparable_schema(&a.schema());
        if e_schema != a_schema {
            return Err(DataFusionError::Execution(format!(
                "schema mismatch:\n expected: {e_schema:?}\n actual:   {a_schema:?}"
            )));
        }
    }

    for (i, (erow, arow)) in expected_rows.iter().zip(actual_rows.iter()).enumerate() {
        for (j, (ecell, acell)) in erow.iter().zip(arow.iter()).enumerate() {
            if !cells_equal(ecell, acell) {
                return Err(DataFusionError::Execution(format!(
                    "result mismatch at row {i}, column {j}: expected `{ecell}`, got `{acell}`"
                )));
            }
        }
    }

    Ok(())
}

/// Specialised String representation
pub fn col_str(column: &ArrayRef, row_index: usize) -> String {
    if column.is_null(row_index) {
        return "NULL".to_string();
    }

    // Special case ListArray as there is no pretty print support for it yet
    if let DataType::FixedSizeList(_, n) = column.data_type() {
        let array = column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .value(row_index);

        let mut r = Vec::with_capacity(*n as usize);
        for i in 0..*n {
            r.push(col_str(&array, i as usize));
        }
        return format!("[{}]", r.join(","));
    }

    array_value_to_string(column, row_index).unwrap()
}

pub fn find_path(path: &str, table: &str, ext: &str) -> Result<String> {
    // Object-store URLs (e.g. `s3://bucket/prefix`) cannot be probed with the
    // local filesystem, so register the per-table directory under the base URL
    // directly. The trailing slash marks it as a directory so the listing table
    // enumerates the Parquet files inside (e.g. `s3://bucket/prefix/lineitem/`).
    if path.contains("://") {
        return Ok(format!("{path}/{table}/"));
    }

    let path1 = format!("{path}/{table}.{ext}");
    let path2 = format!("{path}/{table}");
    if Path::new(&path1).exists() {
        Ok(path1)
    } else if Path::new(&path2).exists() {
        Ok(path2)
    } else {
        Err(DataFusionError::Plan(format!(
            "Could not find {ext} files at {path1} or {path2}"
        )))
    }
}

/// Index of the statement whose result is the query answer: the last `SELECT`
/// or `WITH` statement, or the last statement if none qualifies. TPC-H query
/// files may wrap the answer in setup/teardown statements (e.g. q15 creates and
/// drops a view); only the query statement's result is the answer.
pub fn answer_statement_index(statements: &[String]) -> usize {
    statements
        .iter()
        .rposition(|s| {
            let head = s.trim_start().to_ascii_lowercase();
            head.starts_with("select") || head.starts_with("with")
        })
        .unwrap_or_else(|| statements.len().saturating_sub(1))
}

/// Executes all statements of a query in order and returns the batches of the
/// answer statement (see `answer_statement_index`).
pub async fn execute_query_capturing_answer(
    ctx: &SessionContext,
    statements: &[String],
    debug: bool,
) -> Result<Vec<RecordBatch>> {
    let answer_idx = answer_statement_index(statements);
    let mut answer = vec![];
    for (idx, sql) in statements.iter().enumerate() {
        if debug {
            println!("Executing: {sql}");
        }
        let df = ctx.sql(sql).await?;
        let collected = df.collect().await?;
        if idx == answer_idx {
            answer = collected;
        }
    }
    Ok(answer)
}

/// The type of a Hive partition column that only exists in the directory
/// names, given the table and column name, or `None` to read it as a string.
pub type PathColumnType = fn(&str, &str) -> Option<DataType>;

/// The value that Hive and Spark put in a partition directory's name when the
/// partition key is NULL, as in `ss_sold_date_sk=__HIVE_DEFAULT_PARTITION__/`.
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// A Parquet table's location and layout. Inferring the layout reads the
/// files, so the runners do it once per table and register the result on each
/// per-query session, which then reads nothing.
pub struct ParquetTable {
    pub name: String,
    pub path: String,
    pub schema: SchemaRef,
    pub partition_cols: Vec<(String, DataType)>,
    /// The partition columns that have a `__HIVE_DEFAULT_PARTITION__`
    /// directory (see [`ParquetTable::register`]).
    pub null_partition_cols: Vec<String>,
}

impl ParquetTable {
    /// Infers the layout of the Parquet table at `path`, including the Hive
    /// partition columns that its directory names encode (e.g.
    /// `lineitem/l_shipdate=1994-01-01/`) unless `partition_cols` is false
    /// (see [`parquet_table_layout`]).
    pub async fn infer(
        ctx: &SessionContext,
        name: &str,
        path: &str,
        partition_cols: bool,
        path_column_type: PathColumnType,
    ) -> Result<Self> {
        let state = ctx.state();
        let options = ParquetReadOptions::default()
            .to_listing_options(state.config(), state.default_table_options());
        let table_url = ListingTableUrl::parse(path)?;
        let (schema, partition_cols) = parquet_table_layout(
            &state,
            name,
            &table_url,
            &options,
            partition_cols,
            path_column_type,
        )
        .await?;
        let null_partition_cols =
            null_partition_cols(&state, &table_url, &options, &partition_cols).await?;
        Ok(Self {
            name: name.to_string(),
            path: path.to_string(),
            schema,
            partition_cols,
            null_partition_cols,
        })
    }

    /// Registers the table on `ctx` with the inferred layout. Filters on the
    /// partition columns then skip whole directories at planning time instead
    /// of reading every file's footer.
    ///
    /// DataFusion casts each partition directory's value to the column's type,
    /// and `__HIVE_DEFAULT_PARTITION__` only casts to a string
    /// (<https://github.com/apache/datafusion/issues/18083>). So when a column
    /// has that directory, the files are registered as `<name>_raw` with the
    /// column as a string, and `<name>` is a view that maps that directory to
    /// NULL and casts the other values back. Filters on the column still skip
    /// directories.
    pub async fn register(&self, ctx: &SessionContext) -> Result<()> {
        if self.null_partition_cols.is_empty() {
            return self
                .register_files(ctx, &self.name, self.partition_cols.clone())
                .await;
        }

        let mut raw_partition_cols = vec![];
        let mut columns: Vec<Expr> = self
            .schema
            .fields()
            .iter()
            .map(|field| ident(field.name()))
            .collect();
        for (name, data_type) in &self.partition_cols {
            if self.null_partition_cols.contains(name) {
                raw_partition_cols.push((name.clone(), DataType::Utf8));
                let value = nullif(ident(name), lit(HIVE_DEFAULT_PARTITION));
                columns.push(cast(value, data_type.clone()).alias(name));
            } else {
                raw_partition_cols.push((name.clone(), data_type.clone()));
                columns.push(ident(name));
            }
        }

        let raw_name = format!("{}_raw", self.name);
        self.register_files(ctx, &raw_name, raw_partition_cols)
            .await?;
        let view = ctx.table(raw_name.as_str()).await?.select(columns)?;
        ctx.register_table(self.name.as_str(), view.into_view())?;
        Ok(())
    }

    /// Registers the files as table `name` with the given partition columns.
    async fn register_files(
        &self,
        ctx: &SessionContext,
        name: &str,
        partition_cols: Vec<(String, DataType)>,
    ) -> Result<()> {
        let options = ParquetReadOptions::default()
            .schema(&self.schema)
            .table_partition_cols(partition_cols);
        ctx.register_parquet(name, &self.path, options).await
    }
}

/// The columns in `partition_cols` that have a `__HIVE_DEFAULT_PARTITION__`
/// directory anywhere under `table_url`. Finding them lists every file, so an
/// unpartitioned table skips the listing.
async fn null_partition_cols(
    state: &SessionState,
    table_url: &ListingTableUrl,
    options: &ListingOptions,
    partition_cols: &[(String, DataType)],
) -> Result<Vec<String>> {
    if partition_cols.is_empty() {
        return Ok(vec![]);
    }
    let null_dir_suffix = format!("={HIVE_DEFAULT_PARTITION}");
    let store = state.runtime_env().object_store(table_url)?;
    let mut files = table_url
        .list_all_files(state, store.as_ref(), &options.file_extension)
        .await?;
    let mut null_cols = HashSet::new();
    while let Some(file) = files.try_next().await? {
        let dirs = table_url.strip_prefix(&file.location).into_iter().flatten();
        for dir in dirs {
            if let Some(name) = dir.strip_suffix(&null_dir_suffix) {
                null_cols.insert(name.to_string());
            }
        }
    }
    Ok(partition_cols
        .iter()
        .map(|(name, _)| name)
        .filter(|name| null_cols.contains(*name))
        .cloned()
        .collect())
}

/// Infers the layout of each named table under `path`, concurrently. Works for
/// both a single `<table>.parquet` file and a partitioned `<table>/` directory
/// (see `find_path`).
pub async fn infer_parquet_tables(
    ctx: &SessionContext,
    tables: &[&str],
    path: &str,
    debug: bool,
    partition_cols: bool,
    path_column_type: PathColumnType,
) -> Result<Vec<ParquetTable>> {
    try_join_all(tables.iter().map(|&table| async move {
        let table_path = find_path(path, table, "parquet")?;
        if debug {
            println!(
                "Inferring the layout of table '{table}' from Parquet at {table_path}"
            );
        }
        ParquetTable::infer(ctx, table, &table_path, partition_cols, path_column_type)
            .await
            .map_err(|e| DataFusionError::Plan(format!("{table}: {e:?}")))
    }))
    .await
}

/// Registers `tables` on `ctx` (see [`ParquetTable::register`]).
pub async fn register_parquet_tables(
    ctx: &SessionContext,
    tables: &[ParquetTable],
) -> Result<()> {
    for table in tables {
        table.register(ctx).await?;
    }
    Ok(())
}

/// Infers a Parquet table's layout and registers it (see [`ParquetTable`]).
pub async fn register_parquet_table(
    ctx: &SessionContext,
    table: &str,
    path: &str,
    partition_cols: bool,
    path_column_type: PathColumnType,
) -> Result<()> {
    ParquetTable::infer(ctx, table, path, partition_cols, path_column_type)
        .await?
        .register(ctx)
        .await
}

/// Splits a Parquet table's columns into the file schema and the Hive
/// partition columns that its directory names encode.
///
/// A partition column that the files also store keeps the files' type and
/// leaves the file schema, because `ListingTable` appends partition columns
/// itself and rejects duplicate names. A column that only exists in the path
/// takes its type from `path_column_type`, so that, for example, dates stay
/// dates. Any other partition column is a string.
pub async fn parquet_table_layout(
    state: &SessionState,
    table: &str,
    table_url: &ListingTableUrl,
    options: &ListingOptions,
    detect_partitions: bool,
    path_column_type: PathColumnType,
) -> Result<(SchemaRef, Vec<(String, DataType)>)> {
    let file_schema = options.infer_schema(state, table_url).await?;
    if !detect_partitions {
        return Ok((file_schema, vec![]));
    }
    let partition_cols: Vec<(String, DataType)> = options
        .infer_partitions(state, table_url)
        .await?
        .into_iter()
        .map(|name| {
            let data_type = file_schema
                .field_with_name(&name)
                .map(|field| field.data_type().clone())
                .ok()
                .or_else(|| path_column_type(table, &name))
                .unwrap_or(DataType::Utf8);
            (name, data_type)
        })
        .collect();
    let file_fields: Vec<_> = file_schema
        .fields()
        .iter()
        .filter(|field| partition_cols.iter().all(|(name, _)| name != field.name()))
        .cloned()
        .collect();
    let file_schema =
        Schema::new_with_metadata(file_fields, file_schema.metadata().clone());
    Ok((Arc::new(file_schema), partition_cols))
}

/// The session config the benchmark runners use: S3 support, the given
/// target partitions and batch size, statistics collection, and the given
/// `key=value` config overrides.
pub fn benchmark_session_config(
    partitions: usize,
    batch_size: usize,
    config_overrides: &[String],
) -> SessionConfig {
    let mut config = session_config_with_s3_support()
        .with_target_partitions(partitions)
        .with_batch_size(batch_size)
        .with_collect_statistics(true);

    for kv in config_overrides {
        if let Some((key, value)) = kv.split_once('=') {
            if let Err(e) = config.options_mut().set(key.trim(), value.trim()) {
                println!("Warning: could not set config '{kv}': {e}");
            }
        } else {
            println!(
                "Warning: ignoring invalid config override '{kv}'. \
                 Expected format: key=value"
            );
        }
    }
    config
}

/// Connects a new session with `config` to the Ballista scheduler at
/// `address` (a `df://host:port` URL). The benchmark runners build one per
/// query so that `job_name` identifies the query in the scheduler.
pub async fn ballista_context(
    address: &str,
    job_name: &str,
    config: SessionConfig,
) -> Result<SessionContext> {
    let state = session_state_with_s3_support(config.with_ballista_job_name(job_name))?;
    SessionContext::remote_with_state(address, state).await
}

/// A single-process session with `config` that can read from S3, for work
/// that must not go to the cluster, such as inferring table layouts.
pub fn local_context(config: SessionConfig) -> Result<SessionContext> {
    Ok(SessionContext::new_with_state(
        session_state_with_s3_support(config)?,
    ))
}

/// Reads query `query`'s SQL from `<dir>/q<query>.sql`, relative to either the
/// current directory or the repository root.
pub fn read_query_file(dir: &str, query: usize) -> Result<String> {
    let possibilities = [
        format!("{dir}/q{query}.sql"),
        format!("benchmarks/{dir}/q{query}.sql"),
    ];
    let mut errors = vec![];
    for filename in &possibilities {
        match fs::read_to_string(filename) {
            Ok(contents) => return Ok(contents),
            Err(e) => errors.push(format!("{filename}: {e}")),
        }
    }
    Err(DataFusionError::Plan(format!(
        "Could not find query {query}: {errors:?}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use std::sync::Arc;

    fn f64_batch(values: Vec<f64>) -> RecordBatch {
        let schema =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(values))]).unwrap()
    }

    fn i64_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn compare_results_equal_ok() {
        assert!(
            compare_results(&[i64_batch(vec![1, 2])], &[i64_batch(vec![1, 2])]).is_ok()
        );
    }

    #[test]
    fn compare_results_row_count_mismatch() {
        let err = compare_results(&[i64_batch(vec![1])], &[i64_batch(vec![1, 2])])
            .unwrap_err()
            .to_string();
        assert!(err.contains("expected 1 rows, got 2 rows"), "{err}");
    }

    #[test]
    fn compare_results_value_mismatch() {
        let err = compare_results(&[i64_batch(vec![1])], &[i64_batch(vec![2])])
            .unwrap_err()
            .to_string();
        assert!(err.contains("row 0, column 0"), "{err}");
    }

    #[test]
    fn compare_results_floats_within_tolerance_ok() {
        // differ only beyond the tolerance's significant digits
        assert!(
            compare_results(
                &[f64_batch(vec![123.4567890])],
                &[f64_batch(vec![123.4567891])]
            )
            .is_ok()
        );
    }

    #[test]
    fn compare_results_floats_outside_tolerance_err() {
        assert!(
            compare_results(&[f64_batch(vec![100.0])], &[f64_batch(vec![100.5])])
                .is_err()
        );
    }

    #[test]
    fn answer_statement_index_picks_select_not_drop() {
        let stmts = vec![
            "create view revenue0 as select 1".to_string(),
            "select * from revenue0 order by a".to_string(),
            "drop view revenue0".to_string(),
        ];
        assert_eq!(answer_statement_index(&stmts), 1);
    }

    #[test]
    fn answer_statement_index_single_select() {
        let stmts = vec!["select 1".to_string()];
        assert_eq!(answer_statement_index(&stmts), 0);
    }

    #[test]
    fn answer_statement_index_with_cte() {
        let stmts = vec!["WITH t AS (select 1) select * from t".to_string()];
        assert_eq!(answer_statement_index(&stmts), 0);
    }

    #[test]
    fn compare_results_ignores_utf8view_vs_utf8() {
        let view_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let utf8_schema =
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let a = RecordBatch::try_new(
            view_schema,
            vec![Arc::new(StringViewArray::from(vec!["x", "y"]))],
        )
        .unwrap();
        let b = RecordBatch::try_new(
            utf8_schema,
            vec![Arc::new(StringArray::from(vec!["x", "y"]))],
        )
        .unwrap();
        assert!(compare_results(&[a], &[b]).is_ok());
    }
}
