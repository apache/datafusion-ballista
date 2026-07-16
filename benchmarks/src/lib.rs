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
//! path resolution, answer-statement selection, and Parquet table registration.

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::path::Path;

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

/// Register each named table as a Parquet source under `path`, inferring the
/// schema from the files. Works for both a single `<table>.parquet` file and a
/// partitioned `<table>/` directory (see `find_path`).
pub async fn register_parquet_tables(
    ctx: &SessionContext,
    tables: &[&str],
    path: &str,
    debug: bool,
) -> Result<()> {
    for &table in tables {
        let table_path = find_path(path, table, "parquet")?;
        if debug {
            println!("Registering table '{table}' from Parquet at {table_path}");
        }
        ctx.register_parquet(table, &table_path, ParquetReadOptions::default())
            .await
            .map_err(|e| DataFusionError::Plan(format!("{e:?}")))?;
    }
    Ok(())
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
