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

//! End-to-end tests for Hive partition pruning.
//!
//! A filter on a partition column must keep the other partitions' files out
//! of the job entirely: the scheduler must not read their footers while it
//! plans, and no task may scan them. The table used here has a partition
//! whose file is not Parquet, so a job that touches it fails.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod partition_pruning {
    use std::fs::{self, File};
    use std::path::Path;
    use std::sync::Arc;

    use crate::common::{remote_context, standalone_context};
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::error::Result;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use rstest::rstest;

    /// Writes table `t`, partitioned by `d` into three days. Rows have
    /// `v = id * 10`. The file for 2024-01-03 is not Parquet.
    fn write_table(root: &Path) {
        for (day, ids) in [("2024-01-01", [1, 2]), ("2024-01-02", [3, 4])] {
            let id: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
            let v: ArrayRef = Arc::new(Int64Array::from_iter_values(ids.map(|i| i * 10)));
            let batch = RecordBatch::try_from_iter([("id", id), ("v", v)]).unwrap();
            let dir = root.join(format!("d={day}"));
            fs::create_dir_all(&dir).unwrap();
            let file = File::create(dir.join("part-0.parquet")).unwrap();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let poisoned = root.join("d=2024-01-03");
        fs::create_dir_all(&poisoned).unwrap();
        fs::write(poisoned.join("part-0.parquet"), "not a parquet file").unwrap();
    }

    /// Writes `t`, registers it with `d` as a `DATE` partition column and runs
    /// `sql`. The schema is given up front, so registering opens no files.
    async fn run(ctx: SessionContext, aqe: bool, sql: &str) -> Result<Vec<RecordBatch>> {
        let dir = tempfile::tempdir().unwrap();
        write_table(dir.path());

        ctx.sql(&format!("SET ballista.planner.adaptive.enabled = {aqe}"))
            .await?
            .collect()
            .await?;
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]);
        let options = ParquetReadOptions::default()
            .schema(&schema)
            .table_partition_cols(vec![("d".to_string(), DataType::Date32)]);
        ctx.register_parquet("t", dir.path().to_str().unwrap(), options)
            .await?;

        ctx.sql(sql).await?.collect().await
    }

    // TPC-H Q6's shape: a date range that keeps two days and drops the
    // poisoned one by its partition value.
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn range_filter_skips_other_partitions(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        #[values(true, false)] aqe: bool,
    ) -> Result<()> {
        let batches = run(
            ctx,
            aqe,
            "SELECT d, sum(v) AS total FROM t \
             WHERE d >= DATE '2024-01-01' AND d < DATE '2024-01-03' \
             GROUP BY d ORDER BY d",
        )
        .await?;
        assert_batches_eq!(
            [
                "+------------+-------+",
                "| d          | total |",
                "+------------+-------+",
                "| 2024-01-01 | 30    |",
                "| 2024-01-02 | 70    |",
                "+------------+-------+",
            ],
            &batches
        );
        Ok(())
    }

    // An equality on the partition column narrows the listing to a single
    // directory, and the filter on `v` still applies to that file's rows.
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn partition_and_data_filters_skip_other_partitions(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        #[values(true, false)] aqe: bool,
    ) -> Result<()> {
        let batches = run(
            ctx,
            aqe,
            "SELECT id FROM t WHERE d = DATE '2024-01-02' AND v > 30",
        )
        .await?;
        assert_batches_eq!(["+----+", "| id |", "+----+", "| 4  |", "+----+"], &batches);
        Ok(())
    }

    // When no partition matches, the scan has no files at all. An aggregate
    // without GROUP BY must still return its one row.
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn filter_matching_no_partition_returns_empty_aggregate(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        #[values(true, false)] aqe: bool,
    ) -> Result<()> {
        let batches = run(
            ctx,
            aqe,
            "SELECT count(*) AS n, sum(v) AS total FROM t WHERE d > DATE '2024-01-03'",
        )
        .await?;
        assert_batches_eq!(
            [
                "+---+-------+",
                "| n | total |",
                "+---+-------+",
                "| 0 |       |",
                "+---+-------+",
            ],
            &batches
        );
        Ok(())
    }

    // Guards the tests above: they only prove pruning while a job that
    // reaches the poisoned partition fails.
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn unpruned_scan_fails_on_poisoned_partition(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        #[values(true, false)] aqe: bool,
    ) {
        let result = run(ctx, aqe, "SELECT sum(v) FROM t").await;
        assert!(
            result.is_err(),
            "a job reading the poisoned partition should fail, got {result:?}"
        );
    }
}
