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

mod common;

// Regression coverage for the DataFusion 54 upgrade tracked in
// https://github.com/apache/datafusion-ballista/issues/1776 (see also the
// linked datafusion-distributed issue #460 and PR #467 about FileScanConfig
// work stealing).
//
// DataFusion 54's `FileScanConfig::create_sibling_state` returns a
// `SharedWorkSource` populated with every file in the scan. When any
// partition of that DataSourceExec opens its stream, it pulls files off the
// shared queue until empty. In a single-process DataFusion run this is
// harmless because all partitions of the same DataSourceExec instance share
// the queue and the queue is drained exactly once across them.
//
// Ballista breaks that invariant: each task deserialises its *own* copy of
// the plan and executes a single partition. Each task therefore has its own
// shared queue containing every file, and the partition it runs drains the
// whole queue. The result is that every file is scanned once per task, so a
// 6-file table with 6 tasks reads 36 files and returns 6x the correct row
// count.
//
// These tests are deliberately left enabled but #[ignore]d so they document
// the failure mode without blocking CI. They should turn green once Ballista
// either pre-splits FileScanConfig file_groups per task before serialisation
// (the approach datafusion-distributed took in PR #467) or otherwise stops
// each task from inheriting the full shared work queue.
#[cfg(test)]
#[cfg(feature = "standalone")]
mod work_stealing {
    use ballista::prelude::SessionContextExt;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::config::TableParquetOptions;
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Writes `num_files` parquet files into `dir`, each holding the rows
    /// `[file_idx * rows_per_file .. (file_idx + 1) * rows_per_file)`.
    /// Returns the total number of rows written and the expected sum across
    /// the `value` column, which the tests use to detect duplicated or missing
    /// reads.
    async fn write_parquet_dataset(
        dir: &std::path::Path,
        num_files: usize,
        rows_per_file: usize,
    ) -> Result<(usize, i64)> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        // DataFusion-only context for writing the fixture so we don't depend
        // on the cluster being healthy for setup.
        let writer_ctx = SessionContext::new();
        for file_idx in 0..num_files {
            let start = (file_idx * rows_per_file) as i64;
            let values: Vec<i64> = (start..start + rows_per_file as i64).collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values))],
            )?;
            let df = writer_ctx.read_batch(batch)?;
            let path = dir.join(format!("part-{file_idx:04}.parquet"));
            df.write_parquet(
                path.to_str().unwrap(),
                DataFrameWriteOptions::default(),
                Some(TableParquetOptions::default()),
            )
            .await?;
        }

        let total_rows = num_files * rows_per_file;
        let total_sum = (0..total_rows as i64).sum();
        Ok((total_rows, total_sum))
    }

    // Each Ballista task currently drains the full shared work queue, so the
    // returned row count is `num_files * tasks` instead of `num_files *
    // rows_per_file`. Re-enable once the upstream issue is addressed.
    #[ignore = "FileScanConfig shared work queue causes per-task over-reads under DF 54"]
    #[tokio::test]
    async fn multi_file_parquet_scan_counts_every_row_exactly_once() -> Result<()> {
        let tmp_dir = TempDir::new().unwrap();
        let (expected_rows, expected_sum) =
            write_parquet_dataset(tmp_dir.path(), 6, 7).await?;

        let ctx = SessionContext::standalone().await?;
        ctx.register_parquet(
            "t",
            tmp_dir.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

        let batches = ctx
            .sql("SELECT COUNT(*) AS row_count, SUM(value) AS value_sum FROM t")
            .await?
            .collect()
            .await?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let row_count = batch
            .column_by_name("row_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let value_sum = batch
            .column_by_name("value_sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        assert_eq!(
            row_count, expected_rows as i64,
            "Ballista returned the wrong row count; work stealing causes \
             duplicated rows here"
        );
        assert_eq!(
            value_sum, expected_sum,
            "Ballista returned the wrong column sum; duplicated reads inflate \
             this"
        );

        Ok(())
    }

    #[ignore = "FileScanConfig shared work queue causes per-task over-reads under DF 54"]
    #[tokio::test]
    async fn multi_file_parquet_group_by_returns_each_value_once() -> Result<()> {
        let tmp_dir = TempDir::new().unwrap();
        let (expected_rows, _) = write_parquet_dataset(tmp_dir.path(), 4, 5).await?;

        let ctx = SessionContext::standalone().await?;
        ctx.register_parquet(
            "t",
            tmp_dir.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

        // GROUP BY across the whole dataset exercises a shuffle on top of the
        // multi-file scan. If the scan double-counts, the per-key counts
        // become 2 or higher.
        let batches = ctx
            .sql("SELECT value, COUNT(*) AS c FROM t GROUP BY value")
            .await?
            .collect()
            .await?;

        let mut total_keys = 0usize;
        for batch in &batches {
            let counts = batch
                .column_by_name("c")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..counts.len() {
                assert_eq!(
                    counts.value(i),
                    1,
                    "value at row {i} of batch was read {} times instead of \
                     once; work stealing surfaces as a count > 1 here",
                    counts.value(i)
                );
                total_keys += 1;
            }
        }
        assert_eq!(
            total_keys, expected_rows,
            "expected every distinct value to be present exactly once"
        );

        Ok(())
    }
}
