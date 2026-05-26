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
// `SharedWorkSource` populated with every file in the scan, and each
// partition's stream drains files from that queue. In a single-process
// DataFusion run that's fine because all partitions of the same
// DataSourceExec instance cooperatively drain one queue, but Ballista
// deserialises a fresh DataSourceExec for every task and runs a single
// partition against it. Without intervention the partition that does run
// drains the whole queue and reads every file, so a 6-file table executed
// by 6 tasks returns 6x the data.
//
// `restrict_file_scan_to_partition` in ballista-core sets
// `preserve_order = true` on every FileScanConfig before execution, which
// short-circuits `FileScanConfig::create_sibling_state` to `None`. Each
// partition then falls back to `WorkSource::Local(file_groups[partition])`
// and scans exactly the files the planner assigned to it, so a 6-file scan
// dispatched as 6 tasks reads 6 files instead of 36. These tests would fail
// without that helper.
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

    // Regression for an earlier version of the work-stealing fix that emptied
    // out file_groups for all partition slots except the running task's. That
    // broke TPC-H Q11: in a broadcast hash join the build-side
    // DataSourceExec is read with execute(0..K) by the join itself, so
    // emptying the other slots starved the hash table and the join hung.
    // This test joins two multi-file parquet tables under a configuration
    // that strongly biases the planner toward broadcast hash join, and
    // checks the join still returns every matched row.
    #[tokio::test]
    async fn multi_file_parquet_broadcast_hash_join_returns_full_result() -> Result<()> {
        let left_dir = TempDir::new().unwrap();
        let right_dir = TempDir::new().unwrap();
        // Left side is intentionally larger so the planner picks the small
        // right side as the broadcast build input.
        let (left_rows, _) = write_parquet_dataset(left_dir.path(), 5, 8).await?;
        let (right_rows, _) = write_parquet_dataset(right_dir.path(), 4, 4).await?;

        let ctx = SessionContext::standalone().await?;
        ctx.register_parquet(
            "l",
            left_dir.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
        ctx.register_parquet(
            "r",
            right_dir.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

        let batches = ctx
            .sql("SELECT COUNT(*) AS matched FROM l JOIN r ON l.value = r.value")
            .await?
            .collect()
            .await?;

        let matched = batches[0]
            .column_by_name("matched")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        // Both sides use disjoint ranges (left = 0..40, right = 0..16), so
        // the join must match exactly `right_rows` rows. Anything less means
        // the build-side scan lost data; anything more would mean the probe
        // side double-read.
        assert_eq!(
            matched, right_rows as i64,
            "broadcast hash join over multi-file scans must see every \
             build-side row exactly once; left had {left_rows} rows, right \
             had {right_rows}"
        );

        Ok(())
    }

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
