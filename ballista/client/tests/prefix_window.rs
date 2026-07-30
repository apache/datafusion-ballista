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

//! End-to-end correctness for the prefix-scan window rewrite.
//!
//! `PrefixWindowRule` splits an `UNBOUNDED PRECEDING` window across K tasks,
//! each computing a partition-local running aggregate, and corrects them
//! afterwards with the merged state of all prior partitions. The failure
//! mode it has to be held against is the one that doesn't show up on a
//! single node: each partition's running total silently restarting at zero.
//!
//! These run the real distributed path (scheduler, shuffle, executor) via
//! the standalone in-process cluster, so a wrong stage boundary or a lost
//! accumulator state shows up as wrong numbers rather than a plan diff.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod prefix_window_tests {
    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_PARALLEL_WINDOW_ENABLED,
        BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK,
    };
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Rows per input partition.
    const ROWS_PER_PARTITION: usize = 4;
    /// Input partitions, and therefore the K the rewrite range-repartitions to.
    const PARTITIONS: usize = 4;
    const TOTAL_ROWS: usize = ROWS_PER_PARTITION * PARTITIONS;

    /// `1.0 ..= 16.0`, split across [`PARTITIONS`] partitions in ascending
    /// runs. Every value and every running sum of them is exactly
    /// representable in `f64`, so a mismatch is a semantic bug rather than
    /// floating-point drift.
    fn input_partitions() -> Vec<Vec<f64>> {
        (0..PARTITIONS)
            .map(|p| {
                (0..ROWS_PER_PARTITION)
                    .map(|r| (p * ROWS_PER_PARTITION + r + 1) as f64)
                    .collect()
            })
            .collect()
    }

    /// Running sums of `1.0 ..= 16.0` in ascending order: 1, 3, 6, 10, ...
    ///
    /// Computed here rather than captured from a Ballista run, so the test
    /// can't agree with a uniformly-wrong engine.
    fn expected_running_sums() -> Vec<f64> {
        let mut total = 0.0;
        (1..=TOTAL_ROWS)
            .map(|v| {
                total += v as f64;
                total
            })
            .collect()
    }

    /// `max_partitions_per_task`, so tasks carry a multi-partition slice
    /// rather than one partition each. That exercises the task builder's
    /// index remapping: `PrefixMergeExec`'s state is keyed by global
    /// partition, but a task's `execute(k)` numbers its own slice from zero.
    const MAX_PARTITIONS_PER_TASK: usize = 2;

    async fn context(parallel_window: bool) -> SessionContext {
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(PARTITIONS)
            .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, true)
            .set_str(
                BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK,
                &MAX_PARTITIONS_PER_TASK.to_string(),
            )
            .set_bool(BALLISTA_PARALLEL_WINDOW_ENABLED, parallel_window);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        SessionContext::standalone_with_state(state).await.unwrap()
    }

    /// Register `t(v Float64)` as one parquet file per partition.
    ///
    /// Parquet rather than a `MemTable` because Ballista ships the *logical*
    /// plan to the scheduler, and an in-memory provider has no
    /// `LogicalExtensionCodec` — it fails at serialization before any of the
    /// distributed path runs. One file per partition so the scan really is
    /// [`PARTITIONS`]-wide rather than depending on how DataFusion chooses to
    /// split a single small file.
    ///
    /// `v` is `Float64` because ORRE routes on a T-Digest, which is
    /// Float64-only until the KLL migration. That restriction is what keeps
    /// h2o Q7 (`ORDER BY id3`, `Int64`) from being rewritten today.
    ///
    /// The returned [`TempDir`] owns the files and must outlive the query.
    async fn register_input(ctx: &SessionContext) -> Result<TempDir> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let dir = TempDir::new().expect("temp dir");
        for (partition, values) in input_partitions().into_iter().enumerate() {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Float64Array::from(values))],
            )?;
            let path = dir.path().join(format!("part-{partition}.parquet"));
            let file = File::create(&path).expect("create parquet file");
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
            writer.write(&batch)?;
            writer.close()?;
        }
        ctx.register_parquet(
            "t",
            dir.path().to_str().expect("utf-8 temp path"),
            ParquetReadOptions::default(),
        )
        .await?;
        Ok(dir)
    }

    /// `(v, running_sum)` pairs ordered by `v`.
    async fn running_sums(ctx: &SessionContext) -> Result<Vec<(f64, f64)>> {
        let batches = ctx
            .sql(
                "SELECT v, \
                        sum(v) OVER (ORDER BY v \
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs \
                 FROM t \
                 ORDER BY v",
            )
            .await?
            .collect()
            .await?;

        let mut rows = Vec::with_capacity(TOTAL_ROWS);
        for batch in &batches {
            let v = datafusion::common::cast::as_float64_array(batch.column(0))?;
            let rs = datafusion::common::cast::as_float64_array(batch.column(1))?;
            for i in 0..batch.num_rows() {
                rows.push((v.value(i), rs.value(i)));
            }
        }
        Ok(rows)
    }

    /// The rewrite must not change the answer.
    ///
    /// The failure this guards against is quiet: if prior-partition state
    /// never reaches `PrefixMergeExec`, each task emits its own
    /// partition-local running sum and every partition after the first is
    /// short by exactly the sum of everything before it. Plausible-looking
    /// numbers, wrong totals, no error.
    #[tokio::test]
    async fn prefix_scan_matches_serial_running_sum() -> Result<()> {
        let ctx = context(true).await;
        let _data = register_input(&ctx).await?;
        let rows = running_sums(&ctx).await?;

        let expected_v: Vec<f64> = (1..=TOTAL_ROWS).map(|v| v as f64).collect();
        let actual_v: Vec<f64> = rows.iter().map(|(v, _)| *v).collect();
        assert_eq!(actual_v, expected_v, "input rows must survive the rewrite");

        let actual_rs: Vec<f64> = rows.iter().map(|(_, rs)| *rs).collect();
        assert_eq!(
            actual_rs,
            expected_running_sums(),
            "running sums must be global, not partition-local — a partition \
             whose total restarts near zero means prior-partition state never \
             reached PrefixMergeExec"
        );
        Ok(())
    }

    /// Same query with the rewrite off, as a guard on the test itself: if
    /// this ever fails, the harness is wrong rather than the rewrite.
    #[tokio::test]
    async fn serial_running_sum_is_correct_without_rewrite() -> Result<()> {
        let ctx = context(false).await;
        let _data = register_input(&ctx).await?;
        let rows = running_sums(&ctx).await?;

        let actual_rs: Vec<f64> = rows.iter().map(|(_, rs)| *rs).collect();
        assert_eq!(actual_rs, expected_running_sums());
        Ok(())
    }
}
