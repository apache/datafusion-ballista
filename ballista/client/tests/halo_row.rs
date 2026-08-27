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

//! End-to-end coverage for `HaloRowRule`, which rewrites a bounded ROWS-frame
//! window into `RSE → ORRE → RSE → RangeFilterExec → PBWAG`.
//!
//! Same oracle as [`parallel_window`](../parallel_window.rs): the rewrite is a
//! pure optimization, so single-node DataFusion answers first, then Ballista
//! with the rule off, then with it on, and all three must agree.
//!
//! The key is unique and non-nullable, which is what makes the comparison
//! meaningful: a ROWS frame over duplicate keys is implementation-defined,
//! since peers have no defined order among themselves.
//!
//! The two frames split the work. `CURRENT ROW AND CURRENT ROW` needs no row
//! from outside a task's own range, so it isolates the plan shape. `5
//! PRECEDING` needs five, and no value arithmetic on the cut can reach them —
//! that is the rank-derived halo's job.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod halo_row {
    use std::fs;
    use std::path::Path;

    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_PARALLEL_WINDOW_ENABLED,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::*;
    use rstest::rstest;

    /// Four files interleaving over the whole key range, so no file is
    /// range-disjoint from another and ORRE has real routing to do.
    const ROWS_PER_FILE: i64 = 12;
    const FILES: i64 = 4;
    /// K. With 48 rows over 4 output partitions, a 5 PRECEDING frame reaches
    /// back into the neighbouring partition for the first five rows of each.
    const OUTPUT_PARTITIONS: usize = 4;

    /// One CSV per file, rows dealt round-robin by `id` so file `f` holds keys
    /// `f, f + FILES, f + 2 * FILES, ...` — every file covers the full range.
    fn write_table(dir: &Path) {
        let table = dir.join("w");
        fs::create_dir_all(&table).unwrap();
        for file in 0..FILES {
            let mut csv = String::from("id,k,v\n");
            for row in 0..ROWS_PER_FILE {
                let id = file + row * FILES;
                csv.push_str(&format!("{id},{id},{}\n", id * 10));
            }
            fs::write(table.join(format!("p{file}.csv")), csv).unwrap();
        }
    }

    async fn register(ctx: &SessionContext, dir: &Path) {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]);
        ctx.register_csv(
            "w",
            dir.join("w").to_str().unwrap(),
            CsvReadOptions::new()
                .has_header(true)
                .schema(&schema)
                .file_extension(".csv"),
        )
        .await
        .unwrap();
    }

    async fn run(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    /// `ORDER BY id` on the outside so the comparison is over rows, not over
    /// whichever order the K output partitions happened to be drained in.
    fn query(frame: &str) -> String {
        format!(
            "SELECT id, k, SUM(v) OVER (ORDER BY k ASC NULLS LAST ROWS BETWEEN {frame}) AS w \
             FROM w ORDER BY id"
        )
    }

    async fn ballista(dir: &Path, parallel: bool) -> SessionContext {
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(OUTPUT_PARTITIONS)
            .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, true)
            .set_bool(BALLISTA_PARALLEL_WINDOW_ENABLED, parallel);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let ctx = SessionContext::standalone_with_state(state).await.unwrap();
        register(&ctx, dir).await;
        ctx
    }

    #[rstest]
    #[case::halo_none("CURRENT ROW AND CURRENT ROW")]
    #[case::halo_lower("5 PRECEDING AND CURRENT ROW")]
    #[tokio::test]
    async fn halo_row_agrees_with_datafusion(#[case] frame: &str) {
        let case = frame.replace(' ', "_");
        let dir = std::env::temp_dir().join(format!("ballista_halo_row_{case}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        write_table(&dir);

        let sql = query(frame);

        let oracle_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(OUTPUT_PARTITIONS),
        );
        register(&oracle_ctx, &dir).await;
        let oracle = run(&oracle_ctx, &sql).await;

        let serial = run(&ballista(&dir, false).await, &sql).await;
        assert_eq!(
            oracle, serial,
            "Ballista disagrees with DataFusion before the rule is even on\n{sql}"
        );

        let parallel = run(&ballista(&dir, true).await, &sql).await;
        assert_eq!(
            oracle, parallel,
            "the halo-row rewrite changed the answer\n{sql}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// The rewrite has to fire for the case above to mean anything. A plan
    /// still holding `BoundedWindowAggExec` over a `SortPreservingMergeExec`
    /// is the serial shape, and an agreeing answer from it proves nothing.
    #[tokio::test]
    async fn the_rewrite_actually_fires() {
        let dir = std::env::temp_dir().join("ballista_halo_row_explain");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        write_table(&dir);

        let sql = query("5 PRECEDING AND CURRENT ROW");
        let ctx = ballista(&dir, true).await;
        // `EXPLAIN` alone shows only the first stage split. The rewrite lands
        // when AQE re-plans stage 1 against stage 0's reported sketch, which
        // is reached by executing.
        let plan = run(&ctx, &format!("EXPLAIN ANALYZE {sql}")).await;

        for operator in [
            "OrderedRangeRepartitionExec",
            "RangeFilterExec",
            "RuntimeStatsExec",
            "PartitionedBoundedWindowAggExec",
        ] {
            assert!(
                plan.contains(operator),
                "rewrite did not plant {operator}:\n{plan}"
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
