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

//! End-to-end coverage for `ParallelWindowRule`, which rewrites a bounded
//! RANGE-frame window into `RSE → ORRE → RSE → RangeFilterExec → PBWAG`.
//!
//! The rewrite is a pure optimization, so single-node DataFusion is the
//! oracle: every case runs there, then on Ballista with the rule off, then on
//! Ballista with the rule on, and all three must agree. A disagreement names
//! which side moved.
//!
//! What the cases vary is what the rewrite has to get right: which end of the
//! order the NULL run occupies, whether the frame widens the lower bound, the
//! upper, both, or neither, and whether a DESC key stays on the serial path.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod parallel_window {
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

    /// Rows per file. Four files of these interleave over the whole key range,
    /// so no file is range-disjoint from another and ORRE has real routing to
    /// do rather than passing each input partition through whole.
    const ROWS_PER_FILE: i64 = 12;
    const FILES: i64 = 4;

    /// Every fourth key is NULL, so the run is long enough to span more than
    /// one output partition's share and cannot be mistaken for a rounding
    /// artifact at whichever end it lands.
    fn key_is_null(id: i64) -> bool {
        id % 4 == 3
    }

    /// One CSV per file, rows dealt round-robin by `id` so file `f` holds keys
    /// `f, f + FILES, f + 2 * FILES, ...` — every file covers the full range.
    fn write_table(dir: &Path, nullable: bool) {
        let table = dir.join("w");
        fs::create_dir_all(&table).unwrap();
        for file in 0..FILES {
            let mut csv = String::from("id,k,v\n");
            for row in 0..ROWS_PER_FILE {
                let id = file + row * FILES;
                let key = if nullable && key_is_null(id) {
                    String::new()
                } else {
                    id.to_string()
                };
                csv.push_str(&format!("{id},{key},{}\n", id * 10));
            }
            fs::write(table.join(format!("p{file}.csv")), csv).unwrap();
        }
    }

    async fn register(ctx: &SessionContext, dir: &Path, nullable: bool) {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, nullable),
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
    fn query(ordering: &str, frame: &str) -> String {
        format!(
            "SELECT id, k, SUM(v) OVER (ORDER BY k {ordering} RANGE BETWEEN {frame}) AS w \
             FROM w ORDER BY id"
        )
    }

    /// A Ballista standalone context with AQE on, the rule at `parallel`, and
    /// K = 4 output partitions.
    async fn ballista(dir: &Path, nullable: bool, parallel: bool) -> SessionContext {
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(4)
            .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, true)
            .set_bool(BALLISTA_PARALLEL_WINDOW_ENABLED, parallel);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let ctx = SessionContext::standalone_with_state(state).await.unwrap();
        register(&ctx, dir, nullable).await;
        ctx
    }

    #[rstest]
    #[case::halo_both("ASC NULLS LAST", "5 PRECEDING AND 5 FOLLOWING")]
    #[case::halo_lower("ASC NULLS LAST", "5 PRECEDING AND CURRENT ROW")]
    #[case::halo_upper("ASC NULLS LAST", "CURRENT ROW AND 5 FOLLOWING")]
    #[case::halo_none("ASC NULLS LAST", "CURRENT ROW AND CURRENT ROW")]
    #[case::nulls_first("ASC NULLS FIRST", "5 PRECEDING AND 5 FOLLOWING")]
    #[case::nulls_first_halo_none("ASC NULLS FIRST", "CURRENT ROW AND CURRENT ROW")]
    // DESC is gated to the serial path by `ParallelWindowRule` — RANGE frame
    // semantics invert with the sort direction. These cases assert the gate
    // leaves the answer alone rather than that the rewrite fired.
    #[case::desc_nulls_last("DESC NULLS LAST", "5 PRECEDING AND 5 FOLLOWING")]
    #[case::desc_nulls_first("DESC NULLS FIRST", "5 PRECEDING AND 5 FOLLOWING")]
    #[tokio::test]
    async fn parallel_window_agrees_with_datafusion(
        #[case] ordering: &str,
        #[case] frame: &str,
        // A nullable key is the case the rewrite has to place the NULL run for;
        // a non-nullable one has no run and exercises the plain value paths.
        #[values(false, true)] nullable: bool,
    ) {
        let case = format!("{ordering}_{frame}_{nullable}").replace(' ', "_");
        let dir = std::env::temp_dir().join(format!("ballista_parallel_window_{case}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        write_table(&dir, nullable);

        let sql = query(ordering, frame);

        let oracle_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(4),
        );
        register(&oracle_ctx, &dir, nullable).await;
        let oracle = run(&oracle_ctx, &sql).await;

        let serial = run(&ballista(&dir, nullable, false).await, &sql).await;
        assert_eq!(
            oracle, serial,
            "Ballista disagrees with DataFusion before the rule is even on\n{sql}"
        );

        let parallel = run(&ballista(&dir, nullable, true).await, &sql).await;
        assert_eq!(
            oracle, parallel,
            "the parallel-window rewrite changed the answer\n{sql}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// The rewrite has to fire for the cases above to mean anything. A plan
    /// still holding `BoundedWindowAggExec` over a `SortPreservingMergeExec`
    /// is the serial shape, and an agreeing answer from it proves nothing.
    #[tokio::test]
    async fn the_rewrite_actually_fires() {
        let dir = std::env::temp_dir().join("ballista_parallel_window_explain");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        write_table(&dir, true);

        let sql = query("ASC NULLS LAST", "5 PRECEDING AND 5 FOLLOWING");
        let ctx = ballista(&dir, true, true).await;
        // `EXPLAIN` alone shows only the first stage split. The rewrite lands
        // when AQE re-plans stage 1 against stage 0's reported sketch, which
        // is reached by executing.
        let plan = run(&ctx, &format!("EXPLAIN ANALYZE {sql}")).await;

        for operator in [
            "OrderedRangeRepartitionExec",
            "RangeFilterExec",
            "RuntimeStatsExec",
        ] {
            assert!(
                plan.contains(operator),
                "rewrite did not plant {operator}:\n{plan}"
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
