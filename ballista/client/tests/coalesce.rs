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

//! End-to-end checks for adaptive shuffle-partition coalescing.
//!
//! With `ballista.planner.coalesce.enabled` on, a standalone cluster must
//! return what single-process DataFusion returns, row for row. Each query
//! runs at several coalesce targets, so a stage reads its shuffles as one
//! partition, as a few, or uncoalesced.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod coalesce {
    use std::fs;
    use std::path::Path;

    use crate::common::{
        check_matches, collect_text, datafusion_context, register_csv_tables, set_option,
    };
    use ballista::prelude::SessionContextExt;
    use datafusion::prelude::*;
    use tempfile::TempDir;

    /// `t1` and `t2` as 4 CSV files each, so every scan has 4 partitions.
    fn write_tables(dir: &Path) {
        for (name, rows) in [("t1", 50), ("t2", 30)] {
            let table = dir.join(name);
            fs::create_dir_all(&table).unwrap();
            for file in 0..4 {
                let mut csv = String::from("id,k,v\n");
                for row in 0..rows {
                    let id = file * rows + row;
                    csv.push_str(&format!("{id},{},{}\n", id % 7, id * 37 % 101));
                }
                fs::write(table.join(format!("p{file}.csv")), csv).unwrap();
            }
        }
    }

    const QUERIES: &[&str] = &[
        // A final aggregate reading a hash shuffle, sorted after it.
        "select k, count(*) as n, sum(v) as s from t1 group by k order by k",
        // A join, sorted after it.
        "select t1.id, t2.id as id2 from t1 join t2 on t1.k = t2.k order by t1.id, id2",
        // A global aggregate over a join: the join's stage writes through a
        // passthrough boundary below a `CoalescePartitionsExec`.
        "select count(*) as n, sum(t1.v * t2.v) as s from t1 join t2 on t1.k = t2.k",
        // A sorted scan: the final merge reads sorted shuffle output.
        "select id, v from t1 order by v desc, id",
    ];

    /// Coalesce targets in bytes. At the default every shuffle here fits in
    /// one partition. The smaller targets pack them into a few partitions.
    const TARGETS: &[&str] = &["67108864", "4096", "3072", "2048", "1536"];

    #[tokio::test]
    async fn coalesced_results_match_datafusion() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_tables(dir);

        let df_ctx = datafusion_context();
        register_csv_tables(&df_ctx, dir, &["t1", "t2"]).await;
        let mut expected = vec![];
        for query in QUERIES {
            expected.push(collect_text(&df_ctx, query).await);
        }

        let mut mismatches = vec![];
        // With broadcast joins allowed the join's small side is broadcast.
        // Without them both sides are hash shuffled and coalesced together.
        for broadcast in [true, false] {
            let ctx = SessionContext::standalone().await.unwrap();
            set_option(&ctx, "datafusion.execution.target_partitions", "4").await;
            set_option(&ctx, "ballista.planner.coalesce.enabled", "true").await;
            if !broadcast {
                for key in [
                    "ballista.optimizer.broadcast_join_threshold_bytes",
                    "ballista.optimizer.broadcast_join_threshold_rows",
                    "datafusion.optimizer.hash_join_single_partition_threshold",
                    "datafusion.optimizer.hash_join_single_partition_threshold_rows",
                ] {
                    set_option(&ctx, key, "0").await;
                }
            }
            register_csv_tables(&ctx, dir, &["t1", "t2"]).await;

            for target in TARGETS {
                set_option(
                    &ctx,
                    "ballista.planner.coalesce.target_partition_bytes",
                    target,
                )
                .await;
                for (query, expected) in QUERIES.iter().zip(&expected) {
                    let case = format!("broadcast={broadcast} target={target} `{query}`");
                    check_matches(&ctx, &case, query, expected, &mut mismatches).await;
                }
            }
        }

        assert!(mismatches.is_empty(), "{}", mismatches.join("\n\n"));
    }
}
