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

/// End-to-end test of a query that reads an empty table under adaptive query
/// planning.
#[cfg(test)]
#[cfg(feature = "standalone")]
mod empty_input {
    use crate::common::{ballista_state, collect_lines, register_csv_tables};
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::BALLISTA_ADAPTIVE_PLANNER_ENABLED;
    use datafusion::prelude::SessionContext;
    use std::io::Write;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;

    /// The join finishes and returns the same rows as a run of the static
    /// planner.
    #[tokio::test]
    async fn left_join_with_an_empty_table_finishes() {
        let sql = "SELECT c.id, e.id AS other FROM customers c \
                   LEFT JOIN empty e ON e.email = c.email ORDER BY c.id";
        let dir = TempDir::new().unwrap();
        for (name, rows) in [("customers", 100), ("empty", 0)] {
            let table = dir.path().join(name);
            std::fs::create_dir(&table).unwrap();
            let mut out = std::fs::File::create(table.join("part-0.csv")).unwrap();
            writeln!(out, "id,email").unwrap();
            for id in 0..rows {
                writeln!(out, "{id},user{id}@example.com").unwrap();
            }
        }
        let reference =
            context(dir.path(), (BALLISTA_ADAPTIVE_PLANNER_ENABLED, "false")).await;
        let ctx = context(dir.path(), (BALLISTA_ADAPTIVE_PLANNER_ENABLED, "true")).await;

        let expected = collect_lines(&reference, sql).await;
        let actual =
            tokio::time::timeout(Duration::from_secs(60), collect_lines(&ctx, sql))
                .await
                .expect("the query finishes in 60 seconds");
        assert_eq!(actual, expected);
    }

    /// A standalone cluster over [`ballista_state`] with `option`, with the
    /// tables in `dir` registered, that plans every hash join as partitioned.
    async fn context(dir: &Path, option: (&str, &str)) -> SessionContext {
        let state = ballista_state(&[
            option,
            (
                "datafusion.optimizer.hash_join_single_partition_threshold",
                "0",
            ),
            (
                "datafusion.optimizer.hash_join_single_partition_threshold_rows",
                "0",
            ),
        ]);
        let ctx = SessionContext::standalone_with_state(state).await.unwrap();
        register_csv_tables(&ctx, dir, &["customers", "empty"]).await;
        ctx
    }
}
