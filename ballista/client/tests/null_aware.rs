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

#[cfg(test)]
#[cfg(feature = "standalone")]
mod null_aware {
    use std::fs;
    use std::path::Path;

    use crate::common::{
        check_matches, collect_text, datafusion_context, register_csv_tables, set_option,
    };
    use ballista::prelude::SessionContextExt;
    use datafusion::prelude::*;
    use tempfile::TempDir;

    fn write_tables(dir: &Path, t2_has_null: bool) {
        // t1: 4 files -> 4 partitions, values 0..19
        let t1 = dir.join("t1");
        fs::create_dir_all(&t1).unwrap();
        for i in 0..4i32 {
            let mut s = String::from("a\n");
            for v in 0..5i32 {
                s.push_str(&format!("{}\n", i * 5 + v));
            }
            fs::write(t1.join(format!("p{i}.csv")), s).unwrap();
        }

        // t2: 4 files -> 4 partitions. One file optionally holds a NULL key.
        // The second column keeps the empty field unambiguous, since a bare
        // blank line is skipped by the CSV reader rather than read as NULL.
        let t2 = dir.join("t2");
        fs::create_dir_all(&t2).unwrap();
        fs::write(t2.join("p0.csv"), "b,tag\n0,x\n1,x\n").unwrap();
        fs::write(
            t2.join("p1.csv"),
            if t2_has_null {
                "b,tag\n,x\n"
            } else {
                "b,tag\n2,x\n"
            },
        )
        .unwrap();
        fs::write(t2.join("p2.csv"), "b,tag\n3,x\n").unwrap();
        fs::write(t2.join("p3.csv"), "b,tag\n4,x\n").unwrap();
    }

    const QUERIES: &[(&str, &str)] = &[
        (
            "order_by",
            "select a from t1 where a not in (select b from t2) order by a",
        ),
        // An aggregate above the join, so `EnsureRequirements` reruns over its stage.
        (
            "group_by",
            "select a % 3, count(*) from t1 where a not in (select b from t2) \
             group by a % 3 order by 1",
        ),
    ];

    async fn run_case(case: &str, t2_has_null: bool) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_tables(dir, t2_has_null);

        let df_ctx = datafusion_context();
        register_csv_tables(&df_ctx, dir, &["t1", "t2"]).await;
        let mut expected = vec![];
        for (_, sql) in QUERIES {
            expected.push(collect_text(&df_ctx, sql).await);
        }

        let mut mismatches = vec![];
        // "default" exercises the NOT IN logical rewrite under Ballista's
        // stock configuration (sort-merge joins): the rewrite removes the
        // subquery before decorrelation, so no null-aware join is planned at
        // all. "prefer_hash_join" and "aqe" exercise the rewrite under the
        // hash-join planning paths. "rewrite_disabled" turns the rewrite off
        // with hash joins preferred, covering the fallback path where the
        // null-aware join survives and is lowered to a single task, under the
        // default adaptive planner; "rewrite_disabled_static" does the same
        // under the static planner. Both run one partition per task, as on a
        // cluster.
        for variant in [
            "default",
            "prefer_hash_join",
            "aqe",
            "rewrite_disabled",
            "rewrite_disabled_static",
        ] {
            let ctx = SessionContext::standalone().await.unwrap();
            if variant == "aqe" {
                set_option(&ctx, "ballista.planner.adaptive.enabled", "true").await;
            }
            if variant != "default" {
                set_option(&ctx, "datafusion.optimizer.prefer_hash_join", "true").await;
            }
            if variant.starts_with("rewrite_disabled") {
                set_option(&ctx, "ballista.optimizer.not_in_subquery_rewrite", "false")
                    .await;
                set_option(&ctx, "ballista.scheduler.max_partitions_per_task", "1").await;
            }
            if variant == "rewrite_disabled_static" {
                set_option(&ctx, "ballista.planner.adaptive.enabled", "false").await;
            }
            register_csv_tables(&ctx, dir, &["t1", "t2"]).await;

            for ((name, sql), expected) in QUERIES.iter().zip(&expected) {
                let label = format!("{case}/{variant}/{name}");
                check_matches(&ctx, &label, sql, expected, &mut mismatches).await;
            }
        }

        assert!(mismatches.is_empty(), "{}", mismatches.join("\n\n"));
    }

    #[tokio::test]
    async fn not_in_with_null_in_subquery() {
        run_case("with_null", true).await;
    }

    #[tokio::test]
    async fn not_in_without_null_in_subquery() {
        run_case("without_null", false).await;
    }
}
