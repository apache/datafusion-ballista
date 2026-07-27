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

#[cfg(test)]
#[cfg(feature = "standalone")]
mod null_aware {
    use std::fs;
    use std::path::Path;

    use ballista::prelude::SessionContextExt;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::*;

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

    async fn register(ctx: &SessionContext, dir: &Path) {
        for name in ["t1", "t2"] {
            ctx.register_csv(
                name,
                dir.join(name).to_str().unwrap(),
                CsvReadOptions::new()
                    .has_header(true)
                    .file_extension(".csv"),
            )
            .await
            .unwrap();
        }
    }

    const QUERY: &str = "select a from t1 where a not in (select b from t2) order by a";

    async fn run_case(case: &str, t2_has_null: bool) {
        let dir = std::env::temp_dir().join(format!("ballista_null_aware_{case}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        write_tables(&dir, t2_has_null);

        let df_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(4),
        );
        register(&df_ctx, &dir).await;
        let df_batches = df_ctx.sql(QUERY).await.unwrap().collect().await.unwrap();
        let expected = pretty_format_batches(&df_batches).unwrap().to_string();

        let mut mismatches = vec![];
        // The default Ballista setting selects SortMergeJoinExec and loses the
        // null-aware flag before scheduler lowering, so this test covers the
        // hash-join planning paths that retain the flag.
        for variant in ["prefer_hash_join", "aqe"] {
            let ctx = SessionContext::standalone().await.unwrap();
            if variant == "aqe" {
                ctx.sql("SET ballista.planner.adaptive.enabled = true")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
            }
            ctx.sql("SET datafusion.optimizer.prefer_hash_join = true")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            register(&ctx, &dir).await;

            match ctx.sql(QUERY).await.unwrap().collect().await {
                Ok(batches) => {
                    let actual = pretty_format_batches(&batches).unwrap().to_string();
                    if actual.trim() != expected.trim() {
                        mismatches.push(format!(
                            "[{case}/{variant}] expected:\n{expected}\nactual:\n{actual}"
                        ));
                    }
                }
                Err(error) => {
                    mismatches.push(format!("[{case}/{variant}] failed: {error}"));
                }
            }
        }

        let _ = fs::remove_dir_all(&dir);
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
