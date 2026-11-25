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

#![cfg(feature = "standalone")]

use ballista::prelude::SessionContextExt;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use std::sync::Arc;

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    })
    .await
    .expect("runtime filter must not deadlock the query");
    pretty_format_batches(&batches).unwrap().to_string()
}

#[tokio::test]
async fn distributed_runtime_filter_matches_unfiltered_joins() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["build", "probe"] {
        std::fs::create_dir(dir.path().join(name)).unwrap();
    }
    std::fs::write(dir.path().join("build/a.csv"), "k,v\n2,1\n,2\n").unwrap();
    std::fs::write(dir.path().join("build/b.csv"), "k,v\n8,3\n2,4\n").unwrap();
    for part in 0..4 {
        let mut csv = "k,v\n,0\n".to_string();
        for k in 0..25 {
            csv.push_str(&format!("{},{}\n", part * 25 + k, k));
        }
        std::fs::write(dir.path().join(format!("probe/{part}.csv")), csv).unwrap();
    }
    let ctx = SessionContext::standalone().await.unwrap();
    for setting in [
        "ballista.planner.adaptive.enabled = true",
        "datafusion.optimizer.prefer_hash_join = true",
        "datafusion.execution.target_partitions = 4",
    ] {
        run(&ctx, &format!("SET {setting}")).await;
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    // Parquet provides build cardinality, allowing AQE to choose a broadcast
    // stage whose completion naturally precedes the probe scan.
    let build_parquet = dir.path().join("build_parquet");
    let local =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    local
        .read_csv(
            dir.path().join("build").to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await
        .unwrap()
        .write_parquet(build_parquet.to_str().unwrap(), Default::default(), None)
        .await
        .unwrap();
    ctx.register_parquet(
        "build",
        build_parquet.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_csv(
        "probe",
        dir.path().join("probe").to_str().unwrap(),
        CsvReadOptions::new().schema(&schema),
    )
    .await
    .unwrap();
    let queries = [
        "SELECT b.k, b.v, p.v FROM build b JOIN probe p ON b.k = p.k WHERE b.v > 0 AND p.v >= 0 ORDER BY b.k, b.v, p.v",
        "SELECT b.k, b.v, p.v FROM build b JOIN probe p ON b.k IS NOT DISTINCT FROM p.k WHERE b.v > 0 AND p.v >= 0 ORDER BY b.k, b.v, p.v",
        "SELECT p.k FROM build b RIGHT JOIN probe p ON b.k = p.k ORDER BY p.k",
        "SELECT b.k, p.k FROM build b JOIN (SELECT k FROM probe ORDER BY k LIMIT 3) p ON b.k = p.k ORDER BY b.k, p.k",
    ];
    for query in queries {
        run(&ctx, "SET ballista.planner.runtime_filter.enabled = false").await;
        let expected = run(&ctx, query).await;
        run(&ctx, "SET ballista.planner.runtime_filter.enabled = true").await;
        assert_eq!(run(&ctx, query).await, expected, "{query}");
    }
    let plan = run(&ctx, &format!("EXPLAIN ANALYZE {}", queries[0])).await;
    assert!(plan.contains("RuntimeFilterBuildExec"), "{plan}");
    assert!(plan.contains("RuntimeFilterApplyExec"), "{plan}");
    assert!(plan.contains("ready=true"), "{plan}");
    assert!(
        plan.split("rows_pruned=").skip(1).any(|suffix| {
            suffix
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<usize>()
                .is_ok_and(|rows| rows > 0)
        }),
        "runtime filter must actually prune rows: {plan}"
    );
    // Independently runnable partitioned stages must preserve results even
    // when the producer has not reported before the probe is dispatched.
    for setting in [
        "ballista.optimizer.broadcast_join_threshold_bytes = 0",
        "ballista.optimizer.broadcast_join_threshold_rows = 0",
        "ballista.optimizer.stage_build_side = false",
    ] {
        run(&ctx, &format!("SET {setting}")).await;
    }
    let partitioned = run(&ctx, queries[0]).await;
    let plan = run(&ctx, &format!("EXPLAIN ANALYZE {}", queries[0])).await;
    assert!(plan.contains("mode=Partitioned"), "{plan}");
    run(&ctx, "SET ballista.planner.runtime_filter.enabled = false").await;
    assert_eq!(partitioned, run(&ctx, queries[0]).await);
}
