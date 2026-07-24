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

mod fixtures;
mod stats_table;

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::prelude::SessionContext;

use stats_table::TpchStatsTable;

#[tokio::test]
async fn stats_table_reports_injected_rows() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(TpchStatsTable::new(Arc::clone(&schema), 12_345)),
    )
    .unwrap();

    let plan = ctx
        .sql("SELECT a FROM t")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .unwrap();
    assert_eq!(stats.num_rows, Precision::Inexact(12_345));
}

#[tokio::test]
async fn staged_plan_text_is_nonempty_and_shuffled() {
    let text = fixtures::staged_plan_text("q1").await;
    assert!(
        text.contains("ShuffleWriterExec"),
        "q1 plan should contain shuffle stages:\n{text}"
    );
    assert!(
        !text.contains("plan_stability"),
        "job id should be normalized out"
    );
    assert!(text.contains("=== Stage"), "stage banners present");
}

#[tokio::test]
async fn multi_statement_q15_plans() {
    let text = fixtures::staged_plan_text("q15").await;
    assert!(
        text.contains("=== Stage"),
        "q15 (create/select/drop view) should plan:\n{text}"
    );
}

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/tpch_plan_stability/approved")
        .join(format!("{name}.txt"))
}

async fn check_query(name: &str) {
    let actual = fixtures::staged_plan_text(name).await;
    let path = golden_path(name);
    if std::env::var("BALLISTA_GENERATE_GOLDEN").is_ok() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, actual.trim_end().to_string() + "\n").unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!("missing golden {path:?}; regenerate with BALLISTA_GENERATE_GOLDEN=1")
    });
    assert_eq!(
        actual.trim_end(),
        expected.trim_end(),
        "distributed plan drift for {name}. Review the change; if intended, regenerate with \
         BALLISTA_GENERATE_GOLDEN=1 cargo test -p ballista-scheduler --test tpch_plan_stability"
    );
}

macro_rules! plan_stability_test {
    ($($name:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() { check_query(stringify!($name)).await; }
        )+
    };
}

plan_stability_test!(
    q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19,
    q20, q21, q22
);
