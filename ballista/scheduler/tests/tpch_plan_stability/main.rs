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

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::prelude::SessionContext;

use stats_table::TpchStatsTable;

#[tokio::test]
async fn stats_table_reports_injected_rows() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(TpchStatsTable::new(Arc::clone(&schema), 12_345)))
        .unwrap();

    let plan = ctx
        .sql("SELECT a FROM t")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let stats = plan.partition_statistics(None).unwrap();
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
