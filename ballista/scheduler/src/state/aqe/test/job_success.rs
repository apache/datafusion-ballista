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

//! Job-success lifecycle tests for the adaptive execution graph.

use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::aqe::test::{mock_batch, mock_context};
use crate::state::execution_graph::ExecutionGraph;
use crate::test_utils::revive_graph_and_complete_next_stage;
use ballista_core::error::Result;
use ballista_core::serde::protobuf::job_status;
use std::collections::HashSet;

/// Plans `sql` as an adaptive job and completes its stages one by one until
/// the job succeeds. While the job is running no stage may be reported as
/// intermediate, because the final stage is not known yet.
async fn run_to_success(sql: &str) -> Result<AdaptiveExecutionGraph> {
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;
    let logical_plan = ctx.sql(sql).await?.logical_plan().clone();

    let mut graph = AdaptiveExecutionGraph::try_new(
        "localhost:50050",
        &"job".into(),
        "",
        &ctx,
        &logical_plan,
        0,
        None,
    )
    .await?;

    while !graph.is_successful() {
        assert!(graph.intermediate_stage_ids().is_empty());
        revive_graph_and_complete_next_stage(&mut graph)?;
    }
    assert!(matches!(
        graph.status().status.as_ref(),
        Some(job_status::Status::Successful(_))
    ));

    Ok(graph)
}

/// After success, every stage except the one the job's output locations point
/// at is intermediate, so its shuffle data can be reclaimed right away.
fn assert_intermediate_excludes_only_final_stage(graph: &AdaptiveExecutionGraph) {
    let final_stage_ids: HashSet<usize> = graph
        .output_locations()
        .iter()
        .map(|location| location.partition_id.stage_id)
        .collect();
    assert_eq!(final_stage_ids.len(), 1);

    let mut expected: Vec<u32> = graph
        .stages()
        .keys()
        .filter(|stage_id| !final_stage_ids.contains(stage_id))
        .map(|stage_id| *stage_id as u32)
        .collect();
    expected.sort_unstable();
    assert!(!expected.is_empty(), "expected a multi-stage job");

    let mut intermediate = graph.intermediate_stage_ids();
    intermediate.sort_unstable();
    assert_eq!(intermediate, expected);
}

#[tokio::test]
async fn aggregation_reports_non_final_stages_as_intermediate() -> Result<()> {
    let graph = run_to_success("select min(a), c from t group by c").await?;
    assert_intermediate_excludes_only_final_stage(&graph);
    Ok(())
}

#[tokio::test]
async fn nested_aggregation_reports_non_final_stages_as_intermediate() -> Result<()> {
    let graph = run_to_success(
        "select cnt, count(*) from (select c, count(*) as cnt from t group by c) \
         group by cnt",
    )
    .await?;
    assert_intermediate_excludes_only_final_stage(&graph);
    Ok(())
}
