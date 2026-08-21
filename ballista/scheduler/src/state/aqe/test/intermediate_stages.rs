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

//! Intermediate-stage identification for the adaptive execution graph, which
//! drives the immediate reclaim of shuffle data when a job succeeds.

use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::aqe::test::{mock_batch, mock_context};
use crate::state::execution_graph::ExecutionGraph;
use crate::test_utils::revive_graph_and_complete_next_stage;
use ballista_core::error::Result;
use std::collections::HashSet;

/// Builds an adaptive graph for two chained aggregations, which the adaptive
/// planner runs as a chain of stages.
async fn test_two_aggregations_plan() -> AdaptiveExecutionGraph {
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch().unwrap()).unwrap();

    let logical_plan = ctx
        .sql("select c0, count(*) from (select min(a) as c0, c as c1 from t group by c) group by c0")
        .await
        .unwrap()
        .into_unoptimized_plan();

    AdaptiveExecutionGraph::try_new(
        "localhost:50050",
        &"job".into(),
        "",
        &ctx,
        &logical_plan,
        0,
    )
    .await
    .unwrap()
}

/// Drives the graph until every stage is complete, guarding against a stuck
/// loop if no stage makes progress.
fn run_to_success(graph: &mut AdaptiveExecutionGraph) -> Result<()> {
    for _ in 0..16 {
        if graph.is_successful() {
            return Ok(());
        }
        revive_graph_and_complete_next_stage(graph)?;
    }
    panic!("adaptive job did not complete: {:?}", graph.status());
}

// The adaptive planner never populates `output_links`, so the default
// implementation would treat every stage as final and reclaim nothing. The
// override derives the final stage from `output_locations` instead.
#[tokio::test]
async fn test_intermediate_stage_ids_excludes_final_stage() -> Result<()> {
    let mut graph = test_two_aggregations_plan().await;

    // Nothing is reclaimable before the job produces its output locations.
    assert!(
        graph.intermediate_stage_ids().is_empty(),
        "a running job must not have its stage data reclaimed"
    );

    run_to_success(&mut graph)?;

    let final_stage_ids: HashSet<usize> = graph
        .output_locations()
        .iter()
        .map(|location| location.partition_id.stage_id)
        .collect();
    assert!(
        !final_stage_ids.is_empty(),
        "a successful job must report its output locations"
    );

    let all_stage_ids: HashSet<usize> = graph.stages().keys().copied().collect();
    assert!(
        all_stage_ids.len() > final_stage_ids.len(),
        "expected a multi-stage job, found stages {all_stage_ids:?}"
    );

    let intermediate: HashSet<usize> = graph
        .intermediate_stage_ids()
        .into_iter()
        .map(|stage_id| stage_id as usize)
        .collect();

    // Exactly the non-terminal stages, and never a stage that is still
    // referenced by the job's output locations.
    let expected: HashSet<usize> = all_stage_ids
        .difference(&final_stage_ids)
        .copied()
        .collect();
    assert_eq!(intermediate, expected);
    assert!(intermediate.is_disjoint(&final_stage_ids));

    Ok(())
}
