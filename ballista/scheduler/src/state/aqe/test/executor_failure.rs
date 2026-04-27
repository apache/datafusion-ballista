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

//! Tests for executor failure handling in AdaptiveExecutionGraph.
//! Mirrors the static-graph tests in
//! `ballista/scheduler/src/state/execution_graph.rs`.

use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::aqe::test::{test_aqe_aggregation_plan, test_aqe_join_plan};
use crate::state::execution_graph::ExecutionGraph;
use crate::test_utils::{
    mock_completed_task, mock_executor,
    revive_graph_and_complete_next_stage_with_executor,
};
use ballista_core::error::Result;
use ballista_core::serde::scheduler::ExecutorMetadata;

/// Local equivalent of static `drain_tasks`: keep popping tasks and reporting
/// completion via the given surviving executor until no more tasks are
/// available even after a final revive.
fn drain_aqe(
    graph: &mut AdaptiveExecutionGraph,
    executor: &ExecutorMetadata,
) -> Result<()> {
    loop {
        graph.revive();
        let mut popped = false;
        while let Some(task) = graph.pop_next_task(&executor.id)? {
            popped = true;
            let task_status = mock_completed_task(task, &executor.id);
            graph.update_task_status(executor, vec![task_status], 4, 4)?;
        }
        if !popped {
            // No more tasks to pop. Try one more revive to flush state, then exit.
            if !graph.revive() {
                break;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_reset_completed_stage_executor_lost() -> Result<()> {
    let executor1 = mock_executor("executor-id1".to_string());
    let executor2 = mock_executor("executor-id2".to_string());
    let mut join_graph = test_aqe_join_plan(4).await;

    // Initial state: graph has the leaf stages resolved.
    let initial_stage_count = join_graph.stage_count();
    assert!(initial_stage_count >= 2, "expected at least 2 leaf stages");

    join_graph.revive();

    // Complete the first leaf stage on executor1.
    revive_graph_and_complete_next_stage_with_executor(
        &mut join_graph,
        &executor1,
    )?;

    // Complete the second leaf stage on executor2.
    revive_graph_and_complete_next_stage_with_executor(
        &mut join_graph,
        &executor2,
    )?;

    join_graph.revive();

    // Pop and complete one task in the next stage on executor1.
    if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
        let task_status = mock_completed_task(task, &executor1.id);
        join_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
    }
    // Pop a second task on executor1 but don't report status (running).
    let _running = join_graph.pop_next_task(&executor1.id)?;

    let reset = join_graph.reset_stages_on_lost_executor(&executor1.id)?;

    // At least one stage was reset (combining rollback + rerun).
    assert!(
        !reset.0.is_empty(),
        "expected at least one stage to be reset"
    );

    // The graph still has tasks available to drive to completion.
    drain_aqe(&mut join_graph, &executor2)?;
    assert!(
        join_graph.is_successful(),
        "join plan failed to complete after executor loss"
    );

    Ok(())
}

#[tokio::test]
async fn test_reset_resolved_stage_executor_lost() -> Result<()> {
    let executor1 = mock_executor("executor-id1".to_string());
    let executor2 = mock_executor("executor-id2".to_string());
    let mut join_graph = test_aqe_join_plan(4).await;

    join_graph.revive();

    // Complete first leaf stage on executor1 (its outputs end up
    // referencing executor1).
    revive_graph_and_complete_next_stage_with_executor(
        &mut join_graph,
        &executor1,
    )?;

    // Complete second leaf stage on executor2.
    revive_graph_and_complete_next_stage_with_executor(
        &mut join_graph,
        &executor2,
    )?;

    // The downstream stage is now Resolved (waiting for revive) but no
    // tasks have been popped yet.
    let reset = join_graph.reset_stages_on_lost_executor(&executor1.id)?;

    // Either the resolved stage was rolled back, or the upstream successful
    // stage was reset for re-execution. We expect at least one of these.
    assert!(
        !reset.0.is_empty(),
        "expected at least one stage to be reset (rollback or rerun)"
    );

    // Plan must still complete on executor2.
    drain_aqe(&mut join_graph, &executor2)?;
    assert!(
        join_graph.is_successful(),
        "join plan failed to complete after Resolved-stage rollback"
    );

    Ok(())
}

#[tokio::test]
async fn test_task_update_after_reset_stage() -> Result<()> {
    let executor1 = mock_executor("executor-id1".to_string());
    let executor2 = mock_executor("executor-id2".to_string());
    let mut agg_graph = test_aqe_aggregation_plan(4).await;

    agg_graph.revive();

    // Complete the first stage on executor1 — outputs reference executor1.
    revive_graph_and_complete_next_stage_with_executor(
        &mut agg_graph,
        &executor1,
    )?;

    agg_graph.revive();

    // First task in the second stage, on executor2.
    if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
        let task_status = mock_completed_task(task, &executor2.id);
        agg_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
    }

    // Second task in the second stage, on executor1.
    if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
        let task_status = mock_completed_task(task, &executor1.id);
        agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
    }

    // Third task — popped (running) but not completed, on executor1.
    let task = agg_graph.pop_next_task(&executor1.id)?;

    let reset = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;

    // The 3rd task's late status arrives after the reset.
    if let Some(t) = task {
        let task_status = mock_completed_task(t, &executor1.id);
        agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
    }

    assert!(!reset.0.is_empty(), "expected stages to be reset");

    // Idempotent: a second reset call must produce no further changes.
    let reset2 = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;
    assert!(
        reset2.0.is_empty(),
        "expected second reset call to be a no-op, got {:?}",
        reset2.0
    );

    drain_aqe(&mut agg_graph, &executor2)?;
    assert!(
        agg_graph.is_successful(),
        "agg plan failed to complete after reset/late-status"
    );

    Ok(())
}

