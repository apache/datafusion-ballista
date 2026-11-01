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

//! A stage transition whose plan rewrite fails must leave the stage in the
//! graph unchanged, matching the static graph.

use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::aqe::test::{mock_batch, mock_context};
use crate::state::execution_graph::ExecutionGraph;
use crate::state::execution_stage::ExecutionStage;
use crate::test_utils::{fail_plan_rewrites, revive_graph_and_complete_next_stage};
use ballista_core::error::Result;
use std::collections::HashSet;
use std::sync::Arc;

/// Builds an adaptive graph for a two-stage aggregation.
async fn test_aggregation_plan() -> Result<AdaptiveExecutionGraph> {
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;
    let logical_plan = ctx
        .sql("select min(a), c from t group by c")
        .await?
        .logical_plan()
        .clone();

    AdaptiveExecutionGraph::try_new(
        "localhost:50050",
        &"job".into(),
        "",
        &ctx,
        &logical_plan,
        0,
    )
    .await
}

/// Completes the first stage and returns the id of the resolved stage that
/// reads its shuffle output.
async fn graph_with_resolved_reader_stage() -> Result<(AdaptiveExecutionGraph, usize)> {
    let mut graph = test_aggregation_plan().await?;
    revive_graph_and_complete_next_stage(&mut graph)?;
    let stage_id = graph
        .stages
        .iter()
        .find_map(|(stage_id, stage)| {
            matches!(stage, ExecutionStage::Resolved(stage) if !stage.inputs.is_empty())
                .then_some(*stage_id)
        })
        .expect("expected a resolved stage with inputs");
    Ok((graph, stage_id))
}

#[tokio::test]
async fn test_resolve_stage_preserves_stage_on_plan_rewrite_error() -> Result<()> {
    let (mut graph, stage_id) = graph_with_resolved_reader_stage().await?;
    // The adaptive planner hands out resolved stages, so roll one back to get
    // an unresolved stage to resolve.
    assert!(graph.rollback_resolved_stage(stage_id)?);
    let stage_count = graph.stage_count();
    let original_plan = match graph.stages.get_mut(&stage_id) {
        Some(ExecutionStage::UnResolved(stage)) => {
            assert!(stage.resolvable());
            fail_plan_rewrites(&mut stage.plan)
        }
        _ => panic!("expected an unresolved stage"),
    };

    assert!(graph.resolve_stage(stage_id).is_err());
    assert_eq!(graph.stage_count(), stage_count);
    match graph.stages.get(&stage_id) {
        Some(ExecutionStage::UnResolved(stage)) => {
            assert!(Arc::ptr_eq(&stage.plan, &original_plan));
        }
        _ => panic!("expected the original unresolved stage"),
    }

    Ok(())
}

#[tokio::test]
async fn test_rollback_resolved_stage_preserves_stage_on_plan_rewrite_error() -> Result<()>
{
    let (mut graph, stage_id) = graph_with_resolved_reader_stage().await?;
    let stage_count = graph.stage_count();
    let (original_plan, original_attempt) = match graph.stages.get_mut(&stage_id) {
        Some(ExecutionStage::Resolved(stage)) => {
            (fail_plan_rewrites(&mut stage.plan), stage.stage_attempt_num)
        }
        _ => unreachable!(),
    };

    assert!(graph.rollback_resolved_stage(stage_id).is_err());
    assert_eq!(graph.stage_count(), stage_count);
    match graph.stages.get(&stage_id) {
        Some(ExecutionStage::Resolved(stage)) => {
            assert_eq!(stage.stage_attempt_num, original_attempt);
            assert!(Arc::ptr_eq(&stage.plan, &original_plan));
        }
        _ => panic!("expected the original resolved stage"),
    }

    Ok(())
}

#[tokio::test]
async fn test_rollback_running_stage_preserves_stage_on_plan_rewrite_error() -> Result<()>
{
    let (mut graph, stage_id) = graph_with_resolved_reader_stage().await?;
    assert!(graph.revive());
    assert!(graph.pop_next_task("executor-id")?.is_some());
    let stage_count = graph.stage_count();
    let (original_plan, original_attempt, original_pending, original_tasks) =
        match graph.stages.get_mut(&stage_id) {
            Some(ExecutionStage::Running(stage)) => (
                fail_plan_rewrites(&mut stage.plan),
                stage.stage_attempt_num,
                stage.available_tasks(),
                stage.running_tasks(),
            ),
            _ => panic!("expected a running stage"),
        };

    assert!(
        graph
            .rollback_running_stage(stage_id, HashSet::from(["executor-id".to_owned()]))
            .is_err()
    );
    assert_eq!(graph.stage_count(), stage_count);
    match graph.stages.get(&stage_id) {
        Some(ExecutionStage::Running(stage)) => {
            assert_eq!(stage.stage_attempt_num, original_attempt);
            assert_eq!(stage.available_tasks(), original_pending);
            assert_eq!(stage.running_tasks(), original_tasks);
            assert!(Arc::ptr_eq(&stage.plan, &original_plan));
        }
        _ => panic!("expected the original running stage"),
    }

    Ok(())
}
