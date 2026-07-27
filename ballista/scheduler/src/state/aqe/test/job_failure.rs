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

//! Job-failure lifecycle tests for the adaptive execution graph.

use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::execution_graph::ExecutionGraph;
use crate::state::execution_stage::ExecutionStage;
use crate::test_utils::mock_executor;
use ballista_core::error::Result;
use ballista_core::serde::protobuf::{
    FailedTask, JobStatus, ShuffleWritePartition, SuccessfulTask, failed_task,
    job_status, task_status,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::functions_aggregate::sum::sum;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::{JoinType, col};
use datafusion::test_util::scan_empty_with_partitions;

/// Builds an adaptive graph for a join (two concurrent leaf stages).
async fn test_join_plan(partition: usize) -> AdaptiveExecutionGraph {
    let mut config = SessionConfig::new().with_target_partitions(partition);
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    let ctx = SessionContext::new_with_config(config);

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("gmv", DataType::UInt64, false),
    ]);

    let left_plan = scan_empty_with_partitions(Some("left"), &schema, None, 2).unwrap();
    let right_plan = scan_empty_with_partitions(Some("right"), &schema, None, 2)
        .unwrap()
        .build()
        .unwrap();
    let sort_expr = SortExpr::new(col("id"), false, false);
    let logical_plan = left_plan
        .join(right_plan, JoinType::Inner, (vec!["id"], vec!["id"]), None)
        .unwrap()
        .aggregate(vec![col("left.id")], vec![sum(col("left.gmv"))])
        .unwrap()
        .sort(vec![sort_expr])
        .unwrap()
        .build()
        .unwrap();

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

// Same contract as the static graph's test: aborting transitions every running
// stage to Failed and returns its in-flight tasks for cancellation.
#[tokio::test]
async fn test_abort_running_cancels_stages_and_returns_inflight_tasks() -> Result<()> {
    let executor = mock_executor("executor-id1".to_string());
    let mut graph = test_join_plan(2).await;

    // Call revive to move the two leaf Resolved stages to Running
    graph.revive();
    assert!(
        graph.running_stages().len() >= 2,
        "expected two concurrently running leaf stages, found {:?}",
        graph.running_stages()
    );

    // Dispatch a task so there is an in-flight task to cancel
    let _task = graph.pop_next_task(&executor.id)?.unwrap();

    // Aborting cancels every running stage and returns its in-flight tasks
    let cancelled = graph.abort_running("job aborted".to_string());

    assert!(
        !cancelled.is_empty(),
        "abort_running must return the in-flight tasks to cancel"
    );
    assert!(
        graph.running_stages().is_empty(),
        "every running stage must be cancelled, found {:?}",
        graph.running_stages()
    );
    assert!(
        matches!(
            graph.status(),
            JobStatus {
                status: Some(job_status::Status::Failed(_)),
                ..
            }
        ),
        "the job must be Failed after abort"
    );

    // In-flight tasks of the cancelled stage are recorded as Failed(TaskKilled)
    let has_killed_task = graph.stages.values().any(|stage| match stage {
        ExecutionStage::Failed(failed) => failed.task_infos.iter().any(|info| {
            matches!(
                &info.task_status,
                task_status::Status::Failed(FailedTask {
                    failed_reason: Some(failed_task::FailedReason::TaskKilled(_)),
                    ..
                })
            )
        }),
        _ => false,
    });
    assert!(
        has_killed_task,
        "in-flight tasks must be recorded as Failed(TaskKilled) after abort"
    );

    Ok(())
}

// Reproduces the orphan-stage lifecycle: when the build side of a join
// produces no data, the replan cancels the (already running) probe stage.
// The cancelled stage must be retired from the graph so the job does not
// wait on it forever, and a late task completion from it must be discarded
// rather than fail the whole update.
#[tokio::test]
async fn test_replan_cancelled_stage_is_retired_and_late_task_discarded() -> Result<()> {
    let executor = mock_executor("executor-id1".to_string());
    let mut graph = test_join_plan(2).await;

    // Move the two leaf stages to Running so tasks can be dispatched
    graph.revive();
    let running = graph.running_stages();
    assert!(
        running.len() >= 2,
        "expected two leaf stages, found {running:?}"
    );

    // The two leaf stages run concurrently. Dispatch every available task,
    // then hold back exactly one (the "probe" task) in flight; complete all
    // the rest with empty output so the replan sees that side produced no
    // data and cancels the stage the held task belongs to. The dispatch
    // order is not deterministic (stages live in a HashMap), so pick the
    // held task arbitrarily and derive its stage id from the task itself.
    let mut held_task = None;
    let mut complete_tasks = Vec::new();
    while let Some(task) = graph.pop_next_task(&executor.id)? {
        if held_task.is_none() {
            held_task = Some(task);
        } else {
            complete_tasks.push(task);
        }
    }
    let held_task = held_task.expect("expected at least one dispatchable task");
    let held_stage_id = held_task.key.stage_id;
    assert!(
        !complete_tasks.is_empty(),
        "expected tasks from the sibling stage to drive the replan"
    );

    // Complete every other (sibling-stage) task with empty output so the
    // replan sees that side produced no data and cancels the held task's
    // stage.
    for task in complete_tasks {
        let status = ballista_core::serde::protobuf::TaskStatus {
            task_id: task.key.task_id as u32,
            job_id: graph.job_id().clone().into(),
            stage_id: task.key.stage_id as u32,
            stage_attempt_num: 0,
            launch_time: 0,
            start_exec_time: 0,
            end_exec_time: 0,
            metrics: vec![],
            status: Some(task_status::Status::Successful(SuccessfulTask {
                executor_id: executor.id.clone(),
                partitions: vec![ShuffleWritePartition {
                    partition_id: task.key.task_id as u64,
                    num_batches: 0,
                    num_rows: 0,
                    num_bytes: 0,
                    file_id: None,
                    is_sort_shuffle: false,
                }],
            })),
        };
        graph.update_task_status(&executor, vec![status], 4, 4)?;
    }

    // The held task's stage must have been retired: the job no longer tracks
    // it as running, so it cannot wedge the job waiting for it.
    assert!(
        !graph.running_stages().contains(&held_stage_id),
        "cancelled stage must not remain running, running={:?}",
        graph.running_stages()
    );
    assert!(
        matches!(
            graph.stages.get(&held_stage_id),
            Some(ExecutionStage::Failed(_)) | None
        ),
        "cancelled stage must be Failed or removed, found {:?}",
        graph.stages.get(&held_stage_id)
    );

    // A late completion from the already-cancelled held task must be
    // discarded instead of failing the whole status update (which used to
    // error with "Invalid stage ID" and wedge the job).
    let late = ballista_core::serde::protobuf::TaskStatus {
        task_id: held_task.key.task_id as u32,
        job_id: graph.job_id().clone().into(),
        stage_id: held_stage_id as u32,
        stage_attempt_num: 0,
        launch_time: 0,
        start_exec_time: 0,
        end_exec_time: 0,
        metrics: vec![],
        status: Some(task_status::Status::Successful(SuccessfulTask {
            executor_id: executor.id.clone(),
            partitions: vec![],
        })),
    };
    graph
        .update_task_status(&executor, vec![late], 4, 4)
        .expect("late task status from a replan-cancelled stage must be discarded");

    Ok(())
}
