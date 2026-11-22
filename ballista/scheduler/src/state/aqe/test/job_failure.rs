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

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::aqe::test::{mock_batch, mock_context};
use crate::state::execution_graph::ExecutionGraph;
use crate::state::execution_stage::ExecutionStage;
use crate::test_utils::{
    fetch_partition_failure, join_aggregate_sort_logical_plan, mock_executor,
    mock_failed_task, revive_graph_and_complete_next_stage_with_executor,
};
use ballista_core::error::Result;
use ballista_core::serde::protobuf::{
    FailedTask, JobStatus, ShuffleWritePartition, SuccessfulTask, failed_task,
    job_status, task_status,
};
use datafusion::execution::context::{SessionConfig, SessionContext};

/// Builds an adaptive graph for a join (two concurrent leaf stages).
async fn test_join_plan(partition: usize) -> AdaptiveExecutionGraph {
    let mut config = SessionConfig::new().with_target_partitions(partition);
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    let ctx = SessionContext::new_with_config(config);

    let logical_plan = join_aggregate_sort_logical_plan();

    AdaptiveExecutionGraph::try_new(
        "localhost:50050",
        &"job".into(),
        "",
        &ctx,
        &logical_plan,
        0,
        None,
        None,
    )
    .await
    .unwrap()
}

/// Builds an adaptive graph for an aggregation: a map stage, then a reduce
/// stage reading its shuffle output.
async fn test_aggregation_plan() -> AdaptiveExecutionGraph {
    let ctx = SessionContext::new_with_config(mock_context().copied_config());
    ctx.register_batch("t", mock_batch().unwrap()).unwrap();
    let sql = "select c, sum(a) from t group by c";
    let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
    AdaptiveExecutionGraph::try_new(
        "localhost:50050",
        &"job".into(),
        "",
        &ctx,
        &plan,
        0,
        None,
        None,
    )
    .await
    .unwrap()
}

/// Runs the map stage of an aggregation on executor "a", leaving the reduce
/// stage, which reads its output, running.
async fn aggregation_with_map_output_on_a() -> AdaptiveExecutionGraph {
    let mut graph = test_aggregation_plan().await;
    revive_graph_and_complete_next_stage_with_executor(
        &mut graph,
        &mock_executor("a".to_string()),
    )
    .unwrap();
    graph.revive();
    assert!(matches!(graph.stages[&0], ExecutionStage::Successful(_)));
    assert!(matches!(graph.stages[&1], ExecutionStage::Running(_)));
    graph
}

const CANNOT_RERUN: &str = "cannot rerun a finished stage";

// AQE cannot rerun a finished stage whose output was lost (#1986): its
// planner no longer tracks the stage, so the rerun's tasks could not report
// and the job hung (#2539). Losing the executor with that output fails the
// rollback instead, and the task manager fails the job on that error.
#[tokio::test]
async fn losing_the_output_of_a_finished_stage_fails_the_rollback() {
    let mut graph = aggregation_with_map_output_on_a().await;

    let err = graph.reset_stages_on_lost_executor("a").unwrap_err();

    assert!(err.to_string().contains(CANNOT_RERUN), "{err}");
}

// Nothing in the adaptive graph resolves a stage again once it is rolled back,
// so losing a stage's input fails the job even when the input is not a
// finished stage of this graph, as when it was dropped from it.
#[tokio::test]
async fn losing_the_input_of_a_stage_fails_the_rollback() {
    let mut graph = aggregation_with_map_output_on_a().await;
    graph.stages.remove(&0);

    let err = graph.reset_stages_on_lost_executor("a").unwrap_err();

    assert!(
        err.to_string()
            .contains("stage 1 lost input on executor a, and adaptive query execution cannot roll back a stage"),
        "{err}"
    );
    assert!(matches!(graph.stages[&1], ExecutionStage::Running(_)));
}

// The same loss, found by a reduce task failing to fetch the map output: the
// job fails instead of hanging.
#[tokio::test]
async fn a_fetch_failure_on_the_output_of_a_finished_stage_fails_the_job() -> Result<()> {
    let mut graph = aggregation_with_map_output_on_a().await;
    let b = mock_executor("b".to_string());
    let task = graph.pop_next_task(&b.id)?.unwrap();
    let status = mock_failed_task(task, fetch_partition_failure("a", 0, 0));

    let events = graph.update_task_status(&b, vec![status], 4, 4)?;

    // The event aborts the job and cancels its tasks. The failed stage is the
    // reading stage 1, not the finished stage 0 whose output was lost.
    let fail_message = events
        .iter()
        .find_map(|event| match event {
            QueryStageSchedulerEvent::JobRunningFailed { fail_message, .. } => {
                Some(fail_message)
            }
            _ => None,
        })
        .expect("a JobRunningFailed event");
    assert!(fail_message.contains("stage 1 failed"), "{fail_message}");
    assert!(
        fail_message.contains("finished stage 0 was lost"),
        "{fail_message}"
    );
    assert!(fail_message.contains(CANNOT_RERUN), "{fail_message}");
    assert!(matches!(
        graph.status().status,
        Some(job_status::Status::Failed(_))
    ));
    Ok(())
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
                runtime_stats: vec![],
                window_state: vec![],
                task_column_stats: vec![],
                runtime_filters: vec![],
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
    // The cancelled stage must be gone from the stage map entirely.
    // `is_successful()` requires every stage in the map to be Successful, so
    // a cancelled stage kept as Failed would make the job hang forever once
    // all real work completed (this hung the TPC-DS suite on CI).
    assert!(
        !graph.stages.contains_key(&held_stage_id),
        "cancelled stage must be removed from the graph, found {:?}",
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
            runtime_stats: vec![],
            window_state: vec![],
            task_column_stats: vec![],
            runtime_filters: vec![],
        })),
    };
    graph
        .update_task_status(&executor, vec![late], 4, 4)
        .expect("late task status from a replan-cancelled stage must be discarded");

    Ok(())
}
