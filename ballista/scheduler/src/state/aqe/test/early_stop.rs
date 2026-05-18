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

//! Integration tests for AQE early-stop wiring:
//!
//! * the analyzer is invoked from `AdaptiveExecutionGraph::try_new` and
//!   correctly returns 0 contexts for plans that DataFusion's
//!   limit-pushdown optimizer has stripped the GlobalLimitExec from;
//! * `early_stop_stages` correctly synthesizes successful completion
//!   for unfinished partitions and reports the running tasks for
//!   cancellation;
//! * the AdaptiveExecutionGraph respects the `enabled` config flag;
//! * the full `TaskManager` pipeline (submit → observe → emit
//!   `EarlyStopCancel` → `early_stop_job` finalize) is wired correctly.
//!
//! The analyzer's own eligibility-check coverage (eligible / ineligible
//! mixes of operators) lives in
//! `state::aqe::limit_early_stop::tests` using synthetic plans; here we
//! only exercise the end-to-end wiring against real SQL plans and real
//! graph state. Note: in current DataFusion (53.x), the
//! `LimitPushdown` physical optimizer rewrites `GlobalLimitExec` into
//! `LocalLimitExec` + fetch-bearing parents in most query shapes, so
//! these end-to-end tests for the no-shuffle case end up with empty
//! `limit_contexts`. Extending the analyzer to also recognise
//! `LocalLimitExec` at a stage boundary is tracked in the v2 spec
//! (`aqe-tasks/03b-early-stop-global-limit-v2.md`, section v2.6). For
//! the same reason, the `TaskManager`-level tests below inject the
//! tracker manually rather than relying on the analyzer.

use std::collections::HashSet;
use std::sync::Arc;

use ballista_core::config::BALLISTA_ADAPTIVE_PLANNER_ENABLED;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{
    ShuffleWritePartition, SuccessfulTask, TaskStatus, task_status,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::timestamp_millis;
use crate::state::SchedulerState;
use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::aqe::limit_tracker::JobLimitTracker;
use crate::state::aqe::test::{
    mock_context, mock_context_with_ballista_flag, mock_memory_table,
};
use crate::state::execution_graph::{ExecutionGraph, ExecutionStage};
use crate::test_utils::{mock_executor, test_cluster_context};

/// SQL whose physical plan reliably contains at least one Running
/// producer stage after `revive()`, used by every test below that
/// needs a real graph to short-stop.
const GROUP_BY_SQL: &str = "select a, count(*) from t group by a";

async fn build_graph(sql: &str) -> AdaptiveExecutionGraph {
    let ctx = mock_context();
    ctx.register_table("t", mock_memory_table()).unwrap();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    AdaptiveExecutionGraph::try_new(
        "scheduler-1",
        "job-aqe-early-stop",
        "test",
        ctx.state().session_id(),
        plan,
        timestamp_millis(),
        Arc::new(ctx.copied_config()),
        None,
    )
    .unwrap()
}

/// Return the id of the first stage in `Running` state. Panics if none
/// exists — callers expect the graph to have been `revive()`d.
fn first_running_stage<G: ExecutionGraph + ?Sized>(graph: &G) -> usize {
    graph
        .stages()
        .iter()
        .find_map(|(id, stage)| match stage {
            ExecutionStage::Running(_) => Some(*id),
            _ => None,
        })
        .expect("expected at least one Running stage")
}

#[tokio::test]
async fn analyzer_invoked_for_aqe_jobs() {
    // Any AQE job (with or without an eligible LIMIT) should have the
    // analyzer run. For LIMIT queries whose GlobalLimitExec was
    // stripped by DF's limit-pushdown, the resulting set is empty —
    // that's a correct ineligibility answer (nothing to short-stop).
    let graph =
        build_graph("select a, count(*) from t group by a limit 3").await;
    assert!(graph.limit_contexts.is_empty());
}

#[tokio::test]
async fn disabling_flag_short_circuits_analyzer() {
    use ballista_core::config::BALLISTA_AQE_LIMIT_EARLY_STOP_ENABLED;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    let config = SessionConfig::new_with_ballista()
        .set_str(BALLISTA_AQE_LIMIT_EARLY_STOP_ENABLED, "false")
        .with_target_partitions(2)
        .with_round_robin_repartition(false);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("t", mock_memory_table()).unwrap();
    let plan = ctx
        .sql(GROUP_BY_SQL)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let graph = AdaptiveExecutionGraph::try_new(
        "scheduler-1",
        "job-aqe-early-stop",
        "test",
        ctx.state().session_id(),
        plan,
        timestamp_millis(),
        Arc::new(ctx.copied_config()),
        None,
    )
    .unwrap();
    assert!(graph.limit_contexts.is_empty());
}

#[tokio::test]
async fn early_stop_stages_is_no_op_for_unknown_stage_ids() {
    let mut graph = build_graph(GROUP_BY_SQL).await;
    graph.revive();
    let bogus: HashSet<usize> = [999].into_iter().collect();
    let (cancel, events) = graph.early_stop_stages(&bogus).unwrap();
    assert!(cancel.is_empty());
    assert!(events.is_empty());
}

#[tokio::test]
async fn early_stop_stages_synthesizes_completion_and_reports_running_tasks() {
    let mut graph = build_graph(GROUP_BY_SQL).await;
    graph.revive();
    let producer = first_running_stage(&graph);

    let executor = mock_executor("exec-1".to_string());
    let mut launched = 0usize;
    while let Some(task) = graph.pop_next_task(&executor.id).unwrap() {
        if task.partition.stage_id != producer {
            break;
        }
        launched += 1;
    }
    assert!(
        launched > 0,
        "expected to launch at least one task in stage {producer}"
    );

    let producer_ids: HashSet<usize> = [producer].into_iter().collect();
    let (cancel, _events) = graph.early_stop_stages(&producer_ids).unwrap();
    assert_eq!(
        cancel.len(),
        launched,
        "every launched task should be reported for cancellation"
    );
    for task in &cancel {
        assert_eq!(task.stage_id, producer);
        assert_eq!(task.executor_id, executor.id);
    }
    assert!(
        matches!(
            graph.stages().get(&producer),
            Some(ExecutionStage::Successful(_))
        ),
        "producer stage should have transitioned Running -> Successful"
    );
}

// -------------------------------------------------------------------------
// TaskManager-level integration tests
//
// Each test runs:
//   submit_job  -> active graph cached
//   inject JobLimitTracker (manual; see module-level note about v2.6)
//   update_task_statuses with synthetic successful TaskStatus(es)
//     -> assert presence / absence / cardinality of EarlyStopCancel
// One additional test exercises the early_stop_job finalization path.

type TestSchedulerState = SchedulerState<LogicalPlanNode, PhysicalPlanNode>;

async fn build_state_with_job(
    sql: &str,
    job_id: &str,
) -> (TestSchedulerState, ExecutorMetadata) {
    let cluster = test_cluster_context();
    let state = SchedulerState::new_with_default_scheduler_name(
        cluster,
        BallistaCodec::default(),
    );

    let ctx = mock_context_with_ballista_flag(BALLISTA_ADAPTIVE_PLANNER_ENABLED);
    ctx.register_table("t", mock_memory_table()).unwrap();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    state
        .task_manager
        .queue_job(job_id, "test", timestamp_millis())
        .unwrap();
    state
        .task_manager
        .submit_job(
            job_id,
            "test",
            &ctx.state().session_id(),
            plan,
            timestamp_millis(),
            Arc::new(ctx.copied_config()),
            None,
            None,
        )
        .await
        .unwrap();

    let executor = mock_executor("exec-1".to_string());
    state
        .executor_manager
        .register_executor(
            executor.clone(),
            ExecutorData {
                executor_id: executor.id.clone(),
                total_task_slots: 16,
                available_task_slots: 16,
            },
        )
        .await
        .unwrap();

    (state, executor)
}

async fn producer_stage_id(state: &TestSchedulerState, job_id: &str) -> usize {
    let cached = state
        .task_manager
        .get_active_execution_graph(job_id)
        .expect("job should be in active cache");
    let graph = cached.read().await;
    first_running_stage(&**graph)
}

fn synthetic_status(
    job_id: &str,
    stage_id: u32,
    partition_id: u32,
    executor_id: &str,
    num_rows: u64,
) -> TaskStatus {
    TaskStatus {
        // task_id is synthetic — the observe path keys off (job_id,
        // stage_id) only.
        task_id: partition_id,
        job_id: job_id.to_string(),
        stage_id,
        stage_attempt_num: 0,
        partition_id,
        launch_time: 0,
        start_exec_time: 0,
        end_exec_time: 0,
        metrics: vec![],
        status: Some(task_status::Status::Successful(SuccessfulTask {
            executor_id: executor_id.to_string(),
            partitions: vec![ShuffleWritePartition {
                partition_id: partition_id as u64,
                num_batches: 1,
                num_rows,
                num_bytes: num_rows * 8,
                file_id: None,
                is_sort_shuffle: false,
            }],
        })),
    }
}

/// Inject a tracker for `job_id` with the configured `limit` targeting
/// the given producer stage. Returns the `Arc` so callers can inspect
/// `is_triggered()` / `rows_so_far()` afterwards.
fn inject_tracker(
    state: &TestSchedulerState,
    job_id: &str,
    limit: u64,
    producer_stage: usize,
) -> Arc<JobLimitTracker> {
    let tracker = Arc::new(JobLimitTracker::new(
        limit,
        1.5,
        [producer_stage].into_iter().collect(),
    ));
    state
        .task_manager
        .insert_limit_tracker_for_test(job_id, tracker.clone());
    tracker
}

/// Drive a single batch of synthetic successful statuses for one stage
/// and return the count of `EarlyStopCancel` events emitted for `job_id`.
async fn drive_rows(
    state: &TestSchedulerState,
    executor: &ExecutorMetadata,
    job_id: &str,
    stage_id: usize,
    rows_per_partition: &[u64],
) -> usize {
    let statuses: Vec<TaskStatus> = rows_per_partition
        .iter()
        .enumerate()
        .map(|(idx, rows)| {
            synthetic_status(
                job_id,
                stage_id as u32,
                idx as u32,
                &executor.id,
                *rows,
            )
        })
        .collect();
    let events = state
        .task_manager
        .update_task_statuses(executor, statuses)
        .await
        .unwrap();
    events
        .iter()
        .filter(|e| {
            matches!(
                e,
                QueryStageSchedulerEvent::EarlyStopCancel { job_id: jid }
                    if jid == job_id
            )
        })
        .count()
}

#[tokio::test]
async fn update_task_statuses_emits_early_stop_cancel_at_threshold() {
    let job_id = "job-early-stop-emit";
    let (state, executor) = build_state_with_job(GROUP_BY_SQL, job_id).await;
    let producer = producer_stage_id(&state, job_id).await;
    let tracker = inject_tracker(&state, job_id, 10, producer);

    // limit=10, sf=1.5 -> threshold=15; 20 rows crosses on the first batch.
    let fires = drive_rows(&state, &executor, job_id, producer, &[20]).await;
    assert_eq!(fires, 1, "expected exactly one EarlyStopCancel");
    assert!(tracker.is_triggered());
    assert_eq!(tracker.rows_so_far(), 20);
}

#[tokio::test]
async fn update_task_statuses_does_not_emit_below_threshold() {
    let job_id = "job-early-stop-below";
    let (state, executor) = build_state_with_job(GROUP_BY_SQL, job_id).await;
    let producer = producer_stage_id(&state, job_id).await;
    let tracker = inject_tracker(&state, job_id, 100, producer);

    // limit=100, sf=1.5 -> threshold=150; 50 rows stays below.
    let fires = drive_rows(&state, &executor, job_id, producer, &[50]).await;
    assert_eq!(fires, 0);
    assert!(!tracker.is_triggered());
    assert_eq!(tracker.rows_so_far(), 50);
}

#[tokio::test]
async fn update_task_statuses_emits_early_stop_only_once() {
    let job_id = "job-early-stop-once";
    let (state, executor) = build_state_with_job(GROUP_BY_SQL, job_id).await;
    let producer = producer_stage_id(&state, job_id).await;
    let _tracker = inject_tracker(&state, job_id, 10, producer);

    let fires1 = drive_rows(&state, &executor, job_id, producer, &[50]).await;
    assert_eq!(fires1, 1);
    let fires2 = drive_rows(&state, &executor, job_id, producer, &[50]).await;
    assert_eq!(fires2, 0, "EarlyStopCancel must fire exactly once per job");
}

#[tokio::test]
async fn early_stop_job_finalizes_producer_stage_and_clears_tracker() {
    let job_id = "job-early-stop-finalize";
    let (state, executor) = build_state_with_job(GROUP_BY_SQL, job_id).await;
    let producer = producer_stage_id(&state, job_id).await;

    // Launch tasks so the producer has running work that early_stop_job
    // must report for cancellation.
    {
        let cached = state
            .task_manager
            .get_active_execution_graph(job_id)
            .unwrap();
        let mut graph = cached.write().await;
        let mut launched = 0;
        while let Some(task) = graph.pop_next_task(&executor.id).unwrap() {
            if task.partition.stage_id != producer {
                break;
            }
            launched += 1;
        }
        assert!(launched > 0, "expected to launch at least one task");
    }

    inject_tracker(&state, job_id, 10, producer);

    let (cancel, _events) =
        state.task_manager.early_stop_job(job_id).await.unwrap();
    assert!(
        !cancel.is_empty(),
        "expected at least one running task to be reported for cancellation"
    );
    for task in &cancel {
        assert_eq!(task.stage_id, producer);
        assert_eq!(task.executor_id, executor.id);
    }
    assert!(
        state
            .task_manager
            .get_limit_tracker_for_test(job_id)
            .is_none(),
        "tracker must be removed once early_stop_job fires"
    );

    let cached = state
        .task_manager
        .get_active_execution_graph(job_id)
        .unwrap();
    let graph = cached.read().await;
    assert!(
        matches!(
            graph.stages().get(&producer),
            Some(ExecutionStage::Successful(_))
        ),
        "producer stage should be Successful after early_stop_job"
    );
}

#[tokio::test]
async fn early_stop_job_is_no_op_without_tracker() {
    let job_id = "job-early-stop-missing";
    let (state, _executor) = build_state_with_job(GROUP_BY_SQL, job_id).await;

    let (cancel, events) =
        state.task_manager.early_stop_job(job_id).await.unwrap();
    assert!(cancel.is_empty());
    assert!(events.is_empty());
}
