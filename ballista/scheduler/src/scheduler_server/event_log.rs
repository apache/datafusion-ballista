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

//! Builds `HistoryEvent`s from scheduler event-loop state and appends them to
//! the [`ballista_history::writer::EventLogWriter`]. These are the pure,
//! synchronous event-builder functions; the actual emission -- deciding
//! *when* to call them -- lives at the top of
//! `query_stage_scheduler::QueryStageScheduler::on_receive`.
//!
//! The builders reuse `crate::api::dto_build`, the same DTO builders backing
//! the live REST API, so a job's `JobEnd` event and its `GET /api/job/{id}`
//! response serialize identically for the same graph state.

use crate::api::dto_build::{
    build_job_dot, graph_to_job_response, graph_to_query_stages,
    session_config_to_job_config, task_row_counts, task_status_to_dto,
};
use crate::state::execution_graph::ExecutionGraphBox;
use ballista_api_types::dto::PlanFormat;
use ballista_core::serde::protobuf::{TaskStatus, task_status};
use ballista_history::event::{
    HistoryEvent, JobEndStatus, SCHEMA_VERSION, TaskEndMetrics,
};
use datafusion::physical_plan::displayable;

/// Builds the `JobStart` event for a job that has just been submitted.
pub(crate) fn job_start_event(
    graph: &ExecutionGraphBox,
    queued_at: u64,
    submitted_at: u64,
) -> HistoryEvent {
    HistoryEvent::JobStart {
        version: SCHEMA_VERSION,
        job_id: graph.job_id().to_string(),
        job_name: graph.job_name().to_string(),
        queued_at,
        submitted_at,
        logical_plan: graph.logical_plan().map(|p| p.to_string()),
        // Same rendering the `get_job` handler uses for `PlanFormat::Default`.
        physical_plan: Some(
            displayable(graph.physical_plan().as_ref())
                .indent(false)
                .to_string(),
        ),
    }
}

/// Builds one `TaskEnd` event per finished task in `statuses`.
///
/// Only terminal statuses are recorded: `Running` updates are transient
/// in-flight reports, and a status-less update carries no outcome to record at
/// all, so both are skipped.
pub(crate) fn task_end_events(
    executor_id: &str,
    statuses: &[TaskStatus],
) -> Vec<HistoryEvent> {
    statuses
        .iter()
        .filter_map(|s| {
            let status = match s.status.as_ref()? {
                task_status::Status::Running(_) => return None,
                terminal => task_status_to_dto(terminal),
            };
            Some(HistoryEvent::TaskEnd {
                stage_id: s.stage_id,
                task_id: s.task_id,
                executor_id: executor_id.to_string(),
                status,
                launch_time: s.launch_time,
                start_exec_time: s.start_exec_time,
                end_exec_time: s.end_exec_time,
                metrics: task_end_metrics(s),
            })
        })
        .collect()
}

/// Sums a task's raw operator metrics into the timeline's `TaskEndMetrics`,
/// using the same extraction the stage-summary REST DTO performs so the
/// timeline and stage views agree. Absent metrics yield zeros.
fn task_end_metrics(status: &TaskStatus) -> TaskEndMetrics {
    let (input_rows, output_rows, elapsed_compute_nanos) =
        task_row_counts(&status.metrics);
    TaskEndMetrics {
        input_rows,
        output_rows,
        elapsed_compute_nanos,
    }
}

/// Builds the `JobEnd` event for a job that has just finished (successfully
/// or not), embedding the same DTOs the REST API would serve for this job at
/// this moment: the job summary, per-stage summaries, the session config, and
/// the DOT-format graph.
pub(crate) fn job_end_event(
    graph: &ExecutionGraphBox,
    status: JobEndStatus,
    queued_at: u64,
    completed_at: u64,
) -> HistoryEvent {
    // `PlanFormat::Default` -- the same default the `get_job` handler falls
    // back to when no `?plan_format=` query param is supplied -- so the
    // stored JSON matches what a live `GET /api/job/{id}` would have
    // returned at this point.
    let job = Box::new(graph_to_job_response(graph, PlanFormat::Default));
    // Rendered as of `completed_at` rather than the wall clock, so the stored
    // record is a snapshot of the job at the moment it ended and replaying it
    // is deterministic.
    let stages = Box::new(graph_to_query_stages(
        graph,
        PlanFormat::Default,
        completed_at as u128,
    ));
    let dot = build_job_dot(graph).unwrap_or_default();
    let config = session_config_to_job_config(graph.session_config().as_ref());

    HistoryEvent::JobEnd {
        version: SCHEMA_VERSION,
        status,
        queued_at,
        started_at: graph.start_time(),
        completed_at,
        job,
        stages,
        config,
        dot,
    }
}

/// Builds the terminal `JobEnd` event for a cancelled job.
///
/// Cancellation is recorded from the event-log tee, which runs *before* the
/// scheduler applies the cancel to the graph, so the graph still reports the
/// job as running here. The embedded job DTO's status fields are therefore
/// overwritten, otherwise the history server would show a cancelled job as
/// perpetually `Running`.
///
/// `queued_at` is not carried on `JobCancel`, so it is recorded as 0.
pub(crate) fn job_cancel_event(
    graph: &ExecutionGraphBox,
    cancelled_at: u64,
) -> HistoryEvent {
    let mut event = job_end_event(
        graph,
        JobEndStatus::Cancelled,
        /* queued_at */ 0,
        cancelled_at,
    );
    if let HistoryEvent::JobEnd { job, .. } = &mut event {
        job.job_status = CANCELLED_STATUS.to_string();
        job.status = CANCELLED_STATUS.to_string();
    }
    event
}

/// The status string a cancelled job is recorded (and served) with. The live
/// REST API has no equivalent -- a cancelled job's graph is dropped before it
/// can be observed -- so this is history-only.
const CANCELLED_STATUS: &str = "Cancelled";

#[cfg(test)]
mod tests {
    use crate::scheduler_server::event_log::*;
    use crate::state::execution_graph::ExecutionGraphBox;
    use crate::state::execution_graph_dot::tests::test_graph;
    use ballista_core::serde::protobuf::{SuccessfulTask, TaskStatus, task_status};
    use ballista_history::writer::EventLogWriter;

    #[test]
    fn task_end_events_map_one_per_finished_task() {
        let statuses = vec![TaskStatus {
            task_id: 7,
            job_id: "job-1".into(),
            stage_id: 1,
            stage_attempt_num: 0,
            launch_time: 100,
            start_exec_time: 110,
            end_exec_time: 200,
            metrics: vec![],
            status: Some(task_status::Status::Successful(SuccessfulTask::default())),
        }];
        let events = task_end_events("exec-1", &statuses);
        assert_eq!(events.len(), 1);
        let line = serde_json::to_string(&events[0]).unwrap();
        assert!(line.contains("\"ev\":\"TaskEnd\""));
        assert!(line.contains("\"task_id\":7"));
        assert!(line.contains("\"executor_id\":\"exec-1\""));
    }

    // `task_end_events` skips `Running` statuses -- they are transient
    // in-flight updates, not the terminal states the timeline records.
    #[test]
    fn task_end_events_skips_running_tasks() {
        let statuses = vec![TaskStatus {
            task_id: 0,
            job_id: "job-1".into(),
            stage_id: 1,
            stage_attempt_num: 0,
            launch_time: 100,
            start_exec_time: 110,
            end_exec_time: 0,
            metrics: vec![],
            status: Some(task_status::Status::Running(Default::default())),
        }];
        assert!(task_end_events("exec-1", &statuses).is_empty());
    }

    // A status update with no status at all reports no outcome, so there is
    // nothing terminal to record for it either.
    #[test]
    fn task_end_events_skips_tasks_with_no_status() {
        let statuses = vec![TaskStatus {
            task_id: 0,
            job_id: "job-1".into(),
            stage_id: 1,
            stage_attempt_num: 0,
            launch_time: 100,
            start_exec_time: 110,
            end_exec_time: 0,
            metrics: vec![],
            status: None,
        }];
        assert!(task_end_events("exec-1", &statuses).is_empty());
    }

    /// A cancelled job is recorded from a graph that has not yet been marked
    /// failed, so the event must carry the cancelled status itself rather than
    /// the graph's in-flight `Running`.
    #[tokio::test]
    async fn job_cancel_event_reports_cancelled_not_running() {
        let graph = test_graph().await.unwrap();
        let graph: ExecutionGraphBox = Box::new(graph);

        let event = job_cancel_event(&graph, /* cancelled_at */ 42);
        let json = serde_json::to_string(&event).unwrap();

        assert!(json.contains("\"ev\":\"JobEnd\""));
        assert!(json.contains("\"status\":\"Cancelled\""));
        assert!(json.contains("\"job_status\":\"Cancelled\""));
        assert!(
            !json.contains("\"Running\""),
            "cancelled job should not be recorded as running, got: {json}"
        );
    }

    /// `job_end_event`'s embedded DTOs (`job`, `stages`, `dot`) are built by
    /// the same `dto_build` functions backing the live REST API, so a direct
    /// builder test on the serialized event is an adequate substitute for
    /// wiring a full `QueryStageScheduler` through `on_receive` here (which
    /// would additionally require standing up executor/task-manager state
    /// just to reach a finished job).
    #[tokio::test]
    async fn job_end_event_embeds_job_and_stage_plan_strings() {
        let graph = test_graph().await.unwrap();
        let graph: ExecutionGraphBox = Box::new(graph);

        let event = job_end_event(
            &graph,
            JobEndStatus::Succeeded,
            /* queued_at */ 1,
            /* completed_at */ 2,
        );
        let json = serde_json::to_string(&event).unwrap();

        assert!(json.contains("\"ev\":\"JobEnd\""));
        assert!(json.contains("\"status\":\"Succeeded\""));
        assert!(json.contains("\"job_id\":\"job_id\""));
        // The job's top-level physical plan, embedded via `build_job_response`.
        assert!(
            json.contains("DataSourceExec: (Memory)"),
            "expected job physical plan in JobEnd event, got: {json}"
        );
        // The per-stage summaries, embedded via `build_query_stages_response`.
        assert!(
            json.contains("\"stage_status\":\"Resolved\""),
            "expected a resolved stage summary in JobEnd event, got: {json}"
        );
        // The DOT graph, embedded via `build_job_dot`.
        assert!(
            json.contains("digraph"),
            "expected a dot graph in JobEnd event, got: {json}"
        );
    }

    #[tokio::test]
    async fn job_start_event_captures_plan_and_timestamps() {
        let graph = test_graph().await.unwrap();
        let graph: ExecutionGraphBox = Box::new(graph);

        let event =
            job_start_event(&graph, /* queued_at */ 5, /* submitted_at */ 10);
        let json = serde_json::to_string(&event).unwrap();

        assert!(json.contains("\"ev\":\"JobStart\""));
        assert!(json.contains("\"job_id\":\"job_id\""));
        assert!(json.contains("\"job_name\":\"job_name\""));
        assert!(json.contains("\"queued_at\":5"));
        assert!(json.contains("\"submitted_at\":10"));
        // `job_start_event` renders the graph's pre-staging physical plan
        // (`graph.physical_plan()`), unlike `job_end_event`'s embedded job
        // DTO which shows the plan reconstructed from stages -- assert on the
        // join at its root rather than the leaf scan string.
        assert!(
            json.contains("HashJoinExec"),
            "expected job physical plan in JobStart event, got: {json}"
        );
    }

    /// End-to-end: drive the real `EventLogWriter` over a temp dir with a
    /// `JobStart` followed by a `JobEnd`, then read the `.eventlog` file back
    /// and assert on line order and content -- proving the builders here
    /// produce events the writer can actually persist and that a consumer
    /// (the eventual history server) can read back as ordered JSONL.
    #[tokio::test]
    async fn writer_round_trip_orders_job_start_before_job_end() {
        let graph = test_graph().await.unwrap();
        let graph: ExecutionGraphBox = Box::new(graph);
        let job_id = graph.job_id().to_string();

        let dir = tempfile::tempdir().unwrap();
        let writer = EventLogWriter::new(dir.path().to_path_buf(), 16);

        writer.append(&job_id, job_start_event(&graph, 1, 2));
        writer.append(
            &job_id,
            job_end_event(&graph, JobEndStatus::Succeeded, 1, 20),
        );
        writer.flush_job(&job_id).await;

        let path = dir.path().join(format!("{job_id}.eventlog"));
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = contents.lines().collect();

        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"ev\":\"JobStart\""));
        assert!(lines[1].contains("\"ev\":\"JobEnd\""));
        assert!(lines[1].contains("DataSourceExec: (Memory)"));
    }

    /// End-to-end DTO parity: the history server, replaying a real event log
    /// through `EventLogWriter` + `HistoryStore::load`, must serve
    /// byte-identical JSON to what the live scheduler would return for the
    /// same job via `dto_build::{build_job_response, build_query_stages_response}`.
    ///
    /// This is `job_end_event`'s only round-trip coverage through the real
    /// writer + `HistoryStore` (as opposed to the direct builder-output
    /// assertions above), so it lives here as a crate-internal `#[cfg(test)]`
    /// module rather than an integration test under `tests/`: `dto_build`'s
    /// builders, `job_end_event` itself, and
    /// `execution_graph_dot::tests::test_graph` are all `pub(crate)` /
    /// cfg(test)-gated and therefore unreachable from a separate `tests/`
    /// integration-test crate, which only sees the scheduler crate's public API.
    #[tokio::test]
    async fn history_store_serves_byte_identical_json_to_live_scheduler() {
        use crate::history::HistoryStore;

        let graph = test_graph().await.unwrap();
        let graph: ExecutionGraphBox = Box::new(graph);
        let job_id = graph.job_id().to_string();

        // The live DTOs a running scheduler would serve for this job right now,
        // via the same builders the REST handlers call.
        // `completed_at` is the snapshot instant for both sides, so the
        // comparison does not race the wall clock.
        const COMPLETED_AT: u64 = 2;
        let live_job = graph_to_job_response(&graph, PlanFormat::Default);
        let live_stages =
            graph_to_query_stages(&graph, PlanFormat::Default, COMPLETED_AT as u128);

        // Produce the same `JobEnd` event the scheduler emits on completion,
        // write it through the real async writer, then load it back via the
        // history server's own `HistoryStore::load` -- exercising the full
        // write -> read -> serve path, not just the event builder.
        let event = job_end_event(&graph, JobEndStatus::Succeeded, 1, COMPLETED_AT);
        let dir = tempfile::tempdir().unwrap();
        let writer = EventLogWriter::new(dir.path().to_path_buf(), 16);
        writer.append(&job_id, event);
        writer.flush_job(&job_id).await;

        let store = HistoryStore::load(dir.path()).unwrap();
        let replayed = store.jobs.get(&job_id).expect("job should be replayed");

        // The core fidelity guarantee: the history server would return
        // byte-identical JSON to what the live scheduler returned for this job.
        assert_eq!(
            serde_json::to_string(&replayed.job).unwrap(),
            serde_json::to_string(&live_job).unwrap(),
        );
        assert_eq!(
            serde_json::to_string(&replayed.stages).unwrap(),
            serde_json::to_string(&live_stages).unwrap(),
        );
    }
}
