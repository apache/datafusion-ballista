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

//! Standalone history server: indexes completed event logs and serves the same
//! `/api/*` responses the live scheduler does, from stored DTOs.

use crate::api::SchedulerErrorResponse;
use axum::response::IntoResponse;
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    routing::get,
};
use ballista_api_types::dto::{JobConfig, JobResponse};
use ballista_core::BALLISTA_VERSION;
use ballista_history::event::JobIndex;
use ballista_history::reader::{
    ReadError, ReplayedJob, read_completed_job, read_job_index,
};
use datafusion::DATAFUSION_VERSION;
use http::StatusCode;
use http::header::CONTENT_TYPE;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Where one completed job lives, and just enough about it to list it.
struct JobEntry {
    /// Frozen summary, everything `GET /api/jobs` reports.
    index: JobIndex,
    /// The `<job_id>.eventlog` the rest of the job is read back from.
    path: PathBuf,
}

/// Index of the completed jobs found in an event-log directory.
///
/// Only each job's [`JobIndex`] is held in memory. The stored payloads (both
/// plan-bearing REST responses, the session config and the DOT graph) run to
/// megabytes for a job with many tasks, and would otherwise sit resident for
/// every job in the directory whether or not anyone ever looks at it. They are
/// read back from disk per request instead, which is fine at the rate a person
/// clicks through a UI.
#[derive(Default)]
pub struct HistoryStore {
    /// Completed jobs keyed by job id.
    jobs: HashMap<String, JobEntry>,
}

/// Why reading one job's stored payload back produced nothing.
#[derive(Debug)]
pub enum JobReadError {
    /// No job with this id was found when the directory was indexed.
    NotFound,
    /// The log was indexed at startup but could not be read now.
    Unreadable(ReadError),
    /// The log no longer has a terminal record, so it was replaced or
    /// truncated after the index was built.
    Vanished,
}

impl std::fmt::Display for JobReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobReadError::NotFound => write!(f, "no such job"),
            JobReadError::Unreadable(e) => write!(f, "event log is unreadable: {e}"),
            JobReadError::Vanished => {
                write!(f, "event log no longer contains a terminal record")
            }
        }
    }
}

impl HistoryStore {
    /// Index every completed job found under `dir`. Missing directories yield
    /// an empty store rather than an error.
    ///
    /// A single unreadable/corrupt `.eventlog` file (e.g. truncated by a
    /// crash mid-write) is logged and skipped rather than failing the whole
    /// load — one bad log must not hide every other completed job. Only a
    /// failure to read the directory itself is propagated.
    ///
    /// Each log is read once here, but only its summary is decoded, so this
    /// costs a pass over the directory rather than a copy of it in memory.
    /// Corruption confined to the payloads therefore surfaces when the job is
    /// requested rather than at startup.
    pub fn load(dir: &Path) -> std::io::Result<HistoryStore> {
        let mut jobs = HashMap::new();
        if dir.exists() {
            for entry in std::fs::read_dir(dir)? {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) != Some("eventlog") {
                    continue;
                }
                match read_job_index(&path) {
                    Ok(Some(index)) => {
                        jobs.insert(index.job_id.clone(), JobEntry { index, path });
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(
                            "skipping unreadable event log {}: {err}",
                            path.display()
                        );
                    }
                }
            }
        }
        Ok(HistoryStore { jobs })
    }

    /// How many completed jobs were indexed.
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Whether the log directory held no completed jobs.
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Read one job's stored payload back from its event log.
    ///
    /// This is blocking file I/O, so the request handlers call it from
    /// `spawn_blocking` rather than on a runtime worker.
    pub fn read_job(&self, job_id: &str) -> Result<ReplayedJob, JobReadError> {
        let entry = self.jobs.get(job_id).ok_or(JobReadError::NotFound)?;
        match read_completed_job(&entry.path) {
            Ok(Some(replayed)) => Ok(replayed),
            Ok(None) => Err(JobReadError::Vanished),
            Err(e) => Err(JobReadError::Unreadable(e)),
        }
    }
}

/// [`HistoryStore::read_job`] moved off the async runtime.
///
/// A log that was indexed at startup and cannot be read now means the file
/// changed underneath us, so both failures are logged rather than only being
/// reported to whoever happened to ask.
async fn read_job_blocking(
    store: Arc<HistoryStore>,
    job_id: String,
) -> Result<ReplayedJob, SchedulerErrorResponse> {
    let result = tokio::task::spawn_blocking(move || {
        store.read_job(&job_id).map_err(|e| (job_id, e))
    })
    .await
    .map_err(|e| {
        tracing::warn!("history server: reading an event log panicked: {e}");
        SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
    })?;

    result.map_err(|(job_id, err)| match err {
        JobReadError::NotFound => SchedulerErrorResponse::new(StatusCode::NOT_FOUND),
        JobReadError::Vanished => {
            tracing::warn!("history server: event log for {job_id} {err}");
            SchedulerErrorResponse::with_error(StatusCode::NOT_FOUND, err.to_string())
        }
        JobReadError::Unreadable(_) => {
            tracing::warn!("history server: event log for {job_id} {err}");
            SchedulerErrorResponse::with_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )
        }
    })
}

/// Build the axum router serving `/api/*` from a loaded [`HistoryStore`].
pub fn history_router(store: Arc<HistoryStore>) -> Router {
    Router::new()
        .route("/api/jobs", get(get_jobs))
        .route("/api/job/{job_id}", get(get_job))
        .route("/api/job/{job_id}/stages", get(get_stages))
        .route("/api/job/{job_id}/config", get(get_config))
        .route("/api/job/{job_id}/dot", get(get_dot))
        .route("/api/executors", get(get_executors_empty))
        .route("/api/state", get(get_state))
        .with_state(store)
}

/// Rebuild a job-list entry from the stored index.
///
/// Built from [`JobIndex`] rather than by editing the stored `/api/job/{id}`
/// payload: the list endpoint omits the plan fields, and the index carries
/// exactly the fields it does include. That keeps this path from having to
/// parse a payload it would only throw most of away.
fn list_entry(index: &JobIndex) -> JobResponse {
    JobResponse {
        job_id: index.job_id.clone(),
        job_name: index.job_name.clone(),
        job_status: index.job_status.clone(),
        status: index.status.clone(),
        num_stages: index.num_stages,
        completed_stages: index.completed_stages,
        percent_complete: index.percent_complete,
        start_time: index.start_time,
        end_time: index.end_time,
        logical_plan: None,
        physical_plan: None,
        stage_plan: None,
    }
}

/// The one endpoint that touches every job, and the reason the index is held
/// in memory at all: it is served without going near the disk.
///
/// Newest first. A job id is a random 7-character string
/// (`TaskManager::generate_job_id`), so ordering by it would be arbitrary,
/// whereas start time is both meaningful and the order the TUI puts the list
/// into once it has it.
async fn get_jobs(State(store): State<Arc<HistoryStore>>) -> Json<Vec<JobResponse>> {
    let mut jobs: Vec<JobResponse> =
        store.jobs.values().map(|j| list_entry(&j.index)).collect();
    // Job id breaks ties, so two jobs that started in the same millisecond
    // cannot swap places between requests.
    jobs.sort_by(|a, b| {
        b.start_time
            .cmp(&a.start_time)
            .then_with(|| a.job_id.cmp(&b.job_id))
    });
    Json(jobs)
}

/// Serve a stored payload exactly as the scheduler wrote it.
///
/// The payload is relayed as raw JSON rather than deserialized and
/// re-serialized, so the bytes a client receives are the bytes the live
/// scheduler produced, and a change to the REST types cannot make an existing
/// log unservable.
fn raw_json(payload: &RawValue) -> axum::response::Response {
    (
        [(CONTENT_TYPE, "application/json")],
        payload.get().to_string(),
    )
        .into_response()
}

async fn get_job(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<axum::response::Response, SchedulerErrorResponse> {
    let job = read_job_blocking(store, job_id).await?;
    Ok(raw_json(&job.job))
}

async fn get_stages(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<axum::response::Response, SchedulerErrorResponse> {
    let job = read_job_blocking(store, job_id).await?;
    Ok(raw_json(&job.stages))
}

async fn get_config(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobConfig>, SchedulerErrorResponse> {
    let job = read_job_blocking(store, job_id).await?;
    Ok(Json(job.config))
}

async fn get_dot(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<String, SchedulerErrorResponse> {
    let job = read_job_blocking(store, job_id).await?;
    Ok(job.dot)
}

async fn get_executors_empty() -> Json<Vec<()>> {
    Json(vec![])
}

/// Static `/api/state` payload. The history server has no live scheduler
/// process behind it, so every field that would normally reflect runtime
/// state (uptime, feature flags, scheduling policy) is a fixed placeholder.
/// Field names/types match the live `/api/state` response
/// (`SchedulerStateResponse` in `api/handlers.rs`) and what the TUI
/// deserializes into (`ballista-cli/src/tui/domain/mod.rs::SchedulerState`),
/// so the TUI's startup call succeeds instead of erroring out.
async fn get_state() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "started": 0,
        "version": BALLISTA_VERSION,
        "datafusion_version": DATAFUSION_VERSION,
        "substrait_support": false,
        "keda_support": false,
        "prometheus_support": false,
        "graphviz_support": false,
        "spark_support": false,
        "scheduling_policy": "history-server",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use ballista_api_types::dto::{QueryStageSummary, QueryStagesResponse};
    use ballista_history::event::{HistoryEvent, JobEnd, JobEndStatus, JobIndex};
    use std::io::Write;
    use tempfile::tempdir;
    use tower::ServiceExt; // oneshot

    const STAGE_ID_MARKER: &str = "stage-42";

    fn sample_replayed_job_with_stage(
        job_id: &str,
        stage_id: &str,
        start_time: u64,
    ) -> ReplayedJob {
        let job = JobResponse {
            job_id: job_id.into(),
            job_name: "q1".into(),
            job_status: "COMPLETED".into(),
            status: "Successful".into(),
            num_stages: 1,
            completed_stages: 1,
            percent_complete: 100,
            start_time,
            end_time: start_time + 1,
            logical_plan: Some("Projection".into()),
            physical_plan: Some("ProjectionExec".into()),
            stage_plan: Some("stage".into()),
        };
        let stages = QueryStagesResponse {
            stages: vec![QueryStageSummary {
                stage_id: stage_id.into(),
                stage_status: "Completed".into(),
                input_rows: 10,
                output_rows: 5,
                elapsed_compute: Some("1ms".into()),
                stage_plan: None,
                task_duration_percentiles: None,
                task_input_percentiles: None,
                tasks: vec![],
            }],
        };
        ReplayedJob {
            index: JobIndex {
                job_id: job_id.into(),
                job_name: "q1".into(),
                status: "Successful".into(),
                job_status: "COMPLETED".into(),
                start_time,
                end_time: start_time + 1,
                num_stages: 1,
                completed_stages: 1,
                percent_complete: 100,
            },
            job: serde_json::value::to_raw_value(&job).unwrap(),
            stages: serde_json::value::to_raw_value(&stages).unwrap(),
            config: Default::default(),
            dot: "digraph {}".into(),
        }
    }

    /// Every test goes through a real directory of logs, because the store no
    /// longer holds payloads it could be handed directly.
    fn store_with_one_job(dir: &tempfile::TempDir) -> Arc<HistoryStore> {
        write_job_end_log(&dir.path().join("job-1.eventlog"), "job-1");
        Arc::new(HistoryStore::load(dir.path()).unwrap())
    }

    #[tokio::test]
    async fn jobs_endpoint_nulls_plan_fields() {
        let dir = tempdir().unwrap();
        let app = history_router(store_with_one_job(&dir));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/jobs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("\"job_id\":\"job-1\""));
        assert!(!body.contains("physical_plan")); // nulled + skip_serializing_if
    }

    #[tokio::test]
    async fn stages_endpoint_returns_stored_dto() {
        let dir = tempdir().unwrap();
        let app = history_router(store_with_one_job(&dir));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/job/job-1/stages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: QueryStagesResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.stages.len(), 1);
        assert_eq!(body.stages[0].stage_id, STAGE_ID_MARKER);
        assert_eq!(body.stages[0].input_rows, 10);
        assert_eq!(body.stages[0].output_rows, 5);
    }

    #[tokio::test]
    async fn missing_job_returns_404_on_job_and_stages() {
        let dir = tempdir().unwrap();
        let app = history_router(store_with_one_job(&dir));

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/job/does-not-exist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/job/does-not-exist/stages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn state_endpoint_returns_static_payload() {
        let dir = tempdir().unwrap();
        let app = history_router(store_with_one_job(&dir));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for field in [
            "started",
            "version",
            "datafusion_version",
            "substrait_support",
            "keda_support",
            "prometheus_support",
            "graphviz_support",
            "spark_support",
            "scheduling_policy",
        ] {
            assert!(value.get(field).is_some(), "missing field: {field}");
        }
    }

    /// Job ids are random 7-character strings, so ordering the list by id puts
    /// it in an order that means nothing. Newest first is what a history view
    /// wants, and it is what makes a future `?limit=` worth having.
    #[tokio::test]
    async fn jobs_are_listed_newest_first() {
        let dir = tempdir().unwrap();
        for (job_id, start_time) in [("zzz", 10u64), ("aaa", 30), ("mmm", 20)] {
            write_job_end_log_with_stage(
                &dir.path().join(format!("{job_id}.eventlog")),
                job_id,
                STAGE_ID_MARKER,
                start_time,
            );
        }
        let app = history_router(Arc::new(HistoryStore::load(dir.path()).unwrap()));

        let (status, body) = get(&app, "/api/jobs").await;
        assert_eq!(status, StatusCode::OK);
        let jobs: Vec<JobResponse> = serde_json::from_str(&body).unwrap();
        let order: Vec<&str> = jobs.iter().map(|j| j.job_id.as_str()).collect();
        assert_eq!(order, ["aaa", "mmm", "zzz"]);
    }

    /// Equal start times must still produce a total order, otherwise the list
    /// can reshuffle between two identical requests.
    #[tokio::test]
    async fn jobs_with_the_same_start_time_are_ordered_by_id() {
        let dir = tempdir().unwrap();
        for job_id in ["ccc", "aaa", "bbb"] {
            write_job_end_log_with_stage(
                &dir.path().join(format!("{job_id}.eventlog")),
                job_id,
                STAGE_ID_MARKER,
                7,
            );
        }
        let app = history_router(Arc::new(HistoryStore::load(dir.path()).unwrap()));

        let (_, body) = get(&app, "/api/jobs").await;
        let jobs: Vec<JobResponse> = serde_json::from_str(&body).unwrap();
        let order: Vec<&str> = jobs.iter().map(|j| j.job_id.as_str()).collect();
        assert_eq!(order, ["aaa", "bbb", "ccc"]);
    }

    /// Fetch one path and return the status and body together.
    async fn get(app: &Router, uri: &str) -> (StatusCode, String) {
        let resp = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    /// The point of the whole design: nothing but the summary is retained, so
    /// a detail request has to go back to the file. Rewriting the log behind a
    /// loaded store and seeing the new contents served is the only way to
    /// observe that from outside.
    #[tokio::test]
    async fn detail_endpoints_read_the_log_on_demand() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("job-1.eventlog");
        let app = history_router(store_with_one_job(&dir));

        let (status, body) = get(&app, "/api/job/job-1/stages").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains(STAGE_ID_MARKER));

        write_job_end_log_with_stage(&path, "job-1", "rewritten-stage", 2);

        let (status, body) = get(&app, "/api/job/job-1/stages").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("rewritten-stage"),
            "payload should be read per request, not cached at load: {body}"
        );
    }

    /// Corruption confined to the payloads is not visible when the directory
    /// is indexed, so it has to be reported at request time. A 500 naming the
    /// problem beats an empty or truncated response.
    #[tokio::test]
    async fn a_log_that_breaks_after_indexing_reports_the_failure() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("job-1.eventlog");
        let app = history_router(store_with_one_job(&dir));

        std::fs::write(&path, [0xff, 0xfe, 0xfd]).unwrap();

        let (status, body) = get(&app, "/api/job/job-1").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("unreadable"), "got: {body}");

        // The job list is served from the index, so it still lists the job.
        let (status, body) = get(&app, "/api/jobs").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("job-1"));
    }

    /// A log whose terminal record has gone (rotated, truncated) is a job that
    /// no longer exists rather than a server fault.
    #[tokio::test]
    async fn a_log_that_loses_its_terminal_record_returns_404() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("job-1.eventlog");
        let app = history_router(store_with_one_job(&dir));

        std::fs::write(
            &path,
            "{\"ev\":\"StageStart\",\"version\":1,\"data\":{\"stage_id\":1}}\n",
        )
        .unwrap();

        let (status, _) = get(&app, "/api/job/job-1/config").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    fn write_job_end_log(path: &Path, job_id: &str) {
        write_job_end_log_with_stage(path, job_id, STAGE_ID_MARKER, 2)
    }

    fn write_job_end_log_with_stage(
        path: &Path,
        job_id: &str,
        stage_id: &str,
        start_time: u64,
    ) {
        let replayed = sample_replayed_job_with_stage(job_id, stage_id, start_time);
        let event = HistoryEvent::JobEnd(Box::new(JobEnd {
            status: JobEndStatus::Succeeded,
            queued_at: 0,
            started_at: 2,
            completed_at: 3,
            index: replayed.index,
            job: replayed.job,
            stages: replayed.stages,
            config: replayed.config,
            dot: replayed.dot,
        }));
        let line = serde_json::to_string(&event.to_record().unwrap()).unwrap();
        std::fs::write(path, format!("{line}\n")).unwrap();
    }

    #[test]
    fn load_skips_corrupt_eventlog_and_keeps_good_one() {
        let dir = tempdir().unwrap();

        // A good, readable event log.
        write_job_end_log(&dir.path().join("job-good.eventlog"), "job-good");

        // A corrupt file: invalid UTF-8, as if a crash truncated a write
        // mid-multibyte-character.
        let mut corrupt =
            std::fs::File::create(dir.path().join("job-bad.eventlog")).unwrap();
        corrupt.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
        drop(corrupt);

        let store = HistoryStore::load(dir.path()).unwrap();
        assert_eq!(store.len(), 1);
        assert!(store.read_job("job-good").is_ok());
        assert!(matches!(
            store.read_job("job-bad"),
            Err(JobReadError::NotFound)
        ));
    }
}
