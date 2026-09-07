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

mod source;
mod trigger;

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
use ballista_history::reader::{ReadError, ReplayedJob};
use datafusion::DATAFUSION_VERSION;
use futures::StreamExt;
use http::StatusCode;
use http::header::CONTENT_TYPE;
use serde_json::value::RawValue;
use source::{EventLogSource, LocalDirSource};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use trigger::{EventLogEvent, EventLogTrigger, NotifyTrigger, OnceTrigger, ScanTrigger};

/// Where one completed job lives, and just enough about it to list it.
struct JobEntry {
    /// Frozen summary, everything `GET /api/jobs` reports.
    index: JobIndex,
    /// The `<job_id>.eventlog` the rest of the job is read back from. A job
    /// still running is named `<job_id>.eventlog.running` and has no entry
    /// here yet.
    path: PathBuf,
}

/// The in-memory job index, behind one lock.
#[derive(Default)]
struct Index {
    /// Completed jobs keyed by job id.
    jobs: HashMap<String, JobEntry>,
}

/// Index of the completed jobs found in an event-log directory.
///
/// Only each job's [`JobIndex`] is held in memory. The stored payloads (both
/// plan-bearing REST responses, the session config and the DOT graph) run to
/// megabytes for a job with many tasks, and would otherwise sit resident for
/// every job in the directory whether or not anyone ever looks at it. They are
/// read back from disk per request instead, which is fine at the rate a person
/// clicks through a UI.
///
/// The directory is normally one that one or more live schedulers are still
/// writing to, so the index is not fixed at startup: [`spawn_service_tasks`]
/// runs background loops that fold newly found logs into it as its triggers
/// fire.
pub struct HistoryStore {
    /// Where this store's completed logs are found and read.
    source: Box<dyn EventLogSource>,
    trigger: Box<dyn EventLogTrigger>,
    scan_trigger: Box<dyn ScanTrigger>,
    index: RwLock<Index>,
}

/// Why reading one job's stored payload back produced nothing.
#[derive(Debug)]
pub enum JobReadError {
    /// No job with this id is in the index.
    NotFound,
    /// The log was indexed but could not be read now.
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
    /// Open a store over the event logs in `dir`, watching it for new ones.
    ///
    /// The initial index is built synchronously here, so the store is usable
    /// the moment it is constructed; [`spawn_service_tasks`] then keeps it
    /// current, both from full directory passes and from the `NotifyTrigger`
    /// set up here as logs are renamed into place.
    ///
    /// `dir` must already exist — the filesystem watch cannot be placed on a
    /// missing path. Callers that may run before any scheduler has written
    /// (the history server) create it first.
    pub fn new(dir: &Path) -> std::io::Result<HistoryStore> {
        let store = HistoryStore {
            source: Box::new(LocalDirSource::new(dir.to_path_buf())),
            scan_trigger: Box::new(OnceTrigger::default()),
            trigger: Box::new(NotifyTrigger::new(dir).map_err(std::io::Error::other)?),
            index: RwLock::new(Default::default()),
        };
        Ok(store)
    }

    /// Open a store over `dir` as a fixed snapshot: the directory is read
    /// once, here, and the index never changes afterwards. Both triggers are
    /// `NoopTrigger`, so pairing this with [`spawn_service_tasks`] is a no-op
    /// — it is for callers (tests, a one-shot server over an archived
    /// directory) that want a frozen view rather than a live one.
    pub async fn new_static(dir: &Path) -> std::io::Result<HistoryStore> {
        let store = HistoryStore {
            source: Box::new(LocalDirSource::new(dir.to_path_buf())),
            scan_trigger: Box::new(trigger::NoopTrigger::default()),
            trigger: Box::new(trigger::NoopTrigger::default()),
            index: RwLock::new(Default::default()),
        };
        store.load_index().await;
        Ok(store)
    }

    /// Fold every completed log currently in the source into the index, once.
    ///
    /// A single full [`EventLogSource::scan_jobs`] pass: every readable log is
    /// upserted, an unreadable one is logged and skipped, a directory-listing
    /// failure is logged. Nothing is reconciled — a log that later disappears
    /// or is rewritten under a new id is not removed. This is the whole index
    /// for a store built by [`HistoryStore::new_static`]; a store built by
    /// [`HistoryStore::new`] gets the same pass from [`spawn_service_tasks`]'s
    /// scan loop instead.
    async fn load_index(&self) {
        let mut paths = self.source.scan_jobs();
        while let Some(item) = paths.next().await {
            match item {
                Ok(path) => index_one(self, path).await,
                Err(err) => tracing::warn!(
                    "history server: listing the log directory failed: {err}"
                ),
            }
        }
    }

    /// How many completed jobs are indexed.
    pub fn len(&self) -> usize {
        self.read_index().jobs.len()
    }

    /// Whether no completed jobs are indexed.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read one job's stored payload back from its event log.
    ///
    /// The read is file I/O; `EventLogSource` implementations move it off the
    /// runtime worker (see `LocalDirSource`), so handlers can `await` this
    /// directly.
    pub async fn read_job(&self, job_id: &str) -> Result<ReplayedJob, JobReadError> {
        // Copy the path out rather than reading with the lock held, so a slow
        // read of one job does not block a rescan or the job list.
        let path = {
            let index = self.read_index();
            index
                .jobs
                .get(job_id)
                .map(|entry| entry.path.clone())
                .ok_or(JobReadError::NotFound)?
        };
        match self.source.read_completed_job(&path).await {
            Ok(Some(replayed)) => Ok(replayed),
            Ok(None) => Err(JobReadError::Vanished),
            Err(e) => Err(JobReadError::Unreadable(e)),
        }
    }

    /// Every indexed job's summary, in no particular order.
    fn summaries(&self) -> Vec<JobIndex> {
        self.read_index()
            .jobs
            .values()
            .map(|entry| entry.index.clone())
            .collect()
    }

    /// A poisoned index means an index write panicked partway through. What is in
    /// there is still a valid, if possibly stale, view of the directory, and
    /// serving it beats taking the whole server down, so poisoning is ignored
    /// on both paths.
    fn read_index(&self) -> std::sync::RwLockReadGuard<'_, Index> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> std::sync::RwLockWriteGuard<'_, Index> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Keep `store`'s index current after the synchronous initial build in
/// [`HistoryStore::new`].
///
/// Two loops, each driven by one of the store's triggers:
///
/// * the watch loop folds in a single log the moment `EventLogTrigger`'s
///   `next_event` reports it appeared, and drops a job the moment its log is
///   reported removed — both without waiting for the next full pass.
/// * the scan loop does a full `EventLogSource::scan_jobs` pass every time
///   `ScanTrigger::scan_tick` fires, folding every readable log it finds
///   into the index.
///
/// The scan loop is upsert-only: a log that is gone or was rewritten under a
/// new job id is not reconciled there. The watch loop does react to a removal,
/// but only one it actually observes. With the triggers wired today
/// (`NotifyTrigger` / `OnceTrigger`) that is one catch-up pass at startup plus
/// a running index that follows every `*.eventlog` renamed in or deleted
/// afterwards.
///
/// The returned [`ServiceTasks`] owns both loops and aborts them when it is
/// dropped, so the caller must keep it alive for as long as the store is
/// served.
pub fn spawn_service_tasks(store: Arc<HistoryStore>) -> ServiceTasks {
    let watch_store = Arc::clone(&store);
    let watch_loop = tokio::spawn(async move {
        loop {
            match watch_store.trigger.next_event().await {
                EventLogEvent::Created(path) => index_one(&watch_store, path).await,
                EventLogEvent::Removed(path) => deindex_one(&watch_store, path),
            }
        }
    });
    let scan_store = Arc::clone(&store);
    let scan_loop = tokio::spawn(async move {
        loop {
            scan_store.scan_trigger.scan_tick().await;
            scan_store.load_index().await;
        }
    });

    ServiceTasks {
        watch_loop,
        scan_loop,
    }
}

/// Owns the background loops started by [`spawn_service_tasks`] and aborts
/// both when dropped, so letting it go out of scope does not leak them.
pub struct ServiceTasks {
    watch_loop: tokio::task::JoinHandle<()>,
    scan_loop: tokio::task::JoinHandle<()>,
}

impl Drop for ServiceTasks {
    fn drop(&mut self) {
        self.watch_loop.abort();
        self.scan_loop.abort();
    }
}

/// Read one log's summary and fold it into the index. A log with no terminal
/// record yet is skipped silently; an unreadable one is logged and skipped, so
/// one bad file never stalls the loop.
async fn index_one(store: &HistoryStore, path: PathBuf) {
    match store.source.read_job_index(&path).await {
        Ok(Some(index)) => {
            store
                .write_index()
                .jobs
                .insert(index.job_id.clone(), JobEntry { index, path });
        }
        Ok(None) => {}
        Err(err) => tracing::warn!(
            "history server: skipping unreadable event log {}: {err}",
            path.display()
        ),
    }
}

/// Drop the job whose event log was `path` from the index.
///
/// A completed log is named `<job_id>.eventlog` (see [`JobEntry`]), so the job
/// id is the file stem — the trigger only ever hands this a `*.eventlog` path,
/// so the stem is the bare id, never `j1.eventlog` from a `j1.eventlog.running`.
/// Keying off the file name also means it does not matter whether `path` is
/// absolute or how the matching entry's path was spelled. A path with no stem,
/// or a job id not in the index (a still-running log, one already reconciled,
/// one that was unreadable when it appeared), is a silent no-op.
fn deindex_one(store: &HistoryStore, path: PathBuf) {
    let Some(job_id) = path.file_stem().and_then(|s| s.to_str()) else {
        return;
    };
    if store.write_index().jobs.remove(job_id).is_some() {
        tracing::debug!("history server: de-indexed job {job_id}");
    }
}

impl From<JobReadError> for SchedulerErrorResponse {
    fn from(error: JobReadError) -> Self {
        match error {
            JobReadError::NotFound => SchedulerErrorResponse::new(StatusCode::NOT_FOUND),
            JobReadError::Vanished => SchedulerErrorResponse::with_error(
                StatusCode::NOT_FOUND,
                error.to_string(),
            ),
            JobReadError::Unreadable(_) => SchedulerErrorResponse::with_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
            ),
        }
    }
}

/// Build the axum router serving `/api/*` from a [`HistoryStore`].
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
    let mut jobs: Vec<JobResponse> = store.summaries().iter().map(list_entry).collect();
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
    let job = store.read_job(&job_id).await?;
    Ok(raw_json(&job.job))
}

async fn get_stages(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<axum::response::Response, SchedulerErrorResponse> {
    let job = store.read_job(&job_id).await?;
    Ok(raw_json(&job.stages))
}

async fn get_config(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobConfig>, SchedulerErrorResponse> {
    let job = store.read_job(&job_id).await?;
    Ok(Json(job.config))
}

async fn get_dot(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<String, SchedulerErrorResponse> {
    let job = store.read_job(&job_id).await?;
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
    use std::time::Duration;
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
    async fn store_with_one_job(dir: &tempfile::TempDir) -> Arc<HistoryStore> {
        write_job_end_log(&dir.path().join("job-1.eventlog"), "job-1");
        Arc::new(HistoryStore::new_static(dir.path()).await.unwrap())
    }

    #[tokio::test]
    async fn jobs_endpoint_nulls_plan_fields() {
        let dir = tempdir().unwrap();
        let app = history_router(store_with_one_job(&dir).await);
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
        let app = history_router(store_with_one_job(&dir).await);
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
        let app = history_router(store_with_one_job(&dir).await);

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
        let app = history_router(store_with_one_job(&dir).await);
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
        let app = history_router(Arc::new(
            HistoryStore::new_static(dir.path()).await.unwrap(),
        ));

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
        let app = history_router(Arc::new(
            HistoryStore::new_static(dir.path()).await.unwrap(),
        ));

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
        let app = history_router(store_with_one_job(&dir).await);

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
        let app = history_router(store_with_one_job(&dir).await);

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
        let app = history_router(store_with_one_job(&dir).await);

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

    #[tokio::test]
    async fn new_skips_corrupt_eventlog_and_keeps_good_one() {
        let dir = tempdir().unwrap();

        // A good, readable event log.
        write_job_end_log(&dir.path().join("job-good.eventlog"), "job-good");

        // A corrupt file: invalid UTF-8, as if a crash truncated a write
        // mid-multibyte-character.
        let mut corrupt =
            std::fs::File::create(dir.path().join("job-bad.eventlog")).unwrap();
        corrupt.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
        drop(corrupt);

        let store = HistoryStore::new_static(dir.path()).await.unwrap();
        assert_eq!(store.len(), 1);
        assert!(store.read_job("job-good").await.is_ok());
        assert!(matches!(
            store.read_job("job-bad").await,
            Err(JobReadError::NotFound)
        ));
    }

    /// Poll `cond` until it holds or the deadline passes; filesystem events are
    /// delivered asynchronously, so the watch loop reacts a beat after the write.
    async fn eventually(mut cond: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(cond(), "condition still false after 5s");
    }

    /// The watch loop folds a log in when it appears and drops it again when the
    /// file is deleted, without any full rescan in between.
    #[tokio::test]
    async fn watch_loop_deindexes_a_removed_log() {
        let dir = tempdir().unwrap();
        // `new` wires a real `NotifyTrigger`; `OnceTrigger` does its single scan
        // pass over the (empty) directory and then parks, so the removal below
        // can only be picked up by the watch loop.
        let store = Arc::new(HistoryStore::new(dir.path()).unwrap());
        let _tasks = spawn_service_tasks(Arc::clone(&store));

        let path = dir.path().join("job-1.eventlog");
        write_job_end_log(&path, "job-1");
        eventually(|| store.len() == 1).await;

        std::fs::remove_file(&path).unwrap();
        eventually(|| store.is_empty()).await;
    }
}
