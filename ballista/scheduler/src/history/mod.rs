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

//! Standalone history server: loads completed event logs and serves the same
//! `/api/*` responses the live scheduler does, from stored DTOs.

use crate::api::SchedulerErrorResponse;
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    routing::get,
};
use ballista_core::BALLISTA_VERSION;
use ballista_history::dto::{JobConfig, JobResponse, QueryStagesResponse};
use ballista_history::reader::{ReplayedJob, read_completed_job};
use datafusion::DATAFUSION_VERSION;
use http::StatusCode;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// In-memory store of completed jobs, loaded once at startup from a directory
/// of `<job_id>.eventlog` files.
#[derive(Default)]
pub struct HistoryStore {
    /// Completed jobs keyed by job id.
    pub jobs: HashMap<String, ReplayedJob>,
}

impl HistoryStore {
    /// Load every completed job found under `dir`. Missing directories yield
    /// an empty store rather than an error.
    ///
    /// A single unreadable/corrupt `.eventlog` file (e.g. truncated by a
    /// crash mid-write) is logged and skipped rather than failing the whole
    /// load — one bad log must not hide every other completed job. Only a
    /// failure to read the directory itself is propagated.
    pub fn load(dir: &Path) -> std::io::Result<HistoryStore> {
        let mut jobs = HashMap::new();
        if dir.exists() {
            for entry in std::fs::read_dir(dir)? {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) != Some("eventlog") {
                    continue;
                }
                match read_completed_job(&path) {
                    Ok(Some(replayed)) => {
                        jobs.insert(replayed.job.job_id.clone(), replayed);
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

async fn get_jobs(State(store): State<Arc<HistoryStore>>) -> Json<Vec<JobResponse>> {
    // The live list endpoint omits plans; null them for byte-identical output.
    let mut jobs: Vec<JobResponse> = store
        .jobs
        .values()
        .map(|j| {
            let mut r = j.job.clone();
            r.logical_plan = None;
            r.physical_plan = None;
            r.stage_plan = None;
            r
        })
        .collect();
    jobs.sort_by(|a, b| a.job_id.cmp(&b.job_id));
    Json(jobs)
}

async fn get_job(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobResponse>, SchedulerErrorResponse> {
    store
        .jobs
        .get(&job_id)
        .map(|j| Json(j.job.clone()))
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
}

async fn get_stages(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<QueryStagesResponse>, SchedulerErrorResponse> {
    store
        .jobs
        .get(&job_id)
        .map(|j| Json(j.stages.clone()))
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
}

async fn get_config(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobConfig>, SchedulerErrorResponse> {
    store
        .jobs
        .get(&job_id)
        .map(|j| Json(j.config.clone()))
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
}

async fn get_dot(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<String, SchedulerErrorResponse> {
    store
        .jobs
        .get(&job_id)
        .map(|j| j.dot.clone())
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
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
    use ballista_history::dto::QueryStageSummary;
    use ballista_history::event::{HistoryEvent, JobEndStatus, SCHEMA_VERSION};
    use std::io::Write;
    use tempfile::tempdir;
    use tower::ServiceExt; // oneshot

    const STAGE_ID_MARKER: &str = "stage-42";

    fn sample_replayed_job(job_id: &str) -> ReplayedJob {
        ReplayedJob {
            job: JobResponse {
                job_id: job_id.into(),
                job_name: "q1".into(),
                job_status: "COMPLETED".into(),
                status: "Successful".into(),
                num_stages: 1,
                completed_stages: 1,
                percent_complete: 100,
                start_time: 2,
                end_time: 3,
                logical_plan: Some("Projection".into()),
                physical_plan: Some("ProjectionExec".into()),
                stage_plan: Some("stage".into()),
            },
            stages: QueryStagesResponse {
                stages: vec![QueryStageSummary {
                    stage_id: STAGE_ID_MARKER.into(),
                    stage_status: "Completed".into(),
                    input_rows: 10,
                    output_rows: 5,
                    elapsed_compute: Some("1ms".into()),
                    stage_plan: None,
                    task_duration_percentiles: None,
                    task_input_percentiles: None,
                    tasks: vec![],
                }],
            },
            config: Default::default(),
            dot: "digraph {}".into(),
        }
    }

    fn store_with_one_job() -> Arc<HistoryStore> {
        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), sample_replayed_job("job-1"));
        Arc::new(HistoryStore { jobs })
    }

    #[tokio::test]
    async fn jobs_endpoint_nulls_plan_fields() {
        let app = history_router(store_with_one_job());
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
        let app = history_router(store_with_one_job());
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
        let app = history_router(store_with_one_job());

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
        let app = history_router(store_with_one_job());
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

    fn write_job_end_log(path: &Path, job_id: &str) {
        let replayed = sample_replayed_job(job_id);
        let event = HistoryEvent::JobEnd {
            version: SCHEMA_VERSION,
            status: JobEndStatus::Succeeded,
            queued_at: 0,
            started_at: 2,
            completed_at: 3,
            job: Box::new(replayed.job),
            stages: Box::new(replayed.stages),
            config: replayed.config,
            dot: replayed.dot,
        };
        let line = serde_json::to_string(&event).unwrap();
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
        assert_eq!(store.jobs.len(), 1);
        assert!(store.jobs.contains_key("job-good"));
        assert!(!store.jobs.contains_key("job-bad"));
    }
}
