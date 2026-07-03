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

use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    routing::get,
};
use ballista_history::dto::{JobConfig, JobResponse, QueryStagesResponse};
use ballista_history::reader::{ReplayedJob, read_completed_job};
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
    pub fn load(dir: &Path) -> std::io::Result<HistoryStore> {
        let mut jobs = HashMap::new();
        if dir.exists() {
            for entry in std::fs::read_dir(dir)? {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) == Some("eventlog")
                    && let Some(replayed) = read_completed_job(&path)?
                {
                    jobs.insert(replayed.job.job_id.clone(), replayed);
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
) -> Json<Option<JobResponse>> {
    Json(store.jobs.get(&job_id).map(|j| j.job.clone()))
}

async fn get_stages(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Json<Option<QueryStagesResponse>> {
    Json(store.jobs.get(&job_id).map(|j| j.stages.clone()))
}

async fn get_config(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> Json<Option<JobConfig>> {
    Json(store.jobs.get(&job_id).map(|j| j.config.clone()))
}

async fn get_dot(
    State(store): State<Arc<HistoryStore>>,
    AxumPath(job_id): AxumPath<String>,
) -> String {
    store
        .jobs
        .get(&job_id)
        .map(|j| j.dot.clone())
        .unwrap_or_default()
}

async fn get_executors_empty() -> Json<Vec<()>> {
    Json(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // oneshot

    fn store_with_one_job() -> Arc<HistoryStore> {
        let mut jobs = HashMap::new();
        jobs.insert(
            "job-1".to_string(),
            ReplayedJob {
                job: JobResponse {
                    job_id: "job-1".into(),
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
                stages: QueryStagesResponse { stages: vec![] },
                config: Default::default(),
                dot: "digraph {}".into(),
            },
        );
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
    }
}
