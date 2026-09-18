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

//! Scheduler REST-API polling shared by both cluster backends.
//!
//! [`crate::cluster::TestCluster`] (local processes) and [`crate::k8s`]
//! (`kind` pods) both drive scenarios by polling the scheduler's REST API —
//! the same endpoints (`/api/executors`, `/api/jobs`, `/api/job/{id}/stages`)
//! and the same JSON shape, differing only in the base URL (loopback vs. the
//! port-forward). These free functions take that base URL so the polling logic
//! lives in one place; each backend wraps them in thin methods.

use serde_json::Value;
use std::time::{Duration, Instant};

/// Per-request timeout. Short so a stalled connection (e.g. a k8s port-forward
/// that dropped) surfaces as a retryable error inside a polling loop rather than
/// hanging the whole wait; harmless for the loopback (process) backend.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|e| e.to_string())
}

async fn get_json(url: String) -> Result<Value, String> {
    client()?
        .get(url)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())
}

/// How many executors the scheduler currently considers registered.
pub(crate) async fn registered_executors(rest_url: &str) -> Result<usize, String> {
    let body = get_json(format!("{rest_url}/api/executors")).await?;
    Ok(body.as_array().map(|a| a.len()).unwrap_or(0))
}

/// The ids of every executor the scheduler currently lists. Lets a scenario
/// prove a *new* executor (a fresh id) replaced a killed one after rescheduling.
/// Only the k8s backend reschedules automatically, so this is k8s-only.
#[cfg(feature = "k8s")]
pub(crate) async fn executor_ids(rest_url: &str) -> Result<Vec<String>, String> {
    let body = get_json(format!("{rest_url}/api/executors")).await?;
    Ok(body
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| e.get("id").and_then(|v| v.as_str()).map(String::from))
        .collect())
}

/// The id of the single job the scheduler currently knows about.
///
/// The harness runs one query at a time, so "the running job" is unambiguous.
pub(crate) async fn running_job_id(rest_url: &str) -> Result<String, String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let body = get_json(format!("{rest_url}/api/jobs")).await?;
        if let Some(job) = body.as_array().and_then(|jobs| jobs.first())
            && let Some(id) = job.get("job_id").and_then(|v| v.as_str())
        {
            return Ok(id.to_string());
        }
        if Instant::now() > deadline {
            return Err("timed out waiting for a job to appear".to_string());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The stage summary for `job_id`.
pub(crate) async fn stages(rest_url: &str, job_id: &str) -> Result<Value, String> {
    get_json(format!("{rest_url}/api/job/{job_id}/stages")).await
}

/// Block until any task in any stage is Running.
///
/// Planner-agnostic sync point: the static and adaptive (AQE) planners number
/// and materialize stages differently, so rather than target a specific stage id
/// we wait until the job is genuinely executing a task somewhere. Used where the
/// scenario only needs a fault to land mid-flight.
pub(crate) async fn await_any_stage_running(
    rest_url: &str,
    job_id: &str,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let stages = stages(rest_url, job_id).await?;
        let running =
            stages
                .get("stages")
                .and_then(|s| s.as_array())
                .is_some_and(|stages| {
                    stages.iter().any(|stage| {
                        stage.get("tasks").and_then(|t| t.as_array()).is_some_and(
                            |tasks| {
                                tasks.iter().any(|t| {
                                    t.get("status").and_then(|s| s.as_str())
                                        == Some("Running")
                                })
                            },
                        )
                    })
                });
        if running {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err("timed out waiting for any stage to start running".to_string());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Block until the scheduler considers exactly `n` executors registered.
///
/// Unlike an at-least-`n` wait, a killed executor is not dropped from
/// `/api/executors` the instant it dies — the scheduler keeps listing it until
/// its heartbeat times out (`executor_timeout_seconds`). A scenario that kills
/// an executor and wants to observe the scheduler actually reaping it (rather
/// than transiently over-counting) must wait for the count to come *down* to
/// `n` exactly, not merely reach it.
pub(crate) async fn await_executor_count(rest_url: &str, n: usize) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(count) = registered_executors(rest_url).await
            && count == n
        {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!(
                "timed out waiting for exactly {n} registered executors"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
