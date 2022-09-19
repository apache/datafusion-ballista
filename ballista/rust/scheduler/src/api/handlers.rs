// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::scheduler_server::SchedulerServer;
use crate::state::execution_graph::ExecutionGraphDot;
use ballista_core::serde::AsExecutionPlan;
use ballista_core::BALLISTA_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use warp::Rejection;

#[derive(Debug, serde::Serialize)]
struct StateResponse {
    executors: Vec<ExecutorMetaResponse>,
    active_jobs: Vec<String>,
    completed_jobs: Vec<String>,
    failed_jobs: Vec<String>,
    started: u128,
    version: &'static str,
}

#[derive(Debug, serde::Serialize)]
pub struct ExecutorMetaResponse {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub last_seen: u128,
}

/// Return current scheduler state, including list of executors and active, completed, and failed
/// job ids.
pub(crate) async fn get_state<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
) -> Result<impl warp::Reply, Rejection> {
    // TODO: Display last seen information in UI
    let state = data_server.state;
    let executors: Vec<ExecutorMetaResponse> = state
        .executor_manager
        .get_executor_state()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(metadata, duration)| ExecutorMetaResponse {
            id: metadata.id,
            host: metadata.host,
            port: metadata.port,
            last_seen: duration.as_millis(),
        })
        .collect();
    let response = StateResponse {
        executors,
        active_jobs: state
            .task_manager
            .get_active_job_ids()
            .await
            .map_err(|_| warp::reject())?,
        completed_jobs: state
            .task_manager
            .get_completed_job_ids()
            .await
            .map_err(|_| warp::reject())?,
        failed_jobs: state
            .task_manager
            .get_failed_job_ids()
            .await
            .map_err(|_| warp::reject())?,
        started: data_server.start_time,
        version: BALLISTA_VERSION,
    };
    Ok(warp::reply::json(&response))
}

#[derive(Debug, serde::Serialize)]
pub struct JobSummaryResponse {
    /// Just show debug output for now but what we really want here is a list of stages with
    /// plans and metrics and the relationship between them
    pub summary: String,
}

/// Get the execution graph for the specified job id
pub(crate) async fn get_job_summary<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
    job_id: String,
) -> Result<impl warp::Reply, Rejection> {
    let graph = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|_| warp::reject())?;

    match graph {
        Some(x) => Ok(warp::reply::json(&JobSummaryResponse {
            summary: format!("{:?}", x),
        })),
        _ => Ok(warp::reply::json(&JobSummaryResponse {
            summary: "Not Found".to_string(),
        })),
    }
}

/// Generate a dot graph for the specified job id and return as plain text
pub(crate) async fn get_job_dot_graph<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
    job_id: String,
) -> Result<String, Rejection> {
    let graph = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|_| warp::reject())?;

    match graph {
        Some(x) => {
            let dot = ExecutionGraphDot::new(x);
            Ok(format!("{}", dot))
        }
        _ => Ok("Not Found".to_string()),
    }
}
