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
use ballista_core::serde::AsExecutionPlan;
use ballista_core::BALLISTA_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use warp::Rejection;

#[derive(Debug, serde::Serialize)]
struct StateResponse {
    executors: Vec<ExecutorMetaResponse>,
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

#[derive(Debug, serde::Serialize)]
pub struct JobResponse {
    pub job_id: String,
    pub job_status: String,
    pub num_stages: usize,
    pub completed_stages: usize,
    pub percent_complete: u8,
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
        started: data_server.start_time,
        version: BALLISTA_VERSION,
    };
    Ok(warp::reply::json(&response))
}

/// Return list of jobs
pub(crate) async fn get_jobs<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
) -> Result<impl warp::Reply, Rejection> {
    // TODO: Display last seen information in UI
    let state = data_server.state;

    let jobs = state
        .task_manager
        .get_jobs()
        .await
        .map_err(|_| warp::reject())?;

    let jobs: Vec<JobResponse> = jobs
        .iter()
        .map(|job| JobResponse {
            job_id: job.job_id.to_string(),
            job_status: format!("{:?}", job.status),
            num_stages: job.num_stages,
            completed_stages: job.completed_stages,
            percent_complete: ((job.completed_stages as f32 / job.num_stages as f32)
                * 100_f32) as u8,
        })
        .collect();

    Ok(warp::reply::json(&jobs))
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
