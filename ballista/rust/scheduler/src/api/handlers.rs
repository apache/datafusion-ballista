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
use crate::state::execution_graph_dot::ExecutionGraphDot;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::AsExecutionPlan;
use ballista_core::BALLISTA_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use graphviz_rust::cmd::{CommandArg, Format};
use graphviz_rust::exec;
use graphviz_rust::printer::PrinterContext;
use warp::Rejection;

#[derive(Debug, serde::Serialize)]
struct SchedulerStateResponse {
    started: u128,
    version: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct ExecutorsResponse {
    executors: Vec<ExecutorMetaResponse>,
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

/// Return current scheduler state
pub(crate) async fn get_scheduler_state<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
) -> Result<impl warp::Reply, Rejection> {
    let response = SchedulerStateResponse {
        started: data_server.start_time,
        version: BALLISTA_VERSION,
    };
    Ok(warp::reply::json(&response))
}

/// Return list of executors
pub(crate) async fn get_executors<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
) -> Result<impl warp::Reply, Rejection> {
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

    Ok(warp::reply::json(&executors))
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
        .map(|job| {
            let status = &job.status;
            let job_status = match &status.status {
                Some(Status::Queued(_)) => "Queued".to_string(),
                Some(Status::Running(_)) => "Running".to_string(),
                Some(Status::Failed(error)) => format!("Failed: {}", error.error),
                Some(Status::Successful(completed)) => {
                    let num_rows = completed
                        .partition_location
                        .iter()
                        .map(|p| {
                            p.partition_stats.as_ref().map(|s| s.num_rows).unwrap_or(0)
                        })
                        .sum::<i64>();
                    let num_rows_term = if num_rows == 1 { "row" } else { "rows" };
                    let num_partitions = completed.partition_location.len();
                    let num_partitions_term = if num_partitions == 1 {
                        "partition"
                    } else {
                        "partitions"
                    };
                    format!(
                        "Completed. Produced {} {} containing {} {}.",
                        num_partitions, num_partitions_term, num_rows, num_rows_term,
                    )
                }
                _ => "Invalid State".to_string(),
            };

            // calculate progress based on completed stages for now, but we could use completed
            // tasks in the future to make this more accurate
            let percent_complete =
                ((job.completed_stages as f32 / job.num_stages as f32) * 100_f32) as u8;
            JobResponse {
                job_id: job.job_id.to_string(),
                job_status,
                num_stages: job.num_stages,
                completed_stages: job.completed_stages,
                percent_complete,
            }
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
        Some(x) => ExecutionGraphDot::generate(x).map_err(|_| warp::reject()),
        _ => Ok("Not Found".to_string()),
    }
}

/// Generate an SVG graph for the specified job id and return it as plain text
pub(crate) async fn get_job_svg_graph<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
    job_id: String,
) -> Result<String, Rejection> {
    let dot = get_job_dot_graph(data_server, job_id).await;
    match dot {
        Ok(dot) => {
            let graph = graphviz_rust::parse(&dot);
            if let Ok(graph) = graph {
                exec(
                    graph,
                    &mut PrinterContext::default(),
                    vec![CommandArg::Format(Format::Svg)],
                )
                .map_err(|_| warp::reject())
            } else {
                Ok("Cannot parse graph".to_string())
            }
        }
        _ => Ok("Not Found".to_string()),
    }
}
