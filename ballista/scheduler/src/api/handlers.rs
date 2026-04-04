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

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::execution_graph::ExecutionStage;
use crate::state::execution_graph_dot::ExecutionGraphDot;
use crate::{api::SchedulerErrorResponse, scheduler_server::SchedulerServer};
use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use ballista_core::BALLISTA_VERSION;
use ballista_core::serde::protobuf::job_status::Status;
use datafusion::DATAFUSION_VERSION;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet, Time};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
#[cfg(feature = "graphviz-support")]
use graphviz_rust::{
    cmd::{CommandArg, Format},
    exec,
    printer::PrinterContext,
};
use http::{StatusCode, header::CONTENT_TYPE};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, serde::Serialize)]
struct SchedulerStateResponse {
    started: u128,
    version: &'static str,
    datafusion_version: &'static str,
    substrait_support: bool,
    keda_support: bool,
    prometheus_support: bool,
    graphviz_support: bool,
    spark_support: bool,
    scheduling_policy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    advertise_flight_sql_endpoint: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct SchedulerVersionResponse {
    version: &'static str,
    datafusion_version: &'static str,
}
#[derive(Debug, serde::Serialize)]
pub struct ExecutorMetaResponse {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub last_seen: Option<u128>,
}

#[derive(Debug, serde::Serialize)]
pub struct JobResponse {
    pub job_id: String,
    pub job_name: String,
    pub job_status: String,
    pub status: String,
    pub num_stages: usize,
    pub completed_stages: usize,
    pub percent_complete: u8,
    /// Timestamp when the job started.
    pub start_time: u64,
    /// Timestamp when the job ended (0 if still running).
    pub end_time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_plan: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct CancelJobResponse {
    pub cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct TaskSummary {
    /// task id
    pub task_id: u32,
    /// partition id
    pub partition_id: u32,
    /// Scheduler schedule time
    pub scheduled_time: u64,
    /// Scheduler launch time
    pub launch_time: u64,
    /// The time the Executor start to run the task
    pub start_exec_time: u64,
    /// The time the Executor finish the task
    pub end_exec_time: u64,
    /// total execution time
    pub exec_duration: u64,
    /// Scheduler side finish time
    pub finish_time: u64,
    /// Number of input rows
    pub input_rows: usize,
    /// Number of output rows
    pub output_rows: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct DurationStats {
    pub min: u64,
    pub p25: u64,
    pub median: u64,
    pub p75: u64,
    pub max: u64,
}

#[derive(Debug, serde::Serialize)]
pub struct QueryStageSummary {
    pub stage_id: String,
    pub stage_status: String,
    pub input_rows: usize,
    pub output_rows: usize,
    pub elapsed_compute: String,
    pub stage_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_duration_stats: Option<DurationStats>,
    pub tasks: Vec<Option<TaskSummary>>,
}

pub async fn get_scheduler_state<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> impl IntoResponse {
    let response = SchedulerStateResponse {
        started: data_server.start_time,
        version: BALLISTA_VERSION,
        datafusion_version: DATAFUSION_VERSION,
        substrait_support: cfg!(feature = "substrait"),
        keda_support: cfg!(feature = "keda-scaler"),
        prometheus_support: cfg!(feature = "prometheus-metrics"),
        graphviz_support: cfg!(feature = "graphviz-support"),
        spark_support: cfg!(feature = "spark-compat"),
        scheduling_policy: data_server.state.config.scheduling_policy.to_string(),
        advertise_flight_sql_endpoint: data_server
            .state
            .config
            .advertise_flight_sql_endpoint
            .clone(),
    };
    Json(response)
}

pub async fn get_scheduler_version() -> impl IntoResponse {
    let response = SchedulerVersionResponse {
        version: BALLISTA_VERSION,
        datafusion_version: DATAFUSION_VERSION,
    };
    Json(response)
}

pub async fn get_executors<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> impl IntoResponse {
    let state = &data_server.state;
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
            last_seen: duration.map(|d| d.as_millis()),
        })
        .collect();

    Json(executors)
}

pub async fn get_jobs<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let state = &data_server.state;

    let jobs = state.task_manager.get_jobs().await.map_err(|e| {
        tracing::error!("Error occurred while getting jobs, reason: {e:?}");
        SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
    })?;

    let jobs: Vec<JobResponse> = jobs
        .iter()
        .map(|job| {
            let (plain_status, job_status) =
                format_job_status(&job.status.status, job.end_time - job.start_time);

            // calculate progress based on completed stages for now, but we could use completed
            // tasks in the future to make this more accurate
            let percent_complete =
                ((job.completed_stages as f32 / job.num_stages as f32) * 100_f32) as u8;
            JobResponse {
                job_id: job.job_id.to_string(),
                job_name: job.job_name.to_string(),
                job_status,
                status: plain_status,
                start_time: job.start_time,
                end_time: job.end_time,
                num_stages: job.num_stages,
                completed_stages: job.completed_stages,
                percent_complete,
                logical_plan: None,
                physical_plan: None,
                stage_plan: None,
            }
        })
        .collect();

    Ok(Json(jobs))
}

pub async fn get_job<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let graph = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|err| {
            tracing::error!("Error occurred while getting the execution graph for job '{job_id}' reason: {err:?}");
            SchedulerErrorResponse::with_error(StatusCode::INTERNAL_SERVER_ERROR, format!("Error occurred while getting the execution graph for job '{job_id}'"))
        })?
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))?;
    let stage_plan = format!("{:?}", graph);
    let job = graph.as_ref();
    let (plain_status, job_status) =
        format_job_status(&job.status().status, job.end_time() - job.start_time());

    let num_stages = job.stage_count();
    let completed_stages = job.completed_stages();
    let percent_complete =
        ((completed_stages as f32 / num_stages as f32) * 100_f32) as u8;

    Ok(Json(JobResponse {
        job_id: job.job_id().to_string(),
        job_name: job.job_name().to_string(),
        job_status,
        status: plain_status,
        start_time: job.start_time(),
        end_time: job.end_time(),
        num_stages,
        completed_stages,
        percent_complete,
        logical_plan: job.logical_plan().map(str::to_owned),
        physical_plan: job.physical_plan().map(str::to_owned),
        stage_plan: Some(stage_plan),
    }))
}

pub async fn cancel_job<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    // 404 if the job doesn't exist
    let job_status = data_server
        .state
        .task_manager
        .get_job_status(&job_id)
        .await
        .map_err(|err| {
            tracing::error!("Error getting job status: {err:?}");
            SchedulerErrorResponse::with_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error getting job status: {err}"),
            )
        })?
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))?;

    match &job_status.status {
        None | Some(Status::Queued(_)) | Some(Status::Running(_)) => {
            data_server
                .query_stage_event_loop
                .get_sender()
                .map_err(|err| {
                    tracing::error!(
                        "Error getting query stage event loop sender: {err:?}"
                    );
                    SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
                })?
                .post_event(QueryStageSchedulerEvent::JobCancel(job_id))
                .await
                .map_err(|_| {
                    SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
                })?;

            Ok((
                StatusCode::OK,
                Json(CancelJobResponse {
                    cancelled: true,
                    reason: None,
                }),
            )
                .into_response())
        }
        Some(Status::Failed(_)) => Ok((
            StatusCode::CONFLICT,
            Json(CancelJobResponse {
                cancelled: false,
                reason: Some("The job has failed".into()),
            }),
        )
            .into_response()),
        Some(Status::Successful(_)) => Ok((
            StatusCode::CONFLICT,
            Json(CancelJobResponse {
                cancelled: false,
                reason: Some("The job is already completed".into()),
            }),
        )
            .into_response()),
    }
}

#[derive(Debug, serde::Serialize)]
pub struct QueryStagesResponse {
    pub stages: Vec<QueryStageSummary>,
}

pub async fn get_query_stages<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|e| {
            tracing::error!("Error occurred while getting the query stages for job '{job_id}' reason: {e:?}");
            SchedulerErrorResponse::with_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error occurred while getting the query stages for job '{job_id}'"),
            )
        })?
    {
        let stages = graph
            .as_ref()
            .stages()
            .iter()
            .map(|(id, stage)| {
                let mut summary = QueryStageSummary {
                    stage_id: id.to_string(),
                    stage_status: stage.variant_name().to_string(),
                    input_rows: 0,
                    output_rows: 0,
                    elapsed_compute: "".to_string(),
                    tasks: vec![],
                    task_duration_stats: None,
                    stage_plan: None
                };
                match stage {
                    ExecutionStage::Running(running_stage) => {
                        summary.stage_plan = Some(displayable(running_stage.plan.as_ref()).indent(false).to_string());
                        summary.input_rows = running_stage
                            .stage_metrics
                            .as_ref()
                            .map(|m| get_combined_count(m.as_slice(), "input_rows"))
                            .unwrap_or(0);
                        summary.output_rows = running_stage
                            .stage_metrics
                            .as_ref()
                            .map(|m| get_combined_count(m.as_slice(), "output_rows"))
                            .unwrap_or(0);
                        summary.elapsed_compute = running_stage
                            .stage_metrics
                            .as_ref()
                            .map(|m| get_elapsed_compute_nanos(m.as_slice()))
                            .unwrap_or_default();
                        summary.tasks = running_stage
                            .task_infos
                            .iter()
                            .enumerate()
                            .map(|(partition_id, task_info)| {
                                task_info.as_ref().map(|info| {
                                    let partition_metrics = running_stage
                                        .stage_metrics
                                        .as_deref()
                                        .and_then(|m| m.get(partition_id));
                                    let input_rows = partition_metrics
                                        .map(|m| {
                                            get_combined_count(
                                                std::slice::from_ref(m),
                                                "input_rows",
                                            )
                                        })
                                        .unwrap_or(0);
                                    let output_rows = partition_metrics
                                        .map(|m| {
                                            get_combined_count(
                                                std::slice::from_ref(m),
                                                "output_rows",
                                            )
                                        })
                                        .unwrap_or(0);

                                    let start_exec_time = info.start_exec_time as u64;
                                    let end_exec_time = info.end_exec_time as u64;
                                    TaskSummary {
                                        task_id: info.task_id as u32,
                                        partition_id: partition_id as u32,
                                        scheduled_time: info.scheduled_time as u64,
                                        launch_time: info.launch_time as u64,
                                        start_exec_time,
                                        end_exec_time,
                                        exec_duration: end_exec_time.saturating_sub(start_exec_time),
                                        finish_time: info.finish_time as u64,
                                        input_rows,
                                        output_rows,
                                    }
                                })
                            })
                            .collect();
                    }
                    ExecutionStage::Successful(completed_stage) => {
                        summary.stage_plan = Some(displayable(completed_stage.plan.as_ref()).indent(false).to_string());
                        summary.input_rows = get_combined_count(
                            &completed_stage.stage_metrics,
                            "input_rows",
                        );
                        summary.output_rows = get_combined_count(
                            &completed_stage.stage_metrics,
                            "output_rows",
                        );
                        summary.elapsed_compute =
                            get_elapsed_compute_nanos(&completed_stage.stage_metrics);

                        summary.tasks = completed_stage
                            .task_infos
                            .iter()
                            .enumerate()
                            .map(|(partition_id, task_info)| {
                                let partition_metrics =
                                    completed_stage.stage_metrics.get(partition_id);
                                let input_rows = partition_metrics
                                    .map(|m| {
                                        get_combined_count(
                                            std::slice::from_ref(m),
                                            "input_rows",
                                        )
                                    })
                                    .unwrap_or(0);
                                let output_rows = partition_metrics
                                    .map(|m| {
                                        get_combined_count(
                                            std::slice::from_ref(m),
                                            "output_rows",
                                        )
                                    })
                                    .unwrap_or(0);
                                let start_exec_time = task_info.start_exec_time as u64;
                                let end_exec_time = task_info.end_exec_time as u64;
                                Some(TaskSummary {
                                    task_id: task_info.task_id as u32,
                                    partition_id: partition_id as u32,
                                    scheduled_time: task_info.scheduled_time as u64,
                                    launch_time: task_info.launch_time as u64,
                                    start_exec_time,
                                    end_exec_time,
                                    exec_duration: end_exec_time.saturating_sub(start_exec_time),
                                    finish_time: task_info.finish_time as u64,
                                    input_rows,
                                    output_rows,
                                })
                            })
                            .collect();
                    }
                    _ => {}
                }
                summary.task_duration_stats = task_duration_stats(&summary.tasks);
                summary
            })
            .collect();

        Ok(Json(QueryStagesResponse { stages }))
    } else {
        Err(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
    }
}

fn percentile_duration(sorted: &[u64], pct: f64) -> u64 {
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn task_duration_stats(tasks: &[Option<TaskSummary>]) -> Option<DurationStats> {
    let mut durations: Vec<u64> =
        tasks.iter().flatten().map(|t| t.exec_duration).collect();

    if durations.is_empty() {
        return None;
    }

    durations.sort_unstable();

    Some(DurationStats {
        min: durations[0],
        p25: percentile_duration(&durations, 25.0),
        median: percentile_duration(&durations, 50.0),
        p75: percentile_duration(&durations, 75.0),
        max: *durations.last().unwrap(),
    })
}

fn format_job_status(status: &Option<Status>, elapsed_ms: u64) -> (String, String) {
    match status {
        Some(Status::Queued(_)) => ("Queued".to_string(), "Queued".to_string()),
        Some(Status::Running(_)) => ("Running".to_string(), "Running".to_string()),
        Some(Status::Failed(error)) => {
            ("Failed".to_string(), format!("Failed: {}", error.error))
        }
        Some(Status::Successful(completed)) => {
            let num_rows = completed
                .partition_location
                .iter()
                .map(|p| p.partition_stats.as_ref().map(|s| s.num_rows).unwrap_or(0))
                .sum::<i64>();
            let num_rows_term = if num_rows == 1 { "row" } else { "rows" };
            let num_partitions = completed.partition_location.len();
            let num_partitions_term = if num_partitions == 1 {
                "partition"
            } else {
                "partitions"
            };
            (
                "Completed".to_string(),
                format!(
                    "Completed. Produced {} {} containing {} {}. Elapsed time: {} ms.",
                    num_partitions,
                    num_partitions_term,
                    num_rows,
                    num_rows_term,
                    elapsed_ms
                ),
            )
        }
        _ => ("Invalid".to_string(), "Invalid State".to_string()),
    }
}

fn get_elapsed_compute_nanos(metrics: &[MetricsSet]) -> String {
    let nanos: usize = metrics
        .iter()
        .flat_map(|vec| {
            vec.iter().map(|metric| match metric.as_ref().value() {
                MetricValue::ElapsedCompute(time) => time.value(),
                _ => 0,
            })
        })
        .sum();
    let t = Time::new();
    t.add_duration(Duration::from_nanos(nanos as u64));
    t.to_string()
}

fn get_combined_count(metrics: &[MetricsSet], name: &str) -> usize {
    metrics
        .iter()
        .flat_map(|vec| {
            vec.iter().map(|metric| {
                let metric_value = metric.value();
                if metric_value.name() == name {
                    metric_value.as_usize()
                } else {
                    0
                }
            })
        })
        .sum()
}

pub async fn get_job_dot_graph<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<String, SchedulerErrorResponse> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|e| {
            tracing::error!("Error occurred while getting the dot graph for job '{job_id}' reason: {e:?}");
            SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
        })?
    {
        ExecutionGraphDot::generate(graph.as_ref())
            .map_err(|e|  {
                tracing::error!("Error occurred while getting the dot graph for job '{job_id}' reason: {e:?}");
                SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
            })
    } else {
        Err(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
    }
}

pub async fn get_query_stage_dot_graph<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path((job_id, stage_id)): Path<(String, usize)>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|_| SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR))?
    {
        ExecutionGraphDot::generate_for_query_stage(graph.as_ref(), stage_id)
            .map_err(|_| SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR))
    } else {
        Err(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
    }
}
#[cfg(feature = "graphviz-support")]
pub async fn get_job_svg_graph<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let dot = get_job_dot_graph(State(data_server.clone()), Path(job_id.clone())).await?;
    match graphviz_rust::parse(&dot) {
        Ok(graph) => {
            let result = exec(
                graph,
                &mut PrinterContext::default(),
                vec![CommandArg::Format(Format::Svg)],
            )
            .map_err(|e| {
                tracing::error!("Error occurred while getting job svg graph for job '{job_id}' reason: {e:?}");
                SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
            })?;

            let svg = String::from_utf8_lossy(&result).to_string();
            Ok(Response::builder()
                .header(CONTENT_TYPE, "image/svg+xml")
                .body(svg)
                .unwrap())
        }
        Err(e) => Err(SchedulerErrorResponse::with_error(
            StatusCode::BAD_REQUEST,
            e.to_string(),
        )),
    }
}

pub async fn get_scheduler_metrics<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> impl IntoResponse {
    match data_server.metrics_collector().gather_metrics() {
        Ok(Some((data, content_type))) => Response::builder()
            .header(CONTENT_TYPE, content_type)
            .body(axum::body::Body::from(data))
            .unwrap(),
        Ok(None) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(axum::body::Body::empty())
            .unwrap(),
        Err(_) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::empty())
            .unwrap(),
    }
}
