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

use crate::display::format_stage_metrics;
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::execution_graph::ExecutionStage;
use crate::state::execution_graph_dot::ExecutionGraphDot;
use crate::state::execution_stage::TaskInfo;
use crate::{api::SchedulerErrorResponse, scheduler_server::SchedulerServer};
use axum::extract::Query;
use axum::response::Redirect;
use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use ballista_core::serde::protobuf::failed_task::FailedReason::{
    ExecutionError, ExecutorLost, FetchPartitionError, IoError, ResultLost, TaskKilled,
};
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    ExecutorMetric, FailedTask, executor_metric::Metric, task_status,
};
use ballista_core::serde::scheduler::{
    ExecutorOperatingSystemSpecification, ExecutorSpecification,
};
use ballista_core::utils::get_current_time;
use ballista_core::{BALLISTA_VERSION, JobId};
use datafusion::DATAFUSION_VERSION;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::{MetricsSet, Time};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
#[cfg(feature = "graphviz-support")]
use graphviz_rust::{
    cmd::{CommandArg, Format},
    exec,
    printer::PrinterContext,
};
use http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use serde::Serialize;
use std::collections::HashMap;
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
pub struct ExecutorResponse {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub last_seen: Option<u128>,
    pub specification: ExecutorSpecification,
    pub metrics: Vec<ExecutorMetricResponse>,
    pub os_info: ExecutorOperatingSystemSpecification,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum ExecutorMetricResponse {
    AvailableMemory(u64),
    TotalMemory(u64),
    UsedMemory(u64),
    ProcPhysicalMemory(u64),
    ProcVirtualMemory(u64),
    PeakPhysicalMemory(u64),
    PeakVirtualMemory(u64),
}

impl ExecutorMetricResponse {
    pub fn from_proto(proto_spec: ExecutorMetric) -> Option<Self> {
        proto_spec.metric.map(|inner| match inner {
            Metric::AvailableMemory(v) => Self::AvailableMemory(v),
            Metric::TotalMemory(v) => Self::TotalMemory(v),
            Metric::UsedMemory(v) => Self::UsedMemory(v),
            Metric::ProcPhysicalMemory(v) => Self::ProcPhysicalMemory(v),
            Metric::ProcVirtualMemory(v) => Self::ProcVirtualMemory(v),
            Metric::PeakPhysicalMemory(v) => Self::PeakPhysicalMemory(v),
            Metric::PeakVirtualMemory(v) => Self::PeakVirtualMemory(v),
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct JobResponse {
    pub job_id: JobId,
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
    pub id: usize,
    /// Task status
    pub status: TaskStatus,
    /// Global partition ids covered by this task. For a single-partition
    /// task this is a one-element list — JSON-compatible with the old
    /// scalar `partition_id` for callers that read `partition_id[0]`, and
    /// honestly plural for multi-partition tasks.
    pub partition_id: Vec<u32>,
    /// Scheduler schedule time
    pub scheduled_time: u64,
    /// Scheduler launch time (ms since epoch)
    pub launch_time: u64,
    /// The time the Executor start to run the task (ms since epoch)
    pub start_exec_time: u64,
    /// The time the Executor finish the task (ms since epoch)
    pub end_exec_time: u64,
    /// total execution time (ms)
    pub exec_duration: u64,
    /// Scheduler side finish time (ms since epoch)
    pub finish_time: u64,
    /// Number of input rows
    pub input_rows: usize,
    /// Number of output rows
    pub output_rows: usize,
}

#[derive(Debug, Clone, Serialize)]
pub enum TaskStatus {
    Running,
    Successful,
    Failed { reason: String, error: String },
}

impl From<&task_status::Status> for TaskStatus {
    fn from(value: &task_status::Status) -> Self {
        match value {
            task_status::Status::Running(_) => TaskStatus::Running,
            task_status::Status::Failed(failed) => TaskStatus::Failed {
                reason: failed_reason(failed),
                error: failed.error.clone(),
            },
            task_status::Status::Successful(_) => TaskStatus::Successful,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Percentiles {
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
    pub elapsed_compute: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_duration_percentiles: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_input_percentiles: Option<Percentiles>,
    pub tasks: Vec<Option<TaskSummary>>,
}

#[derive(Debug, serde::Deserialize, Default)]
pub struct JobQueryParams {
    /// Controls plan format
    pub plan_format: Option<PlanFormat>,
}

#[derive(Debug, serde::Deserialize, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PlanFormat {
    /// ?plan_format=default => plain indent, no metrics
    #[default]
    Default,
    /// ?plan_format=tree => tree render, no metrics
    Tree,
    /// ?plan_format=metrics => indent with aggregated metrics   
    Metrics,
}

/// A handler for GET requests to the root (`/`).
/// It redirects to `https://nightlies.apache.org/datafusion/ballista/tui/<BALLISTA_VERSION>/`
/// forwarding any query parameters
pub async fn get_webtui<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    header_map: HeaderMap,
    Query(mut params): Query<HashMap<String, String>>,
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> Result<Redirect, (StatusCode, String)> {
    const NIGHTLIES_URL: &str = "https://nightlies.apache.org/datafusion/ballista/tui";
    let external_host = &data_server.state.config.external_host;
    let bind_port = data_server.state.config.bind_port;

    let ballista_scheduler_url =
        params.remove("ballista_scheduler_url").unwrap_or_else(|| {
            let default_scheduler_url = &format!("{external_host}:{bind_port}");
            let proto = header_map
                .get("x-forwarded-proto")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("http");
            let host = header_map
                .get("x-forwarded-host")
                .and_then(|hv| hv.to_str().ok())
                .unwrap_or(external_host);
            let port = header_map
                .get("x-forwarded-port")
                .and_then(|hv| hv.to_str().ok())
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(bind_port);
            format!("{proto}://{host}:{port}")
        });

    let mut query_string = String::new();
    query_string.push_str(&format!(
        "ballista_scheduler_url={}",
        url_escape::encode_query(&ballista_scheduler_url)
    ));

    for (k, v) in params.iter() {
        query_string.push_str(&format!(
            "&{}={}",
            url_escape::encode_query(k),
            url_escape::encode_query(v)
        ));
    }

    let target = format!("{NIGHTLIES_URL}/{BALLISTA_VERSION}/?{query_string}");

    Ok(Redirect::to(&target))
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
    let executors: Vec<ExecutorResponse> = state
        .executor_manager
        .get_executors_state()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(metadata, duration, metrics)| ExecutorResponse {
            id: metadata.id,
            host: metadata.host,
            port: metadata.port,
            last_seen: duration.map(|d| d.as_millis()),
            specification: metadata.specification,
            metrics: metrics
                .into_iter()
                .filter_map(ExecutorMetricResponse::from_proto)
                .collect(),
            os_info: metadata.os_info,
        })
        .collect();

    Json(executors)
}

pub async fn get_executor_info<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(executor_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let state = &data_server.state;
    let executor_info = state
        .executor_manager
        .get_executors_state()
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|(metadata, _, _)| metadata.id == executor_id)
        .map(|(metadata, duration, metrics)| ExecutorResponse {
            id: metadata.id,
            host: metadata.host,
            port: metadata.port,
            last_seen: duration.map(|d| d.as_millis()),
            specification: metadata.specification,
            metrics: metrics
                .into_iter()
                .filter_map(ExecutorMetricResponse::from_proto)
                .collect(),
            os_info: metadata.os_info,
        });

    executor_info
        .map(Json)
        .ok_or(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
}

pub async fn get_jobs<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let state = &data_server.state;

    let jobs = state.task_manager.get_all_jobs().await.map_err(|e| {
        tracing::error!("Error occurred while getting jobs, reason: {e:?}");
        SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
    })?;

    let jobs: Vec<JobResponse> = jobs
        .iter()
        .map(|job| {
            let (plain_status, job_status) = format_job_status(
                &job.status.status,
                job_elapsed_ms(job.start_time, job.end_time),
            );

            // calculate progress based on completed stages for now, but we could use completed
            // tasks in the future to make this more accurate
            let percent_complete = if job.num_stages == 0 {
                0
            } else {
                ((job.completed_stages as f32 / job.num_stages as f32) * 100_f32) as u8
            };
            JobResponse {
                job_id: job.job_id.to_owned(),
                job_name: job.job_name.to_owned(),
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
    query: Query<JobQueryParams>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let graph = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id.clone().into())
        .await
        .map_err(|err| {
            tracing::error!("Error occurred while getting the execution graph for job '{job_id}' reason: {err:?}");
            SchedulerErrorResponse::with_error(StatusCode::INTERNAL_SERVER_ERROR, format!("Error occurred while getting the execution graph for job '{job_id}'"))
        })?
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))?;
    let stage_plan = format!("{:?}", graph);
    let job = graph.as_ref();
    let (plain_status, job_status) = format_job_status(
        &job.status().status,
        job_elapsed_ms(job.start_time(), job.end_time()),
    );

    let num_stages = job.stage_count();
    let completed_stages = job.completed_stages();
    let percent_complete =
        ((completed_stages as f32 / num_stages as f32) * 100_f32) as u8;

    let plan_format = query.plan_format.clone().unwrap_or_default();

    let physical_plan = match plan_format {
        PlanFormat::Default | PlanFormat::Metrics => {
            DisplayableExecutionPlan::new(job.physical_plan().as_ref())
                .indent(false)
                .to_string()
        }
        PlanFormat::Tree => displayable(job.physical_plan().as_ref())
            .tree_render()
            .to_string(),
    };

    Ok(Json(JobResponse {
        job_id: job.job_id().to_owned(),
        job_name: job.job_name().to_owned(),
        job_status,
        status: plain_status,
        start_time: job.start_time(),
        end_time: job.end_time(),
        num_stages,
        completed_stages,
        percent_complete,
        logical_plan: job.logical_plan().map(str::to_owned),
        physical_plan: Some(physical_plan),
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
        .get_job_status(&job_id.clone().into())
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
                .post_event(QueryStageSchedulerEvent::JobCancel(job_id.into()))
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
    query: Query<JobQueryParams>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let plan_format = query.plan_format.clone().unwrap_or_default();

    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id.clone().into())
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
                    elapsed_compute: None,
                    tasks: vec![],
                    task_duration_percentiles: None,
                    task_input_percentiles: None,
                    stage_plan: None,
                };
                match stage {
                    ExecutionStage::Running(running_stage) => {
                        let metrics = running_stage.stage_metrics.as_deref().unwrap_or(&[]);
                        summary.stage_plan = Some(match plan_format {
                            PlanFormat::Default => displayable(running_stage.plan.as_ref()).indent(false).to_string(),
                            PlanFormat::Tree => displayable(running_stage.plan.as_ref()).tree_render().to_string(),
                            PlanFormat::Metrics => format_stage_metrics(running_stage.plan.as_ref(), metrics),
                        });
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
                        summary.elapsed_compute = get_running_stage_time(&running_stage
                            .task_infos, get_current_time());
                        summary.tasks = running_stage
                            .task_infos
                            .iter()
                            .map(|info| {
                                let (input_rows, output_rows) = running_stage
                                    .stage_metrics
                                    .as_deref()
                                    .map(|metrics| {
                                        get_partition_counts(
                                            metrics,
                                            &info.global_input_partition_ids,
                                        )
                                    })
                                    .unwrap_or((0, 0));

                                let start_exec_time = info.start_exec_time as u64;
                                let end_exec_time = info.end_exec_time as u64;

                                let task_status: TaskStatus = (&info.task_status).into();

                                Some(TaskSummary {
                                    id: info.task_id,
                                    partition_id: info
                                        .global_input_partition_ids
                                        .iter()
                                        .map(|&p| p as u32)
                                        .collect(),
                                    scheduled_time: info.scheduled_time as u64,
                                    launch_time: info.launch_time as u64,
                                    start_exec_time,
                                    end_exec_time,
                                    exec_duration: end_exec_time.saturating_sub(start_exec_time),
                                    finish_time: info.finish_time as u64,
                                    input_rows,
                                    output_rows,
                                    status: task_status,
                                })
                            })
                            .collect();
                    }
                    ExecutionStage::Successful(completed_stage) => {
                        summary.stage_plan = Some(match plan_format {
                            PlanFormat::Default => displayable(completed_stage.plan.as_ref()).indent(false).to_string(),
                            PlanFormat::Tree => displayable(completed_stage.plan.as_ref()).tree_render().to_string(),
                            PlanFormat::Metrics => format_stage_metrics(completed_stage.plan.as_ref(), &completed_stage.stage_metrics),
                        });
                        summary.input_rows = get_combined_count(
                            &completed_stage.stage_metrics,
                            "input_rows",
                        );
                        summary.output_rows = get_combined_count(
                            &completed_stage.stage_metrics,
                            "output_rows",
                        );
                        summary.elapsed_compute =
                            get_finished_stage_time(&completed_stage.task_infos);

                        summary.tasks = completed_stage
                            .task_infos
                            .iter()
                            .map(|task_info| {
                                let (input_rows, output_rows) = get_partition_counts(
                                    &completed_stage.stage_metrics,
                                    &task_info.global_input_partition_ids,
                                );

                                let start_exec_time = task_info.start_exec_time as u64;
                                let end_exec_time = task_info.end_exec_time as u64;
                                let task_status = (&task_info.task_status).into();
                                Some(TaskSummary {
                                    id: task_info.task_id,
                                    partition_id: task_info
                                        .global_input_partition_ids
                                        .iter()
                                        .map(|&p| p as u32)
                                        .collect(),
                                    scheduled_time: task_info.scheduled_time as u64,
                                    launch_time: task_info.launch_time as u64,
                                    start_exec_time,
                                    end_exec_time,
                                    exec_duration: end_exec_time.saturating_sub(start_exec_time),
                                    finish_time: task_info.finish_time as u64,
                                    input_rows,
                                    output_rows,
                                    status: task_status,
                                })
                            })
                            .collect();
                    }
                    ExecutionStage::Failed(failed_stage) => {
                        let metrics = failed_stage.stage_metrics.as_deref().unwrap_or(&[]);
                        summary.stage_plan = Some(match plan_format {
                            PlanFormat::Default => displayable(failed_stage.plan.as_ref()).indent(false).to_string(),
                            PlanFormat::Tree => displayable(failed_stage.plan.as_ref()).tree_render().to_string(),
                            PlanFormat::Metrics => format_stage_metrics(failed_stage.plan.as_ref(), metrics),
                        });
                        summary.input_rows = get_combined_count(metrics, "input_rows");
                        summary.output_rows = get_combined_count(metrics, "output_rows");
                        summary.elapsed_compute =
                            get_finished_stage_time(&failed_stage.task_infos);

                        summary.tasks = failed_stage
                            .task_infos
                            .iter()
                            .map(|info| {
                                let (input_rows, output_rows) = get_partition_counts(
                                    metrics,
                                    &info.global_input_partition_ids,
                                );

                                let start_exec_time = info.start_exec_time as u64;
                                let end_exec_time = info.end_exec_time as u64;
                                let task_status: TaskStatus = (&info.task_status).into();

                                Some(TaskSummary {
                                    id: info.task_id,
                                    partition_id: info
                                        .global_input_partition_ids
                                        .iter()
                                        .map(|&p| p as u32)
                                        .collect(),
                                    scheduled_time: info.scheduled_time as u64,
                                    launch_time: info.launch_time as u64,
                                    start_exec_time,
                                    end_exec_time,
                                    exec_duration: end_exec_time.saturating_sub(start_exec_time),
                                    finish_time: info.finish_time as u64,
                                    input_rows,
                                    output_rows,
                                    status: task_status,
                                })
                            })
                            .collect();
                    }
                    _ => {}
                }
                summary.task_duration_percentiles = task_duration_percentiles(&summary.tasks);
                summary.task_input_percentiles = task_input_percentiles(&summary.tasks);
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

fn task_input_percentiles(tasks: &[Option<TaskSummary>]) -> Option<Percentiles> {
    let mut durations: Vec<u64> = tasks
        .iter()
        .flatten()
        .map(|t| t.input_rows as u64)
        .collect();

    if durations.is_empty() {
        return None;
    }

    durations.sort_unstable();

    Some(Percentiles {
        min: durations[0],
        p25: percentile_duration(&durations, 25.0),
        median: percentile_duration(&durations, 50.0),
        p75: percentile_duration(&durations, 75.0),
        max: *durations.last().unwrap(),
    })
}

fn task_duration_percentiles(tasks: &[Option<TaskSummary>]) -> Option<Percentiles> {
    let mut durations: Vec<u64> =
        tasks.iter().flatten().map(|t| t.exec_duration).collect();

    if durations.is_empty() {
        return None;
    }

    durations.sort_unstable();

    Some(Percentiles {
        min: durations[0],
        p25: percentile_duration(&durations, 25.0),
        median: percentile_duration(&durations, 50.0),
        p75: percentile_duration(&durations, 75.0),
        max: *durations.last().unwrap(),
    })
}

/// Returns elapsed wall time in milliseconds for API formatting.
///
/// Uses saturating subtraction so inconsistent timestamps (e.g. failed jobs, or
/// `end_time` still zero while `start_time` is set) do not panic on subtract.
fn job_elapsed_ms(start_time: u64, end_time: u64) -> u64 {
    end_time.saturating_sub(start_time)
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

fn get_running_stage_time(task_infos: &[TaskInfo], current_time: u128) -> Option<String> {
    let min_start = task_infos
        .iter()
        .map(|t| t.start_exec_time)
        .filter(|t| *t > 0)
        .min();

    match (min_start, current_time) {
        (Some(start), end) if end >= start => {
            let time = Time::new();
            time.add_duration(Duration::from_millis((end - start) as u64));
            Some(time.to_string())
        }
        _ => None,
    }
}

fn failed_reason(failed: &FailedTask) -> String {
    match &failed.failed_reason {
        Some(ExecutionError(_)) => "ExecutionError",
        Some(FetchPartitionError(_)) => "FetchPartitionError",
        Some(IoError(_)) => "IoError",
        Some(ExecutorLost(_)) => "ExecutorLost",
        Some(ResultLost(_)) => "ResultLost",
        Some(TaskKilled(_)) => "TaskKilled",
        None => "Failed",
    }
        .to_string()
}

fn get_finished_stage_time(task_infos: &[TaskInfo]) -> Option<String> {
    let min_start = task_infos
        .iter()
        .map(|t| t.start_exec_time)
        .filter(|t| *t > 0)
        .min();

    let max_end = task_infos
        .iter()
        .map(|t| t.end_exec_time)
        .filter(|t| *t > 0)
        .max();

    match (min_start, max_end) {
        (Some(start), Some(end)) if end >= start => {
            let time = Time::new();
            time.add_duration(Duration::from_millis((end - start) as u64));
            Some(time.to_string())
        }
        _ => None,
    }
}

/// Sum a task's `input_rows` / `output_rows` across the global partitions the
/// task owns. Metrics are keyed by global partition id — for single-partition
/// tasks `partitions` is a one-element slice; for multi-partition tasks it is
/// the task's `global_input_partition_ids`.
fn get_partition_counts(metrics: &[MetricsSet], partitions: &[usize]) -> (usize, usize) {
    let input_rows = get_partition_count(metrics, partitions, "input_rows");
    let output_rows = get_partition_count(metrics, partitions, "output_rows");
    (input_rows, output_rows)
}

fn get_partition_count(
    metrics: &[MetricsSet],
    partitions: &[usize],
    name: &str,
) -> usize {
    metrics
        .iter()
        .flat_map(|vec| {
            vec.iter().map(|metric| {
                let metric_value = metric.value();
                let owned_by_task = metric
                    .partition()
                    .map(|p| partitions.contains(&p))
                    .unwrap_or(false);
                if owned_by_task && metric_value.name() == name {
                    metric_value.as_usize()
                } else {
                    0
                }
            })
        })
        .sum()
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
        .get_job_execution_graph(&job_id.clone().into())
        .await
        .map_err(|e| {
            tracing::error!("Error occurred while getting the dot graph for job '{job_id}' reason: {e:?}");
            SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
        })?
    {
        ExecutionGraphDot::generate(graph.as_ref())
            .map_err(|e| {
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
        .get_job_execution_graph(&job_id.clone().into())
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

pub async fn get_job_config<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    data_server
        .state
        .task_manager
        .get_job_config(&job_id.clone().into())
        .await
        .map(|e| Json(e.to_props()))
        .map_err(|_| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::execution_stage::TaskInfo;
    use ballista_core::serde::protobuf::task_status;

    fn make_task_info(start: u128, end: u128) -> TaskInfo {
        TaskInfo {
            task_id: 0,
            scheduled_time: 0,
            launch_time: 0,
            start_exec_time: start,
            end_exec_time: end,
            finish_time: 0,
            task_status: task_status::Status::Running(Default::default()),
            global_input_partition_ids: vec![],
            vcores_consumed: 0,
        }
    }

    #[test]
    fn test_job_elapsed_saturates_when_end_precedes_start() {
        assert_eq!(job_elapsed_ms(900, 100), 0);
    }

    // --- get_finished_stage_time ---

    #[test]
    fn test_finished_empty_slice_returns_none() {
        assert_eq!(get_finished_stage_time(&[]), None);
    }

    #[test]
    fn test_finished_all_zero_timestamps_returns_none() {
        let tasks = vec![make_task_info(0, 0), make_task_info(0, 0)];
        assert_eq!(get_finished_stage_time(&tasks), None);
    }

    #[test]
    fn test_finished_single_task_elapsed() {
        // 600 - 100 = 500 ms → "500.00ms"
        let tasks = vec![make_task_info(100, 600)];
        assert_eq!(
            get_finished_stage_time(&tasks),
            Some("500.00ms".to_string())
        );
    }

    #[test]
    fn test_finished_picks_earliest_start_and_latest_end() {
        // min start = 100, max end = 900 → 800 ms
        let tasks = vec![
            make_task_info(100, 500),
            make_task_info(200, 900),
            make_task_info(300, 700),
        ];
        assert_eq!(
            get_finished_stage_time(&tasks),
            Some("800.00ms".to_string())
        );
    }

    #[test]
    fn test_finished_end_before_start_returns_none() {
        let tasks = vec![make_task_info(900, 100)];
        assert_eq!(get_finished_stage_time(&tasks), None);
    }

    // --- get_running_stage_time ---

    #[test]
    fn test_running_empty_slice_returns_none() {
        assert_eq!(get_running_stage_time(&[], 1000), None);
    }

    #[test]
    fn test_running_all_zero_start_returns_none() {
        let tasks: Vec<TaskInfo> = vec![make_task_info(0, 0), make_task_info(0, 0)];
        assert_eq!(get_running_stage_time(&tasks, 1000), None);
    }

    #[test]
    fn test_running_future_start_returns_none() {
        // start_exec_time beyond current time → elapsed clamped to 0
        let tasks = vec![make_task_info(u128::MAX, 0)];
        assert_eq!(get_running_stage_time(&tasks, 1000), None);
    }

    #[test]
    fn test_running_past_start_returns_some() {
        let now = 4_000;
        let start = 1_000;
        let tasks = vec![make_task_info(start, 0)];
        assert_eq!(
            get_running_stage_time(&tasks, now),
            Some("3.00s".to_string())
        );
    }

    #[test]
    fn test_running_mixed_zero_start_uses_earliest_nonzero() {
        let now = 3_000;
        let earlier = 1_000;
        let later = 2_000;
        let tasks = vec![
            make_task_info(0, 0),
            make_task_info(later, 0),
            make_task_info(earlier, 0),
            make_task_info(0, 0),
        ];
        let result = get_running_stage_time(&tasks, now);
        assert_eq!(result, Some("2.00s".to_string()));
    }

    #[test]
    fn test_job_elapsed_ms_normal() {
        assert_eq!(super::job_elapsed_ms(100, 500), 400);
    }

    #[test]
    fn test_job_elapsed_ms_end_before_start_saturates_to_zero() {
        assert_eq!(super::job_elapsed_ms(500, 100), 0);
    }
}
