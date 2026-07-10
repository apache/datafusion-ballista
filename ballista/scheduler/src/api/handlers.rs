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

use crate::api::dto_build;
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::execution_graph_dot::ExecutionGraphDot;
use crate::{api::SchedulerErrorResponse, scheduler_server::SchedulerServer};
use axum::extract::Query;
use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use ballista_core::BALLISTA_VERSION;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{ExecutorMetric, executor_metric::Metric};
use ballista_core::serde::scheduler::{
    ExecutorOperatingSystemSpecification, ExecutorSpecification,
};
use ballista_history::dto::JobResponse;
use datafusion::DATAFUSION_VERSION;
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
struct CancelJobResponse {
    pub cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
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
        .map(dto_build::build_job_response_from_overview)
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

    let plan_format = query.plan_format.clone().unwrap_or_default();

    Ok(Json(dto_build::build_job_response(
        &graph,
        true,
        plan_format,
    )))
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

pub async fn get_query_stages<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
    query: Query<JobQueryParams>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
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
        Ok(Json(dto_build::build_query_stages_response(
            &graph, &query,
        )))
    } else {
        Err(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
    }
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
        dto_build::build_job_dot(&graph).map_err(|e| {
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
