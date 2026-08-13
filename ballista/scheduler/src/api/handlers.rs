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
use axum::response::Redirect;
use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use ballista_api_types::dto::{JobResponse, PlanFormat};
use ballista_core::BALLISTA_VERSION;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{ExecutorMetric, executor_metric::Metric};
use ballista_core::serde::scheduler::{
    ExecutorOperatingSystemSpecification, ExecutorSpecification,
};
use ballista_core::utils::get_current_time;
use datafusion::DATAFUSION_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
#[cfg(feature = "graphviz-support")]
use graphviz_rust::{
    cmd::{CommandArg, Format},
    exec,
    printer::PrinterContext,
};
use http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use std::collections::HashMap;
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
        .map(dto_build::job_overview_to_response)
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

    Ok(Json(dto_build::graph_to_job_response(
        &graph,
        query.plan_format.unwrap_or_default(),
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
        Ok(Json(dto_build::graph_to_query_stages(
            &graph,
            query.plan_format.unwrap_or_default(),
            get_current_time(),
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

    mod get_webtui {
        use super::*;
        use crate::config::SchedulerConfig;
        use crate::metrics::default_metrics_collector;
        use crate::test_utils::test_cluster_context;
        use axum::response::IntoResponse;
        use ballista_core::serde::BallistaCodec;
        use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

        fn test_scheduler(
            config: SchedulerConfig,
        ) -> Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
            let server = SchedulerServer::new(
                "localhost:50050".to_owned(),
                test_cluster_context(),
                BallistaCodec::default(),
                Arc::new(config),
                default_metrics_collector().unwrap(),
            );
            Arc::new(server)
        }

        fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
            let mut map = HeaderMap::new();
            for (k, v) in pairs {
                map.insert(
                    http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    v.parse().unwrap(),
                );
            }
            map
        }

        fn location_of(result: Result<Redirect, (StatusCode, String)>) -> String {
            let response = result.expect("handler should not error").into_response();
            assert_eq!(response.status(), StatusCode::SEE_OTHER);
            response
                .headers()
                .get("location")
                .expect("redirect response must have a location header")
                .to_str()
                .unwrap()
                .to_owned()
        }

        #[tokio::test]
        async fn defaults_to_config_external_host_and_bind_port() {
            let scheduler = test_scheduler(SchedulerConfig::default());

            let result =
                get_webtui(HeaderMap::new(), Query(HashMap::new()), State(scheduler))
                    .await;

            let location = location_of(result);
            assert!(
                location.contains("ballista_scheduler_url=http://localhost:50050"),
                "unexpected location: {location}"
            );
            assert!(
                location
                    .starts_with("https://nightlies.apache.org/datafusion/ballista/tui/")
            );
        }

        #[tokio::test]
        async fn uses_custom_external_host_and_bind_port_from_config() {
            let scheduler = test_scheduler(SchedulerConfig {
                external_host: "scheduler.example.com".into(),
                bind_port: 8080,
                ..Default::default()
            });

            let result =
                get_webtui(HeaderMap::new(), Query(HashMap::new()), State(scheduler))
                    .await;

            let location = location_of(result);
            assert!(
                location
                    .contains("ballista_scheduler_url=http://scheduler.example.com:8080"),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn x_forwarded_headers_take_precedence_over_config() {
            let scheduler = test_scheduler(SchedulerConfig {
                external_host: "internal-host".into(),
                bind_port: 50050,
                ..Default::default()
            });

            let result = get_webtui(
                headers(&[
                    ("x-forwarded-proto", "https"),
                    ("x-forwarded-host", "public-host.example.com"),
                    ("x-forwarded-port", "443"),
                ]),
                Query(HashMap::new()),
                State(scheduler),
            )
            .await;

            let location = location_of(result);
            assert!(
                location.contains(
                    "ballista_scheduler_url=https://public-host.example.com:443"
                ),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn partial_forwarded_headers_fall_back_to_config_for_the_rest() {
            let scheduler = test_scheduler(SchedulerConfig {
                external_host: "internal-host".into(),
                bind_port: 50050,
                ..Default::default()
            });

            let result = get_webtui(
                headers(&[("x-forwarded-proto", "https")]),
                Query(HashMap::new()),
                State(scheduler),
            )
            .await;

            let location = location_of(result);
            assert!(
                location.contains("ballista_scheduler_url=https://internal-host:50050"),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn malformed_forwarded_port_falls_back_to_config_bind_port() {
            let scheduler = test_scheduler(SchedulerConfig {
                external_host: "internal-host".into(),
                bind_port: 50050,
                ..Default::default()
            });

            let result = get_webtui(
                headers(&[
                    ("x-forwarded-proto", "https"),
                    ("x-forwarded-port", "not-a-port"),
                ]),
                Query(HashMap::new()),
                State(scheduler),
            )
            .await;

            let location = location_of(result);
            assert!(
                location.contains("ballista_scheduler_url=https://internal-host:50050"),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn explicit_ballista_scheduler_url_param_takes_precedence() {
            let scheduler = test_scheduler(SchedulerConfig::default());

            let mut params = HashMap::new();
            params.insert(
                "ballista_scheduler_url".to_string(),
                "https://override.example.com:1234".to_string(),
            );

            let result = get_webtui(
                headers(&[("x-forwarded-host", "should-be-ignored.example.com")]),
                Query(params),
                State(scheduler),
            )
            .await;

            let location = location_of(result);
            assert!(
                location
                    .contains("ballista_scheduler_url=https://override.example.com:1234"),
                "unexpected location: {location}"
            );
            assert!(
                !location.contains("should-be-ignored"),
                "explicit ballista_scheduler_url must not be overridden: {location}"
            );
        }

        #[tokio::test]
        async fn additional_query_params_are_forwarded() {
            let scheduler = test_scheduler(SchedulerConfig::default());

            let mut params = HashMap::new();
            params.insert("foo".to_string(), "bar".to_string());

            let result =
                get_webtui(HeaderMap::new(), Query(params), State(scheduler)).await;

            let location = location_of(result);
            assert!(
                location.contains("&foo=bar"),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn query_param_values_are_url_escaped() {
            let scheduler = test_scheduler(SchedulerConfig::default());

            let mut params = HashMap::new();
            params.insert("weird".to_string(), "a#b c".to_string());

            let result =
                get_webtui(HeaderMap::new(), Query(params), State(scheduler)).await;

            let location = location_of(result);
            assert!(
                location.contains("&weird=a%23b%20c"),
                "unexpected location: {location}"
            );
        }
    }
}
