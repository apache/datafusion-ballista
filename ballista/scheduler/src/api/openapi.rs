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

//! OpenAPI specification for the Ballista scheduler REST API.

use crate::api::SchedulerErrorResponse;
use crate::api::handlers::{
    CancelJobResponse, ExecutorMetricResponse,
    ExecutorOperatingSystemSpecificationSchema, ExecutorResponse,
    ExecutorSpecificationSchema, JobQueryParams, SchedulerStateResponse,
    SchedulerVersionResponse,
};
use axum::Json;
use axum::response::IntoResponse;
use ballista_api_types::dto::{
    JobResponse, Percentiles, PlanFormat, QueryStageSummary, QueryStagesResponse,
    TaskStatus, TaskSummary,
};
use utoipa::OpenApi;

/// OpenAPI documentation structure for the Ballista scheduler REST API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Apache DataFusion Ballista Scheduler REST API",
        version = env!("CARGO_PKG_VERSION"),
        description = "REST API for Apache DataFusion Ballista Scheduler",
        license(
            name = "Apache-2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0"
        )
    ),
    paths(
        crate::api::handlers::get_scheduler_state,
        crate::api::handlers::get_scheduler_version,
        crate::api::handlers::get_executors,
        crate::api::handlers::get_executor_info,
        crate::api::handlers::get_jobs,
        crate::api::handlers::get_job,
        crate::api::handlers::cancel_job,
        crate::api::handlers::get_job_config,
        crate::api::handlers::get_query_stages,
        crate::api::handlers::get_job_dot_graph,
        crate::api::handlers::get_query_stage_dot_graph,
        crate::api::handlers::get_scheduler_metrics,
        crate::api::openapi::get_openapi_spec,
    ),
    components(
        schemas(
            SchedulerStateResponse,
            SchedulerVersionResponse,
            ExecutorResponse,
            ExecutorMetricResponse,
            ExecutorSpecificationSchema,
            ExecutorOperatingSystemSpecificationSchema,
            CancelJobResponse,
            JobQueryParams,
            SchedulerErrorResponse,
            JobResponse,
            TaskStatus,
            TaskSummary,
            Percentiles,
            QueryStageSummary,
            QueryStagesResponse,
            PlanFormat,
        )
    ),
    tags(
        (name = "state", description = "Scheduler state and feature configuration"),
        (name = "version", description = "Version information"),
        (name = "executors", description = "Executor management and metrics"),
        (name = "jobs", description = "Job execution, monitoring, and cancellation"),
        (name = "graphs", description = "Execution graph visualization"),
        (name = "metrics", description = "Prometheus cluster metrics"),
        (name = "openapi", description = "OpenAPI specification"),
    )
)]
pub struct ApiDoc;

#[cfg(feature = "graphviz-support")]
#[derive(OpenApi)]
#[openapi(paths(crate::api::handlers::get_job_svg_graph))]
struct GraphvizApiDoc;

/// Generate the OpenAPI specification for the scheduler REST API.
pub fn openapi_spec() -> utoipa::openapi::OpenApi {
    #[allow(unused_mut)]
    let mut spec = ApiDoc::openapi();
    #[cfg(feature = "graphviz-support")]
    spec.merge(GraphvizApiDoc::openapi());
    spec
}

/// Handler for `GET /api/openapi.json`.
#[utoipa::path(
    get,
    path = "/api/openapi.json",
    tag = "openapi",
    responses(
        (status = 200, description = "OpenAPI specification document in JSON format", body = Object)
    )
)]
pub async fn get_openapi_spec() -> impl IntoResponse {
    Json(openapi_spec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchedulerConfig;
    use crate::metrics::default_metrics_collector;
    use crate::scheduler_server::SchedulerServer;
    use crate::test_utils::test_cluster_context;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use ballista_core::serde::BallistaCodec;
    use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
    use std::sync::Arc;
    use tower::ServiceExt;

    #[test]
    fn test_openapi_spec_serialization() {
        let spec = openapi_spec();
        let json = serde_json::to_string_pretty(&spec)
            .expect("OpenAPI spec must serialize to JSON");
        assert!(!json.is_empty());

        let value: serde_json::Value = serde_json::from_str(&json)
            .expect("Serialized OpenAPI spec must be valid JSON");
        assert_eq!(
            value["info"]["title"],
            "Apache DataFusion Ballista Scheduler REST API"
        );
    }

    #[test]
    fn test_openapi_spec_contains_all_registered_paths() {
        let spec = openapi_spec();
        let paths: Vec<&str> = spec.paths.paths.keys().map(|k| k.as_str()).collect();

        let expected_paths = [
            "/api/openapi.json",
            "/api/state",
            "/api/version",
            "/api/executors",
            "/api/executor/{executor_id}",
            "/api/jobs",
            "/api/job/{job_id}",
            "/api/job/{job_id}/config",
            "/api/job/{job_id}/stages",
            "/api/job/{job_id}/dot",
            "/api/job/{job_id}/stage/{stage_id}/dot",
            "/api/metrics",
        ];

        for path in expected_paths {
            assert!(
                paths.contains(&path),
                "OpenAPI spec is missing registered path: {path}. Found paths: {paths:?}"
            );
        }

        #[cfg(feature = "graphviz-support")]
        assert!(
            paths.contains(&"/api/job/{job_id}/dot_svg"),
            "OpenAPI spec is missing graphviz path: /api/job/{{job_id}}/dot_svg"
        );
    }

    #[test]
    fn test_openapi_spec_contains_all_schemas() {
        let spec = openapi_spec();
        let components = spec.components.expect("OpenAPI components must be present");
        let schemas = components.schemas;

        let expected_schemas = [
            "SchedulerStateResponse",
            "SchedulerVersionResponse",
            "ExecutorResponse",
            "ExecutorMetricResponse",
            "ExecutorSpecification",
            "ExecutorOperatingSystemSpecification",
            "CancelJobResponse",
            "JobQueryParams",
            "SchedulerErrorResponse",
            "JobResponse",
            "TaskStatus",
            "TaskSummary",
            "Percentiles",
            "QueryStageSummary",
            "QueryStagesResponse",
            "PlanFormat",
        ];

        for schema_name in expected_schemas {
            assert!(
                schemas.contains_key(schema_name),
                "OpenAPI spec is missing schema: {schema_name}. Found schemas: {:?}",
                schemas.keys().collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn test_openapi_json_endpoint() {
        let config = SchedulerConfig::default();
        let server = SchedulerServer::new(
            "localhost:50050".to_owned(),
            test_cluster_context(),
            BallistaCodec::default(),
            Arc::new(config),
            default_metrics_collector().unwrap(),
        );
        let router =
            crate::api::get_routes::<LogicalPlanNode, PhysicalPlanNode>(Arc::new(server));

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("router oneshot");

        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let spec_value: serde_json::Value = serde_json::from_slice(&body_bytes)
            .expect("Response body must be valid JSON");

        assert_eq!(
            spec_value["info"]["title"],
            "Apache DataFusion Ballista Scheduler REST API"
        );
        assert!(spec_value["paths"]["/api/state"].is_object());
        assert!(spec_value["paths"]["/api/jobs"].is_object());
    }
}
