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

use crate::api::handlers;
use crate::config::SchedulerConfig;
use crate::scheduler_server::SchedulerServer;
use axum::{Router, routing::get};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use http::HeaderValue;
use std::sync::Arc;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

/// All routes configured for rest-api.
pub fn get_routes<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    scheduler_server: Arc<SchedulerServer<T, U>>,
) -> Router {
    let router = Router::new()
        .route("/api/state", get(handlers::get_scheduler_state::<T, U>))
        .route("/api/version", get(handlers::get_scheduler_version))
        .route("/api/executors", get(handlers::get_executors::<T, U>))
        .route(
            "/api/executor/{executor_id}",
            get(handlers::get_executor_info::<T, U>),
        )
        .route("/api/jobs", get(handlers::get_jobs::<T, U>))
        .route(
            "/api/job/{job_id}",
            get(handlers::get_job::<T, U>).patch(handlers::cancel_job::<T, U>),
        )
        .route(
            "/api/job/{job_id}/config",
            get(handlers::get_job_config::<T, U>),
        )
        .route(
            "/api/job/{job_id}/stages",
            get(handlers::get_query_stages::<T, U>),
        )
        .route(
            "/api/job/{job_id}/dot",
            get(handlers::get_job_dot_graph::<T, U>),
        )
        .route(
            "/api/job/{job_id}/stage/{stage_id}/dot",
            get(handlers::get_query_stage_dot_graph::<T, U>),
        )
        .route("/api/metrics", get(handlers::get_scheduler_metrics::<T, U>));

    #[cfg(feature = "graphviz-support")]
    let router = router.route(
        "/api/job/{job_id}/dot_svg",
        get(handlers::get_job_svg_graph::<T, U>),
    );

    router
        .layer(cors(scheduler_server.state.config.clone()))
        .with_state(scheduler_server)
}

fn cors(config: Arc<SchedulerConfig>) -> CorsLayer {
    let allowed_origins = allowed_origins(config.clone());
    let allowed_methods = allowed_methods(config.clone());

    tracing::debug!("CORS allowed origins: {allowed_origins:?}");
    tracing::debug!("CORS allowed methods: {allowed_methods:?}");

    CorsLayer::new()
        .allow_origin(allowed_origins)
        .allow_methods(allowed_methods)
        .allow_headers(AllowHeaders::any())
}

fn allowed_origins(config: Arc<SchedulerConfig>) -> AllowOrigin {
    let default_origins = || {
        vec![
            HeaderValue::from_static("http://localhost:8080"),
            HeaderValue::from_static("https://nightlies.apache.org"),
        ]
    };

    match config.cors_allowed_origins.trim() {
        "" => AllowOrigin::list(default_origins()),
        "*" => AllowOrigin::any(),
        origins => {
            let mut header_values = Vec::new();
            for origin in origins.split(',') {
                let origin = origin.trim();
                if !origin.is_empty() {
                    match origin.parse::<HeaderValue>() {
                        Ok(val) => header_values.push(val),
                        Err(e) => {
                            tracing::warn!(
                                "Failed to parse CORS allowed origin '{}': {}",
                                origin,
                                e
                            );
                        }
                    }
                }
            }
            if header_values.is_empty() {
                header_values = default_origins();
            }
            AllowOrigin::list(header_values)
        }
    }
}

fn allowed_methods(config: Arc<SchedulerConfig>) -> AllowMethods {
    let default_methods = || {
        vec![
            http::Method::GET,
            http::Method::OPTIONS,
            http::Method::PATCH,
        ]
    };

    match config.cors_allowed_methods.trim() {
        "" => AllowMethods::list(default_methods()),
        "*" => AllowMethods::any(),
        methods => {
            let mut allowed_methods = Vec::new();
            for method in methods.split(',') {
                let method = method.trim();
                if !method.is_empty() {
                    match method.parse::<http::Method>() {
                        Ok(val) => allowed_methods.push(val),
                        Err(e) => {
                            tracing::warn!(
                                "Failed to parse CORS allowed method '{method}': {e}"
                            );
                        }
                    }
                }
            }
            if allowed_methods.is_empty() {
                allowed_methods = default_methods();
            }
            AllowMethods::list(allowed_methods)
        }
    }
}
