use crate::api::handlers;
use crate::scheduler_server::SchedulerServer;
use axum::{Router, routing::get};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use std::sync::Arc;

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
        .route("/api/jobs", get(handlers::get_jobs::<T, U>))
        .route(
            "/api/job/{job_id}",
            get(handlers::get_job::<T, U>).patch(handlers::cancel_job::<T, U>),
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

    router.with_state(scheduler_server)
}
