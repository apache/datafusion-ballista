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

//! Kubernetes-style health and readiness probes.
//!
//! `/healthz` always returns 200 while the process is running — it reports
//! liveness only, so a probe failure signals "the process is dead" (which is
//! the sole condition under which k8s should kill and restart the pod).
//!
//! `/readyz` returns 200 only when the scheduler is ready to accept work,
//! defined as having at least `min_ready_executors` live executors registered.
//! A readiness failure removes the pod from Service endpoints but leaves the
//! process alone, which is what we want during rolling upgrades: a fresh
//! scheduler with `BALLISTA_PROTOCOL_VERSION` rejects executors from the
//! previous release, so it will fail /readyz until new executors register.

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use std::sync::Arc;

use crate::scheduler_server::SchedulerServer;

/// Routes for k8s liveness/readiness probes. Always mounted, regardless of
/// the `rest-api` feature flag.
pub(crate) fn health_routes<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    scheduler_server: Arc<SchedulerServer<T, U>>,
) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz::<T, U>))
        .with_state(scheduler_server)
}

async fn healthz() -> Response {
    (StatusCode::OK, "ok").into_response()
}

async fn readyz<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(scheduler): State<Arc<SchedulerServer<T, U>>>,
) -> Response {
    let alive = scheduler.state.executor_manager.get_alive_executors().len();
    let required = scheduler.state.config.min_ready_executors;
    if scheduler.is_ready() {
        (
            StatusCode::OK,
            format!("ready: {alive} executors registered\n"),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {alive}/{required} executors registered\n"),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchedulerConfig;
    use crate::metrics::default_metrics_collector;
    use crate::scheduler_server::SchedulerServer;
    use crate::test_utils::test_cluster_context;
    use ballista_core::serde::BallistaCodec;
    use ballista_core::serde::protobuf::{
        ExecutorHeartbeat, ExecutorStatus, executor_status,
    };
    use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

    async fn scheduler_with_min_ready(
        min_ready: usize,
    ) -> Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
        let config = SchedulerConfig {
            min_ready_executors: min_ready,
            ..Default::default()
        };
        let mut server = SchedulerServer::new(
            "localhost:50050".to_owned(),
            test_cluster_context(),
            BallistaCodec::default(),
            Arc::new(config),
            default_metrics_collector().unwrap(),
        );
        server.init().await.expect("scheduler init");
        Arc::new(server)
    }

    #[tokio::test]
    async fn healthz_is_always_ok() {
        let response = healthz().await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_fails_with_no_executors() {
        let scheduler = scheduler_with_min_ready(1).await;
        let response = readyz(State(scheduler)).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_ok_when_min_ready_is_zero() {
        let scheduler = scheduler_with_min_ready(0).await;
        let response = readyz(State(scheduler)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_ok_when_active_executor_registered() {
        let scheduler = scheduler_with_min_ready(1).await;

        // Publish an active-status heartbeat so the executor counts as alive.
        scheduler
            .state
            .executor_manager
            .save_executor_heartbeat(ExecutorHeartbeat {
                executor_id: "abc".to_owned(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                metrics: vec![],
                status: Some(ExecutorStatus {
                    status: Some(executor_status::Status::Active("".to_string())),
                }),
                peak_proc_physical_memory: 0,
                peak_proc_virtual_memory: 0,
            })
            .await
            .expect("save heartbeat");

        let response = readyz(State(scheduler)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}
