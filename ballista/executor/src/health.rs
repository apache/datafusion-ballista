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

//! Kubernetes-style health and readiness probes for the executor.
//!
//! `/healthz` always returns 200 while the process is running. Liveness is
//! deliberately independent of scheduler connectivity: if the scheduler is
//! flapping or on the wrong protocol version, restarting this pod will not
//! help — that would just cascade into a fleet-wide crash loop. The one
//! condition under which k8s should kill this pod is process death, and
//! `/healthz` covers that by returning nothing when the process is gone.
//!
//! `/readyz` reflects whether the executor's last heartbeat to the scheduler
//! succeeded. Startup begins not-ready; a successful heartbeat flips it
//! ready; a failed heartbeat flips it back. Failing `/readyz` removes the
//! executor from any `Service` endpoint (relevant if the executor is exposed
//! via a k8s Service) but does not restart the pod.

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use ballista_core::error::BallistaError;
use log::{info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::JoinHandle;

/// Shared executor health state, cheap to clone (internally arc'd).
#[derive(Clone, Default)]
pub struct ExecutorHealth {
    heartbeat_ok: Arc<AtomicBool>,
}

impl ExecutorHealth {
    /// Creates a new health handle. Starts in the not-ready state; the
    /// executor's first successful heartbeat flips it to ready.
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the last heartbeat as successful. Called from the heartbeat
    /// loop after the scheduler acknowledges the heartbeat.
    pub fn mark_heartbeat_ok(&self) {
        self.heartbeat_ok.store(true, Ordering::Release);
    }

    /// Marks the last heartbeat as failed. Called from the heartbeat loop
    /// when the scheduler returns an error, including
    /// `FailedPrecondition` for a protocol-version mismatch.
    pub fn mark_heartbeat_failed(&self) {
        self.heartbeat_ok.store(false, Ordering::Release);
    }

    fn is_ready(&self) -> bool {
        self.heartbeat_ok.load(Ordering::Acquire)
    }
}

/// Spawns an HTTP server serving `/healthz` and `/readyz`. The task shuts
/// down when the receiver at `shutdown` fires.
pub fn spawn_health_server(
    addr: SocketAddr,
    health: ExecutorHealth,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> JoinHandle<Result<(), BallistaError>> {
    tokio::spawn(async move {
        let router = Router::new()
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .with_state(health);

        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            BallistaError::General(format!("failed to bind health server on {addr}: {e}"))
        })?;

        info!("Executor health server listening on {addr}");

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(async move {
                let _ = shutdown.await;
            })
            .await
            .map_err(|e| {
                warn!("Executor health server exited with error: {e}");
                BallistaError::General(format!("health server error: {e}"))
            })
    })
}

async fn healthz() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}

async fn readyz(State(health): State<ExecutorHealth>) -> Response {
    if health.is_ready() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: no successful heartbeat yet\n",
        )
            .into_response()
    }
}
