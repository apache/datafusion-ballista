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

//! HTTP endpoints for health probes and metrics.
//!
//! `/healthz` returns 200 while the process runs. `/readyz` returns 200 once
//! the Flight service is listening and 503 after a shutdown signal.
//! `/api/metrics` returns the metrics, or 204 when none are exported.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use log::error;

use crate::metrics::ResultMetricsCollector;

/// Whether the Flight service is accepting fetches. Cheap to clone.
#[derive(Clone, Default)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    pub fn set(&self, ready: bool) {
        self.0.store(ready, Ordering::Release);
    }

    fn is_ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct HttpState {
    readiness: Readiness,
    metrics: Arc<dyn ResultMetricsCollector>,
}

pub fn router(readiness: Readiness, metrics: Arc<dyn ResultMetricsCollector>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/metrics", get(metrics_handler))
        .with_state(HttpState { readiness, metrics })
}

async fn healthz() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}

async fn readyz(State(state): State<HttpState>) -> Response {
    if state.readiness.is_ready() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}

async fn metrics_handler(State(state): State<HttpState>) -> Response {
    match state.metrics.gather_metrics() {
        Ok(Some((data, content_type))) => {
            ([(CONTENT_TYPE, content_type)], Body::from(data)).into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            error!("failed to gather metrics: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::NoopMetricsCollector;

    fn state() -> HttpState {
        HttpState {
            readiness: Readiness::default(),
            metrics: Arc::new(NoopMetricsCollector),
        }
    }

    #[tokio::test]
    async fn healthz_is_always_ok() {
        assert_eq!(healthz().await.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_follows_readiness() {
        let state = state();
        assert_eq!(
            readyz(State(state.clone())).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        state.readiness.set(true);
        assert_eq!(readyz(State(state.clone())).await.status(), StatusCode::OK);

        state.readiness.set(false);
        assert_eq!(
            readyz(State(state)).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn metrics_are_no_content_when_none_are_exported() {
        assert_eq!(
            metrics_handler(State(state())).await.status(),
            StatusCode::NO_CONTENT
        );
    }

    #[cfg(feature = "prometheus-metrics")]
    #[tokio::test]
    async fn metrics_are_served_in_the_prometheus_text_format() {
        let state = HttpState {
            readiness: Readiness::default(),
            metrics: Arc::new(
                crate::metrics::PrometheusMetricsCollector::new(
                    &prometheus::Registry::new(),
                )
                .unwrap(),
            ),
        };
        let response = metrics_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response.headers()[CONTENT_TYPE].to_str().unwrap();
        assert!(content_type.starts_with("text/plain"), "{content_type}");
    }
}
