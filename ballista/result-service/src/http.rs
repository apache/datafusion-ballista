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

//! HTTP endpoints for health probes and metrics, served on the Flight port.
//!
//! `/healthz` and `/readyz` return 200 whenever they're served. After a
//! shutdown signal the service stops accepting connections, so both probes
//! then fail. `/api/metrics` returns the metrics, or 204 when none are
//! exported.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use log::error;

use crate::metrics::ResultMetricsCollector;

pub fn router(metrics: Arc<dyn ResultMetricsCollector>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/metrics", get(metrics_handler))
        .with_state(metrics)
}

async fn healthz() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}

async fn readyz() -> Response {
    (StatusCode::OK, "ready\n").into_response()
}

async fn metrics_handler(
    State(metrics): State<Arc<dyn ResultMetricsCollector>>,
) -> Response {
    match metrics.gather_metrics() {
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
    use axum::http::Request;
    use tower::ServiceExt;

    async fn get(router: Router, path: &str) -> Response {
        let request = Request::get(path).body(Body::empty()).unwrap();
        router.oneshot(request).await.unwrap()
    }

    #[tokio::test]
    async fn probes_are_ok() {
        let router = router(Arc::new(NoopMetricsCollector));
        assert_eq!(
            get(router.clone(), "/healthz").await.status(),
            StatusCode::OK
        );
        assert_eq!(get(router, "/readyz").await.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_are_no_content_when_none_are_exported() {
        let router = router(Arc::new(NoopMetricsCollector));
        assert_eq!(
            get(router, "/api/metrics").await.status(),
            StatusCode::NO_CONTENT
        );
    }

    #[cfg(feature = "prometheus-metrics")]
    #[tokio::test]
    async fn metrics_are_served_in_the_prometheus_text_format() {
        let metrics =
            crate::metrics::PrometheusMetricsCollector::new(&prometheus::Registry::new())
                .unwrap();
        let response = get(router(Arc::new(metrics)), "/api/metrics").await;
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response.headers()[CONTENT_TYPE].to_str().unwrap();
        assert!(content_type.starts_with("text/plain"), "{content_type}");
    }
}
