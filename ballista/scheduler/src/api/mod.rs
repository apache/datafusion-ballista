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

#[cfg(feature = "rest-api")]
mod handlers;
#[cfg(feature = "rest-api")]
mod routes;
#[cfg(feature = "rest-api")]
pub use routes::get_routes;

use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use http::StatusCode;

pub(super) fn route_disabled(reason: String) -> Router {
    Router::new().route(
        "/api/{*path}",
        axum::routing::any(|| async {
            SchedulerErrorResponse::with_error(StatusCode::NOT_FOUND, reason)
        }),
    )
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct SchedulerErrorResponse {
    #[serde(skip)]
    status_code: StatusCode,
    http_code: u16,
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl SchedulerErrorResponse {
    pub(crate) fn new(status_code: StatusCode) -> Self {
        Self {
            status_code,
            reason: status_code.canonical_reason(),
            http_code: status_code.as_u16(),
            error: None,
        }
    }
    pub(crate) fn with_error(status_code: StatusCode, error: String) -> Self {
        Self {
            status_code,
            reason: status_code.canonical_reason(),
            http_code: status_code.as_u16(),
            error: Some(error),
        }
    }
}

impl IntoResponse for SchedulerErrorResponse {
    fn into_response(self) -> Response {
        let status = self.status_code;
        (status, Json(self)).into_response()
    }
}
