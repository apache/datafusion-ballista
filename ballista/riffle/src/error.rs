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

//! Error types for the Riffle shuffle client.

use std::fmt;

/// Error type for Riffle shuffle operations.
#[derive(Debug)]
pub enum RiffleError {
    /// gRPC transport error.
    Transport(tonic::transport::Error),
    /// gRPC status error from the server.
    Status(tonic::Status),
    /// Server returned a non-success status code.
    ServerError {
        status: i32,
        message: String,
    },
    /// Arrow serialization/deserialization error.
    Arrow(arrow::error::ArrowError),
    /// General error.
    General(String),
}

impl fmt::Display for RiffleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "Riffle transport error: {e}"),
            Self::Status(e) => write!(f, "Riffle gRPC error: {e}"),
            Self::ServerError { status, message } => {
                write!(f, "Riffle server error (status {status}): {message}")
            }
            Self::Arrow(e) => write!(f, "Riffle Arrow error: {e}"),
            Self::General(msg) => write!(f, "Riffle error: {msg}"),
        }
    }
}

impl std::error::Error for RiffleError {}

impl From<tonic::transport::Error> for RiffleError {
    fn from(e: tonic::transport::Error) -> Self {
        Self::Transport(e)
    }
}

impl From<tonic::Status> for RiffleError {
    fn from(e: tonic::Status) -> Self {
        Self::Status(e)
    }
}

impl From<arrow::error::ArrowError> for RiffleError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Arrow(e)
    }
}

/// Result type for Riffle operations.
pub type Result<T> = std::result::Result<T, RiffleError>;
