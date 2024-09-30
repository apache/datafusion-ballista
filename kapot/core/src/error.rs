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

//! kapot error types

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, result,
};

use crate::serde::protobuf::failed_task::FailedReason;
use crate::serde::protobuf::{ExecutionError, FailedTask, FetchPartitionError, IoError};
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use futures::future::Aborted;
use sqlparser::parser;

pub type Result<T> = result::Result<T, KapotError>;

/// kapot error
#[derive(Debug)]
pub enum KapotError {
    NotImplemented(String),
    General(String),
    Internal(String),
    ArrowError(ArrowError),
    DataFusionError(DataFusionError),
    SqlError(parser::ParserError),
    IoError(io::Error),
    // ReqwestError(reqwest::Error),
    // HttpError(http::Error),
    // KubeAPIError(kube::error::Error),
    // KubeAPIRequestError(k8s_openapi::RequestError),
    // KubeAPIResponseError(k8s_openapi::ResponseError),
    TonicError(tonic::transport::Error),
    GrpcError(tonic::Status),
    GrpcConnectionError(String),
    TokioError(tokio::task::JoinError),
    GrpcActionError(String),
    // (executor_id, map_stage_id, map_partition_id, message)
    FetchFailed(String, usize, usize, String),
    Cancelled,
}

#[allow(clippy::from_over_into)]
impl<T> Into<Result<T>> for KapotError {
    fn into(self) -> Result<T> {
        Err(self)
    }
}

pub fn kapot_error(message: &str) -> KapotError {
    KapotError::General(message.to_owned())
}

impl From<String> for KapotError {
    fn from(e: String) -> Self {
        KapotError::General(e)
    }
}

impl From<ArrowError> for KapotError {
    fn from(e: ArrowError) -> Self {
        match e {
            ArrowError::ExternalError(e)
                if e.downcast_ref::<KapotError>().is_some() =>
            {
                *e.downcast::<KapotError>().unwrap()
            }
            ArrowError::ExternalError(e)
                if e.downcast_ref::<DataFusionError>().is_some() =>
            {
                KapotError::DataFusionError(*e.downcast::<DataFusionError>().unwrap())
            }
            other => KapotError::ArrowError(other),
        }
    }
}

impl From<parser::ParserError> for KapotError {
    fn from(e: parser::ParserError) -> Self {
        KapotError::SqlError(e)
    }
}

impl From<DataFusionError> for KapotError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::ArrowError(e, _) => Self::from(e),
            _ => KapotError::DataFusionError(e),
        }
    }
}

impl From<io::Error> for KapotError {
    fn from(e: io::Error) -> Self {
        KapotError::IoError(e)
    }
}

// impl From<reqwest::Error> for kapotError {
//     fn from(e: reqwest::Error) -> Self {
//         kapotError::ReqwestError(e)
//     }
// }
//
// impl From<http::Error> for kapotError {
//     fn from(e: http::Error) -> Self {
//         kapotError::HttpError(e)
//     }
// }

// impl From<kube::error::Error> for kapotError {
//     fn from(e: kube::error::Error) -> Self {
//         kapotError::KubeAPIError(e)
//     }
// }

// impl From<k8s_openapi::RequestError> for kapotError {
//     fn from(e: k8s_openapi::RequestError) -> Self {
//         kapotError::KubeAPIRequestError(e)
//     }
// }

// impl From<k8s_openapi::ResponseError> for kapotError {
//     fn from(e: k8s_openapi::ResponseError) -> Self {
//         kapotError::KubeAPIResponseError(e)
//     }
// }

impl From<tonic::transport::Error> for KapotError {
    fn from(e: tonic::transport::Error) -> Self {
        KapotError::TonicError(e)
    }
}

impl From<tonic::Status> for KapotError {
    fn from(e: tonic::Status) -> Self {
        KapotError::GrpcError(e)
    }
}

impl From<tokio::task::JoinError> for KapotError {
    fn from(e: tokio::task::JoinError) -> Self {
        KapotError::TokioError(e)
    }
}

impl From<datafusion_proto_common::from_proto::Error> for KapotError {
    fn from(e: datafusion_proto_common::from_proto::Error) -> Self {
        KapotError::General(e.to_string())
    }
}

impl From<datafusion_proto_common::to_proto::Error> for KapotError {
    fn from(e: datafusion_proto_common::to_proto::Error) -> Self {
        KapotError::General(e.to_string())
    }
}

impl From<futures::future::Aborted> for KapotError {
    fn from(_: Aborted) -> Self {
        KapotError::Cancelled
    }
}

impl Display for KapotError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            KapotError::NotImplemented(ref desc) => {
                write!(f, "Not implemented: {desc}")
            }
            KapotError::General(ref desc) => write!(f, "General error: {desc}"),
            KapotError::ArrowError(ref desc) => write!(f, "Arrow error: {desc}"),
            KapotError::DataFusionError(ref desc) => {
                write!(f, "DataFusion error: {desc:?}")
            }
            KapotError::SqlError(ref desc) => write!(f, "SQL error: {desc:?}"),
            KapotError::IoError(ref desc) => write!(f, "IO error: {desc}"),
            // kapotError::ReqwestError(ref desc) => write!(f, "Reqwest error: {}", desc),
            // kapotError::HttpError(ref desc) => write!(f, "HTTP error: {}", desc),
            // kapotError::KubeAPIError(ref desc) => write!(f, "Kube API error: {}", desc),
            // kapotError::KubeAPIRequestError(ref desc) => {
            //     write!(f, "KubeAPI request error: {}", desc)
            // }
            // kapotError::KubeAPIResponseError(ref desc) => {
            //     write!(f, "KubeAPI response error: {}", desc)
            // }
            KapotError::TonicError(desc) => write!(f, "Tonic error: {desc}"),
            KapotError::GrpcError(desc) => write!(f, "Grpc error: {desc}"),
            KapotError::GrpcConnectionError(desc) => {
                write!(f, "Grpc connection error: {desc}")
            }
            KapotError::Internal(desc) => {
                write!(f, "Internal kapot error: {desc}")
            }
            KapotError::TokioError(desc) => write!(f, "Tokio join error: {desc}"),
            KapotError::GrpcActionError(desc) => {
                write!(f, "Grpc Execute Action error: {desc}")
            }
            KapotError::FetchFailed(executor_id, map_stage, map_partition, desc) => {
                write!(
                    f,
                    "Shuffle fetch partition error from Executor {executor_id}, map_stage {map_stage}, \
                map_partition {map_partition}, error desc: {desc}"
                )
            }
            KapotError::Cancelled => write!(f, "Task cancelled"),
        }
    }
}

impl From<KapotError> for FailedTask {
    fn from(e: KapotError) -> Self {
        match e {
            KapotError::FetchFailed(
                executor_id,
                map_stage_id,
                map_partition_id,
                desc,
            ) => {
                FailedTask {
                    error: desc,
                    // fetch partition error is considered to be non-retryable
                    retryable: false,
                    count_to_failures: false,
                    failed_reason: Some(FailedReason::FetchPartitionError(
                        FetchPartitionError {
                            executor_id,
                            map_stage_id: map_stage_id as u32,
                            map_partition_id: map_partition_id as u32,
                        },
                    )),
                }
            }
            KapotError::IoError(io) => {
                FailedTask {
                    error: format!("Task failed due to kapot IO error: {io:?}"),
                    // IO error is considered to be temporary and retryable
                    retryable: true,
                    count_to_failures: true,
                    failed_reason: Some(FailedReason::IoError(IoError {})),
                }
            }
            KapotError::DataFusionError(DataFusionError::IoError(io)) => {
                FailedTask {
                    error: format!("Task failed due to DataFusion IO error: {io:?}"),
                    // IO error is considered to be temporary and retryable
                    retryable: true,
                    count_to_failures: true,
                    failed_reason: Some(FailedReason::IoError(IoError {})),
                }
            }
            other => FailedTask {
                error: format!("Task failed due to runtime execution error: {other:?}"),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(FailedReason::ExecutionError(ExecutionError {})),
            },
        }
    }
}

impl Error for KapotError {}
