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

//! Ballista error types

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, result,
};

use crate::serde::protobuf::failed_task::FailedReason;
use crate::serde::protobuf::{
    ExecutionError, FailedTask, FetchPartitionError, IoError, TaskKilled,
};
use datafusion::error::DataFusionError;
use datafusion::{arrow::error::ArrowError, sql::sqlparser::parser};
use futures::future::Aborted;

/// Result type alias for Ballista operations.
pub type Result<T> = result::Result<T, BallistaError>;

/// Ballista error types for distributed query execution.
#[derive(Debug)]
pub enum BallistaError {
    /// Feature is not yet implemented.
    NotImplemented(String),
    /// General error with a descriptive message.
    General(String),
    /// Internal error indicating a bug or unexpected state.
    Internal(String),
    /// Configuration error with invalid settings.
    Configuration(String),
    /// Error from Arrow operations.
    ArrowError(Box<ArrowError>),
    /// Error from DataFusion operations.
    DataFusionError(Box<DataFusionError>),
    /// SQL parsing error.
    SqlError(parser::ParserError),
    /// I/O operation error.
    IoError(io::Error),
    /// gRPC transport error.
    TonicError(tonic::transport::Error),
    /// gRPC status error.
    GrpcError(Box<tonic::Status>),
    /// gRPC connection failure.
    GrpcConnectionError(String),
    /// Tokio task join error.
    TokioError(tokio::task::JoinError),
    /// gRPC action error.
    GrpcActionError(String),
    /// Shuffle fetch failed: (executor_id, map_stage_id, map_partition_id, message).
    FetchFailed(String, usize, usize, String),
    /// Operation was cancelled.
    Cancelled,
}

#[allow(clippy::from_over_into)]
impl<T> Into<Result<T>> for BallistaError {
    fn into(self) -> Result<T> {
        Err(self)
    }
}

impl BallistaError {
    /// Keeps a Ballista error structural across DataFusion's stream boundary, so
    /// task-failure classification can recover it instead of parsing a string.
    pub fn into_datafusion(self) -> DataFusionError {
        DataFusionError::External(Box::new(self))
    }
}

/// Creates a general Ballista error from a string message.
pub fn ballista_error(message: &str) -> BallistaError {
    BallistaError::General(message.to_owned())
}

impl From<String> for BallistaError {
    fn from(e: String) -> Self {
        BallistaError::General(e)
    }
}

impl From<ArrowError> for BallistaError {
    fn from(e: ArrowError) -> Self {
        match e {
            ArrowError::ExternalError(e)
                if e.downcast_ref::<BallistaError>().is_some() =>
            {
                *e.downcast::<BallistaError>().unwrap()
            }
            ArrowError::ExternalError(e)
                if e.downcast_ref::<DataFusionError>().is_some() =>
            {
                BallistaError::DataFusionError(Box::new(
                    *e.downcast::<DataFusionError>().unwrap(),
                ))
            }
            other => BallistaError::ArrowError(Box::new(other)),
        }
    }
}

impl From<parser::ParserError> for BallistaError {
    fn from(e: parser::ParserError) -> Self {
        BallistaError::SqlError(e)
    }
}

impl From<DataFusionError> for BallistaError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::ArrowError(e, _) => Self::from(*e),
            // A Ballista error carried across DataFusion arrives as
            // `External(Box<BallistaError>)`; recover the original.
            DataFusionError::External(inner) => match inner.downcast::<BallistaError>() {
                Ok(b) => *b,
                Err(other) => BallistaError::DataFusionError(Box::new(
                    DataFusionError::External(other),
                )),
            },
            _ => BallistaError::DataFusionError(Box::new(e)),
        }
    }
}

impl From<io::Error> for BallistaError {
    fn from(e: io::Error) -> Self {
        BallistaError::IoError(e)
    }
}

impl From<tonic::transport::Error> for BallistaError {
    fn from(e: tonic::transport::Error) -> Self {
        BallistaError::TonicError(e)
    }
}

impl From<tonic::Status> for BallistaError {
    fn from(e: tonic::Status) -> Self {
        BallistaError::GrpcError(Box::new(e))
    }
}

impl From<tokio::task::JoinError> for BallistaError {
    fn from(e: tokio::task::JoinError) -> Self {
        BallistaError::TokioError(e)
    }
}

impl From<datafusion_proto_common::from_proto::Error> for BallistaError {
    fn from(e: datafusion_proto_common::from_proto::Error) -> Self {
        BallistaError::General(e.to_string())
    }
}

impl From<datafusion_proto_common::to_proto::Error> for BallistaError {
    fn from(e: datafusion_proto_common::to_proto::Error) -> Self {
        BallistaError::General(e.to_string())
    }
}

impl From<futures::future::Aborted> for BallistaError {
    fn from(_: Aborted) -> Self {
        BallistaError::Cancelled
    }
}

impl Display for BallistaError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            BallistaError::NotImplemented(desc) => {
                write!(f, "Not implemented: {desc}")
            }
            BallistaError::General(desc) => write!(f, "General error: {desc}"),
            BallistaError::ArrowError(desc) => write!(f, "Arrow error: {desc}"),
            BallistaError::DataFusionError(desc) => {
                write!(f, "DataFusion error: {desc}")
            }
            BallistaError::SqlError(desc) => write!(f, "SQL error: {desc}"),
            BallistaError::IoError(desc) => write!(f, "IO error: {desc}"),
            BallistaError::TonicError(desc) => write!(f, "Tonic error: {desc}"),
            BallistaError::GrpcError(desc) => write!(f, "Grpc error: {desc}"),
            BallistaError::GrpcConnectionError(desc) => {
                write!(f, "Grpc connection error: {desc}")
            }
            BallistaError::Internal(desc) => {
                write!(f, "Internal Ballista error: {desc}")
            }
            BallistaError::TokioError(desc) => write!(f, "Tokio join error: {desc}"),
            BallistaError::GrpcActionError(desc) => {
                write!(f, "Grpc Execute Action error: {desc}")
            }
            BallistaError::FetchFailed(executor_id, map_stage, map_partition, desc) => {
                write!(
                    f,
                    "Shuffle fetch partition error from Executor {executor_id}, map_stage {map_stage}, \
                map_partition {map_partition}, error desc: {desc}"
                )
            }
            BallistaError::Cancelled => write!(f, "Task cancelled"),
            BallistaError::Configuration(desc) => {
                write!(f, "Configuration error: {desc}")
            }
        }
    }
}

struct FetchFailedDetails {
    executor_id: String,
    map_stage_id: usize,
    map_partition_id: usize,
    desc: String,
}

/// Recovers a shuffle fetch failure carried across DataFusion, seeing through
/// any wrapper layers.
fn find_fetch_failed(e: &BallistaError) -> Option<FetchFailedDetails> {
    match e {
        BallistaError::FetchFailed(executor_id, map_stage_id, map_partition_id, desc) => {
            Some(FetchFailedDetails {
                executor_id: executor_id.clone(),
                map_stage_id: *map_stage_id,
                map_partition_id: *map_partition_id,
                desc: desc.clone(),
            })
        }
        BallistaError::DataFusionError(e) => match e.find_root() {
            DataFusionError::External(inner) => inner
                .downcast_ref::<BallistaError>()
                .and_then(find_fetch_failed),
            _ => None,
        },
        _ => None,
    }
}

/// Whether the error is a retryable IO failure, native or carried across
/// DataFusion under any wrapper layer.
fn is_retryable_io(e: &BallistaError) -> bool {
    match e {
        BallistaError::IoError(_) => true,
        BallistaError::DataFusionError(e) => match e.find_root() {
            DataFusionError::IoError(_) => true,
            DataFusionError::External(inner) => inner
                .downcast_ref::<BallistaError>()
                .is_some_and(is_retryable_io),
            _ => false,
        },
        _ => false,
    }
}

impl From<BallistaError> for FailedTask {
    fn from(e: BallistaError) -> Self {
        if let Some(fetch_failed) = find_fetch_failed(&e) {
            return FailedTask {
                error: fetch_failed.desc,
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: fetch_failed.executor_id,
                        map_stage_id: fetch_failed.map_stage_id as u32,
                        map_partition_id: fetch_failed.map_partition_id as u32,
                    },
                )),
            };
        }
        match e {
            BallistaError::Cancelled => FailedTask {
                error: "Task cancelled".to_string(),
                retryable: true,
                count_to_failures: false,
                failed_reason: Some(FailedReason::TaskKilled(TaskKilled {})),
            },
            ref e if is_retryable_io(e) => {
                FailedTask {
                    error: format!("Task failed due to IO error: {e:?}"),
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

impl Error for BallistaError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn io_failed_task(e: BallistaError) -> FailedTask {
        FailedTask::from(e)
    }

    fn fetch_failed(
        executor_id: &str,
        map_stage_id: usize,
        map_partition_id: usize,
        desc: &str,
    ) -> BallistaError {
        BallistaError::FetchFailed(
            executor_id.to_owned(),
            map_stage_id,
            map_partition_id,
            desc.to_owned(),
        )
    }

    #[test]
    fn io_error_is_retryable_through_any_wrapper() {
        // Both a native DataFusion IoError and a BallistaError::IoError carried
        // across DataFusion as External stay retryable, including under the
        // Shared/Context layers DataFusion adds (e.g. a join's shared build side).
        let df_io = || {
            DataFusionError::IoError(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection reset",
            ))
        };
        let cases: Vec<(&str, BallistaError)> = vec![
            ("native", BallistaError::DataFusionError(Box::new(df_io()))),
            (
                "shared",
                BallistaError::DataFusionError(Box::new(DataFusionError::Shared(
                    Arc::new(df_io()),
                ))),
            ),
            (
                "context+shared",
                BallistaError::DataFusionError(Box::new(
                    DataFusionError::Shared(Arc::new(df_io()))
                        .context("reading join build side"),
                )),
            ),
            (
                "external",
                wrap_in_external(BallistaError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection reset",
                ))),
            ),
        ];
        for (label, e) in cases {
            let task = io_failed_task(e);
            assert!(task.retryable, "{label} should be retryable");
            assert!(task.count_to_failures, "{label} should count to failures");
            assert!(
                matches!(task.failed_reason, Some(FailedReason::IoError(_))),
                "{label} should classify as IoError",
            );
        }
    }

    #[test]
    fn non_io_datafusion_error_stays_non_retryable() {
        let e = BallistaError::DataFusionError(Box::new(DataFusionError::Plan(
            "bad plan".to_string(),
        )));
        let task = io_failed_task(e);
        assert!(!task.retryable);
        assert!(matches!(
            task.failed_reason,
            Some(FailedReason::ExecutionError(_))
        ));
    }

    #[test]
    fn cancelled_task_is_retryable_without_counting_to_failures() {
        let task = FailedTask::from(BallistaError::Cancelled);
        assert!(task.retryable);
        assert!(!task.count_to_failures);
        assert!(matches!(
            task.failed_reason,
            Some(FailedReason::TaskKilled(_))
        ));
    }

    #[test]
    fn shared_wrapped_non_io_error_stays_non_retryable() {
        let inner = DataFusionError::Plan("bad plan".to_string());
        let shared = DataFusionError::Shared(Arc::new(inner));
        let e = BallistaError::DataFusionError(Box::new(shared));
        let task = io_failed_task(e);
        assert!(!task.retryable);
        assert!(matches!(
            task.failed_reason,
            Some(FailedReason::ExecutionError(_))
        ));
    }

    /// Builds the single-wrap shape that reaches task failure classification.
    fn wrap_in_external(inner: BallistaError) -> BallistaError {
        BallistaError::DataFusionError(Box::new(inner.into_datafusion()))
    }

    fn wrap_in_shared_external(inner: BallistaError) -> BallistaError {
        let df = DataFusionError::Shared(Arc::new(inner.into_datafusion()));
        BallistaError::DataFusionError(Box::new(df))
    }

    fn wrap_in_context_external(inner: BallistaError) -> BallistaError {
        let df = inner.into_datafusion().context("reading shuffle partition");
        BallistaError::DataFusionError(Box::new(df))
    }

    fn assert_fetch_partition_error(
        task: FailedTask,
        executor_id: &str,
        map_stage_id: u32,
        map_partition_id: u32,
        error: &str,
    ) {
        assert!(!task.retryable);
        assert!(!task.count_to_failures);
        assert_eq!(task.error, error);
        match task.failed_reason {
            Some(FailedReason::FetchPartitionError(fp)) => {
                assert_eq!(fp.executor_id, executor_id);
                assert_eq!(fp.map_stage_id, map_stage_id);
                assert_eq!(fp.map_partition_id, map_partition_id);
            }
            other => panic!("expected FetchPartitionError, got {other:?}"),
        }
    }

    #[test]
    fn bare_fetch_failed_maps_to_fetch_partition_error() {
        let task = FailedTask::from(fetch_failed("exec-1", 3, 7, "boom"));
        assert_fetch_partition_error(task, "exec-1", 3, 7, "boom");
    }

    #[test]
    fn datafusion_external_fetch_failed_converts_to_bare_fetch_failed() {
        let e = BallistaError::from(
            fetch_failed("exec-1", 3, 7, "connection reset").into_datafusion(),
        );

        match e {
            BallistaError::FetchFailed(
                executor_id,
                map_stage_id,
                map_partition_id,
                desc,
            ) => {
                assert_eq!(executor_id, "exec-1");
                assert_eq!(map_stage_id, 3);
                assert_eq!(map_partition_id, 7);
                assert_eq!(desc, "connection reset");
            }
            other => panic!("expected bare FetchFailed, got {other:?}"),
        }
    }

    #[test]
    fn wrapped_fetch_failed_is_recovered_as_fetch_partition_error() {
        let e = wrap_in_external(fetch_failed("exec-1", 3, 7, "connection reset"));
        let task = FailedTask::from(e);
        assert_fetch_partition_error(task, "exec-1", 3, 7, "connection reset");
    }

    #[test]
    fn shared_wrapped_fetch_failed_is_recovered() {
        let e = wrap_in_shared_external(fetch_failed("exec-2", 1, 2, "peer closed"));
        let task = FailedTask::from(e);
        assert_fetch_partition_error(task, "exec-2", 1, 2, "peer closed");
    }

    #[test]
    fn context_wrapped_fetch_failed_is_recovered() {
        let e = wrap_in_context_external(fetch_failed("exec-3", 5, 9, "timeout"));
        let task = FailedTask::from(e);
        assert_fetch_partition_error(task, "exec-3", 5, 9, "timeout");
    }

    #[test]
    fn wrapped_non_fetch_error_stays_execution_error() {
        let e = wrap_in_external(BallistaError::General("boom".to_string()));
        let task = FailedTask::from(e);
        assert!(!task.retryable);
        assert!(matches!(
            task.failed_reason,
            Some(FailedReason::ExecutionError(_))
        ));
    }
}
