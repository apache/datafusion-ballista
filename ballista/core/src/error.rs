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
    fmt::{self, Display, Formatter},
    io, result,
};

use crate::serde::protobuf::{
    execution_error::{
        self, arrow_error,
        datafusion_error::{self, parquet_error, schema_error},
        General, Internal, NotImplemented,
    },
    failed_task::FailedReason,
    ExecutionError, FailedTask, FetchPartitionError, IoError,
};
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use itertools::Itertools;
use sqlparser::parser::{self, ParserError};

pub type Result<T, E = BallistaError> = result::Result<T, E>;

/// Ballista error
#[derive(Debug)]
pub enum BallistaError {
    NotImplemented(String),
    General(String),
    Internal(String),
    ArrowError(ArrowError),
    DataFusionError(DataFusionError),
    SqlError(parser::ParserError),
    IoError(io::Error),
    TonicError(tonic::transport::Error),
    GrpcError(tonic::Status),
    GrpcConnectionError(String),
    TokioError(tokio::task::JoinError),
    GrpcActionError(String),
    // (executor_id, map_stage_id, map_partition_id, message)
    FetchFailed(String, usize, Vec<usize>, String),
    Cancelled,
}

#[allow(clippy::from_over_into)]
impl<T> Into<Result<T>> for BallistaError {
    fn into(self) -> Result<T> {
        Err(self)
    }
}

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
                BallistaError::DataFusionError(*e.downcast::<DataFusionError>().unwrap())
            }
            other => BallistaError::ArrowError(other),
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
            DataFusionError::ArrowError(e) => e.into(),
            _ => BallistaError::DataFusionError(e),
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
        BallistaError::GrpcError(e)
    }
}

impl From<tokio::task::JoinError> for BallistaError {
    fn from(e: tokio::task::JoinError) -> Self {
        BallistaError::TokioError(e)
    }
}

impl From<datafusion_proto::logical_plan::from_proto::Error> for BallistaError {
    fn from(e: datafusion_proto::logical_plan::from_proto::Error) -> Self {
        BallistaError::General(e.to_string())
    }
}

impl From<datafusion_proto::logical_plan::to_proto::Error> for BallistaError {
    fn from(e: datafusion_proto::logical_plan::to_proto::Error) -> Self {
        BallistaError::General(e.to_string())
    }
}

impl From<futures::future::Aborted> for BallistaError {
    fn from(_: futures::future::Aborted) -> Self {
        BallistaError::Cancelled
    }
}

impl From<&ParserError> for execution_error::parser_error::Error {
    fn from(value: &ParserError) -> Self {
        match value {
            parser::ParserError::TokenizerError(message) => {
                execution_error::parser_error::Error::TokenizerError(
                    execution_error::parser_error::TokenizerError {
                        message: message.clone(),
                    },
                )
            }
            parser::ParserError::ParserError(message) => {
                execution_error::parser_error::Error::ParserError(
                    execution_error::parser_error::ParserError {
                        message: message.clone(),
                    },
                )
            }
            parser::ParserError::RecursionLimitExceeded => {
                execution_error::parser_error::Error::RecursionLimitExceeded(
                    execution_error::parser_error::RecursionLimitExceeded {},
                )
            }
        }
    }
}

impl From<&DataFusionError> for datafusion_error::Error {
    fn from(value: &DataFusionError) -> Self {
        match value {
            DataFusionError::External(message) => datafusion_error::Error::External(
                datafusion_error::External {
                    message: message.to_string()
                },
            ),
            DataFusionError::ArrowError(error) => {
                    datafusion_error::Error::ArrowError(
                            execution_error::ArrowError {
                                error: Some(error.into()),
                            },
                        )
            },
            DataFusionError::ParquetError(err) => match err {
                datafusion::parquet::errors::ParquetError::General(message) => {
                    datafusion_error::Error::ParquetError(
                            execution_error::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::General(
                                    parquet_error::General { message: message.clone() },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::NYI(message) => {
                    datafusion_error::Error::ParquetError(
                            execution_error::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::NotYetImplemented(
                                    parquet_error::NotYetImplemented { message: message.clone() },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::EOF(message) => {
                    datafusion_error::Error::ParquetError(
                            execution_error::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::Eof(
                                    parquet_error::Eof { message: message.clone() },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::ArrowError(message) => {
                    datafusion_error::Error::ParquetError(
                            execution_error::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::ArrowError(
                                    parquet_error::ArrowError { message: message.clone() },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::IndexOutOfBound(
                    index,
                    bound,
                ) => {
                    datafusion_error::Error::ParquetError(
                            execution_error::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::IndexOutOfBound(
                                    parquet_error::IndexOutOfBound {
                                        index: *index as u32,
                                        bound: *bound as u32,
                                    },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::External(message) => {
                    datafusion_error::Error::ParquetError(
                        execution_error::datafusion_error::ParquetError {
                            error: Some(parquet_error::Error::External(
                                parquet_error::External { message: message.to_string() },
                            )),
                        },
                    )
                }
            },
            DataFusionError::ObjectStore(err) => match err {
                object_store::Error::Generic { store, source } => {
                    datafusion_error::Error::ObjectStore(
                            execution_error::datafusion_error::ObjectStore {
                                error: Some(execution_error::datafusion_error::object_store::Error::Generic(
                                    execution_error::datafusion_error::object_store::Generic {
                                        store: store.to_string(),
                                        source: source.to_string(),
                                    },
                                )),
                            },
                        )
                }
                object_store::Error::NotFound { path, source } => datafusion_error::Error::ObjectStore(
                        execution_error::datafusion_error::ObjectStore {
                            error: Some(execution_error::datafusion_error::object_store::Error::NotFound(
                                execution_error::datafusion_error::object_store::NotFound {
                                    path: path.clone(),
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::InvalidPath { source } => datafusion_error::Error::ObjectStore(
                        execution_error::datafusion_error::ObjectStore {
                            error: Some(execution_error::datafusion_error::object_store::Error::InvalidPath(
                                execution_error::datafusion_error::object_store::InvalidPath {
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::JoinError { source } => datafusion_error::Error::ObjectStore(
                        execution_error::datafusion_error::ObjectStore {
                            error: Some(execution_error::datafusion_error::object_store::Error::JoinError(
                                execution_error::datafusion_error::object_store::JoinError {
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::NotSupported { source } => datafusion_error::Error::ObjectStore(
                        execution_error::datafusion_error::ObjectStore {
                            error: Some(execution_error::datafusion_error::object_store::Error::NotSupported(
                                execution_error::datafusion_error::object_store::NotSupported{
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::AlreadyExists { path, source } => datafusion_error::Error::ObjectStore(
                        execution_error::datafusion_error::ObjectStore {
                            error: Some(execution_error::datafusion_error::object_store::Error::AlreadyExists(
                                execution_error::datafusion_error::object_store::AlreadyExists {
                                    path: path.clone(),
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::NotImplemented => datafusion_error::Error::ObjectStore(
                        execution_error::datafusion_error::ObjectStore {
                            error: Some(execution_error::datafusion_error::object_store::Error::NotImplemented(
                                execution_error::datafusion_error::object_store::NotImplemented {
                                },
                            )),
                        },
                    ),
                object_store::Error::UnknownConfigurationKey { store, key } => datafusion_error::Error::ObjectStore(
                        execution_error::datafusion_error::ObjectStore {
                            error: Some(execution_error::datafusion_error::object_store::Error::UnknownConfigurationKey(
                                execution_error::datafusion_error::object_store::UnknownConfigurationKey {
                                    store: store.to_string(), key: key.clone()
                                },
                            )),
                        },
                    )
            },
            DataFusionError::IoError(err) => datafusion_error::Error::IoError(
                    execution_error::datafusion_error::IoError {
                        message: err.to_string()
                    },
                ),
            DataFusionError::SQL(error) => {
                datafusion_error::Error::ParserError(
                        execution_error::ParserError { error: Some(error.into())},
                    )
            },
            DataFusionError::NotImplemented(message) => datafusion_error::Error::NotImplemented(
                    execution_error::datafusion_error::NotImplemented {
                        message: message.clone()
                    },
                ),
            DataFusionError::Internal(message) => datafusion_error::Error::Internal(
                    execution_error::datafusion_error::Internal {
                        message: message.clone()
                    },
                ),
            DataFusionError::Plan(message) => datafusion_error::Error::Plan(
                    execution_error::datafusion_error::Plan {
                        message: message.clone()
                    },
                ),
            DataFusionError::SchemaError(err) => match err {
                datafusion::common::SchemaError::AmbiguousReference { field } => {
                    datafusion_error::Error::SchemaError(
                            execution_error::datafusion_error::SchemaError {
                                error: Some(schema_error::Error::AmbiguousReference(
                                    schema_error::AmbiguousReference {
                                        qualifier: field.relation.as_ref().map(|r| r.to_string()),
                                        name: field.name.clone()
                                    },
                                )),
                            },
                        )
                },
                datafusion::common::SchemaError::DuplicateQualifiedField { qualifier, name } => {
                    datafusion_error::Error::SchemaError(
                            execution_error::datafusion_error::SchemaError {
                                error: Some(schema_error::Error::DuplicateQualifiedField(
                                    schema_error::DuplicateQualifiedField {
                                        qualifier: qualifier.as_ref().to_string(),
                                        name: name.clone()
                                    },
                                )),
                            },
                        )
                },
                datafusion::common::SchemaError::DuplicateUnqualifiedField { name } => {
                    datafusion_error::Error::SchemaError(
                            execution_error::datafusion_error::SchemaError {
                                error: Some(schema_error::Error::DuplicateUnqualifiedField(
                                    schema_error::DuplicateUnqualifiedField {
                                        name: name.clone()
                                    },
                                )),
                            },
                        )
                },
                datafusion::common::SchemaError::FieldNotFound { field, valid_fields } => {
                    datafusion_error::Error::SchemaError(
                            execution_error::datafusion_error::SchemaError {
                                error: Some(schema_error::Error::FieldNotFound(
                                    schema_error::FieldNotFound {
                                        field: field.flat_name(),
                                        valid_fields: valid_fields.iter().map(|f| f.flat_name()).collect_vec()
                                    },
                                )),
                            },
                        )
                },
            },
            DataFusionError::Execution(message) => datafusion_error::Error::Execution(
                    execution_error::datafusion_error::Execution {
                        message: message.clone()
                    },
                ),
            DataFusionError::ResourcesExhausted(message) => datafusion_error::Error::ResourcesExhausted(
                    execution_error::datafusion_error::ResourcesExhausted {
                        message: message.clone()
                    },
                ),
            DataFusionError::Context(ctx, error) => datafusion_error::Error::Context(
                    Box::new(execution_error::datafusion_error::Context {
                        ctx: ctx.clone(),
                        error: Some(Box::new(
                            execution_error::DatafusionError { error: Some(error.as_ref().into()) }
                        )),
                    })
                ),
            DataFusionError::Substrait(message) => datafusion_error::Error::Substrair(
                execution_error::datafusion_error::Substrait {
                    message: message.clone()
                },
            ),
    }
    }
}

impl From<&ArrowError> for execution_error::arrow_error::Error {
    fn from(value: &ArrowError) -> Self {
        match value {
            ArrowError::NotYetImplemented(message) => {
                arrow_error::Error::NotYetImplemented(arrow_error::NotYetImplemented {
                    message: message.clone(),
                })
            }
            ArrowError::ExternalError(message) => {
                arrow_error::Error::ExteranlError(arrow_error::ExternalError {
                    message: message.to_string(),
                })
            }
            ArrowError::CastError(message) => {
                arrow_error::Error::CastError(arrow_error::CastError {
                    message: message.clone(),
                })
            }
            ArrowError::MemoryError(message) => {
                arrow_error::Error::MemoryError(arrow_error::MemoryError {
                    message: message.clone(),
                })
            }
            ArrowError::ParseError(message) => {
                arrow_error::Error::ParseError(arrow_error::ParseError {
                    message: message.clone(),
                })
            }
            ArrowError::SchemaError(message) => {
                arrow_error::Error::SchemaError(arrow_error::SchemaError {
                    message: message.clone(),
                })
            }
            ArrowError::ComputeError(message) => {
                arrow_error::Error::ComputeError(arrow_error::ComputeError {
                    message: message.clone(),
                })
            }
            ArrowError::DivideByZero => {
                arrow_error::Error::DivideByZero(arrow_error::DivideByZero {})
            }
            ArrowError::CsvError(message) => {
                arrow_error::Error::CsvError(arrow_error::CsvError {
                    message: message.clone(),
                })
            }
            ArrowError::JsonError(message) => {
                arrow_error::Error::JsonError(arrow_error::JsonError {
                    message: message.clone(),
                })
            }
            ArrowError::IoError(message) => {
                arrow_error::Error::IoError(arrow_error::IoError {
                    message: message.clone(),
                })
            }
            ArrowError::InvalidArgumentError(message) => {
                arrow_error::Error::InvalidArgumentError(
                    arrow_error::InvalidArgumentError {
                        message: message.clone(),
                    },
                )
            }
            ArrowError::ParquetError(message) => {
                arrow_error::Error::ParquetError(arrow_error::ParquetError {
                    message: message.clone(),
                })
            }
            ArrowError::CDataInterface(message) => {
                arrow_error::Error::CDataInterface(arrow_error::CDataInterface {
                    message: message.clone(),
                })
            }
            ArrowError::DictionaryKeyOverflowError => {
                arrow_error::Error::DictionaryKeyOverflowError(
                    arrow_error::DictionaryKeyOverflowError {},
                )
            }
            ArrowError::RunEndIndexOverflowError => {
                arrow_error::Error::RunEndIndexOverflowError(
                    arrow_error::RunEndIndexOverflowError {},
                )
            }
        }
    }
}

impl From<&BallistaError> for execution_error::Error {
    fn from(value: &BallistaError) -> Self {
        match value {
            BallistaError::NotImplemented(message) => {
                execution_error::Error::NotImplemented(NotImplemented {
                    message: message.clone(),
                })
            }
            BallistaError::General(message) => execution_error::Error::General(General {
                message: message.clone(),
            }),
            BallistaError::Internal(message) => {
                execution_error::Error::Internal(Internal {
                    message: message.clone(),
                })
            }
            BallistaError::ArrowError(error) => {
                execution_error::Error::ArrowError(execution_error::ArrowError {
                    error: Some(error.into()),
                })
            }
            BallistaError::DataFusionError(error) => {
                execution_error::Error::DatafusionError(
                    execution_error::DatafusionError {
                        error: Some(error.into()),
                    },
                )
            }
            BallistaError::SqlError(error) => {
                execution_error::Error::SqlError(execution_error::SqlError {
                    error: Some(execution_error::ParserError {
                        error: Some(error.into()),
                    }),
                })
            }
            BallistaError::IoError(error) => {
                execution_error::Error::IoError(execution_error::IoError {
                    message: error.to_string(),
                })
            }
            BallistaError::TonicError(error) => {
                execution_error::Error::TonicError(execution_error::TonicError {
                    message: error.to_string(),
                })
            }
            BallistaError::GrpcError(status) => {
                execution_error::Error::GrpcError(execution_error::GrpcError {
                    message: status.message().to_string(),
                    code: status.code() as i32,
                })
            }
            BallistaError::GrpcConnectionError(message) => {
                execution_error::Error::GrpcConnectionError(
                    execution_error::GrpcConnectionError {
                        message: message.clone(),
                    },
                )
            }
            BallistaError::TokioError(error) => {
                execution_error::Error::TokioError(execution_error::TokioError {
                    message: error.to_string(),
                })
            }
            BallistaError::GrpcActionError(message) => {
                execution_error::Error::GrpcActiveError(
                    execution_error::GrpcActionError {
                        message: message.to_string(),
                    },
                )
            }
            BallistaError::FetchFailed(
                executor_id,
                map_stage_id,
                map_partition_id,
                message,
            ) => execution_error::Error::FetchFailed(execution_error::FetchFailed {
                executor_id: executor_id.clone(),
                map_stage_id: *map_stage_id as u32,
                map_partition_id: map_partition_id
                    .iter()
                    .map(|i| *i as u32)
                    .collect_vec(),
                message: message.clone(),
            }),
            BallistaError::Cancelled => {
                execution_error::Error::Cancelled(execution_error::Cancelled {})
            }
        }
    }
}

impl Display for BallistaError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            BallistaError::NotImplemented(ref desc) => {
                write!(f, "Not implemented: {desc}")
            }
            BallistaError::General(ref desc) => write!(f, "General error: {desc}"),
            BallistaError::ArrowError(ref desc) => write!(f, "Arrow error: {desc}"),
            BallistaError::DataFusionError(ref desc) => {
                write!(f, "DataFusion error: {desc:?}")
            }
            BallistaError::SqlError(ref desc) => write!(f, "SQL error: {desc:?}"),
            BallistaError::IoError(ref desc) => write!(f, "IO error: {desc}"),
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
                map_partitions {map_partition:?}, error desc: {desc}"
                )
            }
            BallistaError::Cancelled => write!(f, "Task cancelled"),
        }
    }
}

impl Display for execution_error::datafusion_error::ParquetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.error.as_ref() {
                Some(t) => match t {
                    execution_error::datafusion_error::parquet_error::Error::General(error) => {
                        write!(f, "DatafusionError/ParquetError/General: {}", error.message)
                    }
                    execution_error::datafusion_error::parquet_error::Error::NotYetImplemented(
                        error,
                    ) => {
                        write!(f,
                            "DatafusionError/ParquetError/NotYetImplemented: {}",
                            error.message
                        )
                    }
                    execution_error::datafusion_error::parquet_error::Error::Eof(error) => {
                        write!(f,"DatafusionError/ParquetError/Eof: {}", error.message)
                    }
                    execution_error::datafusion_error::parquet_error::Error::ArrowError(error) => {
                        write!(f,"DatafusionError/ParquetError/ArrowError: {}", error.message)
                    }
                    execution_error::datafusion_error::parquet_error::Error::IndexOutOfBound(
                        error,
                    ) => write!(f,
                        "DatafusionError/ParquetError/IndexOutOfBound [index: {}, bound: {}]",
                        error.index, error.bound
                    ),
                    execution_error::datafusion_error::parquet_error::Error::External(error) => {
                        write!(f,"DatafusionError/ParquetError/External: {}", error.message)
                    }
                },
                None => write!(f, "DatafusionError/ParquetError no detailed error provided"),
            }
    }
}

impl Display for execution_error::datafusion_error::SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.error.as_ref() {
                Some(t) => match t {
                    execution_error::datafusion_error::schema_error::Error::AmbiguousReference(error) => write!(f,"DatafusionError/SchemaError/AmbiguousReference [name: {}, qualifier: {:?}]", error.name, error.qualifier),
                    execution_error::datafusion_error::schema_error::Error::DuplicateQualifiedField(error) => write!(f,"DatafusionError/SchemaError/DuplicateQualifiedField [name: {}, qualifier: {:?}]", error.name, error.qualifier),
                    execution_error::datafusion_error::schema_error::Error::DuplicateUnqualifiedField(error) => write!(f,"DatafusionError/SchemaError/DuplicateUnqualifiedField: {}", error.name),
                    execution_error::datafusion_error::schema_error::Error::FieldNotFound(error) => write!(f,"DatafusionError/SchemaError/FieldNotFound [field: {}, valid_fields: {:?}]", error.field, error.valid_fields),
            },
                None => write!(f, "DatafusionError/SchemaError no detailed error provided"),
            }
    }
}

impl Display for execution_error::datafusion_error::ObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.error.as_ref() {
                Some(t) => match t {
                    execution_error::datafusion_error::object_store::Error::Generic(error) => write!(f,
                        "DatafusionError/ObjectStoreError/Generic [store: {}, source: {}]",
                        error.store, error.source
                    ),
                    execution_error::datafusion_error::object_store::Error::NotFound(error) => write!(f,
                        "DatafusionError/ObjectStoreError/NotFound [path: {}, source: {}]",
                        error.path, error.source
                    ),
                    execution_error::datafusion_error::object_store::Error::InvalidPath(error) => write!(f,
                        "DatafusionError/ObjectStoreError/InvalidPath: {}",
                        error.source
                    ),
                    execution_error::datafusion_error::object_store::Error::JoinError(error) => write!(f,
                        "DatafusionError/ObjectStoreError/JoinError: {}",
                        error.source
                    ),
                    execution_error::datafusion_error::object_store::Error::NotSupported(error) => write!(f,
                        "DatafusionError/ObjectStoreError/NotSupported: {}",
                        error.source
                    ),
                    execution_error::datafusion_error::object_store::Error::AlreadyExists(error) => write!(f,
                        "DatafusionError/ObjectStoreError/AlreadyExists [path: {}, source: {}]",
                        error.path, error.source
                    ),
                    execution_error::datafusion_error::object_store::Error::NotImplemented(_) => {
                        write!(f, "DatafusionError/ObjectStoreError/NotImplemented")
                    }
                    execution_error::datafusion_error::object_store::Error::UnknownConfigurationKey(error) => {
                        write!(f,
                            "DatafusionError/ObjectStoreError/UnknownConfigurationKey [key: {}, store: {}]",
                            error.key, error.store
                        )
                    }
                },
                None => write!(f, "DatafusionError/ObjectStoreError no detailed error provided"),
    }
    }
}

impl Display for execution_error::ArrowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.error.as_ref() {
            Some(e) => match e {
                execution_error::arrow_error::Error::NotYetImplemented(error) => {
                    write!(f, "NotYetImplemented: {}", error.message)
                }
                execution_error::arrow_error::Error::ExteranlError(error) => {
                    write!(f, "ExteranlError: {}", error.message)
                }
                execution_error::arrow_error::Error::CastError(error) => {
                    write!(f, "CastError: {}", error.message)
                }
                execution_error::arrow_error::Error::MemoryError(error) => {
                    write!(f, "MemoryError: {}", error.message)
                }
                execution_error::arrow_error::Error::ParquetError(error) => {
                    write!(f, "ParquetError: {}", error.message)
                }
                execution_error::arrow_error::Error::SchemaError(error) => {
                    write!(f, "SchemaError: {}", error.message)
                }
                execution_error::arrow_error::Error::ComputeError(error) => {
                    write!(f, "ComputeError: {}", error.message)
                }
                execution_error::arrow_error::Error::DivideByZero(_) => {
                    write!(f, "DivideByZero")
                }
                execution_error::arrow_error::Error::CsvError(error) => {
                    write!(f, "CsvError: {}", error.message)
                }
                execution_error::arrow_error::Error::JsonError(error) => {
                    write!(f, "JsonError: {}", error.message)
                }
                execution_error::arrow_error::Error::IoError(error) => {
                    write!(f, "IoError: {}", error.message)
                }
                execution_error::arrow_error::Error::InvalidArgumentError(error) => {
                    write!(f, "InvalidArgumentError: {}", error.message)
                }
                execution_error::arrow_error::Error::CDataInterface(error) => {
                    write!(f, "CDataInterface: {}", error.message)
                }
                execution_error::arrow_error::Error::DictionaryKeyOverflowError(_) => {
                    write!(f, "DictionaryKeyOverflowError")
                }
                execution_error::arrow_error::Error::RunEndIndexOverflowError(_) => {
                    write!(f, "RunEndIndexOverflowError")
                }
                execution_error::arrow_error::Error::ParseError(error) => {
                    write!(f, "ParseError: {}", error.message)
                }
                execution_error::arrow_error::Error::SqlError(error) => {
                    write!(f, "SqlError/{}", error)
                }
            },
            None => write!(f, "Unknown"),
        }
    }
}

impl Display for execution_error::DatafusionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.error.as_ref() {
            Some(e) => match e {
                execution_error::datafusion_error::Error::ArrowError(arrow_error) => {
                    write!(f, "DatafusionError/ArrowError/{}", arrow_error)
                }
                execution_error::datafusion_error::Error::ParquetError(parquet_error) => {
                    write!(f, "{}", parquet_error)
                }
                execution_error::datafusion_error::Error::AvroError(error) => {
                    write!(f, "DatafusionError/AvroError: {}", error.message)
                }
                execution_error::datafusion_error::Error::ObjectStore(
                    object_store_error,
                ) => {
                    write!(f, "{}", object_store_error)
                }
                execution_error::datafusion_error::Error::IoError(error) => {
                    write!(f, "DatafusionError/IoError: {}", error.message)
                }
                execution_error::datafusion_error::Error::SqlError(error) => {
                    write!(f, "DatafusionError/{}", error)
                }
                execution_error::datafusion_error::Error::NotImplemented(error) => {
                    write!(f, "DatafusionError/NotImplemented: {}", error.message)
                }
                execution_error::datafusion_error::Error::Internal(error) => {
                    write!(f, "DatafusionError/Internal: {}", error.message)
                }
                execution_error::datafusion_error::Error::Plan(error) => {
                    write!(f, "DatafusionError/Plan: {}", error.message)
                }
                execution_error::datafusion_error::Error::SchemaError(schema_error) => {
                    write!(f, "{}", schema_error)
                }
                execution_error::datafusion_error::Error::Execution(error) => {
                    write!(f, "DatafusionError/Execution: {}", error.message)
                }
                execution_error::datafusion_error::Error::ResourcesExhausted(error) => {
                    write!(f, "DatafusionError/ResourcesExhausted: {}", error.message)
                }
                execution_error::datafusion_error::Error::External(error) => {
                    // to be better in UI, we will show only message
                    write!(f, "{}", error.message)
                }
                execution_error::datafusion_error::Error::JitError(error) => {
                    write!(f, "DatafusionError/JitError: {}", error.message)
                }
                execution_error::datafusion_error::Error::Context(error) => write!(
                    f,
                    "DatafusionError/Context [ctx: {}, error: {}]",
                    error.ctx,
                    error
                        .error
                        .as_ref()
                        .map_or_else(String::default, |e| e.to_string())
                ),
                execution_error::datafusion_error::Error::Substrair(error) => {
                    write!(f, "DatafusionError/Substrair: {}", error.message)
                }
                execution_error::datafusion_error::Error::ParserError(error) => {
                    write!(f, "DatafusionError/{}", error)
                }
            },
            _ => write!(f, "DatafusionError no detailed error provided"),
        }
    }
}

impl Display for execution_error::ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.error.as_ref() {
            Some(e) => match e {
                execution_error::parser_error::Error::TokenizerError(error) => {
                    write!(f, "TokenizerError: {}", error.message)
                }
                execution_error::parser_error::Error::ParserError(error) => {
                    write!(f, "ParserError: {}", error.message)
                }
                execution_error::parser_error::Error::RecursionLimitExceeded(_) => {
                    write!(f, "RecursionLimitExceeded")
                }
            },
            None => write!(f, "No detailed error provided"),
        }
    }
}

impl Display for execution_error::SqlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.error.as_ref() {
            Some(e) => write!(f, "ParserError/{}", e),
            _ => write!(f, "No detailed error provided"),
        }
    }
}

impl Display for execution_error::Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            execution_error::Error::NotImplemented(error) => {
                write!(f, "NotImplemented: {}", error.message)
            }
            execution_error::Error::General(error) => {
                write!(f, "General: {}", error.message)
            }
            execution_error::Error::Internal(error) => {
                write!(f, "Internal: {}", error.message)
            }
            execution_error::Error::ArrowError(arrow_error) => {
                write!(f, "ArrowError/{}", arrow_error)
            }
            execution_error::Error::DatafusionError(error) => {
                write!(f, "{}", error)
            }
            execution_error::Error::SqlError(error) => {
                write!(f, "SqlError/{}", error)
            }
            execution_error::Error::IoError(error) => {
                write!(f, "IoError: {}", error.message)
            }
            execution_error::Error::TonicError(error) => {
                write!(f, "TonicError: {}", error.message)
            }
            execution_error::Error::GrpcError(error) => {
                write!(f, "GrpcError: {}", error.message)
            }
            execution_error::Error::GrpcConnectionError(error) => {
                write!(f, "GrpcConnectionError: {}", error.message)
            }
            execution_error::Error::TokioError(error) => {
                write!(f, "TokioError: {}", error.message)
            }
            execution_error::Error::GrpcActiveError(error) => {
                write!(f, "GrpcActiveError: {}", error.message)
            }
            execution_error::Error::FetchFailed(error) => {
                write!(f, "FetchFailed: {}", error.message)
            }
            execution_error::Error::Cancelled(_) => write!(f, "Cancelled"),
        }
    }
}

impl From<BallistaError> for FailedTask {
    fn from(e: BallistaError) -> Self {
        match e {
            BallistaError::FetchFailed(
                executor_id,
                map_stage_id,
                map_partitions,
                desc,
            ) => {
                FailedTask {
                    // fetch partition error is considered to be non-retryable
                    retryable: false,
                    count_to_failures: false,
                    failed_reason: Some(FailedReason::FetchPartitionError(
                        FetchPartitionError {
                            executor_id,
                            map_stage_id: map_stage_id as u32,
                            map_partitions: map_partitions
                                .into_iter()
                                .map(|p| p as u32)
                                .collect(),
                            message: desc,
                        },
                    )),
                }
            }
            BallistaError::IoError(io) => {
                FailedTask {
                    // IO error is considered to be temporary and retryable
                    retryable: true,
                    count_to_failures: true,
                    failed_reason: Some(FailedReason::IoError(IoError {
                        message: io.to_string(),
                    })),
                }
            }
            other => FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(FailedReason::ExecutionError(ExecutionError {
                    error: Some((&other).into()),
                })),
            },
        }
    }
}

impl Error for BallistaError {}
