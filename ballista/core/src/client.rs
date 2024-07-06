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

//! Client API for sending requests to executors.

use std::collections::HashMap;
use std::sync::Arc;

use std::{
    convert::{TryFrom, TryInto},
    task::{Context, Poll},
};

use crate::error::{BallistaError, Result};
use crate::serde::scheduler::{Action, PartitionId};

use arrow_flight;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use arrow_flight::{flight_service_client::FlightServiceClient, FlightData};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::{
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use datafusion::error::DataFusionError;

use crate::serde::protobuf;
use crate::utils::create_grpc_client_connection;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use log::{debug, warn};
use prost::Message;
use tonic::{Code, Streaming};

/// Client for interacting with Ballista executors.
#[derive(Clone)]
pub struct BallistaClient {
    flight_client: FlightServiceClient<tonic::transport::channel::Channel>,
}

//TODO make this configurable
const IO_RETRIES_TIMES: u8 = 3;
const IO_RETRY_WAIT_TIME_MS: u64 = 3000;

impl BallistaClient {
    /// Create a new BallistaClient to connect to the executor listening on the specified
    /// host and port
    pub async fn try_new(host: &str, port: u16) -> Result<Self> {
        let addr = format!("http://{host}:{port}");
        debug!("BallistaClient connecting to {}", addr);
        let connection =
            create_grpc_client_connection(addr.clone())
                .await
                .map_err(|e| {
                    BallistaError::GrpcConnectionError(format!(
                    "Error connecting to Ballista scheduler or executor at {addr}: {e:?}"
                ))
                })?;
        let flight_client = FlightServiceClient::new(connection);
        debug!("BallistaClient connected OK");

        Ok(Self { flight_client })
    }

    /// Fetch a partition from an executor
    pub async fn fetch_partition(
        &mut self,
        executor_id: &str,
        partition_id: &PartitionId,
        path: &str,
        host: &str,
        port: u16,
    ) -> Result<SendableRecordBatchStream> {
        let action = Action::FetchPartition {
            job_id: partition_id.job_id.clone(),
            stage_id: partition_id.stage_id,
            partition_id: partition_id.partition_id,
            path: path.to_owned(),
            host: host.to_owned(),
            port,
        };
        self.execute_action(&action)
            .await
            .map_err(|error| match error {
                // map grpc connection error to partition fetch error.
                BallistaError::GrpcActionError(msg) => BallistaError::FetchFailed(
                    executor_id.to_owned(),
                    partition_id.stage_id,
                    partition_id.partition_id,
                    msg,
                ),
                other => other,
            })
    }

    /// Execute an action and retrieve the results
    pub async fn execute_action(
        &mut self,
        action: &Action,
    ) -> Result<SendableRecordBatchStream> {
        let serialized_action: protobuf::Action = action.to_owned().try_into()?;

        let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());

        serialized_action
            .encode(&mut buf)
            .map_err(|e| BallistaError::GrpcActionError(format!("{e:?}")))?;

        for i in 0..IO_RETRIES_TIMES {
            if i > 0 {
                warn!(
                    "Remote shuffle read fail, retry {} times, sleep {} ms.",
                    i, IO_RETRY_WAIT_TIME_MS
                );
                tokio::time::sleep(std::time::Duration::from_millis(
                    IO_RETRY_WAIT_TIME_MS,
                ))
                .await;
            }

            let request = tonic::Request::new(Ticket {
                ticket: buf.clone().into(),
            });
            let result = self.flight_client.do_get(request).await;
            let res = match result {
                Ok(res) => res,
                Err(ref err) => {
                    // IO related error like connection timeout, reset... will warp with Code::Unknown
                    // This means IO related error will retry.
                    if i == IO_RETRIES_TIMES - 1 || err.code() != Code::Unknown {
                        return BallistaError::GrpcActionError(format!(
                            "{:?}",
                            result.unwrap_err()
                        ))
                        .into();
                    }
                    // retry request
                    continue;
                }
            };

            let mut stream = res.into_inner();

            match stream.message().await {
                Ok(res) => {
                    return match res {
                        Some(flight_data) => {
                            let schema = Arc::new(Schema::try_from(&flight_data)?);

                            // all the remaining stream messages should be dictionary and record batches
                            Ok(Box::pin(FlightDataStream::new(stream, schema)))
                        }
                        None => Err(BallistaError::GrpcActionError(
                            "Did not receive schema batch from flight server".to_string(),
                        )),
                    };
                }
                Err(e) => {
                    if i == IO_RETRIES_TIMES - 1 || e.code() != Code::Unknown {
                        return BallistaError::GrpcActionError(format!(
                            "{:?}",
                            e.to_string()
                        ))
                        .into();
                    }
                    continue;
                }
            }
        }
        unreachable!("Did not receive schema batch from flight server");
    }
}

struct FlightDataStream {
    stream: Streaming<FlightData>,
    schema: SchemaRef,
    dictionaries_by_id: HashMap<i64, ArrayRef>,
}

impl FlightDataStream {
    pub fn new(stream: Streaming<FlightData>, schema: SchemaRef) -> Self {
        Self {
            stream,
            schema,
            dictionaries_by_id: HashMap::new(),
        }
    }
}

impl Stream for FlightDataStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx).map(|x| match x {
            Some(flight_data_chunk_result) => {
                let converted_chunk = flight_data_chunk_result
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)).into())
                    .and_then(|flight_data_chunk| {
                        flight_data_to_arrow_batch(
                            &flight_data_chunk,
                            self.schema.clone(),
                            &self.dictionaries_by_id,
                        )
                        .map_err(|e| DataFusionError::ArrowError(e, None))
                    });
                Some(converted_chunk)
            }
            None => None,
        })
    }
}

impl RecordBatchStream for FlightDataStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
