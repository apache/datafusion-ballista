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

use std::{
    convert::TryInto,
    task::{Context, Poll},
};

use crate::error::{BallistaError, Result};
use crate::serde::scheduler::Action;

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::error::DataFusionError;

use crate::serde::protobuf;
use crate::utils::create_grpc_client_connection;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt, TryStreamExt};
use log::debug;
use prost::Message;

// Set the max gRPC message size to 64 MiB. This is quite large
// but we have to send execution plans over gRPC and they can be large.
const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Client for interacting with Ballista executors.
#[derive(Clone)]
pub struct BallistaClient {
    flight_client: FlightServiceClient<tonic::transport::channel::Channel>,
}

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
        let flight_client = FlightServiceClient::new(connection)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);

        debug!("BallistaClient connected OK");

        Ok(Self { flight_client })
    }

    /// Fetch a partition from an executor
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_partition(
        &mut self,
        executor_id: &str,
        job_id: &str,
        stage_id: usize,
        output_partition: usize,
        map_partitions: &[usize],
        path: &str,
        host: &str,
        port: u16,
    ) -> Result<SendableRecordBatchStream> {
        let action = Action::FetchPartition {
            job_id: job_id.to_string(),
            stage_id,
            partition_id: output_partition,
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
                    stage_id,
                    map_partitions.to_vec(),
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

        let request = tonic::Request::new(Ticket { ticket: buf.into() });

        let stream = self
            .flight_client
            .do_get(request)
            .await
            .map_err(|e| BallistaError::GrpcActionError(format!("{e:?}")))?
            .into_inner();

        // the schema should be the first message returned, else client should error
        let stream = stream.map_err(FlightError::Tonic);
        let mut stream = FlightDataDecoder::new(stream);
        let schema = loop {
            match stream.next().await {
                None => {}
                Some(Ok(fd)) => match &fd.payload {
                    DecodedPayload::None => {}
                    DecodedPayload::Schema(schema) => {
                        break schema.clone();
                    }
                    DecodedPayload::RecordBatch(_) => {}
                },
                Some(Err(e)) => return Err(BallistaError::Internal(e.to_string())),
            }
        };
        let stream = FlightDataStream::new(stream, schema);
        Ok(Box::pin(stream))
    }
}

struct FlightDataStream {
    stream: FlightDataDecoder,
    schema: SchemaRef,
}

impl FlightDataStream {
    pub fn new(stream: FlightDataDecoder, schema: SchemaRef) -> Self {
        Self { stream, schema }
    }
}

impl Stream for FlightDataStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(fd)) => match fd.payload {
                DecodedPayload::None => None,
                DecodedPayload::Schema(_) => {
                    Some(Err(DataFusionError::Internal("Got 2 schemas".to_string())))
                }
                DecodedPayload::RecordBatch(batch) => Some(Ok(batch)),
            },
            Some(Err(e)) => Some(Err(DataFusionError::Internal(e.to_string()))),
            None => None,
        })
    }
}

impl RecordBatchStream for FlightDataStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
