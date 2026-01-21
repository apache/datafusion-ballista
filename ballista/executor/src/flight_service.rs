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

//! Implementation of the Apache Arrow Flight protocol that wraps an executor.

use datafusion::arrow::ipc::reader::StreamReader;
use std::convert::TryFrom;
use std::fs::File;
use std::path::Path;
use std::pin::Pin;
use tokio_util::io::ReaderStream;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::sort_shuffle::{
    get_index_path, is_sort_shuffle_output, stream_sort_shuffle_partition,
};
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;
use datafusion::arrow::ipc::CompressionType;

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::{error::ArrowError, record_batch::RecordBatch};
use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, info};
use std::io::{BufReader, Read, Seek};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::SendError;
use tokio::{sync::mpsc::Sender, task};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

/// Arrow Flight service for transferring shuffle data between executors.
///
/// This service implements the Apache Arrow Flight protocol to enable efficient
/// transfer of intermediate query results (shuffle data) between executor nodes.
/// It supports both decoded streaming via `do_get` and optimized block transfer
/// via the `IO_BLOCK_TRANSPORT` action.
#[derive(Clone)]
pub struct BallistaFlightService {}

impl BallistaFlightService {
    /// Creates a new BallistaFlightService instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for BallistaFlightService {
    fn default() -> Self {
        Self::new()
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// shuffle file block transfer size    
const BLOCK_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

#[tonic::async_trait]
impl FlightService for BallistaFlightService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let action =
            decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))?;

        match &action {
            BallistaAction::FetchPartition {
                path, partition_id, ..
            } => {
                debug!("FetchPartition reading partition {partition_id} from {path}");
                let data_path = Path::new(path);

                // Check if this is a sort-based shuffle output
                if is_sort_shuffle_output(data_path) {
                    debug!("Detected sort-based shuffle format for {path}");
                    let index_path = get_index_path(data_path);
                    let stream = stream_sort_shuffle_partition(
                        data_path,
                        &index_path,
                        *partition_id,
                    )
                    .map_err(|e| from_ballista_err(&e))?;

                    let schema = stream.schema();
                    // Map DataFusionError to FlightError
                    let stream =
                        stream.map_err(|e| FlightError::from(ArrowError::from(e)));

                    let write_options: IpcWriteOptions = IpcWriteOptions::default()
                        .try_with_compression(Some(CompressionType::LZ4_FRAME))
                        .map_err(|e| from_arrow_err(&e))?;
                    let flight_data_stream = FlightDataEncoderBuilder::new()
                        .with_schema(schema)
                        .with_options(write_options)
                        .build(stream)
                        .map_err(|err| Status::from_error(Box::new(err)));

                    return Ok(Response::new(
                        Box::pin(flight_data_stream) as Self::DoGetStream
                    ));
                }

                // Standard hash-based shuffle - read the entire file
                let file = File::open(path)
                    .map_err(|e| {
                        BallistaError::General(format!(
                            "Failed to open partition file at {path}: {e:?}"
                        ))
                    })
                    .map_err(|e| from_ballista_err(&e))?;
                let file = BufReader::new(file);
                // Safety: setting `skip_validation` requires `unsafe`, user assures data is valid
                let reader = unsafe {
                    StreamReader::try_new(file, None)
                        .map_err(|e| from_arrow_err(&e))?
                        .with_skip_validation(cfg!(feature = "arrow-ipc-optimizations"))
                };

                let (tx, rx) = channel(2);
                let schema = reader.schema();
                task::spawn_blocking(move || {
                    if let Err(e) = read_partition(reader, tx) {
                        log::warn!("error streaming shuffle partition: {e}");
                    }
                });

                let write_options: IpcWriteOptions = IpcWriteOptions::default()
                    .try_with_compression(Some(CompressionType::LZ4_FRAME))
                    .map_err(|e| from_arrow_err(&e))?;
                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .with_options(write_options)
                    .build(ReceiverStream::new(rx))
                    .map_err(|err| Status::from_error(Box::new(err)));

                Ok(Response::new(
                    Box::pin(flight_data_stream) as Self::DoGetStream
                ))
            }
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let token = uuid::Uuid::new_v4();
        info!("do_handshake token={}", token);

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<
            Pin<Box<dyn Stream<Item = Result<_, Status>> + Send + 'static>>,
        > = Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();

        while let Some(data) = request.next().await {
            let _data = data?;
        }

        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            // Block transfer will transfer arrow ipc file block by block
            // without decoding or decompressing, this will provide less resource utilization
            // as file are not decoded nor decompressed/compressed. Usually this would transfer less data across
            // as files are better compressed due to its size.
            //
            // For further discussion regarding performance implications, refer to:
            // https://github.com/apache/datafusion-ballista/issues/1315
            "IO_BLOCK_TRANSPORT" => {
                let action =
                    decode_protobuf(&action.body).map_err(|e| from_ballista_err(&e))?;

                match &action {
                    BallistaAction::FetchPartition { path, .. } => {
                        debug!("FetchPartition reading {path}");
                        let data_path = Path::new(path);

                        // Block transport doesn't support sort-based shuffle because it
                        // transfers the entire file, which contains all partitions.
                        // Use flight transport (do_get) for sort-based shuffle.
                        if is_sort_shuffle_output(data_path) {
                            return Err(Status::unimplemented(
                                "IO_BLOCK_TRANSPORT does not support sort-based shuffle. \
                                 Set ballista.shuffle.remote_read_prefer_flight=true to use \
                                 flight transport instead.",
                            ));
                        }

                        let file = tokio::fs::File::open(&path).await.map_err(|e| {
                            Status::internal(format!("Failed to open file: {e}"))
                        })?;

                        debug!(
                            "streaming file: {} with size: {}",
                            path,
                            file.metadata().await?.len()
                        );
                        let reader = tokio::io::BufReader::with_capacity(
                            BLOCK_BUFFER_CAPACITY,
                            file,
                        );
                        let file_stream =
                            ReaderStream::with_capacity(reader, BLOCK_BUFFER_CAPACITY);

                        let flight_data_stream = file_stream.map(|result| {
                            result
                                .map(|bytes| arrow_flight::Result { body: bytes })
                                .map_err(|e| Status::internal(format!("I/O error: {e}")))
                        });

                        Ok(Response::new(
                            Box::pin(flight_data_stream) as Self::DoActionStream
                        ))
                    }
                }
            }
            action_type => Err(Status::unimplemented(format!(
                "do_action does not implement: {}",
                action_type
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: "IO_BLOCK_TRANSFER".to_owned(),
            description: "optimized shuffle data transfer".to_owned(),
        })];

        Ok(Response::new(
            Box::pin(futures::stream::iter(actions)) as Self::ListActionsStream
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
}

fn read_partition<T>(
    reader: StreamReader<std::io::BufReader<T>>,
    tx: Sender<Result<RecordBatch, FlightError>>,
) -> Result<(), FlightError>
where
    T: Read + Seek,
{
    if tx.is_closed() {
        return Err(FlightError::Tonic(Box::new(Status::internal(
            "Can't send a batch, channel is closed",
        ))));
    }

    for batch in reader {
        tx.blocking_send(batch.map_err(|err| err.into()))
            .map_err(|err| {
                if let SendError(Err(err)) = err {
                    err
                } else {
                    FlightError::Tonic(Box::new(Status::internal(format!(
                        "Can't send a batch, something went wrong: {err:?}"
                    ))))
                }
            })?
    }
    Ok(())
}

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("ArrowError: {e:?}"))
}

fn from_ballista_err(e: &ballista_core::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {e:?}"))
}
