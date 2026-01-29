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

use crate::error::{BallistaError, Result as BResult};
use crate::serde::scheduler::{Action, PartitionId};

use arrow_flight;
use arrow_flight::Ticket;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightData, flight_service_client::FlightServiceClient};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::buffer::{Buffer, MutableBuffer};
use datafusion::arrow::ipc::convert::try_schema_from_ipc_buffer;
use datafusion::arrow::ipc::reader::StreamDecoder;
use datafusion::arrow::{
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use datafusion::error::DataFusionError;
use datafusion::error::Result;

use crate::extension::BallistaConfigGrpcEndpoint;
use crate::serde::protobuf;

use crate::utils::create_grpc_client_endpoint;

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
    pub async fn try_new(
        host: &str,
        port: u16,
        max_message_size: usize,
        use_tls: bool,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    ) -> BResult<Self> {
        let scheme = if use_tls { "https" } else { "http" };

        let addr = format!("{scheme}://{host}:{port}");
        debug!("BallistaClient connecting to {addr}");

        let mut endpoint = create_grpc_client_endpoint(addr.clone(), None)
            .map_err(|e| {
                BallistaError::GrpcConnectionError(format!(
                    "Error creating endpoint to Ballista scheduler or executor at {addr}: {e:?}"
                ))
            })?;

        if let Some(customize) = customize_endpoint {
            endpoint = customize
                .configure_endpoint(endpoint)
                .map_err(|e| {
                    BallistaError::GrpcConnectionError(format!(
                        "Error creating endpoint to Ballista scheduler or executor at {addr}: {e:?}"
                    ))
                })?;
        }

        let connection = endpoint.connect().await.map_err(|e| {
            BallistaError::GrpcConnectionError(format!(
                "Error connecting to Ballista scheduler or executor at {addr}: {e:?}"
            ))
        })?;

        let flight_client = FlightServiceClient::new(connection)
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);

        debug!("BallistaClient connected OK: {flight_client:?}");

        Ok(Self { flight_client })
    }

    /// Retrieves a partition from an executor.
    ///
    /// Depending on the value of the `flight_transport` parameter, this method will utilize either
    /// the Arrow Flight protocol for compatibility, or a more efficient block-based transfer mechanism.
    /// The block-based transfer is optimized for performance and reduces computational overhead on the server.
    pub async fn fetch_partition(
        &mut self,
        executor_id: &str,
        partition_id: &PartitionId,
        path: &str,
        host: &str,
        port: u16,
        flight_transport: bool,
    ) -> BResult<SendableRecordBatchStream> {
        let action = Action::FetchPartition {
            job_id: partition_id.job_id.clone(),
            stage_id: partition_id.stage_id,
            partition_id: partition_id.partition_id,
            path: path.to_owned(),
            host: host.to_owned(),
            port,
        };

        let result = if flight_transport {
            self.execute_do_get(&action).await
        } else {
            self.execute_do_action(&action).await
        };

        result
            .map_err(|error| match error {
                // map grpc connection error to partition fetch error.
                BallistaError::GrpcActionError(msg) => {
                    log::warn!(
                        "grpc client failed to fetch partition: {partition_id:?} , message: {msg:?}"
                    );
                    BallistaError::FetchFailed(
                        executor_id.to_owned(),
                        partition_id.stage_id,
                        partition_id.partition_id,
                        msg,
                    )
                }
                error => {
                    log::warn!(
                        "grpc client failed to fetch partition: {partition_id:?} , error: {error:?}"
                    );
                    error
                }
            })
    }

    #[allow(rustdoc::private_intra_doc_links)]
    /// Executes the specified action and retrieves the results from the remote executor.
    ///
    /// This method establishes a [FlightDataStream] to facilitate the transfer of data
    /// using the Arrow Flight protocol. The [FlightDataStream] handles the streaming
    /// of record batches from the server to the client in an efficient and structured manner.
    pub async fn execute_do_get(
        &mut self,
        action: &Action,
    ) -> BResult<SendableRecordBatchStream> {
        let serialized_action: protobuf::Action = action.to_owned().try_into()?;

        let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());

        serialized_action
            .encode(&mut buf)
            .map_err(|e| BallistaError::GrpcActionError(format!("{e:?}")))?;

        for i in 0..IO_RETRIES_TIMES {
            if i > 0 {
                warn!(
                    "Remote shuffle read fail, retry {i} times, sleep {IO_RETRY_WAIT_TIME_MS} ms."
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

    /// Executes the specified action and retrieves the results from the remote executor
    /// using an optimized block-based transfer operation. This method establishes a
    /// [BlockDataStream] to facilitate efficient transmission of data blocks, reducing
    /// computational overhead and improving performance compared to flight protocols.
    pub async fn execute_do_action(
        &mut self,
        action: &Action,
    ) -> BResult<SendableRecordBatchStream> {
        let serialized_action: protobuf::Action = action.to_owned().try_into()?;

        let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());

        serialized_action
            .encode(&mut buf)
            .map_err(|e| BallistaError::GrpcActionError(format!("{e:?}")))?;

        for i in 0..IO_RETRIES_TIMES {
            if i > 0 {
                warn!(
                    "Remote shuffle read fail, retry {i} times, sleep {IO_RETRY_WAIT_TIME_MS} ms."
                );
                tokio::time::sleep(std::time::Duration::from_millis(
                    IO_RETRY_WAIT_TIME_MS,
                ))
                .await;
            }

            let request = tonic::Request::new(arrow_flight::Action {
                body: buf.clone().into(),
                r#type: "IO_BLOCK_TRANSPORT".to_string(),
            });
            let result = self.flight_client.do_action(request).await;
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

            let stream = res.into_inner();
            let stream = stream.map(|m| {
                m.map(|b| b.body).map_err(|e| {
                    DataFusionError::ArrowError(
                        Box::new(ArrowError::IpcError(e.to_string())),
                        None,
                    )
                })
            });

            return Ok(Box::pin(BlockDataStream::try_new(stream).await?));
        }
        unreachable!("Did not receive schema batch from flight server");
    }
}

/// [FlightDataStream] facilitates the transfer of shuffle data using the Arrow Flight protocol.
/// Internally, it invokes the `do_get` method on the Arrow Flight server, which returns a stream
/// of messages, each representing a record batch.
///
/// The Flight server is responsible for decompressing and decoding the shuffle file, and then
/// transmitting each batch as an individual message. Each message is compressed independently.
///
/// This approach increases the computational load on the Flight server due to repeated
/// decompression and compression operations. Furthermore, compression efficiency is reduced
/// compared to file-level compression, as it operates on smaller data segments.
///
/// For further discussion regarding performance implications, refer to:
/// <https://github.com/apache/datafusion-ballista/issues/1315>
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
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
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
#[allow(rustdoc::private_intra_doc_links)]
/// [BlockDataStream] facilitates the transfer of original shuffle files in a block-by-block manner.
/// This implementation utilizes a custom `do_action` method on the Arrow Flight server.
/// The primary distinction from [FlightDataStream] is that it does not decompress or decode
/// the original partition file on the server side. This approach reduces computational overhead
/// on the Flight server and enables the transmission of less data, owing to improved file-level compression.
///
/// For a detailed discussion of the performance advantages, see:
/// <https://github.com/apache/datafusion-ballista/issues/1315>
pub struct BlockDataStream<S: Stream<Item = Result<prost::bytes::Bytes>> + Unpin> {
    decoder: StreamDecoder,
    state_buffer: Buffer,
    ipc_stream: S,
    transmitted: usize,
    /// The schema of the data being streamed.
    pub schema: SchemaRef,
}

/// maximum length of message with schema definition
const MAXIMUM_SCHEMA_BUFFER_SIZE: usize = 8_388_608;

impl<S: Stream<Item = Result<prost::bytes::Bytes>> + Unpin> BlockDataStream<S> {
    /// Creates a new `BlockDataStream` from the given IPC byte stream.
    ///
    /// Reads the schema from the stream header and initializes the decoder.
    pub async fn try_new(
        mut ipc_stream: S,
    ) -> std::result::Result<Self, DataFusionError> {
        let mut state_buffer = Buffer::default();

        loop {
            if state_buffer.len() > MAXIMUM_SCHEMA_BUFFER_SIZE {
                return Err(ArrowError::IpcError(format!(
                    "Schema buffer length exceeded maximum buffer size, expected {} actual: {}",
                    MAXIMUM_SCHEMA_BUFFER_SIZE,
                    state_buffer.len()
                )).into());
            }

            match ipc_stream.next().await {
                Some(Ok(blob)) => {
                    state_buffer =
                        Self::combine_buffers(&state_buffer, &Buffer::from(blob));

                    match try_schema_from_ipc_buffer(state_buffer.as_slice()) {
                        Ok(schema) => {
                            return Ok(Self {
                                decoder: StreamDecoder::new(),
                                transmitted: state_buffer.len(),
                                state_buffer,
                                ipc_stream,
                                schema: Arc::new(schema),
                            });
                        }
                        Err(ArrowError::ParseError(_)) => {
                            //
                            // parse errors are ignored as may have not received whole message
                            // thus schema may not be extracted
                            //
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Some(Err(e)) => return Err(ArrowError::IpcError(e.to_string()).into()),
                None => {
                    return Err(ArrowError::IpcError(
                        "Premature end of the stream while decoding schema".to_owned(),
                    )
                    .into());
                }
            }
        }
    }
}

impl<S: Stream<Item = Result<prost::bytes::Bytes>> + Unpin> BlockDataStream<S> {
    fn combine_buffers(first: &Buffer, second: &Buffer) -> Buffer {
        let mut combined = MutableBuffer::new(first.len() + second.len());
        combined.extend_from_slice(first.as_slice());
        combined.extend_from_slice(second.as_slice());
        combined.into()
    }

    fn decode(&mut self) -> std::result::Result<Option<RecordBatch>, ArrowError> {
        self.decoder.decode(&mut self.state_buffer)
    }

    fn extend_bytes(&mut self, blob: prost::bytes::Bytes) {
        //
        //TODO: do we want to limit maximum buffer size here as well?
        //
        self.transmitted += blob.len();
        self.state_buffer = Self::combine_buffers(&self.state_buffer, &Buffer::from(blob))
    }
}

impl<S: Stream<Item = Result<prost::bytes::Bytes>> + Unpin> Stream
    for BlockDataStream<S>
{
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.decode() {
            //
            // if there is a batch to be read from state buffer return it
            //
            Ok(Some(batch)) => std::task::Poll::Ready(Some(Ok(batch))),
            //
            // there is no batch in the state buffer, try to pull new data
            // from remote ipc decode it try to return next batch
            //
            Ok(None) => match self.ipc_stream.poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(flight_data_result)) => {
                    match flight_data_result {
                        Ok(blob) => {
                            self.extend_bytes(blob);

                            match self.decode() {
                                Ok(Some(batch)) => {
                                    std::task::Poll::Ready(Some(Ok(batch)))
                                }
                                Ok(None) => {
                                    cx.waker().wake_by_ref();
                                    std::task::Poll::Pending
                                }
                                Err(e) => std::task::Poll::Ready(Some(Err(
                                    ArrowError::IpcError(e.to_string()).into(),
                                ))),
                            }
                        }
                        Err(e) => std::task::Poll::Ready(Some(Err(
                            ArrowError::IpcError(e.to_string()).into(),
                        ))),
                    }
                }
                //
                // end of IPC stream
                //
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                // its expected that underlying stream will register waker callback
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            Err(e) => std::task::Poll::Ready(Some(Err(ArrowError::IpcError(
                e.to_string(),
            )
            .into()))),
        }
    }
}

impl<S: Stream<Item = Result<prost::bytes::Bytes>> + Unpin> RecordBatchStream
    for BlockDataStream<S>
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::{
        array::{DictionaryArray, Int32Array, RecordBatch},
        datatypes::Int32Type,
        ipc::writer::StreamWriter,
    };
    use futures::{StreamExt, TryStreamExt};
    use prost::bytes::Bytes;

    use crate::client::BlockDataStream;

    fn generate_batches() -> Vec<RecordBatch> {
        let batch0 = RecordBatch::try_from_iter([
            ("a", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as _),
            (
                "b",
                Arc::new(Int32Array::from(vec![11, 22, 33, 44, 55])) as _,
            ),
            (
                "c",
                Arc::new(DictionaryArray::<Int32Type>::from_iter([
                    "hello", "hello", "world", "some", "other",
                ])) as _,
            ),
        ])
        .unwrap();

        let batch1 = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as _,
            ),
            (
                "b",
                Arc::new(Int32Array::from(vec![110, 220, 330, 440, 550])) as _,
            ),
            (
                "c",
                Arc::new(DictionaryArray::<Int32Type>::from_iter([
                    "hello", "some", "world", "some", "other",
                ])) as _,
            ),
        ])
        .unwrap();

        vec![batch0, batch1]
    }

    fn generate_ipc_stream(batches: &[RecordBatch]) -> Vec<u8> {
        let mut result = vec![];
        let mut writer =
            StreamWriter::try_new(&mut result, &batches[0].schema()).unwrap();
        for b in batches {
            writer.write(b).unwrap();
        }

        writer.finish().unwrap();
        result
    }

    #[tokio::test]
    async fn should_process_chunked() {
        let batches = generate_batches();
        let ipc_blob = generate_ipc_stream(&batches);
        let stream = futures::stream::iter(ipc_blob)
            .chunks(2)
            .map(|b| Ok(Bytes::from(b)));

        let result: datafusion::error::Result<Vec<RecordBatch>> =
            BlockDataStream::try_new(stream)
                .await
                .unwrap()
                .try_collect()
                .await;

        assert_eq!(batches, result.unwrap())
    }

    #[tokio::test]
    async fn should_process_single_message() {
        let batches = generate_batches();
        let blob = generate_ipc_stream(&batches);
        let stream = futures::stream::iter(vec![Ok(Bytes::from(blob))]);

        let result: datafusion::error::Result<Vec<RecordBatch>> =
            BlockDataStream::try_new(stream)
                .await
                .unwrap()
                .try_collect()
                .await;

        assert_eq!(batches, result.unwrap())
    }

    #[tokio::test]
    #[should_panic = "Premature end of the stream while decoding schema"]
    async fn should_process_panic_if_not_correct_stream() {
        let batches = generate_batches();
        let ipc_blob = generate_ipc_stream(&batches);
        let stream = futures::stream::iter(ipc_blob[..5].to_vec())
            .chunks(2)
            .map(|b| Ok(Bytes::from(b)));

        let result: datafusion::error::Result<Vec<RecordBatch>> =
            BlockDataStream::try_new(stream)
                .await
                .unwrap()
                .try_collect()
                .await;

        assert_eq!(batches, result.unwrap())
    }
}
