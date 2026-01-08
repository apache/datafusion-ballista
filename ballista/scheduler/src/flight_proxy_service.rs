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

use crate::state::SchedulerState;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use ballista_core::error::BallistaError;
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;
use ballista_core::utils::{GrpcClientConfig, create_grpc_client_connection};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::{Stream, TryFutureExt};
use log::debug;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

/// Service implementing a proxy from scheduler to executor Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightProxyService<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    pub state: Arc<SchedulerState<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    BallistaFlightProxyService<T, U>
{
    pub fn new(state: Arc<SchedulerState<T, U>>) -> Self {
        Self { state }
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> FlightService
    for BallistaFlightProxyService<T, U>
{
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let action =
            decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))?;

        let alive_executors = self.state.executor_manager.get_alive_executors();
        let valid_hosts: HashSet<(String, u16)> = HashSet::from_iter(
            self.state
                .executor_manager
                .get_executor_state()
                .map_err(|e| from_ballista_err(&e))
                .await?
                .iter()
                .filter_map(|(meta, _)| {
                    if alive_executors.contains(&meta.id) {
                        Some((meta.host.clone(), meta.port))
                    } else {
                        None
                    }
                }),
        );
        debug!("Active executors: {:?}", valid_hosts);

        match &action {
            BallistaAction::FetchPartition {
                host, port, job_id, ..
            } => {
                if valid_hosts.contains(&(host.to_owned(), *port)) {
                    debug!("Fetching results for job id: {job_id} from {host}:{port}");
                    let mut client = get_flight_client(
                        host,
                        *port,
                        self.state.config.grpc_server_max_decoding_message_size as usize,
                        self.state.config.grpc_server_max_encoding_message_size as usize,
                    )
                    .map_err(|e| from_ballista_err(&e))
                    .await?;
                    client.do_get(Request::new(ticket)).await.map(|r| {
                        Response::new(Box::pin(r.into_inner()) as Self::DoGetStream)
                    })
                } else {
                    Err(Status::internal(format!("Not a valid host: {host}")))
                }
            }
        }
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

fn from_ballista_err(e: &ballista_core::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {e:?}"))
}

async fn get_flight_client(
    host: &str,
    port: u16,
    max_decoding_message_size: usize,
    max_encoding_message_size: usize,
) -> Result<FlightServiceClient<tonic::transport::channel::Channel>, BallistaError> {
    let addr = format!("http://{host}:{port}");
    let grpc_config = GrpcClientConfig::default();
    let connection = create_grpc_client_connection(addr.clone(), &grpc_config)
        .await
        .map_err(|e| {
            BallistaError::GrpcConnectionError(format!(
                "Error connecting to Ballista scheduler or executor at {addr}: {e:?}"
            ))
        })?;
    let flight_client = FlightServiceClient::new(connection)
        .max_decoding_message_size(max_decoding_message_size)
        .max_encoding_message_size(max_encoding_message_size);

    debug!("FlightProxyService connected: {flight_client:?}");
    Ok(flight_client)
}
