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

use crate::extension::BallistaConfigGrpcEndpoint;
use crate::serving::{BoxedFlightStream, ForwardingBackend, ServingFlightService};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

/// Service implementing a proxy from scheduler to executor Apache Arrow Flight Protocol
///
/// The proxy only implements the FlightService::do_get api and forwards the requests
/// to the respective executors. Equivalent to a [`ServingFlightService`] over a
/// [`ForwardingBackend`].
#[derive(Clone)]
pub struct BallistaFlightProxyService {
    inner: ServingFlightService<ForwardingBackend>,
}

impl BallistaFlightProxyService {
    /// Creates a proxy which forwards partition fetches to executors, applying
    /// the given message size limits and TLS/endpoint customization when
    /// dialling them.
    pub fn new(
        max_decoding_message_size: usize,
        max_encoding_message_size: usize,
        use_tls: bool,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    ) -> Self {
        Self {
            inner: ServingFlightService::new(ForwardingBackend::new(
                max_decoding_message_size,
                max_encoding_message_size,
                use_tls,
                customize_endpoint,
            )),
        }
    }
}

#[tonic::async_trait]
impl FlightService for BallistaFlightProxyService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.inner.handshake(request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.inner.list_flights(request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.inner.get_flight_info(request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.inner.poll_flight_info(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.inner.get_schema(request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.inner.do_get(request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.inner.do_put(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.inner.do_exchange(request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.inner.do_action(request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.inner.list_actions(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ticket that is not a Ballista action is the client's mistake, not a
    /// server failure.
    #[tokio::test]
    async fn undecodable_tickets_are_invalid_arguments() {
        let proxy = BallistaFlightProxyService::new(4_194_304, 4_194_304, false, None);
        let result = proxy
            .do_get(Request::new(Ticket {
                ticket: vec![0xff, 0xff, 0xff].into(),
            }))
            .await;

        match result {
            Ok(_) => panic!("an undecodable ticket must be rejected"),
            Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
        }
    }
}
