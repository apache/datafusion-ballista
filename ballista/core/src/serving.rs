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

//! Shared serving core for Ballista result/shuffle fetches over Arrow Flight.
//!
//! A fetch request (`do_get` on a [`Ticket`] carrying a [`FetchPartition`]) is
//! answered the same way everywhere — decode the ticket, produce a stream of
//! [`FlightData`] — and the only thing that varies is *where the bytes come
//! from*. That difference is a [`ResultBackend`]:
//!
//! - the executor serves from its local work directory (a backend living in the
//!   executor crate, over its shuffle-format helpers), and
//! - the standalone Result Service and the scheduler's embedded proxy forward to
//!   the producing executor ([`ForwardingBackend`]).
//!
//! [`serve_do_get`] is the shared shell both paths run; [`ServingFlightService`]
//! wraps a backend as a `do_get`-only [`FlightService`] for callers (the Result
//! Service, the embedded proxy) that serve nothing else. The executor keeps its
//! own [`FlightService`] impl — it also answers `do_action` block transfers,
//! which stay executor-internal and are deliberately not part of this core.
//!
//! [`Ticket`]: arrow_flight::Ticket
//! [`FetchPartition`]: crate::serde::scheduler::Action::FetchPartition
//! [`FlightData`]: arrow_flight::FlightData
//! [`FlightService`]: arrow_flight::flight_service_server::FlightService
//! [`ResultBackend`]: crate::serving::ResultBackend
//! [`ForwardingBackend`]: crate::serving::ForwardingBackend
//! [`serve_do_get`]: crate::serving::serve_do_get
//! [`ServingFlightService`]: crate::serving::ServingFlightService

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, TryFutureExt};
use log::debug;
use tonic::{Request, Response, Status, Streaming};

use crate::error::BallistaError;
use crate::extension::BallistaConfigGrpcEndpoint;
use crate::serde::decode_protobuf;
use crate::serde::scheduler::Action as BallistaAction;
use crate::utils::{GrpcClientConfig, create_grpc_client_endpoint};

/// A boxed, `Send` Flight stream — the return shape every serving path shares.
pub type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Maps a [`BallistaError`] into a gRPC [`Status`], matching the executor's own
/// serving errors so the two paths report failures identically.
pub fn from_ballista_err(e: &BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {e:?}"))
}

/// Where a fetched partition's bytes come from.
///
/// The one variant a fetch carries today is
/// [`FetchPartition`](BallistaAction::FetchPartition); an implementation reads it
/// and returns the bytes as a stream of [`FlightData`]. The original [`Ticket`]
/// is passed through untouched so a forwarding backend can relay it verbatim
/// without re-encoding.
#[tonic::async_trait]
pub trait ResultBackend: Send + Sync + 'static {
    /// Produce the [`FlightData`] stream that answers `action`.
    async fn fetch(
        &self,
        action: BallistaAction,
        ticket: Ticket,
    ) -> Result<BoxedFlightStream<FlightData>, Status>;
}

/// The shared `do_get` shell: decode the ticket to a [`BallistaAction`] and hand
/// it to `backend`. Both the executor's `do_get` and [`ServingFlightService`]
/// run this so the decode-and-dispatch contract lives in exactly one place.
///
/// A ticket that does not decode as a Ballista action is the client's mistake,
/// so it is reported as `InvalidArgument` rather than as a server failure.
pub async fn serve_do_get<B: ResultBackend + ?Sized>(
    backend: &B,
    request: Request<Ticket>,
) -> Result<Response<BoxedFlightStream<FlightData>>, Status> {
    let ticket = request.into_inner();
    let action = decode_protobuf(&ticket.ticket)
        .map_err(|e| Status::invalid_argument(format!("invalid ticket: {e}")))?;
    let stream = backend.fetch(action, ticket).await?;
    Ok(Response::new(stream))
}

/// A `do_get`-only [`FlightService`] over a [`ResultBackend`].
///
/// Used by callers that serve results and nothing else — the standalone Result
/// Service and the scheduler's embedded proxy. Every other Flight method returns
/// `unimplemented`, exactly as the previous scheduler proxy did.
pub struct ServingFlightService<B: ResultBackend> {
    backend: Arc<B>,
}

impl<B: ResultBackend> ServingFlightService<B> {
    /// Wraps `backend` as a Flight service.
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }
}

// Manual `Clone` (not derived) so it holds for any `B`, not only `B: Clone` —
// the backend is behind an `Arc`, and tonic requires the service to be `Clone`.
impl<B: ResultBackend> Clone for ServingFlightService<B> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

#[tonic::async_trait]
impl<B: ResultBackend> FlightService for ServingFlightService<B> {
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
        serve_do_get(self.backend.as_ref(), request).await
    }

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

/// A [`ResultBackend`] that forwards fetches to the producing executor.
///
/// The executor's address travels inside the fetch request itself, so this
/// backend reads it from the decoded action, opens a Flight client to that
/// executor, and relays the original ticket's `do_get` stream straight back.
/// This is the byte-relay that the standalone Result Service and the scheduler's
/// embedded proxy both use; it holds no per-request state and is cheap to clone.
#[derive(Clone)]
pub struct ForwardingBackend {
    max_decoding_message_size: usize,
    max_encoding_message_size: usize,
    /// Whether to use TLS when connecting to executors.
    use_tls: bool,
    /// Optional hook to customize the gRPC endpoint (e.g. for TLS).
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
}

impl ForwardingBackend {
    /// Creates a forwarding backend. The message sizes configure this backend's
    /// own client to the executors it forwards to.
    pub fn new(
        max_decoding_message_size: usize,
        max_encoding_message_size: usize,
        use_tls: bool,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    ) -> Self {
        Self {
            max_decoding_message_size,
            max_encoding_message_size,
            use_tls,
            customize_endpoint,
        }
    }
}

#[tonic::async_trait]
impl ResultBackend for ForwardingBackend {
    async fn fetch(
        &self,
        action: BallistaAction,
        ticket: Ticket,
    ) -> Result<BoxedFlightStream<FlightData>, Status> {
        match action {
            BallistaAction::FetchPartition {
                host, port, job_id, ..
            } => {
                debug!("Fetching results for job id: {job_id} from {host}:{port}");
                let mut client = get_flight_client(
                    &host,
                    port,
                    self.max_decoding_message_size,
                    self.max_encoding_message_size,
                    self.use_tls,
                    self.customize_endpoint.clone(),
                )
                .map_err(|e| from_ballista_err(&e))
                .await?;
                let response = client.do_get(Request::new(ticket)).await?;
                Ok(Box::pin(response.into_inner()) as BoxedFlightStream<FlightData>)
            }
        }
    }
}

async fn get_flight_client(
    host: &str,
    port: u16,
    max_decoding_message_size: usize,
    max_encoding_message_size: usize,
    use_tls: bool,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
) -> Result<FlightServiceClient<tonic::transport::channel::Channel>, BallistaError> {
    let scheme = if use_tls { "https" } else { "http" };
    let addr = format!("{scheme}://{host}:{port}");
    let grpc_config = GrpcClientConfig::default();

    let mut endpoint = create_grpc_client_endpoint(addr.clone(), Some(&grpc_config))
        .map_err(|e| {
            BallistaError::GrpcConnectionError(format!(
                "Error creating endpoint for Ballista executor at {addr}: {e:?}"
            ))
        })?;

    if let Some(ref customize) = customize_endpoint {
        endpoint = customize.configure_endpoint(endpoint).map_err(|e| {
            BallistaError::GrpcConnectionError(format!(
                "Error customizing endpoint for Ballista executor at {addr}: {e}"
            ))
        })?;
    }

    let connection = endpoint.connect().await.map_err(|e| {
        BallistaError::GrpcConnectionError(format!(
            "Error connecting to Ballista executor at {addr}: {e:?}"
        ))
    })?;

    let flight_client = FlightServiceClient::new(connection)
        .max_decoding_message_size(max_decoding_message_size)
        .max_encoding_message_size(max_encoding_message_size);

    debug!("ForwardingBackend connected: {flight_client:?}");
    Ok(flight_client)
}
