use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use ballista_core::error::BallistaError;
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;
use ballista_core::utils::{create_grpc_client_connection, GrpcClientConfig};
use futures::{Stream, TryFutureExt};
use log::debug;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

/// Service implementing a proxy from scheduler to executor Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightProxyService {}

impl BallistaFlightProxyService {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for BallistaFlightProxyService {
    fn default() -> Self {
        Self::new()
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

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

        match &action {
            BallistaAction::FetchPartition {
                host, port, job_id, ..
            } => {
                debug!("Fetching results for job id: {job_id} from {host}:{port}");
                let mut client = get_flight_client(host, port)
                    .map_err(|e| from_ballista_err(&e))
                    .await?;
                client
                    .do_get(Request::new(ticket))
                    .await
                    .map(|r| Response::new(Box::pin(r.into_inner()) as Self::DoGetStream))
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
    host: &String,
    port: &u16,
) -> Result<FlightServiceClient<tonic::transport::channel::Channel>, BallistaError> {
    let addr = format!("http://{host}:{port}");
    let grpc_config = GrpcClientConfig::default();
    debug!("FlightProxyService connecting to {addr}");
    let connection = create_grpc_client_connection(addr.clone(), &grpc_config)
        .await
        .map_err(|e| {
            BallistaError::GrpcConnectionError(format!(
                "Error connecting to Ballista scheduler or executor at {addr}: {e:?}"
            ))
        })?;
    let flight_client = FlightServiceClient::new(connection)
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024);

    debug!("FlightProxyService connected OK: {flight_client:?}");
    Ok(flight_client)
}
