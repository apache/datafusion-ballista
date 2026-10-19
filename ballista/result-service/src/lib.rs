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

#![warn(missing_docs)]

//! Ballista Result Service: serves query results to clients by forwarding each
//! partition fetch to the executor that holds the partition. Point the
//! scheduler's `--advertise-flight-endpoint` at it.
//!
//! The `ballista-result-service` binary runs [`serve`] with a configuration
//! from its command line. Call [`serve`] directly to customize the connections
//! to executors, as `SchedulerConfig::override_create_grpc_client_endpoint`
//! does for the scheduler's embedded proxy.

mod http;
mod metered;
mod metrics;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::error::{BallistaError, Result};
use ballista_core::extension::{BallistaConfigGrpcEndpoint, EndpointOverrideFn};
use ballista_core::serving::{ForwardingBackend, ResultBackend, ServingFlightService};
use ballista_core::utils::{
    GrpcServerConfig, create_grpc_server, create_grpc_server_incoming,
    serve_until_shutdown,
};
use futures::{Stream, StreamExt};
use log::info;
use tokio::sync::oneshot;
use tonic::service::Routes;
use tonic::transport::server::{Router, TcpIncoming};

use crate::metered::MeteredBackend;
use crate::metrics::{ResultMetricsCollector, default_metrics_collector};

/// Configuration for [`serve`].
#[derive(Clone)]
pub struct ResultServiceConfig {
    /// Address the Flight service, health probes (`/healthz` and `/readyz`)
    /// and metrics (`/api/metrics`) bind to.
    pub bind_address: SocketAddr,
    /// Maximum gRPC message size, in bytes, the service decodes.
    pub grpc_max_decoding_message_size: usize,
    /// Maximum gRPC message size, in bytes, the service encodes.
    pub grpc_max_encoding_message_size: usize,
    /// Time to let in-flight result streams finish after the shutdown signal
    /// before stopping with an error. Zero waits as long as it takes.
    pub graceful_shutdown_timeout: Duration,
    /// Connect to executors with `https://` URLs. The connections are only
    /// encrypted in a build with one of tonic's TLS features, typically with
    /// [`Self::override_create_grpc_client_endpoint`] setting the TLS
    /// configuration.
    pub use_tls: bool,
    /// Customizes the endpoint of each connection to an executor, as
    /// `SchedulerConfig::override_create_grpc_client_endpoint` does for the
    /// embedded proxy.
    pub override_create_grpc_client_endpoint: Option<EndpointOverrideFn>,
}

impl Default for ResultServiceConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([0, 0, 0, 0], 50055)),
            grpc_max_decoding_message_size: 16 * 1024 * 1024,
            grpc_max_encoding_message_size: 16 * 1024 * 1024,
            graceful_shutdown_timeout: Duration::from_secs(10),
            use_tls: false,
            override_create_grpc_client_endpoint: None,
        }
    }
}

/// Serves results until `shutdown` completes, then refuses new connections and
/// gives in-flight result streams up to
/// [`ResultServiceConfig::graceful_shutdown_timeout`] to finish.
pub async fn serve(config: ResultServiceConfig, shutdown: impl Future) -> Result<()> {
    let backend = ForwardingBackend::new(
        config.grpc_max_decoding_message_size,
        config.grpc_max_encoding_message_size,
        config.use_tls,
        config
            .override_create_grpc_client_endpoint
            .clone()
            .map(|f| Arc::new(BallistaConfigGrpcEndpoint::new(f))),
    );
    let addr = config.bind_address;
    let incoming = create_grpc_server_incoming(addr, &GrpcServerConfig::default())
        .map_err(|e| {
            BallistaError::General(format!(
                "failed to bind result service on {addr}: {e}"
            ))
        })?;
    info!("Ballista Result Service listening on {addr}");

    serve_incoming(
        backend,
        &config,
        default_metrics_collector()?,
        incoming,
        shutdown,
    )
    .await
}

/// [`serve`] from an already-bound listener, with any backend.
async fn serve_incoming<B: ResultBackend>(
    backend: B,
    config: &ResultServiceConfig,
    metrics: Arc<dyn ResultMetricsCollector>,
    incoming: TcpIncoming,
    shutdown: impl Future,
) -> Result<()> {
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    // tonic keeps its listener open until every connection has closed, so a
    // connection arriving during the drain would wait unanswered. Ending the
    // incoming stream drops the listener and refuses such connections. With a
    // shutdown signal that never fires, tonic still drains the connections it
    // has once the stream ends.
    let server = server(backend, config, metrics).serve_with_incoming_shutdown(
        until_stopped(incoming, stop_rx),
        std::future::pending(),
    );

    // On shutdown there is nothing to finish before the server stops taking
    // connections: it refuses new ones, and gives its in-flight streams the
    // bound to finish.
    let mut stop_tx = Some(stop_tx);
    serve_until_shutdown(
        server,
        shutdown,
        || std::future::ready(()),
        || {
            if let Some(stop_tx) = stop_tx.take() {
                let _ = stop_tx.send(());
            }
        },
        config.graceful_shutdown_timeout,
    )
    .await?;

    info!("Ballista Result Service stopped");
    Ok(())
}

/// `incoming`'s connections until `stop` completes. The stream then ends and
/// drops the listener.
fn until_stopped(
    incoming: TcpIncoming,
    stop: oneshot::Receiver<()>,
) -> impl Stream<Item = <TcpIncoming as Stream>::Item> {
    futures::stream::unfold((incoming, stop), |(mut incoming, mut stop)| async move {
        tokio::select! {
            _ = &mut stop => None,
            next = incoming.next() => next.map(|connection| (connection, (incoming, stop))),
        }
    })
}

pub use ballista_core::utils::shutdown_signal;

/// The Flight service, health probes and metrics, served on one port.
fn server<B: ResultBackend>(
    backend: B,
    config: &ResultServiceConfig,
    metrics: Arc<dyn ResultMetricsCollector>,
) -> Router {
    let backend = MeteredBackend::new(backend, metrics.clone());
    let service = FlightServiceServer::new(ServingFlightService::new(backend))
        .max_decoding_message_size(config.grpc_max_decoding_message_size)
        .max_encoding_message_size(config.grpc_max_encoding_message_size);
    let routes = Routes::new(service)
        .into_axum_router()
        .merge(http::router(metrics));

    create_grpc_server(&GrpcServerConfig::default())
        // Health probes and metrics scrapers use HTTP/1.1.
        .accept_http1(true)
        .add_routes(Routes::from(routes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::NoopMetricsCollector;
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::{FlightData, Ticket};
    use ballista_core::JobId;
    use ballista_core::serde::protobuf;
    use ballista_core::serde::scheduler::Action as BallistaAction;
    use ballista_core::serving::BoxedFlightStream;
    use prost::Message;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::{Notify, watch};
    use tonic::Status;

    /// Holds each fetch's stream open until `release` is set.
    struct HeldBackend {
        started: Arc<Notify>,
        release: watch::Receiver<bool>,
    }

    #[tonic::async_trait]
    impl ResultBackend for HeldBackend {
        async fn fetch(
            &self,
            _action: BallistaAction,
            _ticket: Ticket,
        ) -> std::result::Result<BoxedFlightStream<FlightData>, Status> {
            self.started.notify_one();
            let mut release = self.release.clone();
            Ok(Box::pin(futures::stream::once(async move {
                let _ = release.wait_for(|released| *released).await;
                Ok(FlightData::new())
            })))
        }
    }

    fn fetch_ticket() -> Ticket {
        let action: protobuf::Action = BallistaAction::FetchPartition {
            job_id: JobId::new("job"),
            stage_id: 1,
            partition_id: 0,
            host: "executor".to_owned(),
            port: 50051,
            file_id: None,
            layout: Default::default(),
            file_kind: Default::default(),
            byte_ranges: vec![],
        }
        .try_into()
        .unwrap();
        Ticket::new(action.encode_to_vec())
    }

    fn forwarding_backend() -> ForwardingBackend {
        ForwardingBackend::new(16 * 1024 * 1024, 16 * 1024 * 1024, false, None)
    }

    #[tokio::test]
    async fn health_probes_are_served_over_http1_on_the_flight_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = server(
            forwarding_backend(),
            &ResultServiceConfig::default(),
            Arc::new(NoopMetricsCollector),
        )
        .serve_with_incoming(TcpIncoming::from(listener));
        let server = tokio::spawn(server);

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        server.abort();

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("ready"), "{response}");
    }

    #[tokio::test]
    async fn the_drain_refuses_new_connections_and_finishes_open_streams() {
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = incoming.local_addr().unwrap();
        let started = Arc::new(Notify::new());
        let (release_tx, release_rx) = watch::channel(false);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let backend = HeldBackend {
            started: started.clone(),
            release: release_rx,
        };
        let server = tokio::spawn(async move {
            let config = ResultServiceConfig::default();
            serve_incoming(
                backend,
                &config,
                Arc::new(NoopMetricsCollector),
                incoming,
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });

        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = FlightServiceClient::new(channel);
        let mut stream = client.do_get(fetch_ticket()).await.unwrap().into_inner();
        started.notified().await;

        shutdown_tx.send(()).unwrap();
        let refused = tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::net::TcpStream::connect(addr).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(refused.is_ok(), "the drain must refuse new connections");

        release_tx.send(true).unwrap();
        assert!(stream.message().await.unwrap().is_some());
        assert!(stream.message().await.unwrap().is_none());
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("the drain must finish once the open stream has")
            .unwrap()
            .unwrap();
    }
}
