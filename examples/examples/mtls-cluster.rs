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

//! # mTLS Cluster Configuration Example
//!
//! This example demonstrates how to configure mutual TLS (mTLS) for secure
//! communication between Ballista scheduler, executors, and clients.
//!
//! ## Overview
//!
//! mTLS provides two-way authentication:
//! - The client verifies the server's identity using the CA certificate
//! - The server verifies the client's identity using client certificates
//!
//! ## Architecture
//!
//! This example uses pull-based scheduling where:
//! - The scheduler runs a gRPC server with server-side TLS
//! - Executors poll the scheduler for work (client TLS) and serve Flight data (server TLS)
//! - Clients connect to the scheduler with client TLS
//!
//! ## Running
//!
//! ```bash
//! # Generate certificates
//! cargo run --example mtls-cluster --features tls -- certs
//!
//! # Terminal 1: Start scheduler with TLS
//! cargo run --example mtls-cluster --features tls -- scheduler
//!
//! # Terminal 2: Start executor with TLS  
//! cargo run --example mtls-cluster --features tls -- executor
//!
//! # Terminal 3: Run client query with TLS
//! cargo run --example mtls-cluster --features tls -- client
//! ```

use std::net::SocketAddr;
use std::process::Command;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::ConfigProducer;
use ballista_core::extension::{SessionConfigExt, SessionStateExt};
use ballista_core::serde::protobuf::executor_resource::Resource;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::protobuf::{
    ExecutorRegistration, ExecutorResource, ExecutorSpecification,
};
use ballista_core::serde::{
    BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use ballista_core::utils::create_grpc_client_endpoint;
use ballista_executor::execution_loop;
use ballista_executor::executor::Executor;
use ballista_executor::flight_service::BallistaFlightService;
use ballista_executor::metrics::LoggingMetricsCollector;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use log::info;
use tonic::transport::{
    Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};

/// Directory for certificate files
const CERTS_DIR: &str = "certs";
const CA_CERT_PATH: &str = "certs/ca.crt";
const CA_KEY_PATH: &str = "certs/ca.key";
const SERVER_CERT_PATH: &str = "certs/server.crt";
const SERVER_KEY_PATH: &str = "certs/server.key";
const CLIENT_CERT_PATH: &str = "certs/client.crt";
const CLIENT_KEY_PATH: &str = "certs/client.key";

/// Holds loaded TLS certificates for mTLS configuration
#[derive(Clone)]
struct TlsConfig {
    ca_cert: Certificate,
    server_identity: Identity,
    client_tls: ClientTlsConfig,
}

impl TlsConfig {
    fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let ca_cert_pem = std::fs::read_to_string(CA_CERT_PATH)?;
        let server_cert_pem = std::fs::read_to_string(SERVER_CERT_PATH)?;
        let server_key_pem = std::fs::read_to_string(SERVER_KEY_PATH)?;
        let client_cert_pem = std::fs::read_to_string(CLIENT_CERT_PATH)?;
        let client_key_pem = std::fs::read_to_string(CLIENT_KEY_PATH)?;

        let ca_cert = Certificate::from_pem(&ca_cert_pem);
        let server_identity = Identity::from_pem(&server_cert_pem, &server_key_pem);

        let client_tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&ca_cert_pem))
            .identity(Identity::from_pem(&client_cert_pem, &client_key_pem))
            .domain_name("localhost");

        Ok(Self {
            ca_cert,
            server_identity,
            client_tls,
        })
    }

    fn server_tls_config(&self) -> ServerTlsConfig {
        ServerTlsConfig::new()
            .identity(self.server_identity.clone())
            .client_ca_root(self.ca_cert.clone())
    }
}

/// Generate certificates for mTLS using OpenSSL
fn generate_certs() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(CERTS_DIR)?;
    println!("Created {CERTS_DIR}/ directory");

    let openssl_conf = r#"
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[req_distinguished_name]
CN = Ballista CA

[v3_ca]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash

[server_ext]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer

[client_ext]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = @alt_names
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
"#;
    let conf_path = format!("{CERTS_DIR}/openssl.cnf");
    std::fs::write(&conf_path, openssl_conf)?;

    println!("Generating CA...");
    run_openssl(&["genrsa", "-out", CA_KEY_PATH, "4096"])?;
    run_openssl(&[
        "req",
        "-new",
        "-x509",
        "-days",
        "365",
        "-key",
        CA_KEY_PATH,
        "-out",
        CA_CERT_PATH,
        "-config",
        &conf_path,
        "-extensions",
        "v3_ca",
    ])?;

    println!("Generating server certificate...");
    run_openssl(&["genrsa", "-out", SERVER_KEY_PATH, "4096"])?;
    run_openssl(&[
        "req",
        "-new",
        "-key",
        SERVER_KEY_PATH,
        "-out",
        &format!("{CERTS_DIR}/server.csr"),
        "-subj",
        "/CN=localhost",
    ])?;
    run_openssl(&[
        "x509",
        "-req",
        "-days",
        "365",
        "-in",
        &format!("{CERTS_DIR}/server.csr"),
        "-CA",
        CA_CERT_PATH,
        "-CAkey",
        CA_KEY_PATH,
        "-CAcreateserial",
        "-out",
        SERVER_CERT_PATH,
        "-extfile",
        &conf_path,
        "-extensions",
        "server_ext",
    ])?;

    println!("Generating client certificate...");
    run_openssl(&["genrsa", "-out", CLIENT_KEY_PATH, "4096"])?;
    run_openssl(&[
        "req",
        "-new",
        "-key",
        CLIENT_KEY_PATH,
        "-out",
        &format!("{CERTS_DIR}/client.csr"),
        "-subj",
        "/CN=ballista-client",
    ])?;
    run_openssl(&[
        "x509",
        "-req",
        "-days",
        "365",
        "-in",
        &format!("{CERTS_DIR}/client.csr"),
        "-CA",
        CA_CERT_PATH,
        "-CAkey",
        CA_KEY_PATH,
        "-CAcreateserial",
        "-out",
        CLIENT_CERT_PATH,
        "-extfile",
        &conf_path,
        "-extensions",
        "client_ext",
    ])?;

    // Cleanup
    for f in ["server.csr", "client.csr", "openssl.cnf", "ca.srl"] {
        let _ = std::fs::remove_file(format!("{CERTS_DIR}/{f}"));
    }

    println!("\nCertificates generated in {CERTS_DIR}/");
    println!("  - ca.crt / ca.key       : Certificate Authority");
    println!("  - server.crt / server.key : Server certificate (for scheduler/executor)");
    println!("  - client.crt / client.key : Client certificate (for executors/clients)");
    Ok(())
}

fn run_openssl(args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("openssl").args(args).status()?;
    if !status.success() {
        return Err(format!("openssl {} failed", args.join(" ")).into());
    }
    Ok(())
}

/// Create a ConfigProducer that configures TLS for task execution (shuffle fetching)
fn create_tls_config_producer(client_tls: ClientTlsConfig) -> ConfigProducer {
    Arc::new(move || {
        let tls = client_tls.clone();
        SessionConfig::new_with_ballista()
            .with_ballista_use_tls(true)
            .with_ballista_override_create_grpc_client_endpoint(Arc::new(
                move |endpoint: Endpoint| {
                    endpoint.tls_config(tls.clone()).map_err(|e| {
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                    })
                },
            ))
    })
}

/// Start an mTLS-enabled scheduler
async fn run_scheduler() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "0.0.0.0:50050".parse()?;
    info!("Starting mTLS scheduler on {addr}");

    let tls = TlsConfig::load()?;

    // Configure scheduler with TLS for outbound connections to executors
    let client_tls = tls.client_tls.clone();
    let endpoint_override: ballista_scheduler::config::EndpointOverrideFn =
        Arc::new(move |endpoint: Endpoint| endpoint.tls_config(client_tls.clone()));

    let config = SchedulerConfig {
        bind_host: "0.0.0.0".to_string(),
        external_host: "localhost".to_string(),
        bind_port: 50050,
        override_create_grpc_client_endpoint: Some(endpoint_override),
        ..Default::default()
    };

    let cluster = BallistaCluster::new_from_config(&config).await?;

    // Create scheduler server
    let codec = BallistaCodec::new(
        Arc::new(BallistaLogicalExtensionCodec::default()),
        Arc::new(BallistaPhysicalExtensionCodec::default()),
    );
    let metrics_collector = ballista_scheduler::metrics::default_metrics_collector()?;

    let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            config.scheduler_name(),
            cluster,
            codec,
            Arc::new(config),
            metrics_collector,
        );
    scheduler.init().await?;

    // Build gRPC server with mTLS
    let scheduler_grpc = SchedulerGrpcServer::new(scheduler)
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024);

    info!("Scheduler listening with mTLS on {addr}");

    Server::builder()
        .tls_config(tls.server_tls_config())?
        .add_service(scheduler_grpc)
        .serve(addr)
        .await?;

    Ok(())
}

/// Start an mTLS-enabled executor using pull-based scheduling
async fn run_executor() -> Result<(), Box<dyn std::error::Error>> {
    let flight_addr: SocketAddr = "0.0.0.0:50051".parse()?;
    info!("Starting mTLS executor, flight service on {flight_addr}");

    let tls = TlsConfig::load()?;

    // Create executor
    let executor_id = uuid::Uuid::new_v4().to_string();
    let work_dir = tempfile::tempdir()?;
    let work_dir_str = work_dir.path().to_string_lossy().to_string();

    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        host: Some("localhost".to_string()),
        port: 50051,
        grpc_port: 0, // Not used in pull-based scheduling
        specification: Some(ExecutorSpecification {
            resources: vec![ExecutorResource {
                resource: Some(Resource::TaskSlots(4)),
            }],
        }),
    };

    let config_producer = create_tls_config_producer(tls.client_tls.clone());

    // Create runtime producer
    let wd = work_dir_str.clone();
    let runtime_producer: ballista_core::RuntimeProducer = Arc::new(move |_| {
        Ok(Arc::new(
            RuntimeEnvBuilder::new()
                .with_temp_file_path(wd.clone())
                .build()?,
        ))
    });

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir_str,
        runtime_producer,
        config_producer,
        Default::default(), // function_registry
        Arc::new(LoggingMetricsCollector::default()), // metrics_collector
        4,                  // concurrent_tasks
        None,               // execution_engine
    ));

    // Start Flight service with mTLS for serving shuffle data
    let flight_service = FlightServiceServer::new(BallistaFlightService::new())
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024);

    // Spawn Flight server with TLS
    let server_tls = tls.server_tls_config();
    let flight_handle = tokio::spawn(async move {
        info!("Executor Flight service listening with mTLS on {flight_addr}");
        Server::builder()
            .tls_config(server_tls)
            .expect("Failed to configure TLS")
            .add_service(flight_service)
            .serve(flight_addr)
            .await
    });

    // Connect to scheduler with TLS
    let scheduler_url = "https://localhost:50050";
    info!("Connecting to scheduler at {scheduler_url}");

    let endpoint = create_grpc_client_endpoint(scheduler_url.to_string(), None)?
        .tls_config(tls.client_tls.clone())?;
    let connection = endpoint.connect().await?;

    let scheduler = SchedulerGrpcClient::new(connection)
        .max_encoding_message_size(16 * 1024 * 1024)
        .max_decoding_message_size(16 * 1024 * 1024);

    let codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> = BallistaCodec::new(
        Arc::new(BallistaLogicalExtensionCodec::default()),
        Arc::new(BallistaPhysicalExtensionCodec::default()),
    );

    // Run the pull-based execution loop
    // This registers the executor and starts polling for tasks
    info!("Starting execution poll loop...");
    let poll_handle = tokio::spawn(async move {
        execution_loop::poll_loop(scheduler, executor, codec).await
    });

    tokio::select! {
        result = flight_handle => {
            result??;
        }
        result = poll_handle => {
            result??;
        }
    }

    Ok(())
}

/// Run a client query against the mTLS-enabled cluster
async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    info!("Connecting to mTLS scheduler at https://localhost:50050");

    let tls = TlsConfig::load()?;

    // Configure session with mTLS
    let client_tls = tls.client_tls.clone();
    let session_config = SessionConfig::new_with_ballista()
        .with_ballista_use_tls(true)
        .with_ballista_override_create_grpc_client_endpoint(Arc::new(
            move |endpoint: Endpoint| {
                endpoint
                    .tls_config(client_tls.clone())
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
        ));

    let session_state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config)
        .build()
        .upgrade_for_ballista("https://localhost:50050".to_string())?;

    let ctx = SessionContext::new_with_state(session_state);

    info!("Executing query...");
    let df = ctx.sql("SELECT 1 + 1 as result").await?;
    df.show().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install the ring crypto provider for rustls
    // This is required when using tonic with TLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .init();

    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    match mode {
        "certs" => generate_certs()?,
        "scheduler" => run_scheduler().await?,
        "executor" => run_executor().await?,
        "client" => run_client().await?,
        _ => {
            println!(
                r#"Usage: {} <certs|scheduler|executor|client>

mTLS Cluster Example
====================

This example demonstrates how to configure mutual TLS (mTLS) for secure
communication between Ballista scheduler, executors, and clients.

Commands:
  certs      Generate TLS certificates (requires openssl)
  scheduler  Start mTLS-enabled scheduler on port 50050
  executor   Start mTLS-enabled executor on port 50051  
  client     Run a test query against the cluster

Quick start:
  cargo run --example mtls-cluster --features tls -- certs
  cargo run --example mtls-cluster --features tls -- scheduler  # terminal 1
  cargo run --example mtls-cluster --features tls -- executor   # terminal 2
  cargo run --example mtls-cluster --features tls -- client     # terminal 3

How it works:
  - Generates X.509 v3 certificates with proper extensions (SAN, keyUsage)
  - Scheduler runs gRPC server with ServerTlsConfig for mTLS
  - Executor uses pull-based scheduling (polls scheduler for tasks)
  - Executor's Flight service uses ServerTlsConfig for shuffle data
  - Client uses SessionConfigExt to configure TLS for all connections
"#,
                args.first().unwrap_or(&"mtls-cluster".to_string())
            );
        }
    }

    Ok(())
}
