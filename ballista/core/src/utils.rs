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

use crate::config::BallistaConfig;
use crate::error::{BallistaError, Result};
use crate::extension::SessionConfigExt;
use crate::serde::scheduler::PartitionStats;

use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, metrics};
use futures::StreamExt;
use log::error;
use std::io::BufWriter;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs::File, pin::Pin};
use tonic::codegen::StdError;
use tonic::transport::{Channel, Error, Server};

/// Configuration for gRPC client connections.
///
/// This struct holds timeout and keep-alive settings that are applied
/// when establishing gRPC connections from executors to schedulers or
/// between distributed components.
///
/// # Examples
///
/// ```
/// use ballista_core::config::BallistaConfig;
/// use ballista_core::utils::GrpcClientConfig;
///
/// let ballista_config = BallistaConfig::default();
/// let grpc_config = GrpcClientConfig::from(&ballista_config);
/// ```
#[derive(Debug, Clone)]
pub struct GrpcClientConfig {
    /// Connection timeout in seconds
    pub connect_timeout_seconds: u64,
    /// Request timeout in seconds
    pub timeout_seconds: u64,
    /// TCP keep-alive interval in seconds
    pub tcp_keepalive_seconds: u64,
    /// HTTP/2 keep-alive ping interval in seconds
    pub http2_keepalive_interval_seconds: u64,
}

impl From<&BallistaConfig> for GrpcClientConfig {
    fn from(config: &BallistaConfig) -> Self {
        Self {
            connect_timeout_seconds: config.default_grpc_client_connect_timeout_seconds()
                as u64,
            timeout_seconds: config.default_grpc_client_timeout_seconds() as u64,
            tcp_keepalive_seconds: config.default_grpc_client_tcp_keepalive_seconds()
                as u64,
            http2_keepalive_interval_seconds: config
                .default_grpc_client_http2_keepalive_interval_seconds()
                as u64,
        }
    }
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout_seconds: 20,
            timeout_seconds: 20,
            tcp_keepalive_seconds: 3600,
            http2_keepalive_interval_seconds: 300,
        }
    }
}

/// Configuration for gRPC server.
///
/// This struct holds timeout and keep-alive settings that are applied
/// when creating gRPC servers in executors and schedulers.
///
/// # Examples
///
/// ```
/// use ballista_core::utils::GrpcServerConfig;
///
/// let server_config = GrpcServerConfig::default();
/// let server = ballista_core::utils::create_grpc_server(&server_config);
/// ```
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// Request timeout in seconds
    pub timeout_seconds: u64,
    /// TCP keep-alive interval in seconds
    pub tcp_keepalive_seconds: u64,
    /// HTTP/2 keep-alive ping interval in seconds
    pub http2_keepalive_interval_seconds: u64,
    /// HTTP/2 keep-alive ping timeout in seconds
    pub http2_keepalive_timeout_seconds: u64,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 20,
            tcp_keepalive_seconds: 3600,
            http2_keepalive_interval_seconds: 300,
            http2_keepalive_timeout_seconds: 20,
        }
    }
}

/// Default session builder using the provided configuration
pub fn default_session_builder(
    config: SessionConfig,
) -> datafusion::common::Result<SessionState> {
    Ok(SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build()?))
        .build())
}

/// Creates a default session configuration with Ballista extensions.
pub fn default_config_producer() -> SessionConfig {
    SessionConfig::new_with_ballista()
}

/// Stream data to disk in Arrow IPC format
pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &str,
    disk_write_metric: &metrics::Time,
) -> Result<PartitionStats> {
    let file = BufWriter::new(File::create(path).map_err(|e| {
        error!("Failed to create partition file at {path}: {e:?}");
        BallistaError::IoError(e)
    })?);

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;

    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))?;

    let mut writer =
        StreamWriter::try_new_with_options(file, stream.schema().as_ref(), options)?;

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch.get_array_memory_size();
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;

        let timer = disk_write_metric.timer();
        writer.write(&batch)?;
        timer.done();
    }
    let timer = disk_write_metric.timer();
    writer.finish()?;
    timer.done();
    Ok(PartitionStats::new(
        Some(num_rows as u64),
        Some(num_batches),
        Some(num_bytes as u64),
    ))
}

/// Collects all record batches from a stream into a vector.
pub async fn collect_stream(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Creates a gRPC client connection with the specified configuration.
pub async fn create_grpc_client_connection<D>(
    dst: D,
    config: &GrpcClientConfig,
) -> std::result::Result<Channel, Error>
where
    D: std::convert::TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    let endpoint = tonic::transport::Endpoint::new(dst)?
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds))
        .timeout(Duration::from_secs(config.timeout_seconds))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
        .http2_keep_alive_interval(Duration::from_secs(
            config.http2_keepalive_interval_seconds,
        ))
        // Use a fixed timeout for keep-alive pings to keep configuration simple
        // since this is a standalone configuration
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);
    endpoint.connect().await
}

/// Creates a gRPC server builder with the specified configuration.
pub fn create_grpc_server(config: &GrpcServerConfig) -> Server {
    Server::builder()
        .timeout(Duration::from_secs(config.timeout_seconds))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
        .http2_keepalive_interval(Some(Duration::from_secs(
            config.http2_keepalive_interval_seconds,
        )))
        .http2_keepalive_timeout(Some(Duration::from_secs(
            config.http2_keepalive_timeout_seconds,
        )))
}

/// Recursively collects metrics from an execution plan and all its children.
pub fn collect_plan_metrics(plan: &dyn ExecutionPlan) -> Vec<MetricsSet> {
    let mut metrics_array = Vec::<MetricsSet>::new();
    if let Some(metrics) = plan.metrics() {
        metrics_array.push(metrics);
    }
    plan.children().iter().for_each(|c| {
        collect_plan_metrics(c.as_ref())
            .into_iter()
            .for_each(|e| metrics_array.push(e))
    });
    metrics_array
}

/// Given an interval in seconds, get the time in seconds before now
pub fn get_time_before(interval_seconds: u64) -> u64 {
    let now_epoch_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    now_epoch_ts
        .checked_sub(Duration::from_secs(interval_seconds))
        .unwrap_or_else(|| Duration::from_secs(0))
        .as_secs()
}
