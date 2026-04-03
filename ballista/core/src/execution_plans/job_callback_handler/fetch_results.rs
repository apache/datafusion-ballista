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

use crate::client::BallistaClient;
use crate::execution_plans::distributed_query::CompletedJob;
use crate::execution_plans::job_callback_handler::JobCompletionHandler;
use crate::extension::{BallistaConfigGrpcEndpoint, SessionConfigExt};
use crate::serde::protobuf::get_job_status_result::FlightProxy;
use crate::serde::protobuf::{ExecutorMetadata, PartitionLocation};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionConfig;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use std::sync::Arc;
use url::Url;

pub(crate) struct FetchResultsHandler {
    pub(crate) scheduler_url: String,
    pub(crate) schema: SchemaRef,
    pub(crate) max_message_size: usize,
    pub(crate) session_config: SessionConfig,
    pub(crate) metrics: Arc<ExecutionPlanMetricsSet>,
    pub(crate) partition: usize,
}

#[async_trait]
impl JobCompletionHandler for FetchResultsHandler {
    async fn handle(
        self: Box<Self>,
        completed: CompletedJob,
    ) -> Result<SendableRecordBatchStream> {
        let FetchResultsHandler {
            scheduler_url,
            schema,
            max_message_size,
            session_config,
            metrics,
            partition,
        } = *self;

        let customize_endpoint =
            session_config.ballista_override_create_grpc_client_endpoint();
        let use_tls = session_config.ballista_use_tls();

        // Calculate job execution time (server-side execution)
        let job_execution_ms = completed.ended_at.saturating_sub(completed.started_at);
        // Calculate scheduling time (server-side queue time)
        // This includes network latency and actual queue time
        let scheduling_ms = completed.started_at.saturating_sub(completed.queued_at);
        // Calculate total query time (end-to-end from client perspective)
        let total_ms = completed.query_start_time.elapsed().as_millis();

        log::info!(
            "Job {} finished executing in {:?}",
            completed.job_id,
            std::time::Duration::from_millis(job_execution_ms)
        );

        MetricBuilder::new(&metrics)
            .gauge("job_execution_time_ms", partition)
            .set(job_execution_ms as usize);
        MetricBuilder::new(&metrics)
            .gauge("job_scheduling_in_ms", partition)
            .set(scheduling_ms as usize);
        MetricBuilder::new(&metrics)
            .gauge("total_query_time_ms", partition)
            .set(total_ms as usize);

        let metric_row_count = MetricBuilder::new(&metrics).output_rows(partition);
        let metric_total_bytes =
            MetricBuilder::new(&metrics).counter("transferred_bytes", partition);

        // Note: data_transfer_time_ms is not set here because partition fetching
        // happens lazily when the stream is consumed, not during execute_query.
        // This could be added in a future enhancement by wrapping the stream.
        let flight_proxy = completed.flight_proxy;
        let streams = completed.partition_location.into_iter().map(move |loc| {
            let f = fetch_partition(
                loc,
                max_message_size,
                true,
                scheduler_url.clone(),
                flight_proxy.clone(),
                customize_endpoint.clone(),
                use_tls,
            )
            .map_err(|e| ArrowError::ExternalError(Box::new(e)));

            futures::stream::once(f).try_flatten()
        });

        let stream = futures::stream::iter(streams)
            .flatten()
            .inspect(move |batch| {
                metric_total_bytes.add(
                    batch
                        .as_ref()
                        .map(|b| b.get_array_memory_size())
                        .unwrap_or(0),
                );
                metric_row_count.add(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
            });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

pub(crate) fn get_client_host_port(
    executor_metadata: &ExecutorMetadata,
    scheduler_url: &str,
    flight_proxy: &Option<FlightProxy>,
) -> Result<(String, u16)> {
    fn split_host_port(address: &str) -> Result<(String, u16)> {
        let url: Url = address.parse().map_err(|e| {
            DataFusionError::Execution(format!(
                "Cannot parse host:port in {address:?}: {e}"
            ))
        })?;
        let host = url
            .host_str()
            .ok_or(DataFusionError::Execution(format!(
                "No host in {address:?}"
            )))?
            .to_string();
        let port: u16 = url.port().ok_or(DataFusionError::Execution(format!(
            "No port in {address:?}"
        )))?;
        Ok((host, port))
    }

    match flight_proxy {
        Some(FlightProxy::External(address)) => {
            log::debug!("Fetching results from external flight proxy: {}", address);
            split_host_port(format!("http://{address}").as_str())
        }
        Some(FlightProxy::Local(true)) => {
            log::debug!("Fetching results from scheduler: {}", scheduler_url);
            split_host_port(scheduler_url)
        }
        Some(FlightProxy::Local(false)) | None => {
            log::debug!(
                "Fetching results from executor: {}:{}",
                executor_metadata.host,
                executor_metadata.port
            );
            Ok((
                executor_metadata.host.clone(),
                executor_metadata.port as u16,
            ))
        }
    }
}

pub(crate) async fn fetch_partition(
    location: PartitionLocation,
    max_message_size: usize,
    flight_transport: bool,
    scheduler_url: String,
    flight_proxy: Option<FlightProxy>,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    use_tls: bool,
) -> Result<SendableRecordBatchStream> {
    let metadata = location.executor_meta.ok_or_else(|| {
        DataFusionError::Internal("Received empty executor metadata".to_owned())
    })?;

    let partition_id = location.partition_id.ok_or_else(|| {
        DataFusionError::Internal("Received empty partition id".to_owned())
    })?;
    let host = metadata.host.as_str();
    let port = metadata.port as u16;

    let (client_host, client_port) =
        get_client_host_port(&metadata, &scheduler_url, &flight_proxy)?;

    let mut ballista_client = BallistaClient::try_new(
        client_host.as_str(),
        client_port,
        max_message_size,
        use_tls,
        customize_endpoint,
    )
    .await
    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
    ballista_client
        .fetch_partition_proxied(
            &metadata.id,
            &partition_id.into(),
            host,
            port,
            &location.path,
            flight_transport,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

#[cfg(test)]
mod test {
    use super::get_client_host_port;
    use crate::serde::protobuf::ExecutorMetadata;
    use crate::serde::protobuf::get_job_status_result::FlightProxy;

    #[test]
    fn test_client_host_port() {
        let scheduler_host = "scheduler";
        let scheduler_port: u16 = 5000;

        let scheduler_url = format!("http://{scheduler_host}:{scheduler_port}");
        let executor = ExecutorMetadata {
            id: "test".to_string(),
            host: "executor".to_string(),
            port: 12345,
            grpc_port: 1,
            specification: None,
        };

        // no flight proxy -> client should fetch results from executor
        assert_eq!(
            get_client_host_port(&executor, &scheduler_url, &None).unwrap(),
            (executor.host.clone(), executor.port as u16)
        );

        // same, no flight proxy
        assert_eq!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::Local(false))
            )
            .unwrap(),
            (executor.host.clone(), executor.port as u16)
        );

        // embedded flight proxy on scheduler
        assert_eq!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::Local(true))
            )
            .unwrap(),
            (scheduler_host.to_string(), scheduler_port)
        );

        // external proxy
        assert_eq!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::External("proxy:1234".to_string()))
            )
            .unwrap(),
            ("proxy".to_string(), 1234_u16)
        );
    }
}
