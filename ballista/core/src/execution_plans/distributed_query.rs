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
use crate::config::BallistaConfig;
use crate::extension::{BallistaConfigGrpcEndpoint, SessionConfigExt};
use crate::serde::protobuf::get_job_status_result::FlightProxy;
use crate::serde::protobuf::{
    ExecuteQueryParams, GetJobStatusParams, GetJobStatusResult, KeyValuePair,
    PartitionLocation, execute_query_params::Query, execute_query_result, job_status,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use crate::serde::protobuf::{ExecutorMetadata, SuccessfulJob};
use crate::utils::{GrpcClientConfig, create_grpc_client_endpoint};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::SessionConfig;
use datafusion_proto::logical_plan::{
    AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use log::{debug, error, info};
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// This operator sends a logical plan to a Ballista scheduler for execution and
/// polls the scheduler until the query is complete and then fetches the resulting
/// batches directly from the executors that hold the results from the final
/// query stage.
#[derive(Debug, Clone)]
pub struct DistributedQueryExec<T: 'static + AsLogicalPlan> {
    /// Ballista scheduler URL
    scheduler_url: String,
    /// Ballista configuration
    config: BallistaConfig,
    /// Logical plan to execute
    plan: LogicalPlan,
    /// Codec for LogicalPlan extensions
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    /// Phantom data for serializable plan message
    plan_repr: PhantomData<T>,
    /// Session id
    session_id: String,
    /// Plan properties
    properties: PlanProperties,
    /// Execution metrics, currently exposes:
    /// - output_rows: Total number of rows returned
    /// - transferred_bytes: Total bytes transferred from executors
    /// - job_execution_time_ms: Time spent executing on the cluster (server-side)
    /// - job_scheduling_in_ms: Time from query submission to job start (includes queue time)
    /// - job_execution_time_ms: Time spent executing on the cluster (ended_at - started_at)
    /// - job_scheduling_in_ms: Time job waited in scheduler queue (started_at - queued_at)
    metrics: ExecutionPlanMetricsSet,
}

impl<T: 'static + AsLogicalPlan> DistributedQueryExec<T> {
    /// Creates a new distributed query execution plan.
    pub fn new(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        session_id: String,
    ) -> Self {
        let properties =
            Self::compute_properties(plan.schema().as_arrow().clone().into());
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Creates a new distributed query execution plan with a custom extension codec.
    pub fn with_extension(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        session_id: String,
    ) -> Self {
        let properties =
            Self::compute_properties(plan.schema().as_arrow().clone().into());
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl<T: 'static + AsLogicalPlan> DisplayAs for DistributedQueryExec<T> {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DistributedQueryExec: scheduler_url={}",
                    self.scheduler_url
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "scheduler_url={}", self.scheduler_url)
            }
        }
    }
}

impl<T: 'static + AsLogicalPlan> ExecutionPlan for DistributedQueryExec<T> {
    fn name(&self) -> &str {
        "DistributedQueryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema().as_arrow().clone().into()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedQueryExec {
            scheduler_url: self.scheduler_url.clone(),
            config: self.config.clone(),
            plan: self.plan.clone(),
            extension_codec: self.extension_codec.clone(),
            plan_repr: self.plan_repr,
            session_id: self.session_id.clone(),
            properties: Self::compute_properties(
                self.plan.schema().as_arrow().clone().into(),
            ),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);

        let mut buf: Vec<u8> = vec![];
        let plan_message = T::try_from_logical_plan(
            &self.plan,
            self.extension_codec.as_ref(),
        )
        .map_err(|e| {
            DataFusionError::Internal(format!("failed to serialize logical plan: {e:?}"))
        })?;
        plan_message.try_encode(&mut buf).map_err(|e| {
            DataFusionError::Execution(format!("failed to encode logical plan: {e:?}"))
        })?;

        let settings = context
            .session_config()
            .options()
            .entries()
            .iter()
            .map(
                |datafusion::config::ConfigEntry { key, value, .. }| KeyValuePair {
                    key: key.to_owned(),
                    value: value.clone(),
                },
            )
            .collect();
        let operation_id = uuid::Uuid::now_v7().to_string();
        debug!(
            "Distributed query with session_id: {}, execution operation_id: {}",
            self.session_id, operation_id
        );
        let query = ExecuteQueryParams {
            query: Some(Query::LogicalPlan(buf)),
            settings,
            session_id: self.session_id.clone(),
            operation_id,
        };

        let metric_row_count = MetricBuilder::new(&self.metrics).output_rows(partition);
        let metric_total_bytes =
            MetricBuilder::new(&self.metrics).counter("transferred_bytes", partition);

        let session_config = context.session_config().clone();

        let stream = futures::stream::once(
            execute_query(
                self.scheduler_url.clone(),
                self.session_id.clone(),
                query,
                self.config.default_grpc_client_max_message_size(),
                GrpcClientConfig::from(&self.config),
                Arc::new(self.metrics.clone()),
                partition,
                session_config,
            )
            .map_err(|e| ArrowError::ExternalError(Box::new(e))),
        )
        .try_flatten()
        .inspect(move |batch| {
            metric_total_bytes.add(
                batch
                    .as_ref()
                    .map(|b| b.get_array_memory_size())
                    .unwrap_or(0),
            );

            metric_row_count.add(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
        });

        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        // This execution plan sends the logical plan to the scheduler without
        // performing the node by node conversion to a full physical plan.
        // This implies that we cannot infer the statistics at this stage.
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_query(
    scheduler_url: String,
    session_id: String,
    query: ExecuteQueryParams,
    max_message_size: usize,
    grpc_config: GrpcClientConfig,
    metrics: Arc<ExecutionPlanMetricsSet>,
    partition: usize,
    session_config: SessionConfig,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send> {
    let grpc_interceptor = session_config.ballista_grpc_interceptor();
    let customize_endpoint =
        session_config.ballista_override_create_grpc_client_endpoint();
    let use_tls = session_config.ballista_use_tls();

    // Capture query submission time for total_query_time_ms
    let query_start_time = std::time::Instant::now();

    info!("Connecting to Ballista scheduler at {scheduler_url}");
    // TODO reuse the scheduler to avoid connecting to the Ballista scheduler again and again
    let mut endpoint =
        create_grpc_client_endpoint(scheduler_url.clone(), Some(&grpc_config))
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    if let Some(ref customize) = customize_endpoint {
        endpoint = customize
            .configure_endpoint(endpoint)
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
    }

    let connection = endpoint
        .connect()
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    let mut scheduler = SchedulerGrpcClient::with_interceptor(
        connection,
        grpc_interceptor.as_ref().clone(),
    )
    .max_encoding_message_size(max_message_size)
    .max_decoding_message_size(max_message_size);

    let query_result = scheduler
        .execute_query(query)
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
        .into_inner();

    let query_result = match query_result.result.unwrap() {
        execute_query_result::Result::Success(success_result) => success_result,
        execute_query_result::Result::Failure(failure_result) => {
            return Err(DataFusionError::Execution(format!(
                "Fail to execute query due to {failure_result:?}"
            )));
        }
    };

    assert_eq!(
        session_id, query_result.session_id,
        "Session id inconsistent between Client and Server side in DistributedQueryExec."
    );

    let job_id = query_result.job_id;
    let mut prev_status: Option<job_status::Status> = None;

    loop {
        let GetJobStatusResult {
            status,
            flight_proxy,
        } = scheduler
            .get_job_status(GetJobStatusParams {
                job_id: job_id.clone(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner();
        let status = status.and_then(|s| s.status);
        let wait_future = tokio::time::sleep(Duration::from_millis(100));
        let has_status_change = prev_status != status;
        match status {
            None => {
                if has_status_change {
                    info!("Job {job_id} is in initialization ...");
                }
                wait_future.await;
                prev_status = status;
            }
            Some(job_status::Status::Queued(_)) => {
                if has_status_change {
                    info!("Job {job_id} is queued...");
                }
                wait_future.await;
                prev_status = status;
            }
            Some(job_status::Status::Running(_)) => {
                if has_status_change {
                    info!("Job {job_id} is running...");
                }
                wait_future.await;
                prev_status = status;
            }
            Some(job_status::Status::Failed(err)) => {
                let msg = format!("Job {} failed: {}", job_id, err.error);
                error!("{msg}");
                break Err(DataFusionError::Execution(msg));
            }
            Some(job_status::Status::Successful(SuccessfulJob {
                queued_at,
                started_at,
                ended_at,
                partition_location,
                ..
            })) => {
                // Calculate job execution time (server-side execution)
                let job_execution_ms = ended_at.saturating_sub(started_at);
                let duration = Duration::from_millis(job_execution_ms);

                info!("Job {job_id} finished executing in {duration:?} ");

                // Calculate scheduling time (server-side queue time)
                // This includes network latency and actual queue time
                let scheduling_ms = started_at.saturating_sub(queued_at);

                // Calculate total query time (end-to-end from client perspective)
                let total_elapsed = query_start_time.elapsed();
                let total_ms = total_elapsed.as_millis();

                // Set timing metrics
                let metric_job_execution = MetricBuilder::new(&metrics)
                    .gauge("job_execution_time_ms", partition);
                metric_job_execution.set(job_execution_ms as usize);

                let metric_scheduling =
                    MetricBuilder::new(&metrics).gauge("job_scheduling_in_ms", partition);
                metric_scheduling.set(scheduling_ms as usize);

                let metric_total_time =
                    MetricBuilder::new(&metrics).gauge("total_query_time_ms", partition);
                metric_total_time.set(total_ms as usize);

                // Note: data_transfer_time_ms is not set here because partition fetching
                // happens lazily when the stream is consumed, not during execute_query.
                // This could be added in a future enhancement by wrapping the stream.

                let streams = partition_location.into_iter().map(move |partition| {
                    let f = fetch_partition(
                        partition,
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

                break Ok(futures::stream::iter(streams).flatten());
            }
        };
    }
}

fn get_client_host_port(
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
            debug!("Fetching results from external flight proxy: {}", address);
            split_host_port(format!("http://{address}").as_str())
        }
        Some(FlightProxy::Local(true)) => {
            debug!("Fetching results from scheduler: {}", scheduler_url);
            split_host_port(scheduler_url)
        }
        Some(FlightProxy::Local(false)) | None => {
            debug!(
                "Fetching results from executor: {}:{}",
                executor_metadata.host, executor_metadata.port
            );
            Ok((
                executor_metadata.host.clone(),
                executor_metadata.port as u16,
            ))
        }
    }
}

async fn fetch_partition(
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
        .fetch_partition(
            &metadata.id,
            &partition_id.into(),
            &location.path,
            host,
            port,
            flight_transport,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

#[cfg(test)]
mod test {
    use crate::execution_plans::distributed_query::get_client_host_port;
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
