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

use crate::JobId;
use crate::client::BallistaClient;
use crate::config::BallistaConfig;
use crate::error::BallistaError;
use crate::extension::{
    BallistaConfigGrpcEndpoint, BallistaGrpcMetadataInterceptor, SessionConfigExt,
};
use crate::serde::protobuf::get_job_status_result::FlightProxy;
use crate::serde::protobuf::{
    CancelJobParams, ExecuteQueryParams, GetJobStatusParams, GetJobStatusResult,
    KeyValuePair, PartitionLocation, execute_query_params::Query, execute_query_result,
    job_status, scheduler_grpc_client::SchedulerGrpcClient,
};
use crate::serde::protobuf::{ExecutorMetadata, SuccessfulJob};
use crate::serving::ResultEndpoint;
use crate::utils::{GrpcClientConfig, create_grpc_client_endpoint};
use crate::version;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionConfig;
use datafusion_proto::logical_plan::{
    AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::MetadataMap;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
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
    properties: Arc<PlanProperties>,
    /// Execution metrics, currently exposes:
    /// - output_rows: Total number of rows returned
    /// - transferred_bytes: Total bytes transferred from executors
    /// - job_execution_time_ms: Time spent executing on the cluster (server-side)
    /// - job_scheduling_in_ms: Time from query submission to job start (includes queue time)
    /// - job_execution_time_ms: Time spent executing on the cluster (ended_at - started_at)
    /// - job_scheduling_in_ms: Time job waited in scheduler queue (started_at - queued_at)
    metrics: ExecutionPlanMetricsSet,
    /// The scheduler job id after the query has been accepted.
    job_id: Arc<Mutex<Option<JobId>>>,
}

impl<T: 'static + AsLogicalPlan> DistributedQueryExec<T> {
    /// Creates a new distributed query execution plan.
    pub fn new(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        session_id: String,
    ) -> Self {
        let properties = Arc::new(Self::compute_properties(
            plan.schema().as_arrow().clone().into(),
        ));
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            job_id: Arc::new(Mutex::new(None)),
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
        let properties = Arc::new(Self::compute_properties(
            plan.schema().as_arrow().clone().into(),
        ));
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            job_id: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the scheduler job id after the query has been accepted.
    pub fn job_id(&self) -> Option<JobId> {
        self.job_id.lock().clone()
    }

    /// Fetch and render the physical plan that executed on the cluster for this
    /// query, one section per query stage.
    ///
    /// This must be called after the query has run (i.e. after
    /// [`ExecutionPlan::execute`]/`collect`), because it relies on the
    /// scheduler-assigned job id and the plan is only available once the stages
    /// have completed. Returns an error if the query has not been executed yet.
    ///
    /// Only stages that completed successfully are included, inherited from the
    /// scheduler's `get_job_metrics`.
    ///
    /// When `with_metrics` is true the output matches `EXPLAIN ANALYZE`; when
    /// false the `metrics=[...]` suffixes are omitted.
    pub async fn explain_executed_plan(
        &self,
        session_config: &SessionConfig,
        with_metrics: bool,
    ) -> Result<String> {
        let job_id = self.job_id().ok_or_else(|| {
            DataFusionError::Execution(
                "Cannot explain executed plan: query has not been executed yet"
                    .to_string(),
            )
        })?;

        let job_metrics =
            crate::execution_plans::distributed_explain_analyze::fetch_job_metrics(
                &self.scheduler_url,
                &job_id,
                session_config.clone(),
            )
            .await?;

        crate::execution_plans::distributed_explain_analyze::format_job_plan(
            &job_metrics,
            with_metrics,
        )
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
                writeln!(
                    f,
                    "DistributedQueryExec: scheduler_url={}",
                    self.scheduler_url
                )?;
                write!(f, "logical_plan:\n{}", self.plan.display_indent())
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

    fn schema(&self) -> SchemaRef {
        self.plan.schema().as_arrow().clone().into()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Owns no physical expressions — it ships a `LogicalPlan` to the
    /// scheduler, which plans and executes it on the cluster.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
            properties: Arc::new(Self::compute_properties(
                self.plan.schema().as_arrow().clone().into(),
            )),
            metrics: ExecutionPlanMetricsSet::new(),
            job_id: Arc::clone(&self.job_id),
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

        if session_config.ballista_config().client_pull() {
            let stream = futures::stream::once(execute_query_pull(
                self.scheduler_url.clone(),
                self.session_id.clone(),
                query,
                self.config.grpc_client_max_message_size(),
                GrpcClientConfig::from(&self.config),
                Arc::new(self.metrics.clone()),
                Arc::clone(&self.job_id),
                partition,
                session_config,
            ))
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
        } else {
            let stream = futures::stream::once(execute_query_push(
                self.scheduler_url.clone(),
                query,
                self.config.grpc_client_max_message_size(),
                GrpcClientConfig::from(&self.config),
                Arc::new(self.metrics.clone()),
                Arc::clone(&self.job_id),
                partition,
                session_config,
            ))
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
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Submits an already-built physical plan directly to a Ballista scheduler for
/// distributed execution, bypassing logical plan creation on the scheduler side.
///
/// This is a lower-level entry point than [DistributedQueryExec]: instead of a
/// [LogicalPlan] that the scheduler turns into a physical plan itself, the caller
/// supplies the physical plan up front - e.g. for plans containing custom
/// operators that have no logical-plan representation.
pub async fn execute_physical_plan<U: 'static + AsExecutionPlan>(
    scheduler_url: String,
    config: &BallistaConfig,
    physical_plan: Arc<dyn ExecutionPlan>,
    codec: &dyn PhysicalExtensionCodec,
    session_id: String,
    session_config: SessionConfig,
) -> Result<SendableRecordBatchStream> {
    let plan_message =
        U::try_from_physical_plan(physical_plan.clone(), codec).map_err(|e| {
            DataFusionError::Internal(format!("failed to serialize physical plan: {e:?}"))
        })?;
    let mut buf: Vec<u8> = vec![];
    plan_message.try_encode(&mut buf).map_err(|e| {
        DataFusionError::Execution(format!("failed to encode physical plan: {e:?}"))
    })?;

    let settings = session_config
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
    let query = ExecuteQueryParams {
        query: Some(Query::PhysicalPlan(buf)),
        settings,
        session_id: session_id.clone(),
        operation_id,
    };

    let max_message_size = config.grpc_client_max_message_size();
    let grpc_config = GrpcClientConfig::from(config);
    let metrics = Arc::new(ExecutionPlanMetricsSet::new());
    let job_id_handle = Arc::new(Mutex::new(None));
    let partition = 0;

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
        if session_config.ballista_config().client_pull() {
            Box::pin(
                execute_query_pull(
                    scheduler_url,
                    session_id,
                    query,
                    max_message_size,
                    grpc_config,
                    metrics,
                    job_id_handle,
                    partition,
                    session_config,
                )
                .await?,
            )
        } else {
            Box::pin(
                execute_query_push(
                    scheduler_url,
                    query,
                    max_message_size,
                    grpc_config,
                    metrics,
                    job_id_handle,
                    partition,
                    session_config,
                )
                .await?,
            )
        };

    let schema = physical_plan.schema();
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

type SchedulerClient =
    SchedulerGrpcClient<InterceptedService<Channel, BallistaGrpcMetadataInterceptor>>;

/// Wraps a job submission with this client's version, which schedulers
/// require from 55.0.0 on. See [`crate::version`].
fn job_submission_request(
    query: ExecuteQueryParams,
) -> tonic::Request<ExecuteQueryParams> {
    let mut request = tonic::Request::new(query);
    version::insert_version_header(request.metadata_mut());
    request
}

/// Checks the version the scheduler reported in its response to a job
/// submission. A different major version is an error, and a different minor
/// or patch version only logs a warning.
fn check_scheduler_version(client_version: &str, metadata: &MetadataMap) -> Result<()> {
    let scheduler_version = version::version_from_metadata(metadata);
    version::check_compatibility(Some(client_version), scheduler_version)
        .map_err(DataFusionError::Execution)?;
    if let Some(scheduler_version) = scheduler_version
        && scheduler_version != client_version
    {
        warn!(
            "Scheduler version {scheduler_version} differs from client version {client_version}"
        );
    }
    Ok(())
}

/// Cancels a job whose results the client won't read. A failure is only
/// logged, since the caller is already returning an error.
async fn cancel_job(scheduler: &mut SchedulerClient, job_id: &str) {
    if let Err(e) = scheduler
        .cancel_job(CancelJobParams {
            job_id: job_id.to_owned(),
        })
        .await
    {
        warn!("Failed to cancel job {job_id}: {e}");
    }
}

/// Client will periodically invoke scheduler to check
/// job status. There is preconfigured wait period between
/// pulls, which increases query latency.
#[allow(clippy::too_many_arguments)]
async fn execute_query_pull(
    scheduler_url: String,
    session_id: String,
    query: ExecuteQueryParams,
    max_message_size: usize,
    grpc_config: GrpcClientConfig,
    metrics: Arc<ExecutionPlanMetricsSet>,
    job_id_handle: Arc<Mutex<Option<JobId>>>,
    partition: usize,
    session_config: SessionConfig,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send> {
    let grpc_interceptor = session_config.ballista_grpc_interceptor();
    let customize_endpoint =
        session_config.ballista_override_create_grpc_client_endpoint();
    let use_tls = session_config.ballista_use_tls();
    let io_retries_times = grpc_config.io_retries_times;
    let io_retry_wait_time_ms = grpc_config.io_retry_wait_time_ms;

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

    let query_response = scheduler
        .execute_query(job_submission_request(query))
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
    let version_check =
        check_scheduler_version(crate::BALLISTA_VERSION, query_response.metadata());
    let query_result = query_response.into_inner();

    let query_result = match query_result.result.unwrap() {
        execute_query_result::Result::Success(success_result) => success_result,
        execute_query_result::Result::Failure(failure_result) => {
            // A version mismatch is the likelier cause, so report it first.
            version_check?;
            return Err(DataFusionError::Execution(format!(
                "Fail to execute query due to {failure_result:?}"
            )));
        }
    };

    if let Err(e) = version_check {
        // A scheduler older than 55.0.0 doesn't check versions, so it has
        // already queued the job.
        cancel_job(&mut scheduler, &query_result.job_id).await;
        return Err(e);
    }

    assert_eq!(
        session_id, query_result.session_id,
        "Session id inconsistent between Client and Server side in DistributedQueryExec."
    );

    let job_id: JobId = query_result.job_id.into();
    *job_id_handle.lock() = Some(job_id.clone());
    let mut prev_status: Option<job_status::Status> = None;

    loop {
        let GetJobStatusResult {
            status,
            flight_proxy,
        } = scheduler
            .get_job_status(GetJobStatusParams {
                job_id: job_id.clone().into(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner();
        let status = status.and_then(|s| s.status);
        let wait_future = tokio::time::sleep(Duration::from_millis(50));
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
                        io_retries_times,
                        io_retry_wait_time_ms,
                    );

                    futures::stream::once(f).try_flatten()
                });

                break Ok(futures::stream::iter(streams).flatten());
            }
        };
    }
}
/// After job is scheduled client waits
/// for job updates, which are streamed back
/// from server to client
#[allow(clippy::too_many_arguments)]
async fn execute_query_push(
    scheduler_url: String,
    query: ExecuteQueryParams,
    max_message_size: usize,
    grpc_config: GrpcClientConfig,
    metrics: Arc<ExecutionPlanMetricsSet>,
    job_id_handle: Arc<Mutex<Option<JobId>>>,
    partition: usize,
    session_config: SessionConfig,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send> {
    let grpc_interceptor = session_config.ballista_grpc_interceptor();
    let customize_endpoint =
        session_config.ballista_override_create_grpc_client_endpoint();
    let use_tls = session_config.ballista_use_tls();
    let io_retries_times = grpc_config.io_retries_times;
    let io_retry_wait_time_ms = grpc_config.io_retry_wait_time_ms;

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

    let query_push_response = scheduler
        .execute_query_push(job_submission_request(query))
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
    let version_check =
        check_scheduler_version(crate::BALLISTA_VERSION, query_push_response.metadata());
    let mut query_status_stream = query_push_response.into_inner();

    if let Err(e) = version_check {
        // A scheduler older than 55.0.0 doesn't check versions, so it has
        // already queued the job. The job id only arrives with the first
        // status update, which can take until planning finishes, so cancel
        // the job in the background rather than hold up the error.
        tokio::spawn(async move {
            if let Some(Ok(GetJobStatusResult {
                status: Some(status),
                ..
            })) = query_status_stream.next().await
            {
                cancel_job(&mut scheduler, &status.job_id).await;
            }
        });
        return Err(e);
    }

    let mut prev_status: Option<job_status::Status> = None;

    loop {
        let item = query_status_stream
            .next()
            .await
            .ok_or(DataFusionError::Execution(
                "Stream closed without job completing".to_string(),
            ))?
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let GetJobStatusResult {
            status,
            flight_proxy,
        } = item;
        let job_id: JobId = status
            .as_ref()
            .map(|s| s.job_id.to_owned())
            .unwrap_or("unknown_job_id".to_string()) // should not happen
            .into();
        if !job_id.as_str().starts_with("unknown_") {
            let mut shared_job_id = job_id_handle.lock();
            if shared_job_id.is_none() {
                *shared_job_id = Some(job_id.clone());
            }
        }
        let status = status.and_then(|s| s.status);
        let has_status_change = prev_status != status;
        match status {
            None => {
                if has_status_change {
                    info!("Job {job_id} is in initialization ...");
                }
                prev_status = status;
            }
            Some(job_status::Status::Queued(_)) => {
                if has_status_change {
                    info!("Job {job_id} is queued...");
                }
                prev_status = status;
            }
            Some(job_status::Status::Running(_)) => {
                if has_status_change {
                    info!("Job {job_id} is running...");
                }
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
                        io_retries_times,
                        io_retry_wait_time_ms,
                    );

                    futures::stream::once(f).try_flatten()
                });

                break Ok(futures::stream::iter(streams).flatten());
            }
        };
    }
}

/// Where to fetch result partitions from: the host, the port, and whether that
/// endpoint dictates TLS (`None` leaves it to the client's own setting).
fn get_client_host_port(
    executor_metadata: &ExecutorMetadata,
    scheduler_url: &str,
    flight_proxy: &Option<FlightProxy>,
) -> Result<(String, u16, Option<bool>)> {
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
            // A Flight location URI (`grpc+tls://...`) or a bare `host:port`.
            let endpoint: ResultEndpoint =
                address.parse().map_err(BallistaError::into_datafusion)?;
            Ok((endpoint.host().to_string(), endpoint.port(), endpoint.tls()))
        }
        Some(FlightProxy::Local(true)) => {
            debug!("Fetching results from scheduler: {}", scheduler_url);
            let (host, port) = split_host_port(scheduler_url)?;
            Ok((host, port, None))
        }
        Some(FlightProxy::Local(false)) | None => {
            debug!(
                "Fetching results from executor: {}:{}",
                executor_metadata.host, executor_metadata.port
            );
            Ok((
                executor_metadata.host.clone(),
                executor_metadata.port as u16,
                None,
            ))
        }
    }
}
#[allow(clippy::too_many_arguments)]
async fn fetch_partition(
    location: PartitionLocation,
    max_message_size: usize,
    flight_transport: bool,
    scheduler_url: String,
    flight_proxy: Option<FlightProxy>,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    use_tls: bool,
    io_retries_times: u8,
    io_retry_wait_time_ms: u64,
) -> Result<SendableRecordBatchStream> {
    let layout = location.layout();
    let metadata = location.executor_meta.ok_or_else(|| {
        DataFusionError::Internal("Received empty executor metadata".to_owned())
    })?;

    let partition_id = location.partition_id.ok_or_else(|| {
        DataFusionError::Internal("Received empty partition id".to_owned())
    })?;
    let host = metadata.host.as_str();
    let port = metadata.port as u16;

    let (client_host, client_port, endpoint_tls) =
        get_client_host_port(&metadata, &scheduler_url, &flight_proxy)?;

    // An advertised `grpc+tls://` endpoint, such as a Result Service behind a
    // TLS-terminating ingress, needs TLS even when the scheduler connection
    // does not; `grpc+tcp://` likewise forces plaintext.
    let mut ballista_client = BallistaClient::try_new(
        client_host.as_str(),
        client_port,
        max_message_size,
        endpoint_tls.unwrap_or(use_tls),
        customize_endpoint,
        io_retries_times,
        io_retry_wait_time_ms,
        0,
        0,
    )
    .await
    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
    ballista_client
        .fetch_partition_proxied(
            &metadata.id,
            &partition_id.into(),
            location.file_id,
            layout,
            host,
            port,
            flight_transport,
        )
        .await
        .map_err(BallistaError::into_datafusion)
}

#[cfg(test)]
mod test {
    use crate::config::BallistaConfig;
    use crate::execution_plans::distributed_query::{
        DistributedQueryExec, check_scheduler_version, execute_query_pull,
        execute_query_push, get_client_host_port, job_submission_request,
    };
    use crate::extension::SessionConfigExt;
    use crate::serde::protobuf::get_job_status_result::FlightProxy;
    use crate::serde::protobuf::scheduler_grpc_server::{
        SchedulerGrpc, SchedulerGrpcServer,
    };
    use crate::serde::protobuf::{
        CancelJobParams, CancelJobResult, CleanJobDataParams, CleanJobDataResult,
        CreateUpdateSessionParams, CreateUpdateSessionResult, ExecuteQueryParams,
        ExecuteQueryResult, ExecuteQuerySuccessResult, ExecutorMetadata,
        ExecutorStoppedParams, ExecutorStoppedResult, GetJobMetricsParams,
        GetJobMetricsResult, GetJobStatusParams, GetJobStatusResult, HeartBeatParams,
        HeartBeatResult, JobStatus, PollWorkParams, PollWorkResult,
        RegisterExecutorParams, RegisterExecutorResult, RemoveSessionParams,
        RemoveSessionResult, UpdateTaskStatusParams, UpdateTaskStatusResult,
        execute_query_result,
    };
    use crate::utils::GrpcClientConfig;
    use crate::{BALLISTA_VERSION, JobId};
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
    use datafusion::prelude::SessionConfig;
    use datafusion_proto::protobuf::LogicalPlanNode;
    use futures::Stream;
    use parking_lot::Mutex;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::metadata::MetadataMap;
    use tonic::{Request, Response, Status};

    #[test]
    fn job_submission_request_carries_client_version() {
        let request = job_submission_request(ExecuteQueryParams::default());

        assert_eq!(
            request
                .metadata()
                .get("ballista-version")
                .and_then(|v| v.to_str().ok()),
            Some(BALLISTA_VERSION)
        );
    }

    #[test]
    fn scheduler_version_is_read_from_response_header() {
        let mut metadata = MetadataMap::new();
        metadata.insert("ballista-version", "55.3.0".parse().unwrap());

        assert!(check_scheduler_version("55.0.0", &metadata).is_ok());
        assert!(check_scheduler_version("56.0.0", &metadata).is_err());
    }

    #[test]
    fn scheduler_without_version_header_is_incompatible() {
        // Schedulers older than 55.0.0 don't report their version.
        assert!(check_scheduler_version("55.0.0", &MetadataMap::new()).is_err());
    }

    /// A scheduler that accepts every job without checking the client's
    /// version, reports version 1.0.0, and records the jobs it is asked to
    /// cancel.
    #[derive(Clone, Default)]
    struct IncompatibleScheduler {
        cancelled: Arc<Mutex<Vec<String>>>,
    }

    impl IncompatibleScheduler {
        /// Serves the scheduler on a local port and returns its URL.
        async fn start(&self) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(SchedulerGrpcServer::new(self.clone()))
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            );
            url
        }

        fn cancelled(&self) -> Vec<String> {
            self.cancelled.lock().clone()
        }
    }

    fn reporting_version_1<T>(message: T) -> Response<T> {
        let mut response = Response::new(message);
        response
            .metadata_mut()
            .insert("ballista-version", "1.0.0".parse().unwrap());
        response
    }

    type GrpcResult<T> = std::result::Result<Response<T>, Status>;

    fn not_called<T>() -> GrpcResult<T> {
        Err(Status::unimplemented("not called before the version check"))
    }

    #[tonic::async_trait]
    impl SchedulerGrpc for IncompatibleScheduler {
        type ExecuteQueryPushStream = Pin<
            Box<
                dyn Stream<Item = std::result::Result<GetJobStatusResult, Status>> + Send,
            >,
        >;

        async fn execute_query(
            &self,
            request: Request<ExecuteQueryParams>,
        ) -> GrpcResult<ExecuteQueryResult> {
            let session_id = request.into_inner().session_id;
            Ok(reporting_version_1(ExecuteQueryResult {
                operation_id: String::new(),
                result: Some(execute_query_result::Result::Success(
                    ExecuteQuerySuccessResult {
                        job_id: "pull-job".to_owned(),
                        session_id,
                    },
                )),
            }))
        }

        async fn execute_query_push(
            &self,
            _request: Request<ExecuteQueryParams>,
        ) -> GrpcResult<Self::ExecuteQueryPushStream> {
            let first_status = GetJobStatusResult {
                status: Some(JobStatus {
                    job_id: "push-job".to_owned(),
                    ..Default::default()
                }),
                flight_proxy: None,
            };
            Ok(reporting_version_1(Box::pin(futures::stream::iter([Ok(
                first_status,
            )]))))
        }

        async fn cancel_job(
            &self,
            request: Request<CancelJobParams>,
        ) -> GrpcResult<CancelJobResult> {
            self.cancelled.lock().push(request.into_inner().job_id);
            Ok(Response::new(CancelJobResult { cancelled: true }))
        }

        async fn poll_work(
            &self,
            _: Request<PollWorkParams>,
        ) -> GrpcResult<PollWorkResult> {
            not_called()
        }

        async fn register_executor(
            &self,
            _: Request<RegisterExecutorParams>,
        ) -> GrpcResult<RegisterExecutorResult> {
            not_called()
        }

        async fn heart_beat_from_executor(
            &self,
            _: Request<HeartBeatParams>,
        ) -> GrpcResult<HeartBeatResult> {
            not_called()
        }

        async fn update_task_status(
            &self,
            _: Request<UpdateTaskStatusParams>,
        ) -> GrpcResult<UpdateTaskStatusResult> {
            not_called()
        }

        async fn create_update_session(
            &self,
            _: Request<CreateUpdateSessionParams>,
        ) -> GrpcResult<CreateUpdateSessionResult> {
            not_called()
        }

        async fn remove_session(
            &self,
            _: Request<RemoveSessionParams>,
        ) -> GrpcResult<RemoveSessionResult> {
            not_called()
        }

        async fn get_job_status(
            &self,
            _: Request<GetJobStatusParams>,
        ) -> GrpcResult<GetJobStatusResult> {
            not_called()
        }

        async fn get_job_metrics(
            &self,
            _: Request<GetJobMetricsParams>,
        ) -> GrpcResult<GetJobMetricsResult> {
            not_called()
        }

        async fn executor_stopped(
            &self,
            _: Request<ExecutorStoppedParams>,
        ) -> GrpcResult<ExecutorStoppedResult> {
            not_called()
        }

        async fn clean_job_data(
            &self,
            _: Request<CleanJobDataParams>,
        ) -> GrpcResult<CleanJobDataResult> {
            not_called()
        }
    }

    fn job_submission() -> ExecuteQueryParams {
        ExecuteQueryParams {
            session_id: "session".to_owned(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn pull_cancels_job_accepted_by_incompatible_scheduler() {
        let scheduler = IncompatibleScheduler::default();
        let url = scheduler.start().await;
        let config = BallistaConfig::default();

        let result = execute_query_pull(
            url,
            "session".to_owned(),
            job_submission(),
            config.grpc_client_max_message_size(),
            GrpcClientConfig::from(&config),
            Arc::new(ExecutionPlanMetricsSet::new()),
            Arc::new(Mutex::new(None)),
            0,
            SessionConfig::new_with_ballista(),
        )
        .await;

        let Err(err) = result else {
            panic!("an incompatible scheduler should be an error");
        };
        assert!(err.to_string().contains("scheduler 1.0.0"), "{err}");
        assert_eq!(scheduler.cancelled(), ["pull-job"]);
    }

    #[tokio::test]
    async fn push_cancels_job_accepted_by_incompatible_scheduler() {
        let scheduler = IncompatibleScheduler::default();
        let url = scheduler.start().await;
        let config = BallistaConfig::default();

        let result = execute_query_push(
            url,
            job_submission(),
            config.grpc_client_max_message_size(),
            GrpcClientConfig::from(&config),
            Arc::new(ExecutionPlanMetricsSet::new()),
            Arc::new(Mutex::new(None)),
            0,
            SessionConfig::new_with_ballista(),
        )
        .await;

        let Err(err) = result else {
            panic!("an incompatible scheduler should be an error");
        };
        assert!(err.to_string().contains("scheduler 1.0.0"), "{err}");

        // The job is cancelled in the background once its id arrives.
        let deadline = Instant::now() + Duration::from_secs(5);
        while scheduler.cancelled().is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(scheduler.cancelled(), ["push-job"]);
    }

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
            os_info: None,
        };

        // no flight proxy -> client should fetch results from executor
        assert_eq!(
            get_client_host_port(&executor, &scheduler_url, &None).unwrap(),
            (executor.host.clone(), executor.port as u16, None)
        );

        // same, no flight proxy
        assert_eq!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::Local(false))
            )
            .unwrap(),
            (executor.host.clone(), executor.port as u16, None)
        );

        // embedded flight proxy on scheduler
        assert_eq!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::Local(true))
            )
            .unwrap(),
            (scheduler_host.to_string(), scheduler_port, None)
        );

        // external proxy, TLS left to the client
        assert_eq!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::External("proxy:1234".to_string()))
            )
            .unwrap(),
            ("proxy".to_string(), 1234_u16, None)
        );

        // external proxy behind a TLS-terminating ingress
        assert_eq!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::External(
                    "grpc+tls://results.example.com".to_string()
                ))
            )
            .unwrap(),
            ("results.example.com".to_string(), 443_u16, Some(true))
        );

        // external proxy that must be reached in plaintext
        assert_eq!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::External("grpc+tcp://proxy:1234".to_string()))
            )
            .unwrap(),
            ("proxy".to_string(), 1234_u16, Some(false))
        );

        // an endpoint the scheduler should never have advertised
        assert!(
            get_client_host_port(
                &executor,
                &scheduler_url,
                &Some(FlightProxy::External(
                    "https://results.example.com/ballista".to_string()
                ))
            )
            .is_err()
        );
    }

    #[test]
    fn test_create_distributed_query_exec_with_job_id() {
        let exec = Arc::new(DistributedQueryExec::<LogicalPlanNode>::new(
            "http://scheduler:50050".to_string(),
            BallistaConfig::default(),
            LogicalPlan::default(),
            "session".to_string(),
        ));
        *exec.job_id.lock() = Some("job-123".into());

        let new_exec = exec
            .clone()
            .replace_children(
                vec![],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        let new_exec = new_exec
            .downcast_ref::<DistributedQueryExec<LogicalPlanNode>>()
            .unwrap();

        assert_eq!(new_exec.job_id(), Some(JobId::new("job-123")));
    }

    #[test]
    fn test_display_includes_logical_plan() {
        let exec = Arc::new(DistributedQueryExec::<LogicalPlanNode>::new(
            "http://scheduler:50050".to_string(),
            BallistaConfig::default(),
            LogicalPlan::default(),
            "session".to_string(),
        ));

        let rendered = displayable(exec.as_ref()).indent(false).to_string();

        assert!(
            rendered.contains("scheduler_url=http://scheduler:50050"),
            "missing scheduler_url line: {rendered}"
        );
        assert!(
            rendered.contains("logical_plan:"),
            "missing logical_plan section: {rendered}"
        );
        // LogicalPlan::default() renders as EmptyRelation
        assert!(
            rendered.contains("EmptyRelation"),
            "missing rendered logical plan: {rendered}"
        );
    }

    #[tokio::test]
    async fn test_explain_executed_plan_errors_before_execution() {
        let exec = DistributedQueryExec::<LogicalPlanNode>::new(
            "http://scheduler:50050".to_string(),
            BallistaConfig::default(),
            LogicalPlan::default(),
            "session".to_string(),
        );
        // job_id is None because the query has not been executed
        let err = exec
            .explain_executed_plan(&SessionConfig::new(), false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("has not been executed"),
            "unexpected error: {err}"
        );
    }
}
