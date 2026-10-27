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
use crate::config::BALLISTA_PIPELINED_REGISTRATION_TIMEOUT_SECONDS;
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
use crate::serde::protobuf::{ExecutorMetadata, RunningJob, SuccessfulJob};
use crate::serde::scheduler::PipelinedPartitionLocation;
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
use futures::stream::BoxStream;
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

    /// Returns the Ballista scheduler URL.
    pub fn scheduler_url(&self) -> &str {
        &self.scheduler_url
    }

    /// Returns the Ballista configuration.
    pub fn config(&self) -> &BallistaConfig {
        &self.config
    }

    /// Returns the logical plan to execute.
    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    /// Returns the session id.
    pub fn session_id(&self) -> &str {
        &self.session_id
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

        let session_config = context.session_config();

        // Prefer the BallistaConfig registered on the task's session config.
        // It can be missing when the task context comes from a session not
        // set up by Ballista (as seen with Ballista Python), so fall back to
        // the config this plan was created with.
        let bc = session_config
            .options()
            .extensions
            .get::<BallistaConfig>()
            .unwrap_or(&self.config);

        let grpc_config = GrpcClientConfig::from(bc);

        if bc.client_pull() {
            let stream = futures::stream::once(execute_query_pull(
                self.scheduler_url.clone(),
                self.session_id.clone(),
                query,
                bc.grpc_client_max_message_size(),
                grpc_config,
                Arc::new(self.metrics.clone()),
                Arc::clone(&self.job_id),
                partition,
                session_config.ballista_grpc_interceptor(),
                session_config.ballista_override_create_grpc_client_endpoint(),
                bc.use_tls(),
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
                bc.grpc_client_max_message_size(),
                grpc_config,
                Arc::new(self.metrics.clone()),
                Arc::clone(&self.job_id),
                partition,
                session_config.ballista_grpc_interceptor(),
                session_config.ballista_override_create_grpc_client_endpoint(),
                bc.use_tls(),
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

    let metrics = Arc::new(ExecutionPlanMetricsSet::new());
    let job_id_handle = Arc::new(Mutex::new(None));
    let partition = 0;

    // Prefer the BallistaConfig registered on the task's session config.
    // It can be missing when the task context comes from a session not
    // set up by Ballista (as seen with Ballista Python), so fall back to
    // the config this plan was created with.
    let bc = session_config
        .options()
        .extensions
        .get::<BallistaConfig>()
        .unwrap_or(config);
    let grpc_config = GrpcClientConfig::from(bc);

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
        if bc.client_pull() {
            Box::pin(
                execute_query_pull(
                    scheduler_url,
                    session_id,
                    query,
                    bc.grpc_client_max_message_size(),
                    grpc_config,
                    metrics,
                    job_id_handle,
                    partition,
                    session_config.ballista_grpc_interceptor(),
                    session_config.ballista_override_create_grpc_client_endpoint(),
                    bc.use_tls(),
                )
                .await?,
            )
        } else {
            Box::pin(
                execute_query_push(
                    scheduler_url,
                    query,
                    bc.grpc_client_max_message_size(),
                    grpc_config,
                    metrics,
                    job_id_handle,
                    partition,
                    session_config.ballista_grpc_interceptor(),
                    session_config.ballista_override_create_grpc_client_endpoint(),
                    bc.use_tls(),
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

/// The registration timeout the query was submitted with, which bounds how
/// long the client waits for a pipelined job's final stage to start serving.
fn pipelined_registration_timeout(query: &ExecuteQueryParams) -> Duration {
    query
        .settings
        .iter()
        .find(|kv| kv.key == BALLISTA_PIPELINED_REGISTRATION_TIMEOUT_SECONDS)
        .and_then(|kv| kv.value.as_ref())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(60))
}

/// How the client reaches the final stage of a pipelined job.
#[derive(Clone)]
struct PipelinedFetch {
    max_message_size: usize,
    scheduler_url: String,
    flight_proxy: Option<FlightProxy>,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    use_tls: bool,
    io_retries_times: u8,
    io_retry_wait_time_ms: u64,
    registration_timeout: Duration,
}

/// Streams a pipelined job's results: every output partition of its final
/// stage at once, while the job runs. The final stage cannot finish until it
/// has been read, so the partitions must be read concurrently rather than
/// one after another.
///
/// One connection is opened per executor (or per proxy) and every partition
/// it serves is multiplexed over it. If `failure` reports that the job
/// failed, the stream ends with that error. If the stream is dropped before
/// it ends, the job is cancelled through `scheduler`.
fn pipelined_results(
    job_id: JobId,
    locations: Vec<crate::serde::protobuf::PipelinedPartitionLocation>,
    fetch: PipelinedFetch,
    failure: Option<tokio::sync::oneshot::Receiver<String>>,
    scheduler: Option<SchedulerClient>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let locations = locations
        .into_iter()
        .map(|location| {
            location.try_into().map_err(|e: BallistaError| {
                DataFusionError::Internal(format!(
                    "invalid pipelined output location for job {job_id}: {e}"
                ))
            })
        })
        .collect::<Result<Vec<PipelinedPartitionLocation>>>()?;

    let mut by_endpoint: std::collections::BTreeMap<
        (String, u16),
        Vec<PipelinedPartitionLocation>,
    > = Default::default();
    for location in locations {
        let executor_meta: ExecutorMetadata = location.executor_meta.clone().into();
        let endpoint = get_client_host_port(
            &executor_meta,
            &fetch.scheduler_url,
            &fetch.flight_proxy,
        )?;
        by_endpoint.entry(endpoint).or_default().push(location);
    }

    let per_endpoint = by_endpoint
        .into_iter()
        .map(move |((host, port), locations)| {
            let fetch = fetch.clone();
            futures::stream::once(async move {
                let client = BallistaClient::try_new(
                    &host,
                    port,
                    fetch.max_message_size,
                    fetch.use_tls,
                    fetch.customize_endpoint.clone(),
                    fetch.io_retries_times,
                    fetch.io_retry_wait_time_ms,
                    0,
                    0,
                )
                .await
                .map_err(BallistaError::into_datafusion)?;
                let partitions = locations.into_iter().map(move |location| {
                    let mut client = client.clone();
                    let timeout = fetch.registration_timeout;
                    futures::stream::once(async move {
                        client
                            .fetch_pipelined_partition(&location, timeout)
                            .await
                            .map_err(BallistaError::into_datafusion)
                    })
                    .try_flatten()
                    .boxed()
                });
                Ok::<_, DataFusionError>(futures::stream::select_all(partitions))
            })
            .try_flatten()
            .boxed()
        });
    let data = futures::stream::select_all(per_endpoint).boxed();
    Ok(Box::pin(PipelinedResults::new(
        job_id, data, failure, scheduler,
    )))
}

/// How long a pipelined result stream whose fetch failed waits for the job's
/// own failure, which says better what went wrong.
const JOB_FAILURE_GRACE: Duration = Duration::from_secs(5);

/// A pipelined job's result stream, cut short with the job's error if the
/// scheduler reports that it failed, and cancelling the job if it is dropped
/// before it ends.
struct PipelinedResults {
    job_id: JobId,
    data: BoxStream<'static, Result<RecordBatch>>,
    failure: Option<tokio::sync::oneshot::Receiver<String>>,
    /// A fetch failed: its error, reported unless the job's own failure
    /// arrives before the deadline.
    fetch_error: Option<(DataFusionError, std::pin::Pin<Box<tokio::time::Sleep>>)>,
    /// How long to wait for the job's failure after a fetch fails.
    failure_grace: Duration,
    /// Cancels the job if the stream is dropped before it ends.
    scheduler: Option<SchedulerClient>,
    done: bool,
}

impl PipelinedResults {
    fn new(
        job_id: JobId,
        data: BoxStream<'static, Result<RecordBatch>>,
        failure: Option<tokio::sync::oneshot::Receiver<String>>,
        scheduler: Option<SchedulerClient>,
    ) -> Self {
        Self {
            job_id,
            data,
            failure,
            fetch_error: None,
            failure_grace: JOB_FAILURE_GRACE,
            scheduler,
            done: false,
        }
    }
}

impl Stream for PipelinedResults {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::future::Future;
        use std::task::Poll;
        loop {
            if self.done {
                return Poll::Ready(None);
            }
            if let Some(failure) = self.failure.as_mut() {
                match std::pin::Pin::new(failure).poll(cx) {
                    Poll::Ready(Ok(reason)) => {
                        self.done = true;
                        let msg = format!("Job {} failed: {reason}", self.job_id);
                        error!("{msg}");
                        return Poll::Ready(Some(Err(DataFusionError::Execution(msg))));
                    }
                    // The watcher stopped without seeing a failure.
                    Poll::Ready(Err(_)) => self.failure = None,
                    Poll::Pending => {}
                }
            }
            let still_watching = self.failure.is_some();
            if let Some((_, deadline)) = self.fetch_error.as_mut() {
                if still_watching && deadline.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.done = true;
                let error = self.fetch_error.take().map(|(error, _)| error);
                return Poll::Ready(error.map(Err));
            }
            match self.data.poll_next_unpin(cx) {
                // A fetch usually fails because the job did, and the scheduler
                // cleaned up the producer it read from. The job's error says
                // why, so give it a moment to arrive.
                Poll::Ready(Some(Err(error))) if still_watching => {
                    let deadline = Box::pin(tokio::time::sleep(self.failure_grace));
                    self.fetch_error = Some((error, deadline));
                }
                Poll::Ready(Some(Err(error))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                other => return other,
            }
        }
    }
}

impl Drop for PipelinedResults {
    /// A client that stops reading early wants no more of the job, and its
    /// producers would otherwise run on until they notice. Cancel it, which
    /// frees its vcores at once.
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Some(mut scheduler) = self.scheduler.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let job_id = self.job_id.clone();
            runtime.spawn(async move {
                info!(
                    "Result stream of pipelined job {job_id} dropped early; cancelling it"
                );
                cancel_job(&mut scheduler, job_id.as_str()).await;
            });
        }
    }
}

/// Follows the rest of a pushed status stream, reporting the error if the job
/// fails. Stops quietly when it succeeds or the stream ends.
fn watch_pushed_status(
    mut statuses: tonic::Streaming<GetJobStatusResult>,
) -> tokio::sync::oneshot::Receiver<String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        while let Some(Ok(item)) = statuses.next().await {
            match item.status.and_then(|status| status.status) {
                Some(job_status::Status::Failed(failed)) => {
                    let _ = tx.send(failed.error);
                    return;
                }
                Some(job_status::Status::Successful(_)) => return,
                _ => {}
            }
        }
    });
    rx
}

/// Polls a job's status until it finishes, reporting the error if it fails.
fn watch_pulled_status(
    mut scheduler: SchedulerClient,
    job_id: JobId,
) -> tokio::sync::oneshot::Receiver<String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        loop {
            if tx.is_closed() {
                return;
            }
            let status = scheduler
                .get_job_status(GetJobStatusParams {
                    job_id: job_id.clone().into(),
                })
                .await;
            match status.map(|r| r.into_inner().status.and_then(|s| s.status)) {
                Ok(Some(job_status::Status::Failed(failed))) => {
                    let _ = tx.send(failed.error);
                    return;
                }
                Ok(Some(job_status::Status::Successful(_))) | Err(_) => return,
                Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    });
    rx
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
    grpc_interceptor: Arc<BallistaGrpcMetadataInterceptor>,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    use_tls: bool,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let io_retries_times = grpc_config.io_retries_times;
    let io_retry_wait_time_ms = grpc_config.io_retry_wait_time_ms;
    let registration_timeout = pipelined_registration_timeout(&query);

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
            Some(job_status::Status::Running(RunningJob {
                pipelined_output_locations,
                ..
            })) if !pipelined_output_locations.is_empty() => {
                info!("Pipelined job {job_id} is running; streaming its results");
                let failure = watch_pulled_status(scheduler.clone(), job_id.clone());
                break pipelined_results(
                    job_id,
                    pipelined_output_locations,
                    PipelinedFetch {
                        max_message_size,
                        scheduler_url,
                        flight_proxy,
                        customize_endpoint,
                        use_tls,
                        io_retries_times,
                        io_retry_wait_time_ms,
                        registration_timeout,
                    },
                    Some(failure),
                    Some(scheduler.clone()),
                );
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
                pipelined_output_locations,
                ..
            })) if !pipelined_output_locations.is_empty() => {
                // Finished before this client saw it running: the final
                // stage still serves its results until the job is cleaned up.
                break pipelined_results(
                    job_id,
                    pipelined_output_locations,
                    PipelinedFetch {
                        max_message_size,
                        scheduler_url,
                        flight_proxy,
                        customize_endpoint,
                        use_tls,
                        io_retries_times,
                        io_retry_wait_time_ms,
                        registration_timeout,
                    },
                    None,
                    None,
                );
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

                break Ok(futures::stream::iter(streams).flatten().boxed());
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
    grpc_interceptor: Arc<BallistaGrpcMetadataInterceptor>,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    use_tls: bool,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let io_retries_times = grpc_config.io_retries_times;
    let io_retry_wait_time_ms = grpc_config.io_retry_wait_time_ms;
    let registration_timeout = pipelined_registration_timeout(&query);

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
            Some(job_status::Status::Running(RunningJob {
                pipelined_output_locations,
                ..
            })) if !pipelined_output_locations.is_empty() => {
                info!("Pipelined job {job_id} is running; streaming its results");
                let failure = watch_pushed_status(query_status_stream);
                break pipelined_results(
                    job_id,
                    pipelined_output_locations,
                    PipelinedFetch {
                        max_message_size,
                        scheduler_url,
                        flight_proxy,
                        customize_endpoint,
                        use_tls,
                        io_retries_times,
                        io_retry_wait_time_ms,
                        registration_timeout,
                    },
                    Some(failure),
                    Some(scheduler.clone()),
                );
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
                pipelined_output_locations,
                ..
            })) if !pipelined_output_locations.is_empty() => {
                // Finished before this client saw it running: the final
                // stage still serves its results until the job is cleaned up.
                break pipelined_results(
                    job_id,
                    pipelined_output_locations,
                    PipelinedFetch {
                        max_message_size,
                        scheduler_url,
                        flight_proxy,
                        customize_endpoint,
                        use_tls,
                        io_retries_times,
                        io_retry_wait_time_ms,
                        registration_timeout,
                    },
                    None,
                    None,
                );
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

                break Ok(futures::stream::iter(streams).flatten().boxed());
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

    let (client_host, client_port) =
        get_client_host_port(&metadata, &scheduler_url, &flight_proxy)?;

    let mut ballista_client = BallistaClient::try_new(
        client_host.as_str(),
        client_port,
        max_message_size,
        use_tls,
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
            Default::default(),
            None,
            false,
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
            Default::default(),
            None,
            false,
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

    fn one_row_batch() -> datafusion::arrow::record_batch::RecordBatch {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn pipelined_results_end_with_the_job_failure() {
        use super::PipelinedResults;
        use futures::StreamExt;
        let (tx, rx) = tokio::sync::oneshot::channel();
        // Results that never arrive, as when the final stage's producer died.
        let mut results = PipelinedResults::new(
            "job".into(),
            futures::stream::pending::<datafusion::error::Result<_>>().boxed(),
            Some(rx),
            None,
        );
        tx.send("task 0 of stage 1 failed".to_owned()).unwrap();
        let err = results.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("Job job failed: task 0"), "{err}");
        assert!(results.next().await.is_none());
    }

    #[tokio::test]
    async fn pipelined_results_pass_data_through_when_the_job_succeeds() {
        use super::PipelinedResults;
        use futures::{StreamExt, TryStreamExt};
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        // The watcher saw the job succeed and stopped without a failure.
        drop(tx);
        let batches = PipelinedResults::new(
            "job".into(),
            futures::stream::iter(vec![Ok(one_row_batch()), Ok(one_row_batch())]).boxed(),
            Some(rx),
            None,
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn pipelined_results_prefer_the_job_failure_to_a_fetch_error() {
        use super::PipelinedResults;
        use futures::StreamExt;
        let (tx, rx) = tokio::sync::oneshot::channel();
        // The job failed and its producers were cleaned up, so the fetch
        // fails first, with an error that does not say why.
        let mut results = PipelinedResults::new(
            "job".into(),
            futures::stream::iter(vec![Err(
                datafusion::error::DataFusionError::Execution(
                    "job job has finished on this executor".to_owned(),
                ),
            )])
            .chain(futures::stream::pending())
            .boxed(),
            Some(rx),
            None,
        );
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tx.send("task 0 of stage 1 failed: divide by zero".to_owned())
        });
        let err = results.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("divide by zero"), "{err}");
        assert!(results.next().await.is_none());
    }

    #[tokio::test]
    async fn pipelined_results_report_a_fetch_error_the_job_does_not_explain() {
        use super::PipelinedResults;
        use futures::StreamExt;
        let fetch_error = || {
            futures::stream::iter(vec![Err(
                datafusion::error::DataFusionError::Execution(
                    "connection reset".to_owned(),
                ),
            )])
            .chain(futures::stream::pending())
            .boxed()
        };

        // The watcher saw the job end without failing.
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        drop(tx);
        let mut results =
            PipelinedResults::new("job".into(), fetch_error(), Some(rx), None);
        let err = results.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("connection reset"), "{err}");

        // The watcher has not heard anything within the grace period.
        let (_tx, rx) = tokio::sync::oneshot::channel::<String>();
        let mut results =
            PipelinedResults::new("job".into(), fetch_error(), Some(rx), None);
        results.failure_grace = Duration::from_millis(20);
        let err = results.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("connection reset"), "{err}");
        assert!(results.next().await.is_none());
    }

    async fn scheduler_client(url: String) -> super::SchedulerClient {
        let channel = tonic::transport::Endpoint::from_shared(url)
            .unwrap()
            .connect()
            .await
            .unwrap();
        crate::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient::with_interceptor(
            channel,
            crate::extension::BallistaGrpcMetadataInterceptor::default(),
        )
    }

    /// A client that stops reading wants no more of the job, so dropping its
    /// result stream early cancels the job; reading it to the end does not.
    #[tokio::test]
    async fn dropping_pipelined_results_early_cancels_the_job() {
        use super::PipelinedResults;
        use futures::{StreamExt, TryStreamExt};
        let scheduler = IncompatibleScheduler::default();
        let url = scheduler.start().await;

        let finished = PipelinedResults::new(
            "finished".into(),
            futures::stream::iter(vec![Ok(one_row_batch())]).boxed(),
            None,
            Some(scheduler_client(url.clone()).await),
        );
        assert_eq!(finished.try_collect::<Vec<_>>().await.unwrap().len(), 1);

        let mut abandoned = PipelinedResults::new(
            "abandoned".into(),
            futures::stream::iter(vec![Ok(one_row_batch())])
                .chain(futures::stream::pending())
                .boxed(),
            None,
            Some(scheduler_client(url).await),
        );
        abandoned.next().await.unwrap().unwrap();
        drop(abandoned);

        let deadline = Instant::now() + Duration::from_secs(5);
        while scheduler.cancelled().is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(scheduler.cancelled(), ["abandoned"]);
    }

    #[test]
    fn pipelined_registration_timeout_comes_from_the_query_settings() {
        use super::pipelined_registration_timeout;
        let mut query = job_submission();
        assert_eq!(
            pipelined_registration_timeout(&query),
            Duration::from_secs(60)
        );
        query.settings.push(crate::serde::protobuf::KeyValuePair {
            key: crate::config::BALLISTA_PIPELINED_REGISTRATION_TIMEOUT_SECONDS
                .to_owned(),
            value: Some("7".to_owned()),
        });
        assert_eq!(
            pipelined_registration_timeout(&query),
            Duration::from_secs(7)
        );
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
