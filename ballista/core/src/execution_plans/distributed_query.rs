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

use async_trait::async_trait;
use crate::client::BallistaClient;
use crate::config::BallistaConfig;
use crate::extension::{BallistaConfigGrpcEndpoint, SessionConfigExt};
use crate::extension::{
    BallistaConfigGrpcEndpoint, BallistaGrpcMetadataInterceptor, SessionConfigExt,
};
use crate::serde::protobuf::get_job_status_result::FlightProxy;
use crate::serde::protobuf::{
    ExecuteQueryParams, GetJobStatusParams, GetJobStatusResult,
    KeyValuePair, PartitionLocation, execute_query_params::Query, execute_query_result,
    job_status, scheduler_grpc_client::SchedulerGrpcClient,
};
use crate::serde::protobuf::{ExecutorMetadata, SuccessfulJob};
use crate::utils::{GrpcClientConfig, create_grpc_client_endpoint};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use log::{debug, error, info};
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use url::Url;

// ── Type alias ────────────────────────────────────────────────────────────────

/// Concrete scheduler gRPC client used throughout this module.
///
/// Capturing this type alias in handler structs lets us keep trait method
/// signatures free of generic parameters (i.e. no `<T: GrpcService<...>>`
/// pollution on [`JobCompletionHandler`]).
pub(crate) type SchedulerClient =
    SchedulerGrpcClient<InterceptedService<Channel, BallistaGrpcMetadataInterceptor>>;

// ── CompletedJob ──────────────────────────────────────────────────────────────

/// All metadata available after a distributed job finishes successfully.
///
/// Every [`JobCompletionHandler`] receives this on completion and can use
/// whichever fields it needs (e.g. `FetchResultsHandler` uses
/// `partition_location`; `ExplainAnalyzeHandler` only uses `job_id`).
pub struct CompletedJob {
    pub job_id: String,
    pub partition_location: Vec<PartitionLocation>,
    pub flight_proxy: Option<FlightProxy>,
    pub queued_at: u64,
    pub started_at: u64,
    pub ended_at: u64,
    /// Wall-clock instant at which the job was submitted to the scheduler.
    pub query_start_time: Instant,
}

// ── JobCompletionHandler trait ────────────────────────────────────────────────

/// Determines what happens on the *client side* after a distributed job
/// finishes.  The scheduler is completely unaware of this logic.
///
/// Implement this trait to add new post-job behaviors.  Handlers are
/// constructed inside [`DistributedQueryExec::execute`] after the
/// [`SchedulerClient`] already exists, so they can cheaply clone and hold it
/// without generic type parameters appearing in the trait signature.
#[async_trait]
pub(crate) trait JobCompletionHandler: Send {
    async fn handle(
        self: Box<Self>,
        completed: CompletedJob,
    ) -> Result<SendableRecordBatchStream>;
}

// ── FetchResultsHandler ───────────────────────────────────────────────────────

/// Fetches result partitions from executors via Arrow Flight and streams them
/// back to the caller. This is the default handler for normal queries.
pub(crate) struct FetchResultsHandler {
    scheduler_url: String,
    schema: SchemaRef,
    max_message_size: usize,
    session_config: SessionConfig,
    metrics: Arc<ExecutionPlanMetricsSet>,
    partition: usize,
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

        // ── Timing metrics ────────────────────────────────────────────────────
        let job_execution_ms = completed.ended_at.saturating_sub(completed.started_at);
        let scheduling_ms = completed.started_at.saturating_sub(completed.queued_at);
        let total_ms = completed.query_start_time.elapsed().as_millis();

        info!(
            "Job {} finished executing in {:?}",
            completed.job_id,
            Duration::from_millis(job_execution_ms)
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

        // ── Partition streams ─────────────────────────────────────────────────
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

/// This operator sends a logical plan to a Ballista scheduler for execution and
/// polls the scheduler until the query is complete and then fetches the resulting
/// batches directly from the executors that hold the results from the final
/// query stage.

// ── JobCompletionAction (public config) ───────────────────────────────────────

/// Specifies which [`JobCompletionHandler`] to use after a distributed job
/// finishes.
///
/// Stored in [`DistributedQueryExec`] so the struct can remain `Clone` and
/// still produce the output schema before `execute()` is called.  Inside
/// `execute()` the enum is matched to construct the concrete handler.
///
/// This keeps the scheduler unaware of callback logic: it always executes a
/// plain job; the client decides how to interpret the result.
#[derive(Debug, Clone)]
pub enum JobCompletionAction {
    /// Fetch result partitions from executors and stream them back (normal query).
    FetchResults,
}


// ── DistributedQueryExec ──────────────────────────────────────────────────────

/// Sends a logical plan to a Ballista scheduler for execution, waits for
/// completion, then dispatches to a [`JobCompletionHandler`] that produces the
/// final result stream.
#[derive(Debug, Clone)]
pub struct DistributedQueryExec<T: 'static + AsLogicalPlan> {
    /// Ballista scheduler URL
    scheduler_url: String,
    /// Ballista configuration
    config: BallistaConfig,
    /// Logical plan to execute (the inner plan, without any wrapping Analyze node)
    plan: LogicalPlan,
    /// Codec for LogicalPlan extensions
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    /// Phantom data for serializable plan message
    plan_repr: PhantomData<T>,
    /// Session id
    session_id: String,
    /// Plan properties
    properties: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Which handler to invoke after the remote job completes.
    action: JobCompletionAction,
}

impl<T: 'static + AsLogicalPlan> DistributedQueryExec<T> {
    /// Creates a new distributed query execution plan.
    pub fn new(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        session_id: String,
    ) -> Self {
        let schema: SchemaRef = plan.schema().as_arrow().clone().into();
        let properties = Self::compute_properties(schema);
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            action: JobCompletionAction::FetchResults,
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
        let schema: SchemaRef = plan.schema().as_arrow().clone().into();
        let properties = Self::compute_properties(schema);
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            action: JobCompletionAction::FetchResults,
        }
    }

    /// Creates a new distributed query execution plan with a custom completion
    /// action. Use this to implement EXPLAIN ANALYZE and future callback-style
    /// patterns.
    ///
    /// For [`JobCompletionAction::ExplainAnalyze`], the output schema is the
    /// standard `(plan_type: Utf8, plan: Utf8)` table, not the inner plan's
    /// schema.
    pub fn with_action(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        session_id: String,
        action: JobCompletionAction,
    ) -> Self {
        let schema: SchemaRef = match &action {
            JobCompletionAction::FetchResults => plan.schema().as_arrow().clone().into(),
        let properties =
            Self::compute_properties(plan.schema().as_arrow().clone().into());
        };
        let properties = Self::compute_properties(schema);
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            action,
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
        match &self.action {
            JobCompletionAction::FetchResults => {
                self.plan.schema().as_arrow().clone().into()
            }
        }
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
            properties: Self::compute_properties(self.schema()),
            metrics: ExecutionPlanMetricsSet::new(),
            action: self.action.clone(),
        }))
    }

    /// Submits the logical plan to the scheduler, waits for completion, and
    /// dispatches to the appropriate [`JobCompletionHandler`].
    ///
    /// Stream type chain in the happy path:
    ///
    /// ```text
    /// once(async {
    ///     scheduler = build_scheduler_client(...)
    ///     completed = submit_and_wait(scheduler, ...)   // CompletedJob
    ///     handler   = FetchResultsHandler | ExplainAnalyzeHandler
    ///     handler.handle(completed)                     // SendableRecordBatchStream
    /// })
    /// // Stream<Item = Result<SendableRecordBatchStream, DataFusionError>>
    /// .try_flatten()
    /// // Stream<Item = Result<RecordBatch, DataFusionError>>  (inner stream's error type)
    /// // Req: DataFusionError: From<DataFusionError> ✓
    /// ```
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);

        // ── Serialize logical plan ────────────────────────────────────────────
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

        // ── Capture fields for the async block ────────────────────────────────
        let session_config = context.session_config().clone();
        let max_message_size = self.config.grpc_client_max_message_size();
        let grpc_config = GrpcClientConfig::from(&self.config);
        let scheduler_url = self.scheduler_url.clone();
        let session_id = self.session_id.clone();
        let action = self.action.clone();
        let metrics = Arc::new(self.metrics.clone());
        let schema = self.schema();
        let client_pull = session_config.ballista_config().client_pull();

        // Clone schema for the outer RecordBatchStreamAdapter; the inner clone
        // is moved into the FetchResultsHandler.
        let schema_for_adapter = schema.clone();

        // ── Core loop ─────────────────────────────────────────────────────────
        //
        // The stream type chain is documented on this method's doc comment.
        let stream = futures::stream::once(async move {
            let mut scheduler = build_scheduler_client(
                scheduler_url.clone(),
                max_message_size,
                &grpc_config,
                &session_config,
            )
            .await?;

            let completed =
                submit_and_wait(&mut scheduler, query, client_pull, &session_id).await?;

            let handler: Box<dyn JobCompletionHandler> = match action {
                JobCompletionAction::FetchResults => Box::new(FetchResultsHandler {
                    scheduler_url,
                    schema,
                    max_message_size,
                    session_config,
                    metrics,
                    partition,
                }),
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
            };

            handler.handle(completed).await
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            stream,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ── build_scheduler_client ────────────────────────────────────────────────────

/// Connects to the Ballista scheduler and returns a ready-to-use gRPC client.
///
/// Extracted from the three former functions (`execute_query_pull`,
/// `execute_query_push`, `execute_explain_analyze`) which all contained
/// identical connection-establishment code.
async fn build_scheduler_client(
    scheduler_url: String,
    max_message_size: usize,
    grpc_config: &GrpcClientConfig,
    session_config: &SessionConfig,
) -> Result<SchedulerClient> {
    let grpc_interceptor = session_config.ballista_grpc_interceptor();
    let customize_endpoint =
        session_config.ballista_override_create_grpc_client_endpoint();

    info!("Connecting to Ballista scheduler at {scheduler_url}");

    let mut endpoint = create_grpc_client_endpoint(scheduler_url, Some(grpc_config))
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

    Ok(
        SchedulerGrpcClient::with_interceptor(connection, grpc_interceptor.as_ref().clone())
            .max_encoding_message_size(max_message_size)
            .max_decoding_message_size(max_message_size),
    )
}

// ── submit_and_wait ───────────────────────────────────────────────────────────

/// Submits a query to the scheduler and blocks until the job reaches a
/// terminal state, returning [`CompletedJob`] on success.
///
/// Supports both *pull* mode (client polls `GetJobStatus` in a loop) and
/// *push* mode (server streams `GetJobStatusResult` messages).
///
/// Extracts the duplicated submit+poll logic that previously appeared
/// separately in `execute_query_pull`, `execute_query_push`, and
/// `execute_explain_analyze`.
async fn submit_and_wait(
    scheduler: &mut SchedulerClient,
    query: ExecuteQueryParams,
    client_pull: bool,
    session_id: &str,
) -> Result<CompletedJob> {
    let query_start_time = Instant::now();

    if client_pull {
        // ── Pull: submit then poll ────────────────────────────────────────────
        let query_result = scheduler
            .execute_query(query)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner();

        let success = match query_result.result.unwrap() {
            execute_query_result::Result::Success(s) => s,
            execute_query_result::Result::Failure(f) => {
                return Err(DataFusionError::Execution(format!(
                    "Fail to execute query due to {f:?}"
                )));
            }
        };

        assert_eq!(
            session_id, success.session_id,
            "Session id inconsistent between Client and Server side in DistributedQueryExec."
        );

        let job_id = success.job_id;
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
            let has_status_change = prev_status != status;
            let wait_future = tokio::time::sleep(Duration::from_millis(50));

            match status {
                None => {
                    if has_status_change {
                        info!("Job {job_id} is in initialization...");
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
                    return Err(DataFusionError::Execution(msg));
                }
                Some(job_status::Status::Successful(SuccessfulJob {
                    queued_at,
                    started_at,
                    ended_at,
                    partition_location,
                    ..
                })) => {
                    return Ok(CompletedJob {
                        job_id,
                        partition_location,
                        flight_proxy,
                        queued_at,
                        started_at,
                        ended_at,
                        query_start_time,
                    });
                }
            }
        }
    } else {
        // ── Push: server-streams status updates ───────────────────────────────
        let mut stream = scheduler
            .execute_query_push(query)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner();

        let mut prev_status: Option<job_status::Status> = None;

        loop {
            let item = stream
                .next()
                .await
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Stream closed without job completing".to_string(),
                    )
                })?
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let GetJobStatusResult {
                status,
                flight_proxy,
            } = item;
            let job_id = status
                .as_ref()
                .map(|s| s.job_id.to_owned())
                .unwrap_or_else(|| "unknown_job_id".to_string());
            let status = status.and_then(|s| s.status);
            let has_status_change = prev_status != status;

            match status {
                None => {
                    if has_status_change {
                        info!("Job {job_id} is in initialization...");
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
                    return Err(DataFusionError::Execution(msg));
                }
                Some(job_status::Status::Successful(SuccessfulJob {
                    queued_at,
                    started_at,
                    ended_at,
                    partition_location,
                    ..
                })) => {
                    return Ok(CompletedJob {
                        job_id,
                        partition_location,
                        flight_proxy,
                        queued_at,
                        started_at,
                        ended_at,
                        query_start_time,
                    });
                }
            }
        }
    }
}

// ── Internal utilities (unchanged) ────────────────────────────────────────────

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
