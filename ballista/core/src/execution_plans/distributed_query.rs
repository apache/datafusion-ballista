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
use crate::execution_plans::job_callback_handler::{
    FetchResultsHandler, JobCompletionAction, JobCompletionHandler,
};
use crate::extension::SessionConfigExt;
use crate::serde::protobuf::SuccessfulJob;
use crate::serde::protobuf::get_job_status_result::FlightProxy;
use crate::serde::protobuf::{
    ExecuteQueryParams, GetJobStatusParams, GetJobStatusResult, KeyValuePair,
    PartitionLocation, execute_query_params::Query, execute_query_result, job_status,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use crate::utils::{GrpcClientConfig, create_grpc_client_endpoint};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::SessionConfig;
use datafusion_proto::logical_plan::{
    AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use futures::{StreamExt, TryStreamExt};
use log::{debug, error, info};
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// All metadata available after a distributed job finishes successfully.
pub struct CompletedJob {
    /// Scheduler-assigned job identifier.
    pub job_id: String,
    /// Locations of all output partitions produced by the job.
    pub partition_location: Vec<PartitionLocation>,
    /// Optional Flight proxy routing override returned by the scheduler.
    pub flight_proxy: Option<FlightProxy>,
    /// Epoch-ms timestamp at which the job entered the scheduler queue.
    pub queued_at: u64,
    /// Epoch-ms timestamp at which the job began executing on executors.
    pub started_at: u64,
    /// Epoch-ms timestamp at which the job finished executing.
    pub ended_at: u64,
    /// Wall-clock instant at which the job was submitted to the scheduler.
    pub query_start_time: Instant,
}

/// This operator sends a logical plan to a Ballista scheduler for execution and
/// polls the scheduler until the query is complete, then dispatches to a
/// [`JobCompletionHandler`] that produces the final result stream.
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
            action: JobCompletionAction::FetchResults,
        }
    }

    /// Creates a new distributed query execution plan with a custom completion
    /// action
    pub fn with_action(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        session_id: String,
        action: JobCompletionAction,
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
            action: self.action.clone(),
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

        let session_config = context.session_config().clone();
        let max_message_size = self.config.grpc_client_max_message_size();
        let grpc_config = GrpcClientConfig::from(&self.config);
        let scheduler_url = self.scheduler_url.clone();
        let session_id = self.session_id.clone();
        let action = self.action.clone();
        let metrics = Arc::new(self.metrics.clone());
        let schema = self.schema();
        let client_pull = session_config.ballista_config().client_pull();
        let schema_for_adapter = schema.clone();

        // Wait for the remote job to complete (pull or push), yielding a CompletedJob receipt
        let stream = futures::stream::once(async move {
            let completed = if client_pull {
                execute_query_pull(
                    scheduler_url.clone(),
                    session_id,
                    query,
                    max_message_size,
                    grpc_config,
                    session_config.clone(),
                )
                .await?
            } else {
                execute_query_push(
                    scheduler_url.clone(),
                    query,
                    max_message_size,
                    grpc_config,
                    session_config.clone(),
                )
                .await?
            };

            // Construct the appropriate JobCompletionHandler based on action
            let handler: Box<dyn JobCompletionHandler> = match action {
                JobCompletionAction::FetchResults => Box::new(FetchResultsHandler {
                    scheduler_url,
                    schema,
                    max_message_size,
                    session_config,
                    metrics,
                    partition,
                }),
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
        // This execution plan sends the logical plan to the scheduler without
        // performing the node by node conversion to a full physical plan.
        // This implies that we cannot infer the statistics at this stage.
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Client periodically polls the scheduler to check job status.
/// Returns a [`CompletedJob`] receipt once the job reaches a terminal state.
#[allow(clippy::too_many_arguments)]
async fn execute_query_pull(
    scheduler_url: String,
    session_id: String,
    query: ExecuteQueryParams,
    max_message_size: usize,
    grpc_config: GrpcClientConfig,
    session_config: SessionConfig,
) -> Result<CompletedJob> {
    let grpc_interceptor = session_config.ballista_grpc_interceptor();
    let customize_endpoint =
        session_config.ballista_override_create_grpc_client_endpoint();

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
                break Ok(CompletedJob {
                    job_id,
                    partition_location,
                    flight_proxy,
                    queued_at,
                    started_at,
                    ended_at,
                    query_start_time,
                });
            }
        };
    }
}

/// After job is scheduled, client waits for job updates streamed from server.
/// Returns a [`CompletedJob`] receipt once the job reaches a terminal state.
#[allow(clippy::too_many_arguments)]
async fn execute_query_push(
    scheduler_url: String,
    query: ExecuteQueryParams,
    max_message_size: usize,
    grpc_config: GrpcClientConfig,
    session_config: SessionConfig,
) -> Result<CompletedJob> {
    let grpc_interceptor = session_config.ballista_grpc_interceptor();
    let customize_endpoint =
        session_config.ballista_override_create_grpc_client_endpoint();

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

    let mut query_status_stream = scheduler
        .execute_query_push(query)
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
        .into_inner();

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
        let job_id = status
            .as_ref()
            .map(|s| s.job_id.to_owned())
            .unwrap_or("unknown_job_id".to_string()); // should not happen
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
                break Ok(CompletedJob {
                    job_id,
                    partition_location,
                    flight_proxy,
                    queued_at,
                    started_at,
                    ended_at,
                    query_start_time,
                });
            }
        };
    }
}
