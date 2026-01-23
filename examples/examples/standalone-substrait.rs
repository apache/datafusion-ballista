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

use ballista::datafusion::common::Result;
use ballista_core::client::BallistaClient;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::execute_query_params::Query::SubstraitPlan;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{
    ExecuteQueryParams, GetJobStatusParams, GetJobStatusResult, PartitionLocation,
    SuccessfulJob, execute_query_result, job_status,
};
use ballista_core::utils::{GrpcClientConfig, create_grpc_client_connection};
use ballista_examples::test_util;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::error::ArrowError;
use datafusion::catalog::MemoryCatalogProviderList;
use datafusion::common::DataFusionError;
use datafusion::execution::{
    SendableRecordBatchStream, SessionState, SessionStateBuilder,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::serializer::serialize_bytes;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use log::{error, info};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Channel;

/// This example demonstrates executing Substrait plans against external tables
/// using datafusion-substrait as our frontend.
///
/// In a distributed setting, an external catalog would be required to resolve Substrait
/// `NamedTable` references. Substrait DDL is currently not supported by
/// [datafusion_substrait]; producing plans results in an [LogicalPlan::EmptyRelation] and
/// consuming DDL Substrait plans will result in an error.
#[tokio::main]
async fn main() -> Result<()> {
    let catalog_list = Arc::new(MemoryCatalogProviderList::new());

    let config = SessionConfig::new_with_ballista();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_catalog_list(catalog_list.clone())
        .build();
    let ctx = SessionContext::new_with_state(state.clone());

    // Use any frontend to serialize a Substrait plan
    let test_data = test_util::examples_test_data();
    let ddl_plan_bytes = serialize_bytes(
        &format!(
            "CREATE EXTERNAL TABLE IF NOT EXISTS another_data \
         STORED AS PARQUET \
         LOCATION '{}/alltypes_plain.parquet'",
            test_data
        ),
        &ctx,
    )
    .await?;
    let select_plan_bytes =
        serialize_bytes("SELECT id, string_col FROM another_data", &ctx).await?;

    let session_id = ctx.session_id();
    let scheduler_url = setup_standalone(Some(&state)).await?;
    let client =
        SubstraitSchedulerClient::new(scheduler_url, session_id.to_string()).await?;

    client.execute_query(ddl_plan_bytes).await?;
    let mut stream = client.execute_query(select_plan_bytes).await?;

    let mut batch_count = 0;
    let mut total_rows = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        batch_count += 1;
        total_rows += batch.num_rows();

        println!("Batch {}: {} rows", batch_count, batch.num_rows());
        println!("{:?}", batch);
    }
    println!("---------");
    println!("Query executed successfully!");
    println!("Total batches: {}, Total rows: {}", batch_count, total_rows);

    Ok(())
}

/// Wrapper class to simplify execution of Substrait plans.
pub struct SubstraitSchedulerClient {
    scheduler_url: String,
    session_id: String,
    grpc_config: GrpcClientConfig,
    max_message_size: usize,
    use_flight_transport: bool,
}

impl SubstraitSchedulerClient {
    /// Creates a new [SubstraitSchedulerClient] with default configuration.
    ///
    /// # Example
    /// ```no_run
    /// use ballista::extension::SubstraitSchedulerClient;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let client = SubstraitSchedulerClient::new(
    ///     "http://localhost:50050".to_string(),
    ///     "session-123".to_string()
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            scheduler_url,
            session_id,
            grpc_config: GrpcClientConfig::default(),
            max_message_size: 16 * 1024 * 1024, // 16MB
            use_flight_transport: false,
        })
    }

    /// Creates a builder for custom configuration.
    ///
    /// # Example
    /// ```no_run
    /// use ballista::extension::SubstraitSchedulerClient;
    /// use ballista_core::utils::GrpcClientConfig;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let custom_config = GrpcClientConfig {
    ///     connect_timeout_seconds: 60,
    ///     timeout_seconds: 120,
    ///     ..Default::default()
    /// };
    ///
    /// let client = SubstraitSchedulerClient::builder(
    ///     "http://localhost:50050".to_string(),
    ///     "session-123".to_string()
    /// )
    /// .with_grpc_config(custom_config)
    /// .with_max_message_size(32 * 1024 * 1024)
    /// .build()
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(
        scheduler_url: String,
        session_id: String,
    ) -> SubstraitSchedulerClientBuilder {
        SubstraitSchedulerClientBuilder::new(scheduler_url, session_id)
    }

    /// Executes a Substrait query plan and returns a stream of results.
    ///
    /// This method:
    /// 1. Submits the Substrait plan to the scheduler
    /// 2. Polls until the job completes
    /// 3. Fetches result partitions from executors
    /// 4. Returns a lazy stream of RecordBatches
    ///
    /// # Arguments
    /// * `plan` - Serialized Substrait plan bytes
    ///
    /// # Returns
    /// A `SendableRecordBatchStream` that lazily fetches and yields result batches.
    ///
    /// # Example
    /// ```no_run
    /// use ballista::extension::SubstraitSchedulerClient;
    /// use futures::StreamExt;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let client = SubstraitSchedulerClient::new(
    ///     "http://localhost:50050".to_string(),
    ///     "session-123".to_string()
    /// ).await?;
    ///
    /// let substrait_bytes = vec![]; // Your Substrait plan
    /// let mut stream = client.execute_query(substrait_bytes).await?;
    ///
    /// while let Some(batch) = stream.next().await {
    ///     let batch = batch?;
    ///     println!("Got {} rows", batch.num_rows());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_query(
        &self,
        plan: Vec<u8>,
    ) -> Result<SendableRecordBatchStream> {
        let query_start_time = Instant::now();

        let connection =
            create_grpc_client_connection(self.scheduler_url.clone(), &self.grpc_config)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to connect to scheduler: {e:?}"
                    ))
                })?;

        let mut scheduler = SchedulerGrpcClient::new(connection)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        let execute_query_params = ExecuteQueryParams {
            session_id: self.session_id.clone(),
            settings: vec![],
            operation_id: uuid::Uuid::now_v7().to_string(),
            query: Some(SubstraitPlan(plan)),
        };

        let response = scheduler
            .execute_query(execute_query_params)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to execute query: {e:?}"))
            })?
            .into_inner();

        let query_result = match response.result {
            Some(execute_query_result::Result::Success(success)) => success,
            Some(execute_query_result::Result::Failure(failure)) => {
                return Err(DataFusionError::Execution(format!(
                    "Query execution failed: {:?}",
                    failure
                )));
            }
            None => {
                return Err(DataFusionError::Execution(
                    "No result received from scheduler".to_string(),
                ));
            }
        };

        if query_result.session_id != self.session_id {
            return Err(DataFusionError::Execution(format!(
                "Session ID mismatch: expected {}, got {}",
                self.session_id, query_result.session_id
            )));
        }

        let successful_job = Self::poll_job(
            &mut scheduler,
            query_result.job_id,
            None,
            0,
            query_start_time,
        )
        .await?;

        let partition_stream = Self::fetch_partitions(
            successful_job.partition_location,
            self.max_message_size,
            self.use_flight_transport,
        )
        .await?;

        Ok(Box::pin(SubstraitResultStream::new(partition_stream)))
    }

    async fn poll_job(
        scheduler: &mut SchedulerGrpcClient<Channel>,
        job_id: String,
        metrics: Option<Arc<ExecutionPlanMetricsSet>>,
        partition: usize,
        query_start_time: Instant,
    ) -> Result<SuccessfulJob> {
        let mut prev_status: Option<job_status::Status> = None;

        loop {
            let GetJobStatusResult { status, .. } = scheduler
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
                Some(job_status::Status::Successful(successful_job)) => {
                    // Calculate job execution time (server-side execution)
                    let job_execution_ms = successful_job
                        .ended_at
                        .saturating_sub(successful_job.started_at);
                    let duration = Duration::from_millis(job_execution_ms);

                    info!("Job {job_id} finished executing in {duration:?}");

                    // Calculate scheduling time (server-side queue time)
                    // This includes network latency and actual queue time
                    let scheduling_ms = successful_job
                        .started_at
                        .saturating_sub(successful_job.queued_at);

                    // Calculate total query time (end-to-end from client perspective)
                    let total_elapsed = query_start_time.elapsed();
                    let total_ms = total_elapsed.as_millis();

                    // Set timing metrics if provided
                    if let Some(ref metrics) = metrics {
                        let metric_job_execution = MetricBuilder::new(metrics)
                            .gauge("job_execution_time_ms", partition);
                        metric_job_execution.set(job_execution_ms as usize);

                        let metric_scheduling = MetricBuilder::new(metrics)
                            .gauge("job_scheduling_in_ms", partition);
                        metric_scheduling.set(scheduling_ms as usize);

                        let metric_total_time = MetricBuilder::new(metrics)
                            .gauge("total_query_time_ms", partition);
                        metric_total_time.set(total_ms as usize);
                    }

                    break Ok(successful_job);
                }
            }
        }
    }

    async fn fetch_partitions(
        partition_locations: Vec<PartitionLocation>,
        max_message_size: usize,
        use_flight_transport: bool,
    ) -> Result<std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let streams = partition_locations.into_iter().map(move |partition| {
            let f = Self::fetch_partition_internal(
                partition,
                max_message_size,
                use_flight_transport,
            )
            .map_err(|e| ArrowError::ExternalError(Box::new(e)));

            futures::stream::once(f).try_flatten()
        });

        Ok(Box::pin(futures::stream::iter(streams).flatten()))
    }

    async fn fetch_partition_internal(
        location: PartitionLocation,
        max_message_size: usize,
        flight_transport: bool,
    ) -> Result<SendableRecordBatchStream> {
        let metadata = location.executor_meta.ok_or_else(|| {
            DataFusionError::Internal("Received empty executor metadata".to_owned())
        })?;
        let partition_id = location.partition_id.ok_or_else(|| {
            DataFusionError::Internal("Received empty partition id".to_owned())
        })?;

        let host = metadata.host.as_str();
        let port = metadata.port as u16;

        let mut ballista_client =
            BallistaClient::try_new(host, port, max_message_size, false, None)
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
}

/// Builder for [SubstraitSchedulerClient]
pub struct SubstraitSchedulerClientBuilder {
    scheduler_url: String,
    session_id: String,
    grpc_config: Option<GrpcClientConfig>,
    max_message_size: Option<usize>,
    use_flight_transport: Option<bool>,
}

impl SubstraitSchedulerClientBuilder {
    fn new(scheduler_url: String, session_id: String) -> Self {
        Self {
            scheduler_url,
            session_id,
            grpc_config: None,
            max_message_size: None,
            use_flight_transport: None,
        }
    }

    /// Overrides default gRPC configuration
    pub fn with_grpc_config(mut self, config: GrpcClientConfig) -> Self {
        self.grpc_config = Some(config);
        self
    }

    /// Sets max message size for gRPC client
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Sets usage of Flight when communicating with executor
    pub fn with_flight_transport(mut self, use_flight: bool) -> Self {
        self.use_flight_transport = Some(use_flight);
        self
    }

    /// Creates a [SubstraitSchedulerClient] instance
    pub async fn build(self) -> datafusion::error::Result<SubstraitSchedulerClient> {
        Ok(SubstraitSchedulerClient {
            scheduler_url: self.scheduler_url,
            session_id: self.session_id,
            grpc_config: self.grpc_config.unwrap_or_default(),
            max_message_size: self.max_message_size.unwrap_or(16 * 1024 * 1024),
            use_flight_transport: self.use_flight_transport.unwrap_or(false),
        })
    }
}

struct SubstraitResultStream<S> {
    inner: std::pin::Pin<Box<S>>,
    schema: Option<datafusion::arrow::datatypes::SchemaRef>,
}

impl<S> SubstraitResultStream<S> {
    fn new(inner: S) -> Self {
        Self {
            inner: Box::pin(inner),
            schema: None,
        }
    }
}

impl<S> futures::Stream for SubstraitResultStream<S>
where
    S: futures::Stream<
            Item = datafusion::error::Result<
                datafusion::arrow::record_batch::RecordBatch,
            >,
        >,
{
    type Item = datafusion::error::Result<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                if self.schema.is_none() {
                    self.schema = Some(batch.schema());
                }
                std::task::Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl<S> datafusion::physical_plan::RecordBatchStream for SubstraitResultStream<S>
where
    S: futures::Stream<
            Item = datafusion::error::Result<
                datafusion::arrow::record_batch::RecordBatch,
            >,
        > + Send,
{
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone().unwrap_or_else(|| {
            std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty())
        })
    }
}

/// Creates in-process scheduler and executor, returning the scheduler URL.
pub async fn setup_standalone(session_state: Option<&SessionState>) -> Result<String> {
    use ballista_core::{serde::BallistaCodec, utils::default_config_producer};

    let addr = match session_state {
        None => ballista_scheduler::standalone::new_standalone_scheduler()
            .await
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?,
        Some(session_state) => {
            ballista_scheduler::standalone::new_standalone_scheduler_from_state(
                session_state,
            )
            .await
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?
        }
    };
    let config = session_state
        .map(|s| s.config().clone())
        .unwrap_or_else(default_config_producer);

    let scheduler_url = format!("http://localhost:{}", addr.port());

    let scheduler = loop {
        match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
            Err(_) => {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                log::info!("Attempting to connect to in-proc scheduler...");
            }
            Ok(scheduler) => break scheduler,
        }
    };

    let concurrent_tasks = config.ballista_standalone_parallelism();

    match session_state {
        None => {
            ballista_executor::new_standalone_executor(
                scheduler,
                concurrent_tasks,
                BallistaCodec::default(),
            )
            .await
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
        }
        Some(session_state) => {
            ballista_executor::new_standalone_executor_from_state(
                scheduler,
                concurrent_tasks,
                session_state,
            )
            .await
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
        }
    }

    Ok(scheduler_url)
}
