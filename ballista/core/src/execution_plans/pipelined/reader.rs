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

//! [`PipelinedShuffleReaderExec`]: the leaf of a pipelined stage that streams
//! its input from the producer tasks of an upstream stage while they run.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures::StreamExt;
use log::debug;

use super::registry::PipelinedRegistry;
use crate::client_pool::BallistaClientPool;
use crate::config::{BallistaConfig, HTTP2_MAX_WINDOW_BYTES};
use crate::error::BallistaError;
use crate::execution_plans::shuffle_reader::{new_ballista_client, with_retry};
use crate::extension::{BallistaConfigGrpcEndpoint, SessionConfigExt};
use crate::serde::scheduler::{PipelinedPartitionLocation, TaskKey};
use crate::utils::GrpcClientConfig;

/// The leaf of a stage in a pipelined job. Each output partition streams the
/// matching partition of every producer task of the upstream stage, while
/// those tasks run.
///
/// All producers are drained concurrently. That matters for correctness, not
/// only speed: producers write into bounded queues, so a reader that drained
/// them one at a time could leave a producer blocked on a full queue for a
/// partition whose consumer is in turn waiting on another producer.
///
/// A producer on the same executor is read straight from the executor's
/// [`PipelinedRegistry`]; any other is read over Arrow Flight, on the one
/// connection the executor's client pool keeps to that producer's executor.
/// Opening a remote stream is retried if the connection fails before the
/// producer's executor has answered. A stream that has started is not: it
/// cannot be resumed, so a broken one fails the query.
#[derive(Debug, Clone)]
pub struct PipelinedShuffleReaderExec {
    /// The producer stage.
    pub stage_id: usize,
    schema: SchemaRef,
    /// For each output partition, the producer tasks to stream it from.
    pub partition: Vec<Vec<PipelinedPartitionLocation>>,
    /// A single logical output partition that streams every partition of
    /// every producer task, as the build side of a broadcast join reads.
    pub broadcast: bool,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    registry: Option<Arc<PipelinedRegistry>>,
    client_pool: Option<Arc<dyn BallistaClientPool>>,
}

impl PipelinedShuffleReaderExec {
    /// Creates a reader whose output partition `i` streams the producer
    /// locations in `partition[i]`.
    pub fn try_new(
        stage_id: usize,
        partition: Vec<Vec<PipelinedPartitionLocation>>,
        schema: SchemaRef,
        partitioning: Partitioning,
    ) -> Result<Self> {
        if partition.len() != partitioning.partition_count() {
            return Err(DataFusionError::Internal(format!(
                "PipelinedShuffleReaderExec for stage {stage_id} has {} partitions of locations \
                 but its partitioning declares {}",
                partition.len(),
                partitioning.partition_count()
            )));
        }
        Ok(Self::new(stage_id, partition, schema, partitioning, false))
    }

    /// Creates a broadcast reader: one output partition that streams every
    /// location.
    pub fn new_broadcast(
        stage_id: usize,
        locations: Vec<PipelinedPartitionLocation>,
        schema: SchemaRef,
    ) -> Self {
        Self::new(
            stage_id,
            vec![locations],
            schema,
            Partitioning::UnknownPartitioning(1),
            true,
        )
    }

    fn new(
        stage_id: usize,
        partition: Vec<Vec<PipelinedPartitionLocation>>,
        schema: SchemaRef,
        partitioning: Partitioning,
        broadcast: bool,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            stage_id,
            schema,
            partition,
            broadcast,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            registry: None,
            client_pool: None,
        }
    }

    /// The executor's registry, for reading co-located producers directly.
    /// When unset, the reader uses the one the executor attaches to the task's
    /// session configuration.
    pub fn with_registry(mut self, registry: Arc<PipelinedRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// The pool remote reads take their Flight clients from. Set by the
    /// executor.
    pub fn with_client_pool(mut self, client_pool: Arc<dyn BallistaClientPool>) -> Self {
        self.client_pool = Some(client_pool);
        self
    }

    /// Returns a copy that reads only `partitions`, in that order, skipping
    /// any it does not have, as the shuffle reader does. Used to restrict a
    /// stage plan to one task's slice of partitions.
    ///
    /// A broadcast reader is returned unchanged: every consumer task reads all
    /// of it, so slicing it would drop the payload.
    pub fn with_partitions(&self, partitions: &[usize]) -> Result<Self> {
        if self.broadcast {
            return Ok(self.clone());
        }
        let selected: Vec<_> = partitions
            .iter()
            .filter_map(|p| self.partition.get(*p).cloned())
            .collect();
        let partitioning = match self.properties.output_partitioning() {
            Partitioning::Hash(exprs, _) => {
                Partitioning::Hash(exprs.clone(), selected.len())
            }
            Partitioning::RoundRobinBatch(_) => {
                Partitioning::RoundRobinBatch(selected.len())
            }
            Partitioning::UnknownPartitioning(_) => {
                Partitioning::UnknownPartitioning(selected.len())
            }
            Partitioning::Range(_) => {
                return Err(DataFusionError::Internal(format!(
                    "cannot restrict a Range-partitioned PipelinedShuffleReaderExec (stage {})",
                    self.stage_id
                )));
            }
        };
        let mut reader = Self::new(
            self.stage_id,
            selected,
            self.schema.clone(),
            partitioning,
            false,
        );
        reader.registry = self.registry.clone();
        reader.client_pool = self.client_pool.clone();
        Ok(reader)
    }
}

impl DisplayAs for PipelinedShuffleReaderExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                if self.broadcast {
                    write!(
                        f,
                        "PipelinedShuffleReaderExec: upstream_stage: {}, broadcast=true, producers: {}",
                        self.stage_id,
                        self.partition.first().map(Vec::len).unwrap_or(0)
                    )
                } else {
                    write!(
                        f,
                        "PipelinedShuffleReaderExec: upstream_stage: {}, partitioning: {}",
                        self.stage_id,
                        self.properties.output_partitioning()
                    )
                }
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={}", self.properties.output_partitioning())
            }
        }
    }
}

impl ExecutionPlan for PipelinedShuffleReaderExec {
    fn name(&self) -> &str {
        "PipelinedShuffleReaderExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "PipelinedShuffleReaderExec does not take children".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partition = if self.broadcast { 0 } else { partition };
        let locations = self.partition.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "PipelinedShuffleReaderExec for stage {} has no partition {partition}",
                self.stage_id
            ))
        })?;
        let config = context.session_config();
        let ballista_config = config.ballista_config();
        let timeout = ballista_config.pipelined_registration_timeout();
        let force_remote = config.ballista_shuffle_reader_force_remote_read();
        let customize_endpoint = config.ballista_override_create_grpc_client_endpoint();
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let local_partitions =
            MetricBuilder::new(&self.metrics).counter("local_partitions", partition);
        let remote_partitions =
            MetricBuilder::new(&self.metrics).counter("remote_partitions", partition);

        // Every producer is drained at once; see the type documentation for
        // why that is required.
        let registry = self
            .registry
            .clone()
            .or_else(|| config.get_extension::<PipelinedRegistry>());
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.schema.clone(),
            locations.len().max(2),
        );
        // Remote streams go over the connection the executor's pool keeps to
        // each producer executor, with windows sized so that sharing it is
        // safe; see `shared_connection_config`.
        let grpc_config = Arc::new(shared_connection_config(
            &GrpcClientConfig::from(&ballista_config),
            &ballista_config,
        ));
        for location in locations {
            let tx = builder.tx();
            let local = registry.clone().filter(|r| {
                !force_remote && r.executor_id() == location.executor_meta.id
            });
            if local.is_some() {
                local_partitions.add(1);
            } else {
                remote_partitions.add(1);
            }
            let grpc_config = grpc_config.clone();
            let customize_endpoint = customize_endpoint.clone();
            let client_pool = self.client_pool.clone();
            let output_rows = output_rows.clone();
            builder.spawn(async move {
                let mut stream = match local {
                    Some(registry) => {
                        let key = TaskKey {
                            job_id: location.partition_id.job_id.clone(),
                            stage_id: location.partition_id.stage_id,
                            task_id: location.task_id,
                        };
                        registry
                            .open(&key, location.partition_id.partition_id, timeout)
                            .await?
                            .stream
                    }
                    None => open_remote(
                        &location,
                        timeout,
                        &grpc_config,
                        customize_endpoint,
                        client_pool,
                    )
                    .await
                    .map_err(BallistaError::into_datafusion)?,
                };
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    output_rows.add(batch.num_rows());
                    if tx.send(Ok(batch)).await.is_err() {
                        // The consumer stopped reading, for example because a
                        // LIMIT was satisfied. Dropping the stream tells the
                        // producer.
                        debug!(
                            "consumer of pipelined partition {:?} task {} went away",
                            location.partition_id, location.task_id
                        );
                        break;
                    }
                }
                Ok(())
            });
        }
        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        // Nothing is known about a stream that has not been produced yet.
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

/// The client configuration for the connection that the streams of every
/// task on an executor share to one producer executor.
///
/// A stream nobody is reading, such as a join's probe side while the build
/// side loads, holds up to its stream window of unread data on the
/// connection. If stalled streams could use up the connection window, they
/// would starve the streams something is waiting on, and a job whose stalled
/// streams wait on its starved ones would never finish. So the connection
/// window is the largest HTTP/2 allows, and the stream window,
/// `ballista.pipelined.stream_window_bytes`, is kept small: stalled streams
/// then hold at most one window each, and the connection has room for
/// `HTTP2_MAX_WINDOW_BYTES / stream window` of them before any starvation.
///
/// Differing from the blocking reader's configuration also keeps these
/// connections apart from the ones the pool checks out one stream at a time.
fn shared_connection_config(
    grpc_config: &GrpcClientConfig,
    ballista_config: &BallistaConfig,
) -> GrpcClientConfig {
    GrpcClientConfig {
        initial_stream_window_size: ballista_config.pipelined_stream_window_bytes(),
        initial_connection_window_size: HTTP2_MAX_WINDOW_BYTES,
        ..grpc_config.clone()
    }
}

/// Opens a producer's partition over Arrow Flight, on the shared connection
/// to its executor.
///
/// A failure of the connection before the producer's executor has answered
/// is retried on a new connection, up to `io_retries_times` times with
/// `io_retry_wait_time_ms` between attempts, as the blocking reader retries
/// its fetches. A wide job opens its streams in one burst, and a listen
/// queue that overflows under it delays some connections and, once the
/// kernel falls back to SYN cookies, resets others as they start sending. A
/// failure the executor answered is not retried: the producer may already
/// have handed the partition to the failed stream.
async fn open_remote(
    location: &PipelinedPartitionLocation,
    registration_timeout: Duration,
    grpc_config: &GrpcClientConfig,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    client_pool: Option<Arc<dyn BallistaClientPool>>,
) -> std::result::Result<SendableRecordBatchStream, BallistaError> {
    with_retry(
        grpc_config.io_retries_times,
        grpc_config.io_retry_wait_time_ms,
        |e| matches!(e, BallistaError::GrpcConnectionError(_)),
        || {
            open_remote_once(
                location,
                registration_timeout,
                grpc_config,
                customize_endpoint.clone(),
                client_pool.clone(),
            )
        },
    )
    .await
}

/// One attempt at [`open_remote`]. A shared connection that fails is
/// discarded from the pool, so the next stream to open on it, here or in any
/// other task, connects again.
async fn open_remote_once(
    location: &PipelinedPartitionLocation,
    registration_timeout: Duration,
    grpc_config: &GrpcClientConfig,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    client_pool: Option<Arc<dyn BallistaClientPool>>,
) -> std::result::Result<SendableRecordBatchStream, BallistaError> {
    let host = location.executor_meta.host.as_str();
    let port = location.executor_meta.port;
    let (mut client, shared) = match client_pool {
        Some(pool) => {
            let shared = pool
                .shared(host, port, grpc_config, customize_endpoint)
                .await?;
            (shared.client(), Some(shared))
        }
        // Without a pool, as when a custom execution engine runs the plan,
        // each stream gets a connection of its own.
        None => (
            new_ballista_client(host, port, grpc_config, customize_endpoint).await?,
            None,
        ),
    };
    client
        .fetch_pipelined_partition(location, registration_timeout)
        .await
        .inspect_err(|e| {
            if let (Some(shared), BallistaError::GrpcConnectionError(_)) = (&shared, e) {
                shared.discard();
            }
        })
}
