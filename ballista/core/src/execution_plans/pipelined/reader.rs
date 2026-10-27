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

use super::registry::{PipelinedRegistry, ProducerKey};
use crate::client::BallistaClient;
use crate::client_pool::BallistaClientPool;
use crate::error::BallistaError;
use crate::extension::{BallistaConfigGrpcEndpoint, SessionConfigExt};
use crate::serde::scheduler::PipelinedPartitionLocation;
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
/// [`PipelinedRegistry`]; any other is read over Arrow Flight. A broken stream
/// fails the query: there is no retry, because a stream that has started
/// cannot be resumed.
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
    pub fn try_new_broadcast(
        stage_id: usize,
        locations: Vec<PipelinedPartitionLocation>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self::new(
            stage_id,
            vec![locations],
            schema,
            Partitioning::UnknownPartitioning(1),
            true,
        ))
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
    pub fn with_partitions(&self, partitions: &[usize]) -> Result<Self> {
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
        let grpc_config = Arc::new(GrpcClientConfig::from(&ballista_config));
        let customize_endpoint = config.ballista_override_create_grpc_client_endpoint();
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let local_streams =
            MetricBuilder::new(&self.metrics).counter("local_streams", partition);
        let remote_streams =
            MetricBuilder::new(&self.metrics).counter("remote_streams", partition);

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
        for location in locations {
            let tx = builder.tx();
            let local = registry.clone().filter(|r| {
                !force_remote && r.executor_id() == location.executor_meta.id
            });
            if local.is_some() {
                local_streams.add(1);
            } else {
                remote_streams.add(1);
            }
            let grpc_config = grpc_config.clone();
            let customize_endpoint = customize_endpoint.clone();
            let client_pool = self.client_pool.clone();
            let output_rows = output_rows.clone();
            builder.spawn(async move {
                let mut stream = match local {
                    Some(registry) => {
                        let key = ProducerKey {
                            job_id: location.partition_id.job_id.to_string(),
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
                        grpc_config,
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

/// Opens a producer's partition over Arrow Flight.
async fn open_remote(
    location: &PipelinedPartitionLocation,
    registration_timeout: std::time::Duration,
    grpc_config: Arc<GrpcClientConfig>,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    client_pool: Option<Arc<dyn BallistaClientPool>>,
) -> std::result::Result<SendableRecordBatchStream, BallistaError> {
    let host = location.executor_meta.host.as_str();
    let port = location.executor_meta.port;
    match client_pool {
        Some(pool) => {
            let mut pooled = pool
                .acquire(host, port, &grpc_config, customize_endpoint)
                .await?;
            let result = pooled
                .fetch_pipelined_partition(location, registration_timeout)
                .await;
            if result.is_err() {
                pooled.discard();
            }
            result
        }
        None => {
            let mut client = BallistaClient::try_new(
                host,
                port,
                grpc_config.max_message_size,
                grpc_config.use_tls,
                customize_endpoint,
                grpc_config.io_retries_times,
                grpc_config.io_retry_wait_time_ms,
                grpc_config.initial_connection_window_size,
                grpc_config.initial_stream_window_size,
            )
            .await?;
            client
                .fetch_pipelined_partition(location, registration_timeout)
                .await
        }
    }
}
