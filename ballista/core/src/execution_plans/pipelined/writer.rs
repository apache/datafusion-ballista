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

//! [`PipelinedShuffleWriterExec`]: the root of a stage in a pipelined job.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics, StatisticsArgs, apply_expression_roots,
    statistics::ChildStats,
};
use futures::StreamExt;
use log::debug;
use tokio::task::JoinSet;

use super::registry::{PartitionSink, PipelinedRegistry, ProducerKey, ProducerOutputs};
use crate::JobId;
use crate::execution_plans::ShuffleWriter;
use crate::execution_plans::shuffle_writer::walk_child_partition_mapping;
use crate::extension::SessionConfigExt;
use crate::serde::protobuf::ShuffleWritePartition;

/// The root of a stage in a pipelined job.
///
/// Where the blocking writers materialize a stage's output to shuffle files,
/// this one routes it into bounded in-memory queues held by the executor's
/// [`PipelinedRegistry`], one per output partition. Consumer tasks stream the
/// queues over Arrow Flight while the stage runs, through
/// `PipelinedShuffleReaderExec`.
///
/// Output partitioning is either `Hash`, in which case the writer partitions
/// rows itself and coalesces each output back up to the session batch size,
/// or passthrough (`None`), in which case input partition `i` becomes the
/// global output partition it maps to — the same mapping
/// `ShuffleWriterExec` uses to name its files.
///
/// The executor drives the writer through
/// [`execute_pipelined`](Self::execute_pipelined) rather than
/// [`ExecutionPlan::execute`]: it has no output stream of its own.
#[derive(Debug, Clone)]
pub struct PipelinedShuffleWriterExec {
    job_id: JobId,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    shuffle_output_partitioning: Option<Partitioning>,
    broadcast: bool,
    task_id: usize,
    global_output_partition_ids: Vec<usize>,
    registry: Option<Arc<PipelinedRegistry>>,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl PipelinedShuffleWriterExec {
    /// Creates a writer for `stage_id` of `job_id`.
    ///
    /// `shuffle_output_partitioning` must be `Hash` when present. `broadcast`
    /// retains every output for replay, because each consumer task reads all
    /// of them.
    pub fn try_new(
        job_id: JobId,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        shuffle_output_partitioning: Option<Partitioning>,
        broadcast: bool,
    ) -> Result<Self> {
        let partitioning = match &shuffle_output_partitioning {
            Some(Partitioning::Hash(_, 0)) => {
                return Err(DataFusionError::Plan(
                    "PipelinedShuffleWriterExec requires at least one output partition"
                        .to_owned(),
                ));
            }
            Some(partitioning @ Partitioning::Hash(_, _)) => partitioning.clone(),
            Some(other) => {
                return Err(DataFusionError::Plan(format!(
                    "PipelinedShuffleWriterExec only supports Hash partitioning, got: {other:?}"
                )));
            }
            None => plan.properties().output_partitioning().clone(),
        };
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(plan.schema()),
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        let input_partitions = plan.properties().output_partitioning().partition_count();
        Ok(Self {
            job_id,
            stage_id,
            plan,
            shuffle_output_partitioning,
            broadcast,
            task_id: 0,
            global_output_partition_ids: (0..input_partitions).collect(),
            registry: None,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }

    /// Binds the writer to its task. Set by the executor.
    pub fn with_task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    /// Binds the writer to the task's global partition slice, which names
    /// passthrough outputs. Set by the executor.
    pub fn with_global_output_partition_ids(mut self, ids: Vec<usize>) -> Self {
        self.global_output_partition_ids = ids;
        self
    }

    /// The registry outputs are served from. When unset, the writer uses the
    /// one the executor attaches to the task's session configuration.
    pub fn with_registry(mut self, registry: Arc<PipelinedRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// The job this stage belongs to.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// The stage this writer is the root of.
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// `Some(Hash)` for a repartitioning stage, `None` for passthrough.
    pub fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    /// Whether every output is retained for replay to every consumer task.
    pub fn broadcast(&self) -> bool {
        self.broadcast
    }

    /// How rows reach the outputs: hashed into `n` partitions, or each local
    /// input partition passed through to the global output partition it
    /// maps to.
    fn routing(&self) -> Routing {
        if let Some(Partitioning::Hash(exprs, n)) = &self.shuffle_output_partitioning {
            return Routing::Hash(exprs.clone(), *n);
        }
        let map =
            walk_child_partition_mapping(&self.plan, &self.global_output_partition_ids);
        let inputs = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();
        Routing::Passthrough(
            (0..inputs)
                .map(|local| map.resolve(local) as usize)
                .collect(),
        )
    }

    /// The global output partitions this task serves.
    pub fn output_partition_ids(&self) -> Vec<usize> {
        self.routing().output_partition_ids()
    }

    /// Runs the task: registers its outputs, drives every input partition
    /// concurrently into them, and marks each output complete once all
    /// inputs are done.
    ///
    /// Returns per-partition row, batch and byte counts. On failure the
    /// error is recorded against the outputs, so consumers fail with it
    /// rather than seeing a truncated partition.
    pub async fn execute_pipelined(
        &self,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        let registry = self
            .registry
            .clone()
            .or_else(|| context.session_config().get_extension::<PipelinedRegistry>())
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "PipelinedShuffleWriterExec has no registry; the executor provides one"
                        .to_owned(),
                )
            })?;
        let config = context.session_config();
        let ballista_config = config.ballista_config();
        let capacity = ballista_config.pipelined_queue_capacity();
        let compression = ballista_config.shuffle_compression_codec()?;
        let batch_size = config.batch_size();
        let schema = self.plan.schema();
        let num_inputs = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();

        let routing = self.routing();
        let output_ids = routing.output_partition_ids();
        let (outputs, sinks) = ProducerOutputs::new(
            schema.clone(),
            output_ids.iter().copied(),
            self.broadcast,
            capacity,
            compression,
            ballista_config.pipelined_registration_timeout(),
        );
        let key = ProducerKey {
            job_id: self.job_id.to_string(),
            stage_id: self.stage_id,
            task_id: self.task_id,
        };
        registry.register(key.clone(), outputs.clone())?;
        debug!(
            "pipelined producer {key} serving partitions {output_ids:?} from {num_inputs} inputs"
        );

        let hashing = matches!(routing, Routing::Hash(..));
        let partitions: Arc<HashMap<usize, Output>> = Arc::new(
            sinks
                .into_iter()
                .map(|(id, sink)| {
                    let coalescer = hashing.then(|| {
                        Mutex::new(BatchCoalescer::new(schema.clone(), batch_size))
                    });
                    (id, Output::new(sink, coalescer))
                })
                .collect(),
        );
        let open = Arc::new(AtomicUsize::new(partitions.len()));

        let result = match self
            .drive_inputs(routing, num_inputs, &context, &partitions, &open)
            .await
        {
            Ok(()) => self.finish_outputs(&partitions, &open).await,
            err => err,
        };
        if let Err(e) = result {
            outputs.fail(&e);
            return Err(e);
        }

        Ok(partitions
            .iter()
            .map(|(id, output)| ShuffleWritePartition {
                partition_id: *id as u64,
                num_batches: output.batches.load(Ordering::Relaxed),
                num_rows: output.rows.load(Ordering::Relaxed),
                num_bytes: output.bytes.load(Ordering::Relaxed),
                file_id: None,
                is_sort_shuffle: false,
            })
            .collect())
    }

    async fn drive_inputs(
        &self,
        routing: Routing,
        num_inputs: usize,
        context: &Arc<TaskContext>,
        partitions: &Arc<HashMap<usize, Output>>,
        open: &Arc<AtomicUsize>,
    ) -> Result<()> {
        let mut tasks = JoinSet::new();
        for input in 0..num_inputs {
            let stream = self.plan.execute(input, context.clone())?;
            let partitions = partitions.clone();
            let open = open.clone();
            let send_wait =
                MetricBuilder::new(&self.metrics).subset_time("send_wait_time", input);
            let input_rows = MetricBuilder::new(&self.metrics).output_rows(input);
            match &routing {
                Routing::Hash(exprs, n) => {
                    let partitioner = BatchPartitioner::try_new(
                        Partitioning::Hash(exprs.clone(), *n),
                        MetricBuilder::new(&self.metrics)
                            .subset_time("repart_time", input),
                        input,
                        num_inputs,
                    )?;
                    tasks.spawn(drive_hashed(
                        stream,
                        partitioner,
                        partitions,
                        open,
                        send_wait,
                        input_rows,
                    ));
                }
                Routing::Passthrough(targets) => {
                    tasks.spawn(drive_passthrough(
                        stream,
                        targets[input],
                        partitions,
                        open,
                        send_wait,
                        input_rows,
                    ));
                }
            }
        }
        // Returning early drops `tasks`, which aborts the inputs still running.
        while let Some(joined) = tasks.join_next().await {
            joined.map_err(|e| {
                DataFusionError::Execution(format!(
                    "pipelined writer input task failed: {e}"
                ))
            })??;
        }
        Ok(())
    }

    /// Flushes the rows each hash output's coalescer still holds and sends
    /// every output its end-of-stream marker.
    ///
    /// Outputs are finished concurrently: a full queue holds back only its own
    /// partition's marker, not every partition after it.
    async fn finish_outputs(
        &self,
        partitions: &HashMap<usize, Output>,
        open: &AtomicUsize,
    ) -> Result<()> {
        let finishing = partitions.values().map(|output| async move {
            if let Some(coalescer) = &output.coalescer {
                let ready = {
                    let mut coalescer = coalescer.lock().unwrap();
                    coalescer.finish_buffered_batch()?;
                    drain(&mut coalescer)
                };
                for batch in ready {
                    output.send(batch, open).await?;
                }
            }
            output.sink.finish().await
        });
        futures::future::try_join_all(finishing).await?;
        Ok(())
    }
}

/// How a writer routes rows to its output partitions.
enum Routing {
    /// Hash every input row into one of `n` outputs.
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Send local input partition `i` whole to output `targets[i]`.
    Passthrough(Vec<usize>),
}

impl Routing {
    fn output_partition_ids(&self) -> Vec<usize> {
        match self {
            Routing::Hash(_, n) => (0..*n).collect(),
            Routing::Passthrough(targets) => {
                let mut ids = targets.clone();
                ids.sort_unstable();
                ids.dedup();
                ids
            }
        }
    }
}

/// One output partition while the task runs.
struct Output {
    sink: PartitionSink,
    /// Hash outputs only: the rows routed here, coalesced back up to the
    /// batch size before they are sent.
    coalescer: Option<Mutex<BatchCoalescer>>,
    /// Cleared once the consumer has gone away.
    open: AtomicBool,
    rows: AtomicU64,
    batches: AtomicU64,
    bytes: AtomicU64,
}

impl Output {
    fn new(sink: PartitionSink, coalescer: Option<Mutex<BatchCoalescer>>) -> Self {
        Self {
            sink,
            coalescer,
            open: AtomicBool::new(true),
            rows: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::Relaxed)
    }

    /// Sends a batch, counting it. Once the consumer is gone the partition is
    /// closed, and `open` counts down so the writer can stop reading input
    /// nobody wants. Fails if the consumer never opens the partition.
    async fn send(&self, batch: RecordBatch, open: &AtomicUsize) -> Result<()> {
        if !self.is_open() {
            return Ok(());
        }
        let rows = batch.num_rows() as u64;
        let bytes = batch.get_array_memory_size() as u64;
        if self.sink.send(batch).await? {
            self.rows.fetch_add(rows, Ordering::Relaxed);
            self.batches.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
        } else if self.open.swap(false, Ordering::Relaxed) {
            open.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

fn drain(coalescer: &mut BatchCoalescer) -> Vec<RecordBatch> {
    let mut ready = vec![];
    while let Some(batch) = coalescer.next_completed_batch() {
        ready.push(batch);
    }
    ready
}

async fn drive_hashed(
    mut stream: SendableRecordBatchStream,
    mut partitioner: BatchPartitioner,
    partitions: Arc<HashMap<usize, Output>>,
    open: Arc<AtomicUsize>,
    send_wait: metrics::Time,
    input_rows: metrics::Count,
) -> Result<()> {
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        input_rows.add(batch.num_rows());
        for piece in partitioner.partition_iter(batch)? {
            let (partition, piece) = piece?;
            let Some(output) = partitions.get(&partition) else {
                return Err(DataFusionError::Internal(format!(
                    "hash partitioning produced partition {partition}, which this task does not serve"
                )));
            };
            if !output.is_open() {
                continue;
            }
            let ready = {
                let coalescer = output
                    .coalescer
                    .as_ref()
                    .expect("hash outputs always coalesce");
                let mut coalescer = coalescer.lock().unwrap();
                coalescer.push_batch(piece)?;
                drain(&mut coalescer)
            };
            for batch in ready {
                let _timer = send_wait.timer();
                output.send(batch, &open).await?;
            }
        }
        if open.load(Ordering::Relaxed) == 0 {
            debug!("every consumer of this pipelined task has gone away; stopping");
            break;
        }
    }
    Ok(())
}

async fn drive_passthrough(
    mut stream: SendableRecordBatchStream,
    target: usize,
    partitions: Arc<HashMap<usize, Output>>,
    open: Arc<AtomicUsize>,
    send_wait: metrics::Time,
    input_rows: metrics::Count,
) -> Result<()> {
    let Some(output) = partitions.get(&target) else {
        return Err(DataFusionError::Internal(format!(
            "passthrough input mapped to partition {target}, which this task does not serve"
        )));
    };
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        input_rows.add(batch.num_rows());
        {
            let _timer = send_wait.timer();
            output.send(batch, &open).await?;
        }
        if !output.is_open() {
            break;
        }
    }
    Ok(())
}

impl DisplayAs for PipelinedShuffleWriterExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PipelinedShuffleWriterExec: ")?;
                match &self.shuffle_output_partitioning {
                    Some(partitioning) => write!(f, "partitioning={partitioning}")?,
                    None => write!(
                        f,
                        "partitioning={} (passthrough)",
                        self.properties.output_partitioning()
                    )?,
                }
                if self.broadcast {
                    write!(f, ", broadcast=true")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={}", self.properties.output_partitioning())
            }
        }
    }
}

impl ExecutionPlan for PipelinedShuffleWriterExec {
    fn name(&self) -> &str {
        "PipelinedShuffleWriterExec"
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    /// A hash writer partitions rows itself, so the hash expressions are its
    /// own. A passthrough writer owns none.
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        match &self.shuffle_output_partitioning {
            Some(Partitioning::Hash(exprs, _)) => apply_expression_roots(exprs, f),
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(input), true) = (children.pop(), children.is_empty()) else {
            return Err(DataFusionError::Plan(
                "PipelinedShuffleWriterExec expects a single child".to_owned(),
            ));
        };
        let mut writer = PipelinedShuffleWriterExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            input,
            self.shuffle_output_partitioning.clone(),
            self.broadcast,
        )?
        .with_task_id(self.task_id)
        .with_global_output_partition_ids(self.global_output_partition_ids.clone());
        if let Some(registry) = &self.registry {
            writer = writer.with_registry(registry.clone());
        }
        Ok(Arc::new(writer))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Internal(
            "PipelinedShuffleWriterExec is driven by the executor through \
             execute_pipelined, not execute"
                .to_owned(),
        ))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
}

impl ShuffleWriter for PipelinedShuffleWriterExec {
    fn job_id(&self) -> &JobId {
        &self.job_id
    }

    fn stage_id(&self) -> usize {
        self.stage_id
    }

    fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    fn input_partition_count(&self) -> usize {
        self.plan
            .properties()
            .output_partitioning()
            .partition_count()
    }

    fn clone_box(&self) -> Arc<dyn ShuffleWriter> {
        Arc::new(self.clone())
    }
}
