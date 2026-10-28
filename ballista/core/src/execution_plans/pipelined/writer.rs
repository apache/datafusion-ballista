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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

use super::registry::{PartitionSink, PipelinedRegistry, ProducerOutputs};
use crate::JobId;
use crate::execution_plans::ShuffleWriter;
use crate::execution_plans::shuffle_writer::walk_child_partition_mapping;
use crate::extension::SessionConfigExt;
use crate::serde::protobuf::ShuffleWritePartition;
use crate::serde::scheduler::TaskKey;

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
        if let Some(partitioning @ Partitioning::Hash(..)) =
            &self.shuffle_output_partitioning
        {
            return Routing::Hash(partitioning.clone());
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
        let registry = context
            .session_config()
            .get_extension::<PipelinedRegistry>()
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
        let (producer, sinks) = ProducerOutputs::new(
            schema.clone(),
            output_ids.iter().copied(),
            self.broadcast,
            capacity,
            compression,
            ballista_config.pipelined_registration_timeout(),
        );
        let key = TaskKey {
            job_id: self.job_id.clone(),
            stage_id: self.stage_id,
            task_id: self.task_id,
        };
        registry.register(key.clone(), producer.clone())?;
        debug!(
            "pipelined producer {key} serving partitions {output_ids:?} from {num_inputs} inputs"
        );

        let coalesce =
            matches!(routing, Routing::Hash(_)).then_some((schema, batch_size));
        let outputs = Arc::new(Outputs::new(output_ids, sinks, coalesce));

        let result = match self
            .drive_inputs(routing, num_inputs, &context, &outputs)
            .await
        {
            Ok(()) => outputs.finish().await,
            err => err,
        };
        if let Err(e) = result {
            producer.fail(&e);
            return Err(e);
        }

        Ok(outputs
            .slots
            .iter()
            .map(|output| ShuffleWritePartition {
                partition_id: output.id as u64,
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
        outputs: &Arc<Outputs>,
    ) -> Result<()> {
        let mut tasks = JoinSet::new();
        for input in 0..num_inputs {
            let stream = self.plan.execute(input, context.clone())?;
            let outputs = outputs.clone();
            let write_time =
                MetricBuilder::new(&self.metrics).subset_time("write_time", input);
            let input_rows = MetricBuilder::new(&self.metrics).output_rows(input);
            match &routing {
                Routing::Hash(partitioning) => {
                    let partitioner = BatchPartitioner::try_new(
                        partitioning.clone(),
                        MetricBuilder::new(&self.metrics)
                            .subset_time("repart_time", input),
                        input,
                        num_inputs,
                    )?;
                    tasks.spawn(drive_hashed(
                        stream,
                        partitioner,
                        outputs,
                        write_time,
                        input_rows,
                    ));
                }
                Routing::Passthrough(targets) => {
                    let slot = outputs.slot_of(targets[input])?;
                    tasks.spawn(drive_passthrough(
                        stream, slot, outputs, write_time, input_rows,
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
}

/// How a writer routes rows to its output partitions.
enum Routing {
    /// Hash every input row into one of the partitioning's outputs.
    Hash(Partitioning),
    /// Send local input partition `i` whole to output `targets[i]`.
    Passthrough(Vec<usize>),
}

impl Routing {
    /// The global output partitions this routing serves, in ascending order.
    fn output_partition_ids(&self) -> Vec<usize> {
        match self {
            Routing::Hash(partitioning) => (0..partitioning.partition_count()).collect(),
            Routing::Passthrough(targets) => {
                let mut ids = targets.clone();
                ids.sort_unstable();
                ids.dedup();
                ids
            }
        }
    }
}

/// The task's output partitions while it runs, shared by every input it
/// drives.
struct Outputs {
    /// One per output partition, in ascending partition order. For hash
    /// routing the partitions are `0..n`, so a slot's index is its partition.
    slots: Vec<Output>,
}

impl Outputs {
    /// Pairs `ids` with their `sinks`, given in the same order. `coalesce`
    /// gives the schema and batch size when the outputs are hashed into.
    fn new(
        ids: Vec<usize>,
        sinks: Vec<PartitionSink>,
        coalesce: Option<(SchemaRef, usize)>,
    ) -> Self {
        let slots = ids
            .into_iter()
            .zip(sinks)
            .map(|(id, sink)| {
                let coalescer = coalesce.as_ref().map(|(schema, batch_size)| {
                    Mutex::new(BatchCoalescer::new(schema.clone(), *batch_size))
                });
                Output::new(id, sink, coalescer)
            })
            .collect();
        Self { slots }
    }

    /// The slot serving global output partition `id`.
    fn slot_of(&self, id: usize) -> Result<usize> {
        self.slots
            .binary_search_by_key(&id, |output| output.id)
            .map_err(|_| {
                DataFusionError::Internal(format!(
                    "input mapped to output partition {id}, which this task does not serve"
                ))
            })
    }

    /// Whether every consumer has gone away, so the writer can stop reading
    /// input nobody wants.
    fn all_closed(&self) -> bool {
        self.slots.iter().all(|output| !output.is_open())
    }

    /// Finishes every output. Outputs are finished concurrently: a full queue
    /// holds back only its own partition's marker, not every partition after
    /// it.
    async fn finish(&self) -> Result<()> {
        futures::future::try_join_all(self.slots.iter().map(Output::finish)).await?;
        Ok(())
    }
}

/// One output partition while the task runs.
struct Output {
    /// The global output partition.
    id: usize,
    sink: PartitionSink,
    /// Hash routing only: the rows routed here, coalesced back up to the
    /// batch size before they are sent. Passthrough sends input batches as
    /// they are.
    coalescer: Option<Mutex<BatchCoalescer>>,
    /// Cleared once the consumer has gone away.
    open: AtomicBool,
    rows: AtomicU64,
    batches: AtomicU64,
    bytes: AtomicU64,
}

impl Output {
    fn new(
        id: usize,
        sink: PartitionSink,
        coalescer: Option<Mutex<BatchCoalescer>>,
    ) -> Self {
        Self {
            id,
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

    /// Writes rows routed to this output, through the coalescer if it has
    /// one. Does nothing once the consumer is gone. Fails if the consumer
    /// never opens the partition.
    async fn write(&self, batch: RecordBatch) -> Result<()> {
        if !self.is_open() {
            return Ok(());
        }
        match &self.coalescer {
            Some(coalescer) => {
                let ready = {
                    let mut coalescer = coalescer.lock().unwrap();
                    coalescer.push_batch(batch)?;
                    drain(&mut coalescer)
                };
                for batch in ready {
                    self.send(batch).await?;
                }
                Ok(())
            }
            None => self.send(batch).await,
        }
    }

    /// Flushes the rows the coalescer still holds and sends the end-of-stream
    /// marker.
    async fn finish(&self) -> Result<()> {
        if let Some(coalescer) = &self.coalescer {
            let ready = {
                let mut coalescer = coalescer.lock().unwrap();
                coalescer.finish_buffered_batch()?;
                drain(&mut coalescer)
            };
            for batch in ready {
                self.send(batch).await?;
            }
        }
        self.sink.finish().await
    }

    /// Sends a batch to the sink, counting it, and marks the output closed
    /// once the consumer is gone.
    async fn send(&self, batch: RecordBatch) -> Result<()> {
        if !self.is_open() {
            return Ok(());
        }
        let rows = batch.num_rows() as u64;
        let bytes = batch.get_array_memory_size() as u64;
        if self.sink.send(batch).await? {
            self.rows.fetch_add(rows, Ordering::Relaxed);
            self.batches.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.open.store(false, Ordering::Relaxed);
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
    outputs: Arc<Outputs>,
    write_time: metrics::Time,
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
            let output = outputs.slots.get(partition).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "hash partitioning produced partition {partition}, which this task does not serve"
                ))
            })?;
            let _timer = write_time.timer();
            output.write(piece).await?;
        }
        if outputs.all_closed() {
            debug!("every consumer of this pipelined task has gone away; stopping");
            break;
        }
    }
    Ok(())
}

async fn drive_passthrough(
    mut stream: SendableRecordBatchStream,
    slot: usize,
    outputs: Arc<Outputs>,
    write_time: metrics::Time,
    input_rows: metrics::Count,
) -> Result<()> {
    let output = &outputs.slots[slot];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        input_rows.add(batch.num_rows());
        {
            let _timer = write_time.timer();
            output.write(batch).await?;
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
        let writer = PipelinedShuffleWriterExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            input,
            self.shuffle_output_partitioning.clone(),
            self.broadcast,
        )?
        .with_task_id(self.task_id)
        .with_global_output_partition_ids(self.global_output_partition_ids.clone());
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
