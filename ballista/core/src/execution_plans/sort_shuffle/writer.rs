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

//! Sort-based shuffle writer execution plan.
//!
//! This execution plan writes shuffle output as a single consolidated file
//! per input partition, along with an index file mapping partition IDs to
//! byte offsets.

use std::any::Any;
use std::fs::File;
use std::future::Future;
use std::io::{BufWriter, Seek, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use super::super::shuffle_writer_trait::ShuffleWriter;
use super::buffer::BufferedBatches;
use super::config::SortShuffleConfig;
use super::index::ShuffleIndex;
use super::partitioned_batch_iterator::PartitionedBatchIterator;
use super::spill::SpillManager;
use crate::execution_plans::create_shuffle_path;
use crate::serde::protobuf::ShuffleWritePartition;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::create_hashes;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics, displayable,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use log::{debug, info};

use crate::serde::scheduler::PartitionStats;

/// Result of finalizing shuffle output: (data_path, index_path, partition_write_stats)
/// where partition_write_stats is (partition_id, num_batches, num_rows, num_bytes)
type FinalizeResult = (PathBuf, PathBuf, Vec<(usize, u64, u64, u64)>);

/// Sort-based shuffle writer that produces a single consolidated output file
/// per input partition with an index file for partition offsets.
#[derive(Debug, Clone)]
pub struct SortShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: String,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    work_dir: String,
    /// Shuffle output partitioning. `None` means the input is passed through to
    /// a single output partition without re-partitioning.
    shuffle_output_partitioning: Option<Partitioning>,
    /// Sort shuffle configuration
    config: SortShuffleConfig,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: Arc<PlanProperties>,
}

#[derive(Debug, Clone)]
struct SortShuffleWriteMetrics {
    /// Time spent writing batches to the output file
    write_time: metrics::Time,
    /// Time spent partitioning input batches
    repart_time: metrics::Time,
    /// Time spent spilling to disk
    spill_time: metrics::Time,
    /// Number of input rows
    input_rows: metrics::Count,
    /// Number of output rows
    output_rows: metrics::Count,
    /// Number of times the writer flushed buffered partitions to disk under
    /// memory pressure. One event typically writes one batch per non-empty
    /// output partition, so this is strictly less than or equal to the number
    /// of batches written into spill files.
    spill_count: metrics::Count,
    /// Bytes spilled to disk
    spill_bytes: metrics::Count,
}

impl SortShuffleWriteMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            write_time: MetricBuilder::new(metrics).subset_time("write_time", partition),
            repart_time: MetricBuilder::new(metrics)
                .subset_time("repart_time", partition),
            spill_time: MetricBuilder::new(metrics).subset_time("spill_time", partition),
            input_rows: MetricBuilder::new(metrics).counter("input_rows", partition),
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            spill_count: MetricBuilder::new(metrics).counter("spill_count", partition),
            spill_bytes: MetricBuilder::new(metrics).counter("spill_bytes", partition),
        }
    }
}

impl SortShuffleWriterExec {
    /// Create a new sort-based shuffle writer.
    pub fn try_new(
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        shuffle_output_partitioning: Option<Partitioning>,
        config: SortShuffleConfig,
    ) -> Result<Self> {
        if let Some(p) = &shuffle_output_partitioning
            && !matches!(p, Partitioning::Hash(_, _))
        {
            return Err(DataFusionError::Plan(format!(
                "SortShuffleWriterExec only supports Hash or None partitioning, got: {p:?}"
            )));
        }

        // When no shuffle partitioning is requested, mirror ShuffleWriterExec and
        // pass the input plan's partitioning through unchanged.
        let plan_partitioning = shuffle_output_partitioning
            .clone()
            .unwrap_or_else(|| plan.properties().output_partitioning().clone());
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(plan.schema()),
            plan_partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));

        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
            shuffle_output_partitioning,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Get the shuffle output partitioning, or `None` if the writer passes
    /// the input through to a single output partition.
    pub fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    /// Get the sort shuffle configuration
    pub fn config(&self) -> &SortShuffleConfig {
        &self.config
    }

    /// Get the input partition count
    pub fn input_partition_count(&self) -> usize {
        self.plan
            .properties()
            .output_partitioning()
            .partition_count()
    }

    /// Execute the sort-based shuffle write for a single input partition.
    pub fn execute_shuffle_write(
        self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> impl Future<Output = Result<Vec<ShuffleWritePartition>>> {
        let metrics = SortShuffleWriteMetrics::new(input_partition, &self.metrics);
        let config = self.config.clone();
        let plan = self.plan.clone();
        let work_dir = self.work_dir.clone();
        let job_id = self.job_id.clone();
        let stage_id = self.stage_id;
        let partitioning = self.shuffle_output_partitioning.clone();

        async move {
            let now = Instant::now();
            let mut stream = plan.execute(input_partition, context.clone())?;
            let schema = stream.schema();

            // None => single output bucket (pass-through); Hash(_, n) => n buckets.
            let num_output_partitions = match &partitioning {
                Some(Partitioning::Hash(_, n)) => *n,
                None => 1,
                Some(other) => {
                    return Err(DataFusionError::Internal(format!(
                        "Unexpected partitioning in SortShuffleWriterExec: {other:?}"
                    )));
                }
            };

            let mut spill_manager = SpillManager::new(
                &work_dir,
                &job_id,
                stage_id,
                input_partition,
                schema.clone(),
                config.compression,
            )
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            // For Hash partitioning, capture the expressions so we can compute
            // per-row partition assignments below. `None` means pass-through:
            // every row goes to bucket 0.
            let hash_exprs: Option<Vec<Arc<dyn PhysicalExpr>>> = match &partitioning {
                Some(Partitioning::Hash(exprs, _)) => Some(exprs.clone()),
                None => None,
                Some(other) => {
                    return Err(DataFusionError::Internal(format!(
                        "Unexpected partitioning in SortShuffleWriterExec: {other:?}"
                    )));
                }
            };

            let mut buffered =
                BufferedBatches::new(num_output_partitions, schema.clone());

            let mut reservation =
                MemoryConsumer::new(format!("SortShuffleWriter[{input_partition}]"))
                    .with_can_spill(true)
                    .register(&context.runtime_env().memory_pool);

            let mut hash_buffer: Vec<u64> = Vec::new();
            let mut spill_events: u64 = 0;
            // Absolute buffered-bytes counter, independent of the runtime
            // `MemoryPool`. Drives spill decisions so the writer bounds its
            // RSS even when the pool is unbounded.
            let mut buffered_bytes: usize = 0;
            let memory_limit = config.memory_limit_per_task_bytes;

            while let Some(result) = stream.next().await {
                let input_batch = result?;
                metrics.input_rows.add(input_batch.num_rows());

                // Compute partition assignment for every row. With no shuffle
                // partitioning, all rows route to bucket 0.
                let timer = metrics.repart_time.timer();
                let per_partition_rows = match &hash_exprs {
                    Some(exprs) => compute_partition_indices(
                        &input_batch,
                        exprs,
                        num_output_partitions,
                        &mut hash_buffer,
                    )?,
                    None => {
                        vec![(0..input_batch.num_rows() as u32).collect()]
                    }
                };
                timer.done();

                // Estimate memory growth: input batch + index Vec growth.
                let mut growth = input_batch.get_array_memory_size();
                let before = buffered.indices_allocated_size();
                buffered.push_batch(input_batch, &per_partition_rows);
                let after = buffered.indices_allocated_size();
                growth += after.saturating_sub(before);

                // Mirror the growth in the runtime pool reservation so the pool
                // sees this writer's memory usage. Best-effort: if the pool is
                // bounded and rejects the grow, that's fine — the absolute
                // counter below still triggers a spill.
                let _ = reservation.try_grow(growth);
                buffered_bytes = buffered_bytes.saturating_add(growth);

                if buffered_bytes >= memory_limit {
                    let spill_timer = metrics.spill_time.timer();
                    let (event_batches, event_bytes) = spill_all_partitions(
                        &mut buffered,
                        &mut spill_manager,
                        &mut reservation,
                        config.batch_size,
                    )?;
                    spill_timer.done();
                    buffered_bytes = 0;

                    if event_batches > 0 {
                        spill_events += 1;
                        info!(
                            "Sort shuffle writer for input partition {} spilled \
                             event #{}: {} batches, {} bytes \
                             (cumulative: {} events, {} batches, {} bytes)",
                            input_partition,
                            spill_events,
                            event_batches,
                            event_bytes,
                            spill_events,
                            spill_manager.total_spilled_batches(),
                            spill_manager.total_bytes_spilled(),
                        );
                    }
                }
            }

            // Finish spill writers before reading them back during finalize.
            spill_manager
                .finish_writers()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            let timer = metrics.write_time.timer();
            let (data_path, index_path, partition_stats) = finalize_output(
                &work_dir,
                &job_id,
                stage_id,
                input_partition,
                &mut buffered,
                &mut spill_manager,
                &schema,
                &config,
            )?;
            timer.done();

            metrics.spill_count.add(spill_events as usize);
            metrics
                .spill_bytes
                .add(spill_manager.total_bytes_spilled() as usize);

            let total_rows: u64 = partition_stats.iter().map(|(_, _, r, _)| *r).sum();
            metrics.output_rows.add(total_rows as usize);

            // Snapshot spill counters before cleanup (cleanup doesn't touch them
            // but we want to be explicit about ordering for the log line below).
            let total_spilled_batches = spill_manager.total_spilled_batches();
            let total_bytes_spilled = spill_manager.total_bytes_spilled();

            spill_manager
                .cleanup()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            // Reservation drops naturally; nothing left to free.
            drop(reservation);

            info!(
                "Sort shuffle write for partition {} completed in {} seconds. \
                 Output: {:?}, Index: {:?}, Spill events: {}, Spill batches: {}, \
                 Spill bytes: {}",
                input_partition,
                now.elapsed().as_secs(),
                data_path,
                index_path,
                spill_events,
                total_spilled_batches,
                total_bytes_spilled
            );

            let mut results = Vec::new();
            for (part_id, num_batches, num_rows, num_bytes) in partition_stats {
                if num_rows > 0 {
                    results.push(ShuffleWritePartition {
                        partition_id: part_id as u64,
                        num_batches,
                        num_rows,
                        num_bytes,
                        file_id: Some(input_partition as u64),
                        is_sort_shuffle: true,
                    });
                }
            }

            Ok(results)
        }
    }
}

/// Spills *all* buffered partitions: for each partition, materializes its
/// indices through `PartitionedBatchIterator` and appends each yielded batch
/// to that partition's spill file. After this call, `buffered.is_empty()` is
/// true and `reservation.size() == 0`.
///
/// Returns `(batches_written, bytes_written)` for this single spill event so
/// the caller can log per-event diagnostics. A return of `(0, 0)` means there
/// was nothing buffered and no event occurred.
fn spill_all_partitions(
    buffered: &mut BufferedBatches,
    spill_manager: &mut SpillManager,
    reservation: &mut MemoryReservation,
    batch_size: usize,
) -> Result<(u64, u64)> {
    if buffered.is_empty() {
        return Ok((0, 0));
    }
    let mut batches_written: u64 = 0;
    let mut bytes_written: u64 = 0;
    let (batches, indices) = buffered.take();
    for (partition_id, partition_indices) in indices.iter().enumerate() {
        if partition_indices.is_empty() {
            continue;
        }
        let iter = PartitionedBatchIterator::new(&batches, partition_indices, batch_size);
        for result in iter {
            let batch = result?;
            let written = spill_manager
                .spill(partition_id, &batch)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            batches_written += 1;
            bytes_written += written;
        }
    }
    reservation.free();
    Ok((batches_written, bytes_written))
}

/// Finalizes the output by writing the consolidated data file and index file.
///
/// Returns (data_path, index_path, partition_stats) where partition_stats is
/// a vector of (partition_id, num_batches, num_rows, num_bytes) tuples.
#[allow(clippy::too_many_arguments)]
fn finalize_output(
    work_dir: &str,
    job_id: &str,
    stage_id: usize,
    input_partition: usize,
    buffered: &mut BufferedBatches,
    spill_manager: &mut SpillManager,
    schema: &SchemaRef,
    config: &SortShuffleConfig,
) -> Result<FinalizeResult> {
    let num_partitions = buffered.num_partitions();
    let mut index = ShuffleIndex::new(num_partitions);
    let mut partition_stats = Vec::with_capacity(num_partitions);

    let mut output_dir = PathBuf::from(work_dir);
    output_dir.push(job_id);
    output_dir.push(format!("{stage_id}"));
    output_dir.push(format!("{input_partition}"));
    std::fs::create_dir_all(&output_dir)?;

    let data_path = output_dir.join("data.arrow");
    let index_path = output_dir.join("data.arrow.index");

    debug!("Writing consolidated shuffle output to {:?}", data_path);

    let file = File::create(&data_path)?;
    let mut output = BufWriter::new(file);

    let opts =
        IpcWriteOptions::default().try_with_compression(Some(config.compression))?;

    // Leading schema-header stream (schema message + EOS, no batches) so the
    // reader can recover the schema even when the requested partition is empty.
    {
        let mut header =
            StreamWriter::try_new_with_options(&mut output, schema, opts.clone())?;
        header.finish()?;
    }

    let (in_memory_batches, in_memory_indices) = buffered.take();

    for (partition_id, partition_indices) in in_memory_indices.iter().enumerate() {
        // Flush before reading the kernel position so the BufWriter buffer
        // is empty; std::io::copy below also writes directly to the inner
        // File and would land after any pending buffered bytes otherwise.
        output.flush()?;
        let partition_start = output.get_mut().stream_position()? as i64;
        index.set_offset(partition_id, partition_start);

        let (spill_batches, spill_rows, spill_bytes) =
            spill_manager.partition_stats(partition_id);

        if let Some(spill_path) = spill_manager.spill_path(partition_id) {
            let mut spill_file = File::open(spill_path)?;
            std::io::copy(&mut spill_file, output.get_mut())?;
        }

        let mut mem_batches: u64 = 0;
        let mut mem_rows: u64 = 0;
        let mut mem_bytes: u64 = 0;
        if !partition_indices.is_empty() {
            let iter = PartitionedBatchIterator::new(
                &in_memory_batches,
                partition_indices,
                config.batch_size,
            );
            let mut writer =
                StreamWriter::try_new_with_options(&mut output, schema, opts.clone())?;
            for result in iter {
                let batch = result?;
                mem_rows += batch.num_rows() as u64;
                mem_bytes += batch.get_array_memory_size() as u64;
                mem_batches += 1;
                writer.write(&batch)?;
            }
            writer.finish()?;
        }

        partition_stats.push((
            partition_id,
            spill_batches + mem_batches,
            spill_rows + mem_rows,
            spill_bytes + mem_bytes,
        ));
    }

    output.flush()?;
    let total = output.get_mut().stream_position()? as i64;
    index.set_total_length(total);
    index
        .write_to_file(&index_path)
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    Ok((data_path, index_path, partition_stats))
}

impl DisplayAs for SortShuffleWriterExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let partitioning = self
            .shuffle_output_partitioning
            .as_ref()
            .map(|p| p.to_string())
            .unwrap_or_else(|| "None".to_string());
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SortShuffleWriterExec: partitioning={partitioning}")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={partitioning}")
            }
        }
    }
}

impl ExecutionPlan for SortShuffleWriterExec {
    fn name(&self) -> &str {
        "SortShuffleWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            let input = children.pop().ok_or_else(|| {
                DataFusionError::Plan(
                    "SortShuffleWriterExec expects single child".to_owned(),
                )
            })?;

            Ok(Arc::new(SortShuffleWriterExec::try_new(
                self.job_id.clone(),
                self.stage_id,
                input,
                self.work_dir.clone(),
                self.shuffle_output_partitioning.clone(),
                self.config.clone(),
            )?))
        } else {
            Err(DataFusionError::Plan(
                "SortShuffleWriterExec expects single child".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = result_schema();

        let schema_captured = schema.clone();
        let job_id = self.job_id.to_string();
        let work_dir = self.work_dir.to_string();
        let stage_id = self.stage_id;
        let fut_stream = self
            .clone()
            .execute_shuffle_write(partition, context)
            .and_then(move |part_loc| async move {
                // Build metadata result batch
                let num_writers = part_loc.len();
                let mut partition_builder = UInt32Builder::with_capacity(num_writers);
                let mut path_builder =
                    StringBuilder::with_capacity(num_writers, num_writers * 100);
                let mut num_rows_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_batches_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_bytes_builder = UInt64Builder::with_capacity(num_writers);

                for loc in &part_loc {
                    path_builder.append_value(
                        create_shuffle_path(
                            &work_dir,
                            &job_id,
                            stage_id,
                            partition,
                            loc.file_id,
                            true,
                        )?
                        .to_string_lossy(),
                    );

                    partition_builder.append_value(loc.partition_id as u32);
                    num_rows_builder.append_value(loc.num_rows);
                    num_batches_builder.append_value(loc.num_batches);
                    num_bytes_builder.append_value(loc.num_bytes);
                }

                // Build arrays
                let partition_num: ArrayRef = Arc::new(partition_builder.finish());
                let path: ArrayRef = Arc::new(path_builder.finish());
                let field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
                    Box::new(num_rows_builder),
                    Box::new(num_batches_builder),
                    Box::new(num_bytes_builder),
                ];
                let mut stats_builder = StructBuilder::new(
                    PartitionStats::default().arrow_struct_fields(),
                    field_builders,
                );
                for _ in 0..num_writers {
                    stats_builder.append(true);
                }
                let stats = Arc::new(stats_builder.finish());

                // Build result batch containing metadata
                let batch = RecordBatch::try_new(
                    schema_captured.clone(),
                    vec![partition_num, path, stats],
                )?;

                debug!("SORT SHUFFLE RESULTS METADATA:\n{batch:?}");

                MemoryStream::try_new(vec![batch], schema_captured, None)
            })
            .map_err(|e| ArrowError::ExternalError(Box::new(e)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut_stream).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.plan.partition_statistics(partition)
    }
}

impl ShuffleWriter for SortShuffleWriterExec {
    fn job_id(&self) -> &str {
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

fn result_schema() -> SchemaRef {
    let stats = PartitionStats::default();
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt32, false),
        Field::new("path", DataType::Utf8, false),
        stats.arrow_struct_repr(),
    ]))
}

impl std::fmt::Display for SortShuffleWriterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let printable_plan = displayable(self.plan.as_ref())
            .set_show_statistics(true)
            .indent(false);
        write!(
            f,
            "SortShuffleWriterExec: job={} stage={} work_dir={} partitioning={:?} plan: \n {}",
            self.job_id,
            self.stage_id,
            self.work_dir,
            self.shuffle_output_partitioning,
            printable_plan
        )
    }
}

/// Computes per-row output partition assignments for hash partitioning.
///
/// Returns a `Vec` of length `num_partitions`, where entry `p` lists the
/// row indices in `batch` that hash to partition `p`. Hash semantics are
/// byte-identical to `datafusion::physical_plan::repartition::BatchPartitioner::Hash`.
///
/// `hash_buffer` is reused across calls to amortize allocations; it is
/// cleared and resized internally.
fn compute_partition_indices(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    hash_buffer: &mut Vec<u64>,
) -> Result<Vec<Vec<u32>>> {
    let arrays = evaluate_expressions_to_arrays(exprs, batch)?;
    hash_buffer.clear();
    hash_buffer.resize(batch.num_rows(), 0);
    create_hashes(
        &arrays,
        REPARTITION_RANDOM_STATE.random_state(),
        hash_buffer,
    )?;

    let mut out: Vec<Vec<u32>> = (0..num_partitions).map(|_| Vec::new()).collect();
    for (row, &h) in hash_buffer.iter().enumerate() {
        out[(h % num_partitions as u64) as usize].push(row as u32);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, UInt32Array};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    fn create_test_input() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(3)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];

        let memory_data_source =
            Arc::new(MemorySourceConfig::try_new(&partitions, schema, None)?);

        Ok(Arc::new(DataSourceExec::new(memory_data_source)))
    }

    #[test]
    fn compute_partition_indices_distributes_rows_by_hash() {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..1000_i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..1000).map(|i| format!("v{i}")).collect::<Vec<String>>(),
                )),
            ],
        )
        .unwrap();

        let exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
            vec![Arc::new(
                datafusion::physical_expr::expressions::Column::new("k", 0),
            )];

        let mut hash_buffer: Vec<u64> = Vec::new();
        let result =
            compute_partition_indices(&batch, &exprs, 8, &mut hash_buffer).unwrap();

        // 8 partition slots
        assert_eq!(result.len(), 8);
        // Total row count is preserved
        let total: usize = result.iter().map(|v| v.len()).sum();
        assert_eq!(total, 1000);
        // No row index appears in two partitions
        let mut seen = vec![false; 1000];
        for indices in &result {
            for &row in indices {
                assert!(!seen[row as usize], "row {row} appeared twice");
                seen[row as usize] = true;
            }
        }
        // Distribution is non-trivial — at least 2 distinct partitions used
        let used = result.iter().filter(|v| !v.is_empty()).count();
        assert!(used >= 2, "expected hash to use multiple partitions");
    }

    #[tokio::test]
    async fn test_sort_shuffle_writer() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_test_input()?));
        let work_dir = TempDir::new()?;

        let config = SortShuffleConfig::default();

        let writer = SortShuffleWriterExec::try_new(
            "job1".to_string(),
            1,
            input_plan,
            work_dir.path().to_str().unwrap().to_string(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
            config,
        )?;

        let mut stream = writer.execute(0, task_ctx)?;
        let batches: Vec<RecordBatch> = stream
            .by_ref()
            .try_collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 3);

        // Verify output files exist
        let output_dir = work_dir.path().join("job1").join("1").join("0");
        assert!(output_dir.join("data.arrow").exists());
        assert!(output_dir.join("data.arrow.index").exists());

        Ok(())
    }

    /// Shared helper for round-trip tests. Builds `num_batches` batches of
    /// `rows_per_batch` rows each with schema `(k: Int64, v: Int64)`, writes
    /// them through `SortShuffleWriterExec` into `num_partitions` output
    /// partitions with a per-task buffered-bytes budget of
    /// `sort_shuffle_memory_limit_bytes`, reads every partition back via
    /// `stream_sort_shuffle_partition`, and asserts:
    ///   - Every input key `0..total_rows` is present exactly once in the union
    ///     of partition outputs.
    ///   - `spill_count > 0` iff `expect_spills` is `true`.
    ///   - The memory pool has `reserved == 0` after completion.
    async fn run_round_trip(
        num_batches: usize,
        rows_per_batch: usize,
        num_partitions: usize,
        sort_shuffle_memory_limit_bytes: usize,
        expect_spills: bool,
    ) -> Result<()> {
        use super::super::reader::stream_sort_shuffle_partition;
        use datafusion::arrow::array::Int64Array;
        use datafusion::execution::memory_pool::FairSpillPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use std::collections::HashSet;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));

        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|b| {
                let start = (b * rows_per_batch) as i64;
                let keys: Vec<i64> = (start..start + rows_per_batch as i64).collect();
                let values: Vec<i64> = keys.iter().map(|k| k * 2).collect();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(keys)),
                        Arc::new(Int64Array::from(values)),
                    ],
                )
                .unwrap()
            })
            .collect();
        let partitions = vec![batches];
        let memory_data_source = Arc::new(MemorySourceConfig::try_new(
            &partitions,
            schema.clone(),
            None,
        )?);
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(memory_data_source));

        // The runtime pool is sized generously: spill decisions are now driven
        // by `SortShuffleConfig::memory_limit_per_task_bytes`, but we keep a
        // bounded pool so the post-test `pool.reserved() == 0` assertion still
        // checks that the writer releases its `MemoryReservation`.
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(64 * 1024 * 1024)))
                .build()?,
        );
        let session_ctx = SessionContext::new_with_config_rt(
            datafusion::execution::config::SessionConfig::new(),
            runtime_env,
        );
        let task_ctx = session_ctx.task_ctx();

        let work_dir = TempDir::new()?;

        let writer = SortShuffleWriterExec::try_new(
            "round_trip_job".to_string(),
            1,
            input,
            work_dir.path().to_str().unwrap().to_string(),
            Some(Partitioning::Hash(
                vec![Arc::new(Column::new("k", 0))],
                num_partitions,
            )),
            SortShuffleConfig::default()
                .with_memory_limit_per_task_bytes(sort_shuffle_memory_limit_bytes),
        )?;

        let mut stream = writer.execute(0, task_ctx)?;
        let summary_batches: Vec<RecordBatch> = stream
            .by_ref()
            .try_collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
        assert_eq!(summary_batches.len(), 1, "expected one summary batch");

        let metrics = writer.metrics().expect("metrics after execute");
        let spill_count = metrics
            .iter()
            .find(|m| m.value().name() == "spill_count")
            .map(|m| m.value().as_usize())
            .unwrap_or(0);
        if expect_spills {
            assert!(
                spill_count > 0,
                "expected spilling under tight per-task buffer budget, got spill_count={spill_count}"
            );
        } else {
            assert_eq!(
                spill_count, 0,
                "expected no spills with generous per-task buffer budget, got spill_count={spill_count}"
            );
        }

        let data_path = work_dir
            .path()
            .join("round_trip_job")
            .join("1")
            .join("0")
            .join("data.arrow");
        let index_path = data_path.with_extension("arrow.index");
        let mut seen: HashSet<i64> = HashSet::new();
        let total_rows = num_batches * rows_per_batch;
        for partition_id in 0..num_partitions {
            let mut s =
                stream_sort_shuffle_partition(&data_path, &index_path, partition_id)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            while let Some(batch_result) = s.next().await {
                let batch = batch_result?;
                let arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for v in arr.values() {
                    seen.insert(*v);
                }
            }
        }
        assert_eq!(seen.len(), total_rows);
        for k in 0..total_rows as i64 {
            assert!(seen.contains(&k), "key {k} missing from round-trip");
        }

        let pool_reserved = session_ctx.runtime_env().memory_pool.reserved();
        assert_eq!(
            pool_reserved, 0,
            "expected MemoryReservation freed after finalization, but pool reports {pool_reserved} bytes still reserved"
        );

        Ok(())
    }

    #[tokio::test]
    async fn spills_under_memory_pressure_and_round_trips() -> Result<()> {
        // 10 batches of 8192 rows each. With a 512 KiB per-task buffer budget,
        // this forces spilling.
        run_round_trip(
            /* num_batches */ 10,
            /* rows_per_batch */ 8192,
            /* num_partitions */ 4,
            /* sort_shuffle_memory_limit_bytes */ 512 * 1024,
            /* expect_spills */ true,
        )
        .await
    }

    #[tokio::test]
    async fn multi_spill_round_trips() -> Result<()> {
        // Larger total payload + tighter buffer budget than the baseline test,
        // so we expect multiple spill events per partition. The byte-copy
        // finalize path should produce a partition section that is
        // `spill_stream || in_memory_stream` (two concatenated IPC streams),
        // and the reader must yield every input row regardless.
        run_round_trip(
            /* num_batches */ 20,
            /* rows_per_batch */ 8192,
            /* num_partitions */ 2,
            /* sort_shuffle_memory_limit_bytes */ 256 * 1024,
            /* expect_spills */ true,
        )
        .await
    }

    #[tokio::test]
    async fn in_memory_only_round_trips() -> Result<()> {
        // Generous per-task buffer budget so no partition spills. Validates
        // the path where each partition section is exactly one in-memory IPC
        // stream.
        run_round_trip(
            /* num_batches */ 4,
            /* rows_per_batch */ 1024,
            /* num_partitions */ 4,
            /* sort_shuffle_memory_limit_bytes */ 64 * 1024 * 1024,
            /* expect_spills */ false,
        )
        .await
    }

    #[tokio::test]
    async fn empty_partitions_round_trip() -> Result<()> {
        use super::super::reader::stream_sort_shuffle_partition;
        use datafusion::arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let total_rows = 256;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![0_i64; total_rows]))],
        )
        .unwrap();
        let partitions = vec![vec![batch]];
        let memory_data_source = Arc::new(MemorySourceConfig::try_new(
            &partitions,
            schema.clone(),
            None,
        )?);
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(memory_data_source));

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let work_dir = TempDir::new()?;

        let num_partitions = 8;
        let writer = SortShuffleWriterExec::try_new(
            "empty_partitions_job".to_string(),
            1,
            input,
            work_dir.path().to_str().unwrap().to_string(),
            Some(Partitioning::Hash(
                vec![Arc::new(Column::new("k", 0))],
                num_partitions,
            )),
            SortShuffleConfig::default(),
        )?;

        let mut stream = writer.execute(0, task_ctx)?;
        let _summary: Vec<RecordBatch> = stream
            .by_ref()
            .try_collect()
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

        let data_path = work_dir
            .path()
            .join("empty_partitions_job")
            .join("1")
            .join("0")
            .join("data.arrow");
        let index_path = data_path.with_extension("arrow.index");

        let mut row_counts = Vec::with_capacity(num_partitions);
        for partition_id in 0..num_partitions {
            let mut s =
                stream_sort_shuffle_partition(&data_path, &index_path, partition_id)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            let mut count = 0_usize;
            while let Some(batch_result) = s.next().await {
                let batch = batch_result?;
                count += batch.num_rows();
            }
            row_counts.push(count);
        }

        let total_seen: usize = row_counts.iter().sum();
        assert_eq!(total_seen, total_rows, "round-trip lost rows");
        let non_empty = row_counts.iter().filter(|c| **c > 0).count();
        assert!(non_empty >= 1, "expected at least one non-empty partition");
        assert!(
            non_empty < num_partitions,
            "expected at least one empty partition"
        );

        Ok(())
    }

    /// Drift-detection test: verifies that `compute_partition_indices` produces
    /// exactly the same per-row partition assignment as DataFusion's own
    /// `BatchPartitioner::new_hash_partitioner`. If a future DataFusion release
    /// changes the hash seed or algorithm this test will fail, signalling that
    /// the sort-shuffle path has diverged.
    #[test]
    fn compute_partition_indices_matches_batch_partitioner() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::metrics::Time;
        use datafusion::physical_plan::repartition::BatchPartitioner;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let values: Vec<i64> = (0..10).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.clone()))],
        )
        .unwrap();

        let exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
            vec![Arc::new(
                datafusion::physical_expr::expressions::Column::new("k", 0),
            )];

        // Our implementation
        let mut hash_buffer: Vec<u64> = Vec::new();
        let ours =
            compute_partition_indices(&batch, &exprs, 4, &mut hash_buffer).unwrap();

        // Reference: DataFusion's BatchPartitioner::new_hash_partitioner
        let mut ref_partitioner =
            BatchPartitioner::new_hash_partitioner(exprs.clone(), 4, Time::default());
        let mut ref_assignments = [usize::MAX; 10];
        ref_partitioner
            .partition(batch.clone(), |partition, sub_batch| {
                // BatchPartitioner returns the rows for `partition` in the order
                // they appear in the original batch. Recover the row indices via
                // value lookup (unique values in 0..10 make this unambiguous).
                let arr = sub_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for v in arr.values() {
                    let original_row = values.iter().position(|x| x == v).unwrap();
                    ref_assignments[original_row] = partition;
                }
                Ok(())
            })
            .unwrap();

        // Cross-check: every row's partition assignment from our helper must
        // match BatchPartitioner's. Guards against hash-seed or algorithm drift.
        for (row, &ref_partition) in ref_assignments.iter().enumerate() {
            let our_partition = ours
                .iter()
                .position(|v| v.contains(&(row as u32)))
                .expect("row not assigned to any partition");
            assert_eq!(
                our_partition, ref_partition,
                "partition assignment for row {row} differs from BatchPartitioner"
            );
        }
    }
    #[tokio::test]
    async fn test_sort_shuffle_with_no_partitioning() -> Result<()> {
        use super::super::reader::stream_sort_shuffle_partition;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let work_dir = TempDir::new()?;
        let work_dir_str = work_dir.path().to_str().unwrap().to_string();

        let input = create_test_input()?;
        // create_test_input(): 2 input partitions × 2 batches × 2 rows = 8 rows total
        let num_input_partitions =
            input.properties().output_partitioning().partition_count();

        let writer = SortShuffleWriterExec::try_new(
            "job-none".to_string(),
            1,
            input,
            work_dir_str.clone(),
            None,
            SortShuffleConfig::default(),
        )?;

        let mut total_rows = 0u64;
        for input_partition in 0..num_input_partitions {
            let results = writer
                .clone()
                .execute_shuffle_write(input_partition, task_ctx.clone())
                .await?;

            // Exactly one output partition (partition 0) per input partition
            assert_eq!(results.len(), 1, "expected 1 output partition");
            assert_eq!(results[0].partition_id, 0);
            assert!(results[0].is_sort_shuffle);
            total_rows += results[0].num_rows;

            // Verify the data file is readable via the sort-shuffle reader
            let data_path = work_dir
                .path()
                .join("job-none")
                .join("1")
                .join(format!("{input_partition}"))
                .join("data.arrow");
            let index_path = data_path.with_extension("arrow.index");
            assert!(data_path.exists(), "data file missing");
            assert!(index_path.exists(), "index file missing");

            let mut s = stream_sort_shuffle_partition(&data_path, &index_path, 0)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            let mut read_rows = 0u64;
            while let Some(batch_result) = s.next().await {
                let batch = batch_result?;
                read_rows += batch.num_rows() as u64;
            }
            assert_eq!(read_rows, results[0].num_rows);
        }
        assert_eq!(total_rows, 8); // 2 partitions × 2 batches × 2 rows
        Ok(())
    }
}
