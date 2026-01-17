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
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use super::super::shuffle_writer_trait::ShuffleWriter;
use super::buffer::PartitionBuffer;
use super::config::SortShuffleConfig;
use super::index::ShuffleIndex;
use super::spill::SpillManager;
use crate::serde::protobuf::ShuffleWritePartition;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
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
    /// Shuffle output partitioning (must be Hash partitioning)
    shuffle_output_partitioning: Partitioning,
    /// Sort shuffle configuration
    config: SortShuffleConfig,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: PlanProperties,
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
    /// Number of spills
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
        shuffle_output_partitioning: Partitioning,
        config: SortShuffleConfig,
    ) -> Result<Self> {
        // Sort shuffle only supports hash partitioning
        match &shuffle_output_partitioning {
            Partitioning::Hash(_, _) => {}
            other => {
                return Err(DataFusionError::Plan(format!(
                    "SortShuffleWriterExec only supports Hash partitioning, got: {other:?}"
                )));
            }
        }

        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(plan.schema()),
            shuffle_output_partitioning.clone(),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

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

    /// Get the shuffle output partitioning
    pub fn shuffle_output_partitioning(&self) -> &Partitioning {
        &self.shuffle_output_partitioning
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
            let mut stream = plan.execute(input_partition, context)?;
            let schema = stream.schema();

            let Partitioning::Hash(exprs, num_output_partitions) = partitioning else {
                return Err(DataFusionError::Internal(
                    "Expected hash partitioning".to_string(),
                ));
            };

            // Create partition buffers
            let mut buffers: Vec<PartitionBuffer> = (0..num_output_partitions)
                .map(|i| PartitionBuffer::new(i, schema.clone()))
                .collect();

            // Create spill manager
            let mut spill_manager = SpillManager::new(
                &work_dir,
                &job_id,
                stage_id,
                input_partition,
                config.compression,
            )
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            // Create batch partitioner
            let mut partitioner = BatchPartitioner::try_new(
                Partitioning::Hash(exprs, num_output_partitions),
                metrics.repart_time.clone(),
            )?;

            // Process input stream
            while let Some(result) = stream.next().await {
                let input_batch = result?;
                metrics.input_rows.add(input_batch.num_rows());

                // Partition the batch
                partitioner.partition(
                    input_batch,
                    |output_partition, output_batch| {
                        buffers[output_partition].append(output_batch);
                        Ok(())
                    },
                )?;

                // Check if we need to spill
                let total_memory: usize = buffers.iter().map(|b| b.memory_used()).sum();
                if total_memory > config.spill_memory_threshold() {
                    let timer = metrics.spill_time.timer();
                    spill_largest_buffers(
                        &mut buffers,
                        &mut spill_manager,
                        &schema,
                        config.spill_memory_threshold() / 2,
                    )?;
                    timer.done();
                }
            }

            // Finalize: write consolidated output file
            let timer = metrics.write_time.timer();
            let (data_path, index_path, partition_stats) = finalize_output(
                &work_dir,
                &job_id,
                stage_id,
                input_partition,
                &mut buffers,
                &mut spill_manager,
                &schema,
                &config,
            )?;
            timer.done();

            // Update metrics
            metrics.spill_count.add(spill_manager.total_spills());
            metrics
                .spill_bytes
                .add(spill_manager.total_bytes_spilled() as usize);

            let total_rows: u64 = partition_stats.iter().map(|(_, _, r, _)| *r).sum();
            metrics.output_rows.add(total_rows as usize);

            // Cleanup spill files
            spill_manager
                .cleanup()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            info!(
                "Sort shuffle write for partition {} completed in {} seconds. \
                 Output: {:?}, Index: {:?}, Spills: {}, Spill bytes: {}",
                input_partition,
                now.elapsed().as_secs(),
                data_path,
                index_path,
                spill_manager.total_spills(),
                spill_manager.total_bytes_spilled()
            );

            // Build result - one entry per output partition that has data
            let mut results = Vec::new();
            for (part_id, num_batches, num_rows, num_bytes) in partition_stats {
                if num_rows > 0 {
                    results.push(ShuffleWritePartition {
                        partition_id: part_id as u64,
                        path: data_path.to_string_lossy().to_string(),
                        num_batches,
                        num_rows,
                        num_bytes,
                    });
                }
            }

            Ok(results)
        }
    }
}

/// Spills the largest buffers until total memory is below the target.
fn spill_largest_buffers(
    buffers: &mut [PartitionBuffer],
    spill_manager: &mut SpillManager,
    schema: &SchemaRef,
    target_memory: usize,
) -> Result<()> {
    loop {
        let total_memory: usize = buffers.iter().map(|b| b.memory_used()).sum();
        if total_memory <= target_memory {
            break;
        }

        // Find the largest buffer
        let largest_idx = buffers
            .iter()
            .enumerate()
            .max_by_key(|(_, b)| b.memory_used())
            .map(|(i, _)| i);

        match largest_idx {
            Some(idx) if buffers[idx].memory_used() > 0 => {
                let partition_id = buffers[idx].partition_id();
                let batches = buffers[idx].drain();
                spill_manager
                    .spill(partition_id, batches, schema)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            }
            _ => break, // No more buffers to spill
        }
    }
    Ok(())
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
    buffers: &mut [PartitionBuffer],
    spill_manager: &mut SpillManager,
    schema: &SchemaRef,
    config: &SortShuffleConfig,
) -> Result<FinalizeResult> {
    let num_partitions = buffers.len();
    let mut index = ShuffleIndex::new(num_partitions);
    let mut partition_stats = Vec::with_capacity(num_partitions);

    // Create output directory
    let mut output_dir = PathBuf::from(work_dir);
    output_dir.push(job_id);
    output_dir.push(format!("{stage_id}"));
    output_dir.push(format!("{input_partition}"));
    std::fs::create_dir_all(&output_dir)?;

    let data_path = output_dir.join("data.arrow");
    let index_path = output_dir.join("data.arrow.index");

    debug!("Writing consolidated shuffle output to {:?}", data_path);

    // Use FileWriter for random access support via FileReader
    let file = File::create(&data_path)?;
    let mut buffered = BufWriter::new(file);

    let options =
        IpcWriteOptions::default().try_with_compression(Some(config.compression))?;
    let mut writer = FileWriter::try_new_with_options(&mut buffered, schema, options)?;

    // Track cumulative batch counts - index stores the starting batch index for each partition
    // FileReader supports random access to batches by index
    let mut cumulative_batch_count: i64 = 0;

    // Write partitions in order
    for (partition_id, buffer) in buffers.iter_mut().enumerate() {
        // Set the starting batch index for this partition
        index.set_offset(partition_id, cumulative_batch_count);

        let mut partition_rows: u64 = 0;
        let mut partition_batches: u64 = 0;
        let mut partition_bytes: u64 = 0;

        // First, write any spill files for this partition
        if spill_manager.has_spill_files(partition_id) {
            let spill_batches = spill_manager
                .read_spill_files(partition_id)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            for batch in spill_batches {
                partition_rows += batch.num_rows() as u64;
                partition_bytes += batch.get_array_memory_size() as u64;
                partition_batches += 1;
                writer.write(&batch)?;
            }
        }

        // Then write remaining buffered data
        let buffered_batches = buffer.take_batches();
        for batch in buffered_batches {
            partition_rows += batch.num_rows() as u64;
            partition_bytes += batch.get_array_memory_size() as u64;
            partition_batches += 1;
            writer.write(&batch)?;
        }

        partition_stats.push((
            partition_id,
            partition_batches,
            partition_rows,
            partition_bytes,
        ));

        cumulative_batch_count += partition_batches as i64;
    }

    // Finish writing (this writes the IPC footer for random access)
    writer.finish()?;

    // Store total batch count
    index.set_total_length(cumulative_batch_count);

    // Write index file
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
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SortShuffleWriterExec: partitioning={}",
                    self.shuffle_output_partitioning
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={}", self.shuffle_output_partitioning)
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

    fn properties(&self) -> &PlanProperties {
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
        let fut_stream = self
            .clone()
            .execute_shuffle_write(partition, context)
            .and_then(|part_loc| async move {
                // Build metadata result batch
                let num_writers = part_loc.len();
                let mut partition_builder = UInt32Builder::with_capacity(num_writers);
                let mut path_builder =
                    StringBuilder::with_capacity(num_writers, num_writers * 100);
                let mut num_rows_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_batches_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_bytes_builder = UInt64Builder::with_capacity(num_writers);

                for loc in &part_loc {
                    path_builder.append_value(loc.path.clone());
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
        Some(&self.shuffle_output_partitioning)
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
            Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2),
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
}
