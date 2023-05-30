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

//! ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
//! can be executed as one unit with each partition being executed in parallel. The output of each
//! partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
//! will use the ShuffleReaderExec to read these results.

use datafusion::physical_plan::expressions::PhysicalSortExpr;

use crate::utils;
use std::any::Any;
use std::future::Future;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::serde::protobuf::ShuffleWritePartition;
use crate::serde::scheduler::PartitionStats;
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::common::IPCWriter;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};

use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};

use datafusion::arrow::error::ArrowError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use log::{debug, info};

/// ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of each
/// partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
#[derive(Debug, Clone)]
pub struct ShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: String,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Stage partitions executed as part of the this shuffle write
    partitions: Vec<usize>,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    work_dir: String,
    /// Optional shuffle output partitioning
    shuffle_output_partitioning: Option<Partitioning>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone)]
struct ShuffleWriteMetrics {
    /// Time spend writing batches to shuffle files
    write_time: metrics::Time,
    repart_time: metrics::Time,
    input_rows: metrics::Count,
    output_rows: metrics::Count,
}

impl ShuffleWriteMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let write_time = MetricBuilder::new(metrics).subset_time("write_time", 0);
        let repart_time = MetricBuilder::new(metrics).subset_time("repart_time", 0);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", 0);

        let output_rows = MetricBuilder::new(metrics).output_rows(0);

        Self {
            write_time,
            repart_time,
            input_rows,
            output_rows,
        }
    }
}

impl ShuffleWriterExec {
    /// Create a new shuffle writer
    pub fn try_new(
        job_id: String,
        stage_id: usize,
        partitions: Vec<usize>,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        Ok(Self {
            job_id,
            stage_id,
            partitions,
            plan,
            work_dir,
            shuffle_output_partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
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

    pub fn partitions(&self) -> &[usize] {
        &self.partitions
    }

    pub fn with_partitions(&self, partitions: Vec<usize>) -> Result<Self> {
        Self::try_new(
            self.job_id.clone(),
            self.stage_id,
            partitions,
            self.plan.clone(),
            self.work_dir.clone(),
            self.shuffle_output_partitioning.clone(),
        )
    }

    pub fn with_work_dir(&self, work_dir: impl Into<String>) -> Result<Self> {
        Self::try_new(
            self.job_id.clone(),
            self.stage_id,
            self.partitions.to_vec(),
            self.plan.clone(),
            work_dir.into(),
            self.shuffle_output_partitioning.clone(),
        )
    }

    /// Get the true output partitioning
    pub fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    pub fn execute_shuffle_write(
        &self,
        context: Arc<TaskContext>,
    ) -> impl Future<Output = Result<Vec<ShuffleWritePartition>>> {
        let mut path = PathBuf::from(&self.work_dir);
        path.push(&self.job_id);
        path.push(&format!("{}", self.stage_id));

        let write_metrics = ShuffleWriteMetrics::new(&self.metrics);
        let output_partitioning = self.shuffle_output_partitioning.clone();
        let plan = self.plan.clone();

        let partitions = self.partitions.clone();

        // add unique id for output
        let shuffle_id = uuid::Uuid::new_v4();

        async move {
            let now = Instant::now();
            let mut stream = plan.execute(0, context)?;

            match output_partitioning {
                None => {
                    let timer = write_metrics.write_time.timer();
                    path.push(shuffle_id.to_string());
                    debug!("Creating dir {:?}", path);

                    std::fs::create_dir_all(&path)?;

                    path.push("data.arrow");
                    let path = path.to_str().unwrap();
                    debug!("Writing results to {}", path);

                    // stream results to disk
                    let stats = utils::write_stream_to_disk(
                        &mut stream,
                        path,
                        &write_metrics.write_time,
                    )
                    .await
                    .map_err(DataFusionError::from)?;

                    write_metrics
                        .input_rows
                        .add(stats.num_rows.unwrap_or(0) as usize);
                    write_metrics
                        .output_rows
                        .add(stats.num_rows.unwrap_or(0) as usize);
                    timer.done();

                    info!(
                        "Executed partitions {:?} in {} seconds. Statistics: {}",
                        partitions,
                        now.elapsed().as_secs(),
                        stats
                    );

                    Ok(vec![ShuffleWritePartition {
                        partitions: partitions.iter().map(|p| *p as u32).collect(),
                        output_partition: 0,
                        path: path.to_owned(),
                        num_batches: stats.num_batches.unwrap_or(0),
                        num_rows: stats.num_rows.unwrap_or(0),
                        num_bytes: stats.num_bytes.unwrap_or(0),
                    }])
                }

                Some(Partitioning::Hash(exprs, num_output_partitions)) => {
                    // we won't necessary produce output for every possible partition, so we
                    // create writers on demand
                    let mut writers: Vec<Option<IPCWriter>> = vec![];
                    for _ in 0..num_output_partitions {
                        writers.push(None);
                    }

                    let mut partitioner = BatchPartitioner::try_new(
                        Partitioning::Hash(exprs, num_output_partitions),
                        write_metrics.repart_time.clone(),
                    )?;

                    while let Some(result) = stream.next().await {
                        let input_batch = result?;

                        write_metrics.input_rows.add(input_batch.num_rows());

                        partitioner.partition(
                            input_batch,
                            |output_partition, output_batch| {
                                // partition func in datafusion make sure not write empty output_batch.
                                let timer = write_metrics.write_time.timer();
                                match &mut writers[output_partition] {
                                    Some(w) => {
                                        w.write(&output_batch)?;
                                    }
                                    None => {
                                        let mut path = path.clone();
                                        path.push(output_partition.to_string());
                                        std::fs::create_dir_all(&path)?;

                                        path.push(format!("{}.arrow", shuffle_id));
                                        debug!("Writing results to {:?}", path);

                                        let mut writer = IPCWriter::new(
                                            &path,
                                            stream.schema().as_ref(),
                                        )?;

                                        writer.write(&output_batch)?;
                                        writers[output_partition] = Some(writer);
                                    }
                                }
                                write_metrics.output_rows.add(output_batch.num_rows());
                                timer.done();
                                Ok(())
                            },
                        )?;
                    }

                    let mut part_locs = vec![];

                    for (i, w) in writers.iter_mut().enumerate() {
                        match w {
                            Some(w) => {
                                w.finish()?;
                                debug!(
                                    "Finished writing shuffle partition {} at {:?}. Batches: {}. Rows: {}. Bytes: {}.",
                                    i,
                                    w.path(),
                                    w.num_batches,
                                    w.num_rows,
                                    w.num_bytes
                                );

                                part_locs.push(ShuffleWritePartition {
                                    partitions: partitions
                                        .iter()
                                        .map(|p| *p as u32)
                                        .collect(),
                                    output_partition: i as u32,
                                    path: w.path().to_string_lossy().to_string(),
                                    num_batches: w.num_batches,
                                    num_rows: w.num_rows,
                                    num_bytes: w.num_bytes,
                                });
                            }
                            None => {}
                        }
                    }
                    Ok(part_locs)
                }

                _ => Err(DataFusionError::Execution(
                    "Invalid shuffle partitioning scheme".to_owned(),
                )),
            }
        }
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // This operator needs to be executed once for each *input* partition and there
        // isn't really a mechanism yet in DataFusion to support this use case so we report
        // the input partitioning as the output partitioning here. The executor reports
        // output partition meta data back to the scheduler.
        self.plan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ShuffleWriterExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            self.partitions.clone(),
            children[0].clone(),
            self.work_dir.clone(),
            self.shuffle_output_partitioning.clone(),
        )?))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = result_schema();

        let schema_captured = schema.clone();
        let fut_stream = self
            .execute_shuffle_write(context)
            .and_then(|part_loc| async move {
                // build metadata result batch
                let num_writers = part_loc.len();
                let mut partition_builder = UInt32Builder::with_capacity(num_writers);
                let mut path_builder =
                    StringBuilder::with_capacity(num_writers, num_writers * 100);
                let mut num_rows_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_batches_builder = UInt64Builder::with_capacity(num_writers);
                let mut num_bytes_builder = UInt64Builder::with_capacity(num_writers);

                for loc in &part_loc {
                    path_builder.append_value(loc.path.clone());
                    partition_builder.append_value(loc.output_partition);
                    num_rows_builder.append_value(loc.num_rows);
                    num_batches_builder.append_value(loc.num_batches);
                    num_bytes_builder.append_value(loc.num_bytes);
                }

                // build arrays
                let partitions: ArrayRef = Arc::new(partition_builder.finish());
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

                // build result batch containing metadata
                let batch = RecordBatch::try_new(
                    schema_captured.clone(),
                    vec![partitions, path, stats],
                )?;

                debug!("RESULTS METADATA:\n{:?}", batch);

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

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "ShuffleWriterExec: {:?}",
                    self.shuffle_output_partitioning
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.plan.statistics()
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
    use datafusion::arrow::array::{StringArray, StructArray, UInt32Array, UInt64Array};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;

    use crate::execution_plans::CoalesceTasksExec;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    #[ignore]
    #[tokio::test]
    // number of rows in each partition is a function of the hash output, so don't test here
    #[cfg(not(feature = "force_hash_collisions"))]
    async fn test() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            vec![0],
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let path = batch.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let file0 = path.value(0);
        assert!(
            file0.ends_with("/jobOne/1/0/data-0.arrow")
                || file0.ends_with("\\jobOne\\1\\0\\data-0.arrow")
        );
        let file1 = path.value(1);
        assert!(
            file1.ends_with("/jobOne/1/1/data-0.arrow")
                || file1.ends_with("\\jobOne\\1\\1\\data-0.arrow")
        );

        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let num_rows = stats
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(4, num_rows.value(0));
        assert_eq!(4, num_rows.value(1));

        Ok(())
    }

    #[tokio::test]
    // number of rows in each partition is a function of the hash output, so don't test here
    #[cfg(not(feature = "force_hash_collisions"))]
    async fn test_partitioned() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input_plan = create_input_plan()?;
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            vec![0],
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let num_rows = stats
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(2, num_rows.value(0));
        assert_eq!(2, num_rows.value(1));

        Ok(())
    }

    fn create_input_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];

        let task_exec = CoalesceTasksExec::new(
            Arc::new(MemoryExec::try_new(&partitions, schema, None)?),
            vec![0],
        );

        Ok(Arc::new(task_exec))
    }
}
