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

//! Ordering-preserving shuffle reader.
//!
//! The regular [`ShuffleReaderExec`](super::ShuffleReaderExec) fans every
//! upstream source for an output partition into one mpsc channel, `try_flatten`s
//! them in arrival order, then coalesces batches. Each source file is
//! internally sorted, but the concatenation is not — the resulting stream
//! violates the monotonicity that RANGE-frame window operators (and
//! sort-merge-join build sides) require.
//!
//! `RangeShuffleReaderExec` keeps each upstream source alive as its own
//! stream and feeds all N into a `StreamingMerge` keyed on the child's
//! declared output ordering. Batches within an output partition are
//! globally sorted on the merge key.
//!
//! Trade-offs vs. the regular reader:
//!
//! - No permit-based fetch governor. Backpressure flows from the merge
//!   consumer down through each per-source stream — the merge only polls
//!   the source it needs next, so h2 (for remote) and disk (for local)
//!   throttle naturally.
//! - No mid-body fetch retry. `fetch_partition_remote` streams directly
//!   without buffering the whole source, so a transport error mid-body
//!   fails the merged stream; the task-level retry re-executes.
//! - No coalesce or broadcast variants. Ordered outputs come from
//!   `OrderedRangeRepartitionExec`, which is one-to-one and never fanned
//!   into a broadcast.
//!
//! # Future work
//!
//! Today the reader pulls whole upstream files and lets `StreamingMerge`
//! do the work. Once value-indexed shuffle files land end-to-end (writer
//! side in [PR #2204]), the reader will consult per-file ValueIndex
//! offsets and fetch only the byte ranges covering its target output
//! partition — dropping the "read the whole file to throw most of it
//! away" cost that dominates when K is large.
//!
//! [PR #2204]: https://github.com/apache/datafusion-ballista/pull/2204

use crate::client_pool::BallistaClientPool;
use crate::execution_plans::shuffle_reader::{
    fetch_partition_local, fetch_partition_remote, local_remote_read_split,
    stats_for_partition,
};
use crate::extension::SessionConfigExt;
use crate::serde::scheduler::PartitionLocation;
use crate::utils::GrpcClientConfig;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, Partitioning};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::TryStreamExt;
use log::debug;
use std::sync::Arc;

/// Ordering-preserving shuffle reader. See module docs.
#[derive(Debug)]
pub struct RangeShuffleReaderExec {
    /// Upstream stage that produced these files.
    pub stage_id: usize,
    pub(crate) schema: SchemaRef,
    /// M-shape: `partition[k]` lists the upstream partition locations that
    /// output partition `k` needs to merge.
    pub partition: Vec<Vec<PartitionLocation>>,
    /// Sort key the merge preserves. Advertised in `PlanProperties.eq_properties`
    /// so downstream operators (BWAG, SMJ build side) see the output ordering.
    merge_ordering: LexOrdering,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
    work_dir: Option<String>,
    client_pool: Option<Arc<dyn BallistaClientPool>>,
}

impl RangeShuffleReaderExec {
    /// The output partition count is `partition.len()`. Partitioning is
    /// range on the merge key — DF has no `Partitioning::Range`, so it's
    /// reported as `UnknownPartitioning`. Downstream co-partitioning is
    /// carried by the advertised ordering, not by partitioning kind.
    pub fn try_new(
        stage_id: usize,
        partition: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
        merge_ordering: LexOrdering,
    ) -> Result<Self> {
        let output_partition_count = partition.len();
        let eq_properties = EquivalenceProperties::new_with_orderings(
            schema.clone(),
            vec![merge_ordering.clone()],
        );
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(output_partition_count),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Ok(Self {
            stage_id,
            schema,
            partition,
            merge_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            work_dir: None,
            client_pool: None,
        })
    }

    /// Late-bound by the executor.
    pub fn with_work_dir(&self, work_dir: String) -> Self {
        Self {
            stage_id: self.stage_id,
            schema: self.schema.clone(),
            partition: self.partition.clone(),
            merge_ordering: self.merge_ordering.clone(),
            metrics: self.metrics.clone(),
            properties: self.properties.clone(),
            work_dir: Some(work_dir),
            client_pool: self.client_pool.clone(),
        }
    }

    /// Late-bound by the executor.
    pub fn with_client_pool(&self, client_pool: Arc<dyn BallistaClientPool>) -> Self {
        Self {
            stage_id: self.stage_id,
            schema: self.schema.clone(),
            partition: self.partition.clone(),
            merge_ordering: self.merge_ordering.clone(),
            metrics: self.metrics.clone(),
            properties: self.properties.clone(),
            work_dir: self.work_dir.clone(),
            client_pool: Some(client_pool),
        }
    }

    /// Sort key the merge preserves.
    pub fn merge_ordering(&self) -> &LexOrdering {
        &self.merge_ordering
    }
}

impl DisplayAs for RangeShuffleReaderExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RangeShuffleReaderExec: upstream_stage: {}, partitions: {}, ordering: {}",
                    self.stage_id,
                    self.partition.len(),
                    self.merge_ordering,
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "upstream_stage={}", self.stage_id)?;
                writeln!(f, "output_partitions={}", self.partition.len())?;
                writeln!(f, "ordering={}", self.merge_ordering)
            }
        }
    }
}

impl ExecutionPlan for RangeShuffleReaderExec {
    fn name(&self) -> &str {
        "RangeShuffleReaderExec"
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "RangeShuffleReaderExec does not support children plans".to_owned(),
            ));
        }
        Ok(Arc::new(Self {
            stage_id: self.stage_id,
            schema: self.schema.clone(),
            partition: self.partition.clone(),
            merge_ordering: self.merge_ordering.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            properties: self.properties.clone(),
            work_dir: self.work_dir.clone(),
            client_pool: self.client_pool.clone(),
        }))
    }

    fn execute(
        &self,
        output_partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_id = ctx
            .task_id()
            .unwrap_or_else(|| output_partition.to_string());
        debug!("RangeShuffleReaderExec::execute({task_id})");

        let config = ctx.session_config();
        let work_dir = self.work_dir.as_ref().ok_or_else(|| {
            DataFusionError::Configuration(
                "RangeShuffleReaderExec work dir should have been set by executor"
                    .to_owned(),
            )
        })?;

        let locations = self.partition[output_partition].clone();
        let (local_locations, remote_locations) = local_remote_read_split(
            work_dir,
            locations,
            config.ballista_shuffle_reader_force_remote_read(),
        );

        let mut sub_streams: Vec<SendableRecordBatchStream> =
            Vec::with_capacity(local_locations.len() + remote_locations.len());

        for loc in local_locations {
            let stream = fetch_partition_local(work_dir, &loc)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            sub_streams.push(stream);
        }

        if !remote_locations.is_empty() {
            let grpc_config: Arc<GrpcClientConfig> =
                Arc::new((&config.ballista_config()).into());
            let customize_endpoint =
                config.ballista_override_create_grpc_client_endpoint();
            let prefer_flight = config.ballista_shuffle_reader_remote_prefer_flight();
            let client_pool = self.client_pool.clone();

            for loc in remote_locations {
                let schema = self.schema.clone();
                let grpc_config = grpc_config.clone();
                let customize_endpoint = customize_endpoint.clone();
                let client_pool = client_pool.clone();

                // Lazy connect: the first poll from the merge triggers the
                // remote fetch; subsequent polls stream batches directly. No
                // buffering, no retry — task-level retry covers transport
                // failures.
                let lazy = futures::stream::once(async move {
                    fetch_partition_remote(
                        &loc,
                        grpc_config,
                        prefer_flight,
                        customize_endpoint,
                        client_pool,
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))
                })
                .try_flatten();

                sub_streams.push(Box::pin(RecordBatchStreamAdapter::new(schema, lazy)));
            }
        }

        if sub_streams.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::empty(),
            )));
        }

        let baseline = BaselineMetrics::new(&self.metrics, output_partition);
        let reservation = MemoryConsumer::new(format!(
            "RangeShuffleReaderExec[stage={},out={}]",
            self.stage_id, output_partition,
        ))
        .register(ctx.memory_pool());

        let merged = StreamingMergeBuilder::new()
            .with_streams(sub_streams)
            .with_schema(self.schema.clone())
            .with_expressions(&self.merge_ordering)
            .with_batch_size(config.batch_size())
            .with_metrics(baseline)
            .with_reservation(reservation)
            .build()?;

        Ok(merged)
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if let Some(idx) = partition {
            let partition_count = self.partition.len();
            if idx >= partition_count {
                return datafusion::common::internal_err!(
                    "Invalid partition index: {}, the partition count is {}",
                    idx,
                    partition_count
                );
            }
            let stats =
                stats_for_partition(idx, self.schema.fields().len(), &self.partition)?;
            return Ok(Arc::new(stats));
        }
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::scheduler::{
        ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
        PartitionId, PartitionStats,
    };
    use datafusion::arrow::array::{ArrayRef, Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::writer::StreamWriter;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::prelude::SessionContext;
    use std::fs::{File, create_dir_all};
    use tempfile::tempdir;

    /// Write an Arrow IPC stream containing `batches` at `path`.
    fn write_ipc_stream(path: &std::path::Path, batches: &[RecordBatch]) {
        create_dir_all(path.parent().unwrap()).unwrap();
        let file = File::create(path).unwrap();
        let mut writer = StreamWriter::try_new(file, &batches[0].schema()).unwrap();
        for b in batches {
            writer.write(b).unwrap();
        }
        writer.finish().unwrap();
    }

    fn sorted_batch(schema: SchemaRef, values: &[f64]) -> RecordBatch {
        let col: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    fn make_location(job: &str, stage: usize, partition: usize) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: partition,
            partition_id: PartitionId {
                job_id: job.into(),
                stage_id: stage,
                partition_id: partition,
            },
            executor_meta: ExecutorMetadata {
                id: "test-executor".to_string(),
                host: "127.0.0.1".to_string(),
                port: 0,
                grpc_port: 0,
                specification: ExecutorSpecification::default(),
                os_info: ExecutorOperatingSystemSpecification::default(),
            },
            partition_stats: PartitionStats::default(),
            file_id: None,
            is_sort_shuffle: false,
        }
    }

    /// Two upstream sources, each internally sorted; the range reader must
    /// interleave them into a single globally-sorted stream. This is the core
    /// correctness property the regular reader violates.
    #[tokio::test]
    async fn merges_two_sorted_sources() {
        let dir = tempdir().unwrap();
        let work_dir = dir.path();
        let job = "job-merges-two";
        let stage_id = 1usize;

        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));

        // Source 0: [1.0, 3.0, 5.0]
        // Source 1: [2.0, 4.0, 6.0]
        // Merged:   [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        let src0 = sorted_batch(schema.clone(), &[1.0, 3.0, 5.0]);
        let src1 = sorted_batch(schema.clone(), &[2.0, 4.0, 6.0]);

        let loc0 = make_location(job, stage_id, 0);
        let loc1 = make_location(job, stage_id, 1);
        let path0 = loc0.path(work_dir.to_str().unwrap()).expect("path0");
        let path1 = loc1.path(work_dir.to_str().unwrap()).expect("path1");
        write_ipc_stream(&path0, &[src0]);
        write_ipc_stream(&path1, &[src1]);

        // Both sources land in output partition 0.
        let partitions = vec![vec![loc0, loc1]];
        let merge_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            Arc::new(Column::new("v", 0)),
        )])
        .unwrap();

        let reader = RangeShuffleReaderExec::try_new(
            stage_id,
            partitions,
            schema.clone(),
            merge_ordering,
        )
        .unwrap()
        .with_work_dir(work_dir.to_string_lossy().to_string());

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let stream = reader.execute(0, task_ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let all: Vec<f64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(all, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    }

    /// If the caller only has one source per output partition, the reader
    /// must still work — StreamingMerge over 1 stream is a valid degenerate
    /// case.
    #[tokio::test]
    async fn passes_through_single_source() {
        let dir = tempdir().unwrap();
        let work_dir = dir.path();
        let job = "job-single-source";
        let stage_id = 2usize;

        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let src = sorted_batch(schema.clone(), &[10.0, 20.0, 30.0]);

        let loc = make_location(job, stage_id, 0);
        let path = loc.path(work_dir.to_str().unwrap()).expect("path");
        write_ipc_stream(&path, &[src]);

        let partitions = vec![vec![loc]];
        let merge_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            Arc::new(Column::new("v", 0)),
        )])
        .unwrap();

        let reader = RangeShuffleReaderExec::try_new(
            stage_id,
            partitions,
            schema.clone(),
            merge_ordering,
        )
        .unwrap()
        .with_work_dir(work_dir.to_string_lossy().to_string());

        let ctx = SessionContext::new();
        let stream = reader.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let all: Vec<f64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(all, vec![10.0, 20.0, 30.0]);
    }

    /// Empty partitions.len() > 0 with all-empty inner locations must return
    /// an empty stream instead of tripping StreamingMerge on zero sub-streams.
    #[tokio::test]
    async fn handles_empty_partition() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let merge_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            Arc::new(Column::new("v", 0)),
        )])
        .unwrap();

        let reader = RangeShuffleReaderExec::try_new(
            7,
            vec![vec![]],
            schema.clone(),
            merge_ordering,
        )
        .unwrap()
        .with_work_dir("/tmp".to_string());

        let ctx = SessionContext::new();
        let stream = reader.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert!(batches.is_empty());
    }

    /// The reader must advertise its merge ordering so downstream operators
    /// see the sortedness invariant (BWAG's RANGE-frame cursor, SMJ build side).
    #[test]
    fn advertises_merge_ordering() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let merge_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            Arc::new(Column::new("v", 0)),
        )])
        .unwrap();

        let reader = RangeShuffleReaderExec::try_new(
            3,
            vec![vec![]; 4],
            schema,
            merge_ordering.clone(),
        )
        .unwrap();

        let out = reader
            .properties()
            .eq_properties
            .oeq_class()
            .iter()
            .next()
            .cloned();
        let expected = out.expect("expected an advertised ordering");
        assert_eq!(expected, merge_ordering);

        assert_eq!(reader.properties().partitioning.partition_count(), 4);
    }
}
