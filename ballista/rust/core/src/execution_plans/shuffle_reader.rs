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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(not(test))]
use crate::client::BallistaClient;
use crate::serde::scheduler::{PartitionLocation, PartitionStats};

use datafusion::arrow::datatypes::SchemaRef;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt, TryStreamExt};

use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::common::AbortOnDropMany;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use itertools::Itertools;
use log::{error, info};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

/// ShuffleReaderExec reads partitions that have already been materialized by a ShuffleWriterExec
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// Each partition of a shuffle can read data from multiple locations
    pub partition: Vec<Vec<PartitionLocation>>,
    pub(crate) schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl ShuffleReaderExec {
    /// Create a new ShuffleReaderExec
    pub fn try_new(
        partition: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            partition,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // TODO partitioning may be known and could be populated here
        // see https://github.com/apache/arrow-datafusion/issues/758
        Partitioning::UnknownPartitioning(self.partition.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Ballista ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_id = context.task_id().unwrap_or_else(|| partition.to_string());
        info!("ShuffleReaderExec::execute({})", task_id);

        // TODO make the maximum size configurable, or make it depends on global memory control
        let max_request_num = 50usize;
        let mut partition_locations = HashMap::new();
        for p in &self.partition[partition] {
            partition_locations
                .entry(p.executor_meta.id.clone())
                .or_insert_with(Vec::new)
                .push(p.clone());
        }
        // Sort partitions for evenly send fetching partition requests to avoid hot executors within one task
        let mut partition_locations: Vec<PartitionLocation> = partition_locations
            .into_values()
            .flat_map(|ps| ps.into_iter().enumerate())
            .sorted_by(|(p1_idx, _), (p2_idx, _)| Ord::cmp(p1_idx, p2_idx))
            .map(|(_, p)| p)
            .collect();
        // Shuffle partitions for evenly send fetching partition requests to avoid hot executors within multiple tasks
        partition_locations.shuffle(&mut thread_rng());
        let response_receiver =
            send_fetch_partitions(partition_locations, max_request_num);

        let result = RecordBatchStreamAdapter::new(
            Arc::new(self.schema.as_ref().clone()),
            response_receiver.try_flatten(),
        );
        Ok(Box::pin(result))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "ShuffleReaderExec: partitions={}", self.partition.len())
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        stats_for_partitions(
            self.partition
                .iter()
                .flatten()
                .map(|loc| loc.partition_stats),
        )
    }
}

fn stats_for_partitions(
    partition_stats: impl Iterator<Item = PartitionStats>,
) -> Statistics {
    // TODO stats: add column statistics to PartitionStats
    partition_stats.fold(
        Statistics {
            is_exact: true,
            num_rows: Some(0),
            total_byte_size: Some(0),
            column_statistics: None,
        },
        |mut acc, part| {
            // if any statistic is unkown it makes the entire statistic unkown
            acc.num_rows = acc.num_rows.zip(part.num_rows).map(|(a, b)| a + b as usize);
            acc.total_byte_size = acc
                .total_byte_size
                .zip(part.num_bytes)
                .map(|(a, b)| a + b as usize);
            acc
        },
    )
}

/// Adapter for a tokio ReceiverStream that implements the SendableRecordBatchStream interface
struct AbortableReceiverStream {
    inner: ReceiverStream<Result<SendableRecordBatchStream>>,

    #[allow(dead_code)]
    drop_helper: AbortOnDropMany<()>,
}

impl AbortableReceiverStream {
    /// Construct a new SendableRecordBatchReceiverStream which will send batches of the specified schema from inner
    pub fn create(
        rx: tokio::sync::mpsc::Receiver<Result<SendableRecordBatchStream>>,
        join_handles: Vec<JoinHandle<()>>,
    ) -> AbortableReceiverStream {
        let inner = ReceiverStream::new(rx);
        Self {
            inner,
            drop_helper: AbortOnDropMany(join_handles),
        }
    }
}

impl Stream for AbortableReceiverStream {
    type Item = Result<SendableRecordBatchStream>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

fn send_fetch_partitions(
    partition_locations: Vec<PartitionLocation>,
    max_request_num: usize,
) -> AbortableReceiverStream {
    let (response_sender, response_receiver) = mpsc::channel(max_request_num);
    let semaphore = Arc::new(Semaphore::new(max_request_num));
    let mut join_handles = vec![];
    for p in partition_locations.into_iter() {
        let semaphore = semaphore.clone();
        let response_sender = response_sender.clone();
        let join_handle = tokio::spawn(async move {
            // Block if exceeds max request number
            let permit = semaphore.acquire_owned().await.unwrap();
            let r = fetch_partition(&p).await;
            // Block if the channel buffer is full
            if let Err(e) = response_sender.send(r).await {
                error!("Fail to send response event to the channel due to {}", e);
            }
            // Increase semaphore by dropping existing permits.
            drop(permit);
        });
        join_handles.push(join_handle);
    }

    AbortableReceiverStream::create(response_receiver, join_handles)
}

#[cfg(not(test))]
async fn fetch_partition(
    location: &PartitionLocation,
) -> Result<SendableRecordBatchStream> {
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;
    // TODO for shuffle client connections, we should avoid creating new connections again and again.
    // And we should also avoid to keep alive too many connections for long time.
    let host = metadata.host.as_str();
    let port = metadata.port as u16;
    let mut ballista_client = BallistaClient::try_new(host, port)
        .await
        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
    ballista_client
        .fetch_partition(
            &partition_id.job_id,
            partition_id.stage_id as usize,
            partition_id.partition_id as usize,
            &location.path,
            host,
            port,
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))
}

#[cfg(test)]
async fn fetch_partition(
    location: &PartitionLocation,
) -> Result<SendableRecordBatchStream> {
    tests::fetch_test_partition(location)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::scheduler::{ExecutorMetadata, ExecutorSpecification, PartitionId};
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::common;
    use datafusion::physical_plan::stream::RecordBatchReceiverStream;

    #[tokio::test]
    async fn test_stats_for_partitions_empty() {
        let result = stats_for_partitions(std::iter::empty());

        let exptected = Statistics {
            is_exact: true,
            num_rows: Some(0),
            total_byte_size: Some(0),
            column_statistics: None,
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_stats_for_partitions_full() {
        let part_stats = vec![
            PartitionStats {
                num_rows: Some(10),
                num_bytes: Some(84),
                num_batches: Some(1),
            },
            PartitionStats {
                num_rows: Some(4),
                num_bytes: Some(65),
                num_batches: None,
            },
        ];

        let result = stats_for_partitions(part_stats.into_iter());

        let exptected = Statistics {
            is_exact: true,
            num_rows: Some(14),
            total_byte_size: Some(149),
            column_statistics: None,
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_stats_for_partitions_missing() {
        let part_stats = vec![
            PartitionStats {
                num_rows: Some(10),
                num_bytes: Some(84),
                num_batches: Some(1),
            },
            PartitionStats {
                num_rows: None,
                num_bytes: None,
                num_batches: None,
            },
        ];

        let result = stats_for_partitions(part_stats.into_iter());

        let exptected = Statistics {
            is_exact: true,
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_send_fetch_partitions_1() {
        test_send_fetch_partitions(1, 10).await;
    }

    #[tokio::test]
    async fn test_send_fetch_partitions_n() {
        test_send_fetch_partitions(4, 10).await;
    }

    async fn test_send_fetch_partitions(max_request_num: usize, partition_num: usize) {
        let schema = get_test_partition_schema();
        let partition_locations = get_test_partition_locations(partition_num);
        let response_receiver =
            send_fetch_partitions(partition_locations, max_request_num);

        let stream = RecordBatchStreamAdapter::new(
            Arc::new(schema),
            response_receiver.try_flatten(),
        );

        let result = common::collect(Box::pin(stream)).await.unwrap();
        assert_eq!(partition_num, result.len());
    }

    fn get_test_partition_locations(n: usize) -> Vec<PartitionLocation> {
        (0..n)
            .into_iter()
            .map(|partition_id| PartitionLocation {
                partition_id: PartitionId {
                    job_id: "job".to_string(),
                    stage_id: 1,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: format!("exec{}", partition_id),
                    host: "localhost".to_string(),
                    port: 50051,
                    grpc_port: 50052,
                    specification: ExecutorSpecification { task_slots: 12 },
                },
                partition_stats: Default::default(),
                path: format!("/tmp/job/1/{}", partition_id),
            })
            .collect()
    }

    pub(crate) fn fetch_test_partition(
        location: &PartitionLocation,
    ) -> Result<SendableRecordBatchStream> {
        let id_array = Int32Array::from(vec![location.partition_id.partition_id as i32]);
        let schema = Arc::new(get_test_partition_schema());

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)])?;

        let (tx, rx) = tokio::sync::mpsc::channel(2);

        // task simply sends data in order but in a separate
        // thread (to ensure the batches are not available without the
        // DelayedStream yielding).
        let join_handle = tokio::task::spawn(async move {
            println!("Sending batch via delayed stream");
            if let Err(e) = tx.send(Ok(batch)).await {
                println!("ERROR batch via delayed stream: {}", e);
            }
        });

        // returned stream simply reads off the rx stream
        Ok(RecordBatchReceiverStream::create(&schema, rx, join_handle))
    }

    fn get_test_partition_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int32, false)])
    }
}
