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

use async_trait::async_trait;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::common::stats::Precision;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::pin::Pin;
use std::result;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::client::BallistaClient;
use crate::execution_plans::sort_shuffle::{
    get_index_path, is_sort_shuffle_output, stream_sort_shuffle_partition,
};
use crate::extension::SessionConfigExt;
use crate::serde::scheduler::{PartitionLocation, PartitionStats};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt, TryStreamExt};

use crate::error::BallistaError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use itertools::Itertools;
use log::{debug, error, trace};
use rand::prelude::SliceRandom;
use rand::rng;
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;

/// ShuffleReaderExec reads partitions that have already been materialized by a ShuffleWriterExec
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// The query stage id to read from
    pub stage_id: usize,
    pub(crate) schema: SchemaRef,
    /// Each partition of a shuffle can read data from multiple locations
    pub partition: Vec<Vec<PartitionLocation>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl ShuffleReaderExec {
    /// Create a new ShuffleReaderExec
    pub fn try_new(
        stage_id: usize,
        partition: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
        partitioning: Partitioning,
    ) -> Result<Self> {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Ok(Self {
            stage_id,
            schema,
            partition,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }
}

impl DisplayAs for ShuffleReaderExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ShuffleReaderExec: partitioning: {}",
                    self.properties.partitioning,
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitioning={}", self.properties.partitioning)
            }
        }
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn name(&self) -> &str {
        "ShuffleReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "Ballista ShuffleReaderExec does not support children plans".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_id = context.task_id().unwrap_or_else(|| partition.to_string());
        debug!("ShuffleReaderExec::execute({task_id})");

        let config = context.session_config();

        let max_request_num =
            config.ballista_shuffle_reader_maximum_concurrent_requests();
        let max_message_size = config.ballista_grpc_client_max_message_size();
        let force_remote_read = config.ballista_shuffle_reader_force_remote_read();
        let prefer_flight = config.ballista_shuffle_reader_remote_prefer_flight();

        if force_remote_read {
            debug!(
                "All shuffle partitions will be read as remote partitions! To disable this behavior set: `{}=false`",
                crate::config::BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ
            );
        }

        log::debug!(
            "ShuffleReaderExec::execute({task_id}) max_request_num: {max_request_num}, max_message_size: {max_message_size}"
        );
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
        partition_locations.shuffle(&mut rng());
        let response_receiver = send_fetch_partitions(
            partition_locations,
            max_request_num,
            max_message_size,
            force_remote_read,
            prefer_flight,
        );

        let result = RecordBatchStreamAdapter::new(
            Arc::new(self.schema.as_ref().clone()),
            response_receiver.try_flatten(),
        );
        Ok(Box::pin(result))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if let Some(idx) = partition {
            let partition_count = self.properties().partitioning.partition_count();
            if idx >= partition_count {
                return datafusion::common::internal_err!(
                    "Invalid partition index: {}, the partition count is {}",
                    idx,
                    partition_count
                );
            }
            let stat_for_partition =
                stats_for_partition(idx, self.schema.fields().len(), &self.partition);

            trace!(
                "shuffle reader at stage: {} and partition {} returned statistics: {:?}",
                self.stage_id, idx, stat_for_partition
            );
            stat_for_partition
        } else {
            let stats_for_partitions = stats_for_partitions(
                self.schema.fields().len(),
                self.partition
                    .iter()
                    .flatten()
                    .map(|loc| loc.partition_stats),
            );
            trace!(
                "shuffle reader at stage: {} returned statistics for all partitions: {:?}",
                self.stage_id, stats_for_partitions
            );
            Ok(stats_for_partitions)
        }
    }
}

fn stats_for_partition(
    partition: usize,
    num_fields: usize,
    partition_locations: &[Vec<PartitionLocation>],
) -> Result<Statistics> {
    // TODO stats: add column statistics to PartitionStats
    let (num_rows, total_byte_size) = partition_locations
        .iter()
        .map(|location| {
            // extract requested partitions
            location
                .get(partition)
                .map(|p| p.partition_stats)
                .map(|p| (p.num_rows, p.num_bytes))
                .unwrap_or_default()
        })
        .fold(
            (Some(0), Some(0)),
            |(num_rows, total_byte_size), (rows, bytes)| {
                (
                    num_rows.zip(rows).map(|(a, b)| a + b as usize),
                    total_byte_size.zip(bytes).map(|(a, b)| a + b as usize),
                )
            },
        );

    Ok(Statistics {
        num_rows: num_rows.map(Precision::Exact).unwrap_or(Precision::Absent),
        total_byte_size: total_byte_size
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        column_statistics: vec![ColumnStatistics::new_unknown(); num_fields],
    })
}

fn stats_for_partitions(
    num_fields: usize,
    partition_stats: impl Iterator<Item = PartitionStats>,
) -> Statistics {
    // TODO stats: add column statistics to PartitionStats
    let (num_rows, total_byte_size) =
        partition_stats.fold((Some(0), Some(0)), |(num_rows, total_byte_size), part| {
            // if any statistic is unkown it makes the entire statistic unkown
            let num_rows = num_rows.zip(part.num_rows).map(|(a, b)| a + b as usize);
            let total_byte_size = total_byte_size
                .zip(part.num_bytes)
                .map(|(a, b)| a + b as usize);
            (num_rows, total_byte_size)
        });
    Statistics {
        num_rows: num_rows.map(Precision::Exact).unwrap_or(Precision::Absent),
        total_byte_size: total_byte_size
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        column_statistics: vec![ColumnStatistics::new_unknown(); num_fields],
    }
}

struct LocalShuffleStream {
    reader: StreamReader<BufReader<File>>,
}

impl LocalShuffleStream {
    pub fn new(reader: StreamReader<BufReader<File>>) -> Self {
        LocalShuffleStream { reader }
    }
}

impl Stream for LocalShuffleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.reader.next() {
            return Poll::Ready(Some(batch.map_err(|e| e.into())));
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for LocalShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// Adapter for a tokio ReceiverStream that implements the SendableRecordBatchStream interface
struct AbortableReceiverStream {
    inner: ReceiverStream<result::Result<SendableRecordBatchStream, BallistaError>>,

    #[allow(dead_code)]
    drop_helper: Vec<SpawnedTask<()>>,
}

impl AbortableReceiverStream {
    /// Construct a new SendableRecordBatchReceiverStream which will send batches of the specified schema from inner
    pub fn create(
        rx: tokio::sync::mpsc::Receiver<
            result::Result<SendableRecordBatchStream, BallistaError>,
        >,
        spawned_tasks: Vec<SpawnedTask<()>>,
    ) -> AbortableReceiverStream {
        let inner = ReceiverStream::new(rx);
        Self {
            inner,
            drop_helper: spawned_tasks,
        }
    }
}

impl Stream for AbortableReceiverStream {
    type Item = result::Result<SendableRecordBatchStream, ArrowError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner
            .poll_next_unpin(cx)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
    }
}
/// Splits the provided partition locations into local and remote partitions.
/// Local partitions are read directly from local Arrow IPC files,
/// while remote partitions are fetched using the Arrow Flight client.
/// If `force_remote_read` is true, all partitions are treated as remote.
fn local_remote_read_split(
    partition_locations: Vec<PartitionLocation>,
    force_remote_read: bool,
) -> (Vec<PartitionLocation>, Vec<PartitionLocation>) {
    if !force_remote_read {
        partition_locations
            .into_iter()
            .partition(check_is_local_location)
    } else {
        (vec![], partition_locations)
    }
}

fn send_fetch_partitions(
    partition_locations: Vec<PartitionLocation>,
    max_request_num: usize,
    max_message_size: usize,
    force_remote_read: bool,
    flight_transport: bool,
) -> AbortableReceiverStream {
    let (response_sender, response_receiver) = mpsc::channel(max_request_num);
    let semaphore = Arc::new(Semaphore::new(max_request_num));
    let mut spawned_tasks: Vec<SpawnedTask<()>> = vec![];

    let (local_locations, remote_locations): (Vec<_>, Vec<_>) =
        local_remote_read_split(partition_locations, force_remote_read);

    debug!(
        "local shuffle file counts:{}, remote shuffle file count:{}.",
        local_locations.len(),
        remote_locations.len()
    );

    // keep local shuffle files reading in serial order for memory control.
    let response_sender_c = response_sender.clone();
    spawned_tasks.push(SpawnedTask::spawn(async move {
        for p in local_locations {
            let r = PartitionReaderEnum::Local
                .fetch_partition(&p, max_message_size, flight_transport)
                .await;
            if let Err(e) = response_sender_c.send(r).await {
                error!("Fail to send response event to the channel due to {e}");
            }
        }
    }));

    for p in remote_locations.into_iter() {
        let semaphore = semaphore.clone();
        let response_sender = response_sender.clone();
        spawned_tasks.push(SpawnedTask::spawn(async move {
            // Block if exceeds max request number.
            let permit = semaphore.acquire_owned().await.unwrap();
            let r = PartitionReaderEnum::FlightRemote
                .fetch_partition(&p, max_message_size, flight_transport)
                .await;
            // Block if the channel buffer is full.
            if let Err(e) = response_sender.send(r).await {
                error!("Fail to send response event to the channel due to {e}");
            }
            // Increase semaphore by dropping existing permits.
            drop(permit);
        }));
    }

    AbortableReceiverStream::create(response_receiver, spawned_tasks)
}

fn check_is_local_location(location: &PartitionLocation) -> bool {
    std::path::Path::new(location.path.as_str()).exists()
}

/// Partition reader Trait, different partition reader can have
#[async_trait]
trait PartitionReader: Send + Sync + Clone {
    // Read partition data from PartitionLocation
    async fn fetch_partition(
        &self,
        location: &PartitionLocation,
        max_message_size: usize,
        flight_transport: bool,
    ) -> result::Result<SendableRecordBatchStream, BallistaError>;
}

#[derive(Clone)]
enum PartitionReaderEnum {
    Local,
    FlightRemote,
    #[allow(dead_code)]
    ObjectStoreRemote,
}

#[async_trait]
impl PartitionReader for PartitionReaderEnum {
    // Notice return `BallistaError::FetchFailed` will let scheduler re-schedule the task.
    async fn fetch_partition(
        &self,
        location: &PartitionLocation,
        max_message_size: usize,
        flight_transport: bool,
    ) -> result::Result<SendableRecordBatchStream, BallistaError> {
        match self {
            PartitionReaderEnum::FlightRemote => {
                fetch_partition_remote(location, max_message_size, flight_transport).await
            }
            PartitionReaderEnum::Local => fetch_partition_local(location).await,
            PartitionReaderEnum::ObjectStoreRemote => {
                fetch_partition_object_store(location).await
            }
        }
    }
}

async fn fetch_partition_remote(
    location: &PartitionLocation,
    max_message_size: usize,
    flight_transport: bool,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;
    // TODO for shuffle client connections, we should avoid creating new connections again and again.
    // And we should also avoid to keep alive too many connections for long time.
    let host = metadata.host.as_str();
    let port = metadata.port;
    let mut ballista_client = BallistaClient::try_new(host, port, max_message_size)
        .await
        .map_err(|error| match error {
            // map grpc connection error to partition fetch error.
            BallistaError::GrpcConnectionError(msg) => BallistaError::FetchFailed(
                metadata.id.clone(),
                partition_id.stage_id,
                partition_id.partition_id,
                msg,
            ),
            other => other,
        })?;

    ballista_client
        .fetch_partition(
            &metadata.id,
            partition_id,
            &location.path,
            host,
            port,
            flight_transport,
        )
        .await
}

async fn fetch_partition_local(
    location: &PartitionLocation,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    let path = &location.path;
    let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;
    let data_path = std::path::Path::new(path);

    // Check if this is a sort-based shuffle output (has index file)
    if is_sort_shuffle_output(data_path) {
        debug!(
            "Reading sort-based shuffle for partition {} from {:?}",
            partition_id.partition_id, data_path
        );
        let index_path = get_index_path(data_path);
        return stream_sort_shuffle_partition(
            data_path,
            &index_path,
            partition_id.partition_id,
        )
        .map_err(|e| {
            BallistaError::FetchFailed(
                metadata.id.clone(),
                partition_id.stage_id,
                partition_id.partition_id,
                e.to_string(),
            )
        });
    }

    // Standard hash-based shuffle - read the file directly
    let reader = fetch_partition_local_inner(path).map_err(|e| {
        // return BallistaError::FetchFailed may let scheduler retry this task.
        BallistaError::FetchFailed(
            metadata.id.clone(),
            partition_id.stage_id,
            partition_id.partition_id,
            e.to_string(),
        )
    })?;
    Ok(Box::pin(LocalShuffleStream::new(reader)))
}

fn fetch_partition_local_inner(
    path: &str,
) -> result::Result<StreamReader<BufReader<File>>, BallistaError> {
    let file = File::open(path).map_err(|e| {
        BallistaError::General(format!("Failed to open partition file at {path}: {e:?}"))
    })?;
    let file = BufReader::new(file);
    // Safety: setting `skip_validation` requires `unsafe`, user assures data is valid
    let reader = unsafe {
        StreamReader::try_new(file, None)
            .map_err(|e| {
                BallistaError::General(format!(
                    "Failed to create new arrow StreamReader at {path}: {e:?}"
                ))
            })?
            .with_skip_validation(cfg!(feature = "arrow-ipc-optimizations"))
    };

    Ok(reader)
}

async fn fetch_partition_object_store(
    _location: &PartitionLocation,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    Err(BallistaError::NotImplemented(
        "Should not use ObjectStorePartitionReader".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::ShuffleWriterExec;
    use crate::serde::scheduler::{ExecutorMetadata, ExecutorSpecification, PartitionId};
    use crate::utils;
    use datafusion::arrow::array::{Int32Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::writer::StreamWriter;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common;

    use datafusion::prelude::SessionContext;
    use tempfile::{TempDir, tempdir};

    #[tokio::test]
    async fn test_stats_for_partitions_empty() {
        let result = stats_for_partitions(0, std::iter::empty());

        let exptected = Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: vec![],
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

        let result = stats_for_partitions(0, part_stats.into_iter());

        let exptected = Statistics {
            num_rows: Precision::Exact(14),
            total_byte_size: Precision::Exact(149),
            column_statistics: vec![],
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

        let result = stats_for_partitions(0, part_stats.into_iter());

        let exptected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };

        assert_eq!(result, exptected);
    }
    #[tokio::test]
    async fn test_stats_for_partition_statistics_no_specific_partition() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let job_id = "test_job_1";
        let input_stage_id = 2;
        let mut partitions: Vec<PartitionLocation> = vec![];
        for partition_id in 0..4 {
            partitions.push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.to_string(),
                    stage_id: input_stage_id,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification { task_slots: 1 },
                },
                partition_stats: PartitionStats {
                    num_rows: Some(1),
                    num_batches: None,
                    num_bytes: Some(10),
                },
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::try_new(
            input_stage_id,
            vec![partitions.clone(), partitions],
            Arc::new(schema),
            Partitioning::UnknownPartitioning(4),
        )?;

        let stats = shuffle_reader_exec.partition_statistics(None)?;
        assert_eq!(8, *stats.num_rows.get_value().unwrap());
        assert_eq!(80, *stats.total_byte_size.get_value().unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_for_partition_statistics_specific_partition() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let job_id = "test_job_1";
        let input_stage_id = 2;
        let mut partitions: Vec<PartitionLocation> = vec![];
        for partition_id in 0..4 {
            partitions.push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.to_string(),
                    stage_id: input_stage_id,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification { task_slots: 1 },
                },
                partition_stats: PartitionStats {
                    num_rows: Some(1),
                    num_batches: None,
                    num_bytes: Some(10),
                },
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::try_new(
            input_stage_id,
            vec![partitions.clone(), partitions],
            Arc::new(schema),
            Partitioning::UnknownPartitioning(4),
        )?;

        let stats = shuffle_reader_exec.partition_statistics(Some(3))?;
        assert_eq!(2, *stats.num_rows.get_value().unwrap());
        assert_eq!(20, *stats.total_byte_size.get_value().unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_for_partition_statistics_specific_partition_out_of_range()
    -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let job_id = "test_job_1";
        let input_stage_id = 2;
        let mut partitions: Vec<PartitionLocation> = vec![];
        for partition_id in 0..4 {
            partitions.push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.to_string(),
                    stage_id: input_stage_id,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification { task_slots: 1 },
                },
                partition_stats: PartitionStats {
                    num_rows: Some(1),
                    num_batches: None,
                    num_bytes: Some(10),
                },
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::try_new(
            input_stage_id,
            vec![partitions.clone(), partitions],
            Arc::new(schema),
            Partitioning::UnknownPartitioning(4),
        )?;

        let stats = shuffle_reader_exec.partition_statistics(Some(4));
        assert!(stats.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_partitions_error_mapping() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let job_id = "test_job_1";
        let input_stage_id = 2;
        let mut partitions: Vec<PartitionLocation> = vec![];
        for partition_id in 0..4 {
            partitions.push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.to_string(),
                    stage_id: input_stage_id,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification { task_slots: 1 },
                },
                partition_stats: Default::default(),
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::try_new(
            input_stage_id,
            vec![partitions],
            Arc::new(schema),
            Partitioning::UnknownPartitioning(4),
        )?;
        let mut stream = shuffle_reader_exec.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream).await;

        assert!(batches.is_err());

        // BallistaError::FetchFailed -> ArrowError::ExternalError -> ballistaError::FetchFailed
        let ballista_error = batches.unwrap_err();
        assert!(matches!(
            ballista_error,
            BallistaError::FetchFailed(_, _, _, _)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_send_fetch_partitions_1() {
        test_send_fetch_partitions(1, 10).await;
    }

    #[tokio::test]
    async fn test_send_fetch_partitions_n() {
        test_send_fetch_partitions(4, 10).await;
    }

    #[tokio::test]
    async fn test_read_local_shuffle() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let work_dir = TempDir::new().unwrap();
        let input = ShuffleWriterExec::try_new(
            "local_file".to_owned(),
            1,
            create_test_data_plan().unwrap(),
            work_dir.path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 1)),
        )
        .unwrap();

        let mut stream = input.execute(0, task_ctx).unwrap();

        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
            .unwrap();

        let path = batches[0].columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // from to input partitions test the first one with two batches
        let file_path = path.value(0);
        let reader = fetch_partition_local_inner(file_path).unwrap();

        let mut stream: Pin<Box<dyn RecordBatchStream + Send>> =
            async { Box::pin(LocalShuffleStream::new(reader)) }.await;

        let result = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
            .unwrap();

        assert_eq!(result.len(), 2);
        for b in result {
            assert_eq!(b, create_test_batch())
        }
    }

    // tests if force remote read configuration option will
    // qualify all partitions as remote
    #[tokio::test]
    async fn test_remote_local_read() {
        let schema = get_test_partition_schema();
        let data_array = Int32Array::from(vec![1]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(data_array)])
                .unwrap();
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("shuffle_data");
        let file = File::create(&file_path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let partition_locations =
            get_test_partition_locations(1, file_path.to_str().unwrap().to_string());

        let (local, remote) = local_remote_read_split(partition_locations.clone(), false);

        assert!(!local.is_empty());
        assert!(remote.is_empty());

        let (local, remote) = local_remote_read_split(partition_locations, true);

        assert!(local.is_empty());
        assert!(!remote.is_empty());
    }

    async fn test_send_fetch_partitions(max_request_num: usize, partition_num: usize) {
        let schema = get_test_partition_schema();
        let data_array = Int32Array::from(vec![1]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(data_array)])
                .unwrap();
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("shuffle_data");
        let file = File::create(&file_path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let partition_locations = get_test_partition_locations(
            partition_num,
            file_path.to_str().unwrap().to_string(),
        );

        let response_receiver = send_fetch_partitions(
            partition_locations,
            max_request_num,
            4 * 1024 * 1024,
            false,
            true,
        );

        let stream = RecordBatchStreamAdapter::new(
            Arc::new(schema),
            response_receiver.try_flatten(),
        );

        let result = common::collect(Box::pin(stream)).await.unwrap();
        assert_eq!(partition_num, result.len());
    }

    fn get_test_partition_locations(n: usize, path: String) -> Vec<PartitionLocation> {
        (0..n)
            .map(|partition_id| PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: "job".to_string(),
                    stage_id: 1,
                    partition_id,
                },
                executor_meta: ExecutorMetadata {
                    id: format!("exec{partition_id}"),
                    host: "localhost".to_string(),
                    port: 50051,
                    grpc_port: 50052,
                    specification: ExecutorSpecification { task_slots: 12 },
                },
                partition_stats: Default::default(),
                path: path.clone(),
            })
            .collect()
    }

    fn get_test_partition_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int32, false)])
    }

    // create two partitions each has two same batches
    fn create_test_data_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let batch = create_test_batch();
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];
        let memory_data_source = Arc::new(MemorySourceConfig::try_new(
            &partitions,
            create_test_schema(),
            None,
        )?);

        Ok(Arc::new(DataSourceExec::new(memory_data_source)))
    }

    fn create_test_batch() -> RecordBatch {
        RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("rust"),
                    Some("datafusion"),
                    Some("ballista"),
                ])),
            ],
        )
        .unwrap()
    }

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("number", DataType::UInt32, true),
            Field::new("str", DataType::Utf8, true),
        ]))
    }
}
