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

//! The writer and reader run against each other in-process, through the
//! registry, so apart from the connection-sharing test they need no sockets.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::expressions::col;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{StreamExt, TryStreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};

use crate::serving::BoxedFlightStream;

use super::{PipelinedRegistry, PipelinedShuffleReaderExec, PipelinedShuffleWriterExec};
use crate::config::{
    BALLISTA_CLIENT_IO_RETRIES_TIMES, BALLISTA_CLIENT_IO_RETRY_WAIT_TIME_MS,
    BALLISTA_PIPELINED_QUEUE_CAPACITY, BALLISTA_PIPELINED_REGISTRATION_TIMEOUT_SECONDS,
};
use crate::execution_plans::ChaosExec;
use crate::extension::SessionConfigExt;
use crate::serde::scheduler::{
    ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    PartitionId, PipelinedPartitionLocation,
};

const EXECUTOR: &str = "local";
const STAGE: usize = 1;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn batch(keys: impl IntoIterator<Item = i64>) -> RecordBatch {
    let keys: Vec<i64> = keys.into_iter().collect();
    let values: Vec<i64> = keys.iter().map(|k| k * 10).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn input(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&partitions, schema(), None).unwrap()
}

fn task_ctx(queue_capacity: Option<usize>) -> Arc<TaskContext> {
    let mut config = SessionConfig::new_with_ballista();
    if let Some(capacity) = queue_capacity {
        config
            .options_mut()
            .set(BALLISTA_PIPELINED_QUEUE_CAPACITY, &capacity.to_string())
            .unwrap();
    }
    SessionContext::new_with_config(config).task_ctx()
}

/// A context for remote reads that retries a failed stream open `retries`
/// times, `wait_ms` apart.
fn remote_task_ctx(retries: usize, wait_ms: u64) -> Arc<TaskContext> {
    let mut config = SessionConfig::new_with_ballista();
    let options = config.options_mut();
    options
        .set(BALLISTA_CLIENT_IO_RETRIES_TIMES, &retries.to_string())
        .unwrap();
    options
        .set(BALLISTA_CLIENT_IO_RETRY_WAIT_TIME_MS, &wait_ms.to_string())
        .unwrap();
    SessionContext::new_with_config(config).task_ctx()
}

fn location(task_id: usize, partition: usize) -> PipelinedPartitionLocation {
    PipelinedPartitionLocation {
        partition_id: PartitionId::new(&"job".into(), STAGE, partition),
        task_id,
        executor_meta: ExecutorMetadata {
            id: EXECUTOR.to_owned(),
            host: "localhost".to_owned(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification::default(),
            os_info: ExecutorOperatingSystemSpecification::default(),
        },
    }
}

fn hash_on_k(n: usize) -> Partitioning {
    Partitioning::Hash(vec![col("k", schema().as_ref()).unwrap()], n)
}

fn writer(
    task_id: usize,
    input: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
    broadcast: bool,
    registry: &Arc<PipelinedRegistry>,
) -> PipelinedShuffleWriterExec {
    PipelinedShuffleWriterExec::try_new(
        "job".into(),
        STAGE,
        input,
        partitioning,
        broadcast,
    )
    .unwrap()
    .with_task_id(task_id)
    .with_registry(registry.clone())
}

async fn read_partition(
    reader: &PipelinedShuffleReaderExec,
    partition: usize,
    ctx: Arc<TaskContext>,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    reader.execute(partition, ctx)?.try_collect().await
}

fn keys(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn hash_exchange_delivers_every_row_to_exactly_one_partition() {
    let registry = Arc::new(PipelinedRegistry::new(EXECUTOR));
    let input = input(vec![
        vec![batch(0..100), batch(100..200)],
        vec![batch(200..300), batch(300..400)],
    ]);
    let writer = writer(0, input, Some(hash_on_k(3)), false, &registry);
    let reader = PipelinedShuffleReaderExec::try_new(
        STAGE,
        (0..3).map(|p| vec![location(0, p)]).collect(),
        schema(),
        hash_on_k(3),
    )
    .unwrap()
    .with_registry(registry.clone());

    // The readers start first, so they also exercise waiting for the producer
    // to register.
    let ctx = task_ctx(None);
    let reads = futures::future::try_join_all(
        (0..3).map(|p| read_partition(&reader, p, ctx.clone())),
    );
    let (partitions, stats) = tokio::join!(reads, writer.execute_pipelined(ctx.clone()));
    let partitions = partitions.unwrap();
    let stats = stats.unwrap();

    let mut seen = BTreeMap::new();
    for (p, batches) in partitions.iter().enumerate() {
        for k in keys(batches) {
            assert!(seen.insert(k, p).is_none(), "key {k} delivered twice");
        }
        // Hash outputs are coalesced back up to the batch size, not sent as
        // the slivers hashing leaves behind.
        assert!(
            batches.len() <= 2,
            "partition {p} has {} batches",
            batches.len()
        );
    }
    assert_eq!(
        seen.keys().copied().collect::<Vec<_>>(),
        (0..400).collect::<Vec<_>>()
    );

    let stats: BTreeMap<u64, u64> =
        stats.iter().map(|s| (s.partition_id, s.num_rows)).collect();
    for (p, batches) in partitions.iter().enumerate() {
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(stats[&(p as u64)], rows as u64);
    }
}

#[tokio::test]
async fn consumer_partition_merges_every_producer_task() {
    let registry = Arc::new(PipelinedRegistry::new(EXECUTOR));
    let writers = [
        writer(
            0,
            input(vec![vec![batch(0..50)]]),
            Some(hash_on_k(2)),
            false,
            &registry,
        ),
        writer(
            1,
            input(vec![vec![batch(50..100)]]),
            Some(hash_on_k(2)),
            false,
            &registry,
        ),
    ];
    let reader = PipelinedShuffleReaderExec::try_new(
        STAGE,
        (0..2)
            .map(|p| vec![location(0, p), location(1, p)])
            .collect(),
        schema(),
        hash_on_k(2),
    )
    .unwrap()
    .with_registry(registry.clone());

    let ctx = task_ctx(Some(1));
    let reads = futures::future::try_join_all(
        (0..2).map(|p| read_partition(&reader, p, ctx.clone())),
    );
    let writes = futures::future::try_join_all(
        writers.iter().map(|w| w.execute_pipelined(ctx.clone())),
    );
    let (partitions, writes) = tokio::join!(reads, writes);
    writes.unwrap();

    let mut all: Vec<i64> = partitions.unwrap().iter().flat_map(|b| keys(b)).collect();
    all.sort_unstable();
    assert_eq!(all, (0..100).collect::<Vec<_>>());
}

#[tokio::test]
async fn passthrough_exchange_keeps_input_partitions_under_their_global_ids() {
    let registry = Arc::new(PipelinedRegistry::new(EXECUTOR));
    let writer = writer(
        0,
        input(vec![vec![batch(0..10)], vec![batch(10..20)]]),
        None,
        false,
        &registry,
    )
    .with_global_output_partition_ids(vec![4, 7]);
    assert_eq!(writer.output_partition_ids(), vec![4, 7]);

    let reader = PipelinedShuffleReaderExec::try_new(
        STAGE,
        vec![vec![location(0, 4)], vec![location(0, 7)]],
        schema(),
        Partitioning::UnknownPartitioning(2),
    )
    .unwrap()
    .with_registry(registry.clone());

    let ctx = task_ctx(None);
    let reads = futures::future::try_join_all(
        (0..2).map(|p| read_partition(&reader, p, ctx.clone())),
    );
    let (partitions, stats) = tokio::join!(reads, writer.execute_pipelined(ctx.clone()));
    let partitions = partitions.unwrap();
    stats.unwrap();
    assert_eq!(keys(&partitions[0]), (0..10).collect::<Vec<_>>());
    assert_eq!(keys(&partitions[1]), (10..20).collect::<Vec<_>>());
}

#[tokio::test]
async fn broadcast_outputs_replay_to_every_consumer_task() {
    let registry = Arc::new(PipelinedRegistry::new(EXECUTOR));
    let writer = writer(
        0,
        input(vec![vec![batch(0..10)], vec![batch(10..20)]]),
        None,
        true,
        &registry,
    );
    // Two consumer tasks, each reading every partition in full.
    let readers: Vec<_> = (0..2)
        .map(|_| {
            PipelinedShuffleReaderExec::try_new_broadcast(
                STAGE,
                vec![location(0, 0), location(0, 1)],
                schema(),
            )
            .unwrap()
            .with_registry(registry.clone())
        })
        .collect();

    let ctx = task_ctx(Some(1));
    writer.execute_pipelined(ctx.clone()).await.unwrap();
    for reader in &readers {
        let mut all = keys(&read_partition(reader, 0, ctx.clone()).await.unwrap());
        all.sort_unstable();
        assert_eq!(all, (0..20).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn producer_failure_fails_the_consumer_with_the_cause() {
    let registry = Arc::new(PipelinedRegistry::new(EXECUTOR));
    let failing: Arc<dyn ExecutionPlan> = Arc::new(
        ChaosExec::new(input(vec![vec![batch(0..10)]]), 1.0, "fatal", Some(7)).unwrap(),
    );
    let writer = writer(0, failing, Some(hash_on_k(2)), false, &registry);
    let reader = PipelinedShuffleReaderExec::try_new(
        STAGE,
        (0..2).map(|p| vec![location(0, p)]).collect(),
        schema(),
        hash_on_k(2),
    )
    .unwrap()
    .with_registry(registry.clone());

    let ctx = task_ctx(None);
    let (read, write) = tokio::join!(
        read_partition(&reader, 0, ctx.clone()),
        writer.execute_pipelined(ctx)
    );
    assert!(write.is_err());
    let err = read.unwrap_err().to_string();
    assert!(err.contains("Injected FATAL FAILURE"), "{err}");
}

#[tokio::test]
async fn producer_stops_when_its_consumer_goes_away() {
    let registry = Arc::new(PipelinedRegistry::new(EXECUTOR));
    // Far more batches than the queue holds, so the producer can only finish
    // by noticing that its consumer has gone.
    let batches = (0..100).map(|i| batch(i * 10..(i + 1) * 10)).collect();
    let writer = writer(0, input(vec![batches]), None, false, &registry);
    let reader = PipelinedShuffleReaderExec::try_new(
        STAGE,
        vec![vec![location(0, 0)]],
        schema(),
        Partitioning::UnknownPartitioning(1),
    )
    .unwrap()
    .with_registry(registry.clone());

    let ctx = task_ctx(Some(1));
    let consume_one = async {
        let mut stream = reader.execute(0, ctx.clone()).unwrap();
        let first = stream.next().await.unwrap().unwrap();
        // Dropping the stream is how a satisfied LIMIT stops reading.
        drop(stream);
        first.num_rows()
    };
    let (rows, write) = tokio::join!(
        consume_one,
        tokio::time::timeout(
            Duration::from_secs(10),
            writer.execute_pipelined(ctx.clone())
        )
    );
    assert_eq!(rows, 10);
    let stats = write
        .expect("producer should stop once its consumer is gone")
        .unwrap();
    assert!(stats[0].num_rows < 1000, "producer kept sending to nobody");
}

/// A consumer that never comes, such as a client that disconnected before it
/// read anything, fails the producer rather than leaving it, and the vcores
/// its job holds, waiting forever.
#[tokio::test]
async fn producer_fails_when_no_consumer_opens_its_full_queue() {
    let registry = Arc::new(PipelinedRegistry::new(EXECUTOR));
    let batches = (0..100).map(|i| batch(i * 10..(i + 1) * 10)).collect();
    let writer = writer(0, input(vec![batches]), None, false, &registry);
    let mut config = SessionConfig::new_with_ballista();
    config
        .options_mut()
        .set(BALLISTA_PIPELINED_QUEUE_CAPACITY, "1")
        .unwrap();
    config
        .options_mut()
        .set(BALLISTA_PIPELINED_REGISTRATION_TIMEOUT_SECONDS, "0")
        .unwrap();
    let ctx = SessionContext::new_with_config(config).task_ctx();

    let err =
        tokio::time::timeout(Duration::from_secs(10), writer.execute_pipelined(ctx))
            .await
            .expect("producer should give up on a queue nobody opens")
            .unwrap_err();
    assert!(err.to_string().contains("no consumer opened"), "{err}");
}

#[tokio::test]
async fn empty_input_still_ends_every_output() {
    let registry = Arc::new(PipelinedRegistry::new(EXECUTOR));
    let writer = writer(0, input(vec![vec![]]), Some(hash_on_k(2)), false, &registry);
    let reader = PipelinedShuffleReaderExec::try_new(
        STAGE,
        (0..2).map(|p| vec![location(0, p)]).collect(),
        schema(),
        hash_on_k(2),
    )
    .unwrap()
    .with_registry(registry.clone());

    let ctx = task_ctx(None);
    let reads = futures::future::try_join_all(
        (0..2).map(|p| read_partition(&reader, p, ctx.clone())),
    );
    let (partitions, write) = tokio::join!(reads, writer.execute_pipelined(ctx.clone()));
    write.unwrap();
    let rows: usize = partitions
        .unwrap()
        .iter()
        .flatten()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn restricting_a_reader_keeps_only_the_task_slice() {
    let reader = PipelinedShuffleReaderExec::try_new(
        STAGE,
        (0..4).map(|p| vec![location(0, p)]).collect(),
        schema(),
        hash_on_k(4),
    )
    .unwrap();
    let restricted = reader.with_partitions(&[3, 1]).unwrap();
    let partitions: Vec<BTreeSet<usize>> = restricted
        .partition
        .iter()
        .map(|locs| locs.iter().map(|l| l.partition_id.partition_id).collect())
        .collect();
    assert_eq!(partitions, vec![BTreeSet::from([3]), BTreeSet::from([1])]);
    assert_eq!(
        restricted
            .properties()
            .output_partitioning()
            .partition_count(),
        2
    );
}

#[tokio::test]
async fn restricting_a_broadcast_reader_keeps_every_location() {
    let locations: Vec<_> = (0..4).map(|p| location(0, p)).collect();
    let reader =
        PipelinedShuffleReaderExec::try_new_broadcast(STAGE, locations.clone(), schema())
            .unwrap();
    let restricted = reader.with_partitions(&[3]).unwrap();
    assert!(restricted.broadcast);
    assert_eq!(restricted.partition, vec![locations]);
}

/// A pool that counts the connections it hands out per executor. Its clients
/// point at a port nothing listens on, so every fetch fails.
#[derive(Debug, Default)]
struct CountingPool {
    acquired: parking_lot::Mutex<BTreeMap<u16, usize>>,
}

#[async_trait::async_trait]
impl crate::client_pool::BallistaClientPool for CountingPool {
    async fn acquire(
        &self,
        host: &str,
        port: u16,
        _config: &crate::utils::GrpcClientConfig,
        _customize_endpoint: Option<Arc<crate::extension::BallistaConfigGrpcEndpoint>>,
    ) -> crate::error::Result<crate::client_pool::PooledClient> {
        *self.acquired.lock().entry(port).or_default() += 1;
        Ok(crate::client_pool::PooledClient::new(
            crate::client::BallistaClient::new_for_test(host, port),
            Box::new(|_| ()),
        ))
    }

    async fn evict_idle(&self) {}
}

fn unused_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn remote_streams_to_one_executor_share_a_connection() {
    let (port_a, port_b) = (unused_port(), unused_port());
    let remote = |task_id: usize, port: u16| {
        let mut location = location(task_id, 0);
        location.executor_meta.id = format!("remote-{port}");
        location.executor_meta.host = "127.0.0.1".to_owned();
        location.executor_meta.port = port;
        location
    };
    // 130 producer tasks on one executor need three connections at 64 streams
    // each; two on another executor need one.
    let mut locations: Vec<_> = (0..130).map(|t| remote(t, port_a)).collect();
    locations.extend((130..132).map(|t| remote(t, port_b)));
    let pool = Arc::new(CountingPool::default());
    let reader = PipelinedShuffleReaderExec::try_new(
        STAGE,
        vec![locations],
        schema(),
        Partitioning::UnknownPartitioning(1),
    )
    .unwrap()
    .with_client_pool(pool.clone());

    // Nothing listens on either port, so each open fails as a connection
    // failure; with no retries, each slot connects exactly once.
    let results: Vec<_> = reader
        .execute(0, remote_task_ctx(0, 0))
        .unwrap()
        .collect()
        .await;
    assert!(results.iter().all(|r| r.is_err()));
    assert_eq!(
        *pool.acquired.lock(),
        BTreeMap::from([(port_a, 3), (port_b, 1)])
    );
}

/// A Flight server whose `do_get` answers every ticket with `response`,
/// either the batches of a partition or a failure.
struct TestFlightService {
    response: Result<Vec<RecordBatch>, String>,
}

#[tonic::async_trait]
impl arrow_flight::flight_service_server::FlightService for TestFlightService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        _: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        match &self.response {
            Ok(batches) => {
                let data = batches_to_flight_data(&schema(), batches.clone())
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(Box::pin(futures::stream::iter(
                    data.into_iter().map(Ok),
                ))))
            }
            Err(message) => Err(Status::internal(message.clone())),
        }
    }

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

/// Serves `service` on a port of its own and returns the port.
async fn serve_flight(service: TestFlightService) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    port
}

/// Listens on a port of its own and relays connections to `target`, except
/// the first, which is reset as soon as it is accepted: what a client sees
/// when a listen queue overflows and the kernel resets its half-open
/// connection.
async fn resetting_proxy(target: u16) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut accepted = 0;
        while let Ok((mut client, _)) = listener.accept().await {
            accepted += 1;
            if accepted == 1 {
                // Closing with a zero linger sends RST instead of FIN. tokio
                // deprecates the setter because a nonzero linger blocks the
                // thread on drop; a zero one closes at once.
                #[allow(deprecated)]
                client.set_linger(Some(Duration::ZERO)).unwrap();
                drop(client);
                continue;
            }
            tokio::spawn(async move {
                let mut server = TcpStream::connect(("127.0.0.1", target)).await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            });
        }
    });
    port
}

fn remote_location(port: u16) -> PipelinedPartitionLocation {
    let mut location = location(0, 0);
    location.executor_meta.id = format!("remote-{port}");
    location.executor_meta.host = "127.0.0.1".to_owned();
    location.executor_meta.port = port;
    location
}

fn remote_reader(port: u16, pool: &Arc<CountingPool>) -> PipelinedShuffleReaderExec {
    PipelinedShuffleReaderExec::try_new(
        STAGE,
        vec![vec![remote_location(port)]],
        schema(),
        Partitioning::UnknownPartitioning(1),
    )
    .unwrap()
    .with_client_pool(pool.clone())
}

#[tokio::test]
async fn remote_stream_open_is_retried_on_a_new_connection_after_a_reset() {
    let server = serve_flight(TestFlightService {
        response: Ok(vec![batch([1, 2, 3])]),
    })
    .await;
    let port = resetting_proxy(server).await;
    let pool = Arc::new(CountingPool::default());
    let reader = remote_reader(port, &pool);

    let batches = read_partition(&reader, 0, remote_task_ctx(3, 10))
        .await
        .unwrap();

    assert_eq!(keys(&batches), vec![1, 2, 3]);
    // The reset connection was discarded and a second one opened.
    assert_eq!(*pool.acquired.lock(), BTreeMap::from([(port, 2)]));
}

#[tokio::test]
async fn remote_stream_failure_the_server_answered_is_not_retried() {
    let port = serve_flight(TestFlightService {
        response: Err("producer failed".to_owned()),
    })
    .await;
    let pool = Arc::new(CountingPool::default());
    let reader = remote_reader(port, &pool);

    let err = read_partition(&reader, 0, remote_task_ctx(3, 10))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("producer failed"), "{err}");
    assert_eq!(*pool.acquired.lock(), BTreeMap::from([(port, 1)]));
}
