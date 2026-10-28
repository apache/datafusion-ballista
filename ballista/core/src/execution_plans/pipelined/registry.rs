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

//! Executor-wide registry of the output partitions that running pipelined
//! producer tasks serve.
//!
//! A producer task registers its outputs when it starts and writes into them
//! as it runs. A consumer, arriving through the executor's Flight service,
//! opens one output partition and streams it. Either side can arrive first:
//! all tasks of a pipelined job launch together, so a consumer that asks for a
//! partition before its producer has registered waits for it, up to a timeout.
//! The same timeout bounds how long a producer waits on a full queue that no
//! consumer has opened yet, so a consumer that never comes — a client that
//! disconnected, say — fails the job instead of stalling it forever.
//!
//! That bound is measured from the moment the producer registers, not from
//! its last send, so a consumer must open every partition it reads within the
//! timeout of its producer starting. DataFusion operators open their inputs
//! when they are executed, before they drain any of them — a hash join opens
//! its probe side while it is still building — so a consumer task opens all
//! of its partitions as soon as it starts.
//!
//! A producer stays registered until the executor removes its stage or job,
//! which the scheduler asks for as each finishes. Queue partitions are small
//! once claimed; broadcast partitions keep their batches until then.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::Stream;
use tokio::sync::{mpsc, watch};

use crate::JobId;
use crate::serde::scheduler::TaskKey;

/// How many recently cleaned-up jobs the registry remembers, whether the
/// whole job or only some of its stages was cleaned up. A producer task that
/// starts after its job or stage was cleaned up — launched just before the
/// job was cancelled, say, or a leftover task of a stage the scheduler
/// reclaimed when the job succeeded — would otherwise register outputs no
/// consumer will read and wait on them forever. Remembering the job lets it
/// fail instead.
const CLOSED_JOB_MEMORY: usize = 1024;

/// A message on a single-consumer output queue.
#[derive(Debug)]
pub(crate) enum Message {
    Batch(RecordBatch),
    /// The partition is complete. A queue that closes without this marker
    /// belongs to a producer that failed or was cancelled.
    End,
}

/// The output partitions one producer task serves.
pub struct ProducerOutputs {
    schema: SchemaRef,
    /// IPC compression for batches that cross the network, chosen from the
    /// producer's session configuration.
    compression: Option<CompressionType>,
    partitions: HashMap<usize, PartitionOutput>,
    /// The producer's error, if it failed. Reported to a consumer whose
    /// partition ends without being finished.
    failure: OnceLock<String>,
}

impl std::fmt::Debug for ProducerOutputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerOutputs")
            .field("partitions", &self.partitions.keys().collect::<Vec<_>>())
            .finish()
    }
}

enum PartitionOutput {
    /// Read once, by the consumer task that owns the partition. Taken out of
    /// the registry when that consumer connects.
    Queue {
        rx: Mutex<Option<mpsc::Receiver<Message>>>,
        /// Set when the consumer takes `rx`.
        claimed: Arc<AtomicBool>,
    },
    /// Read in full by every consumer task, as the build side of a broadcast
    /// join is.
    Replay(Arc<ReplayBuffer>),
}

/// The producer side of one output partition.
pub(crate) enum PartitionSink {
    Queue(QueueSink),
    Replay(ReplaySink),
}

impl PartitionSink {
    /// Sends a batch, waiting while the consumer is behind. Returns `false`
    /// once the consumer has gone away, after which the partition's remaining
    /// output can be discarded.
    ///
    /// Fails if the queue is full and no consumer has opened it within the
    /// claim timeout.
    pub(crate) async fn send(&self, batch: RecordBatch) -> Result<bool> {
        match self {
            PartitionSink::Queue(queue) => match queue.reserve().await? {
                Some(permit) => {
                    permit.send(Message::Batch(batch));
                    Ok(true)
                }
                None => Ok(false),
            },
            PartitionSink::Replay(sink) => {
                sink.buffer.push(batch);
                Ok(true)
            }
        }
    }

    /// Marks the partition complete. Fails like [`Self::send`].
    pub(crate) async fn finish(&self) -> Result<()> {
        match self {
            PartitionSink::Queue(queue) => {
                // A consumer that has gone away needs no end marker.
                if let Some(permit) = queue.reserve().await? {
                    permit.send(Message::End);
                }
            }
            PartitionSink::Replay(sink) => sink.buffer.end(ReplayEnd::Finished),
        }
        Ok(())
    }
}

/// The producer side of a single-consumer queue.
pub(crate) struct QueueSink {
    partition: usize,
    tx: mpsc::Sender<Message>,
    claimed: Arc<AtomicBool>,
    claim_timeout: Duration,
    /// When the producer stops waiting on a full queue nobody has claimed.
    claim_deadline: tokio::time::Instant,
}

impl QueueSink {
    /// Waits for room in the queue, returning `None` once the consumer has
    /// gone away.
    ///
    /// Until a consumer claims the queue the wait is bounded, because a
    /// consumer that never comes would otherwise leave the producer, and the
    /// vcores its job holds, waiting forever. Waiting on a reservation rather
    /// than a send means a timeout loses no batch.
    async fn reserve(&self) -> Result<Option<mpsc::Permit<'_, Message>>> {
        if !self.claimed.load(Ordering::Acquire) {
            match tokio::time::timeout_at(self.claim_deadline, self.tx.reserve()).await {
                Ok(permit) => return Ok(permit.ok()),
                // Claimed just as the deadline passed: wait for the consumer.
                Err(_) if self.claimed.load(Ordering::Acquire) => {}
                Err(_) => {
                    return Err(DataFusionError::Execution(format!(
                        "no consumer opened output partition {} within {:?}",
                        self.partition, self.claim_timeout
                    )));
                }
            }
        }
        Ok(self.tx.reserve().await.ok())
    }
}

impl ProducerOutputs {
    /// Creates the outputs for `partitions`, returning them together with one
    /// sink per partition, in the same order, for the producer to write into.
    ///
    /// Queues hold at most `capacity` batches, so a producer waits for a slow
    /// consumer rather than growing memory. Broadcast outputs are retained in
    /// full instead, because every consumer task reads all of them; their
    /// size is bounded by the planner's broadcast threshold.
    ///
    /// A producer waits on a full queue that no consumer has opened only
    /// until `claim_timeout` from now; then its send fails.
    pub(crate) fn new(
        schema: SchemaRef,
        partitions: impl IntoIterator<Item = usize>,
        broadcast: bool,
        capacity: usize,
        compression: Option<CompressionType>,
        claim_timeout: Duration,
    ) -> (Arc<Self>, Vec<PartitionSink>) {
        let claim_deadline = tokio::time::Instant::now() + claim_timeout;
        let mut outputs = HashMap::new();
        let mut sinks = Vec::new();
        for partition in partitions {
            if broadcast {
                let buffer = Arc::new(ReplayBuffer::default());
                outputs.insert(partition, PartitionOutput::Replay(buffer.clone()));
                sinks.push(PartitionSink::Replay(ReplaySink { buffer }));
            } else {
                let (tx, rx) = mpsc::channel(capacity.max(1));
                let claimed = Arc::new(AtomicBool::new(false));
                outputs.insert(
                    partition,
                    PartitionOutput::Queue {
                        rx: Mutex::new(Some(rx)),
                        claimed: claimed.clone(),
                    },
                );
                sinks.push(PartitionSink::Queue(QueueSink {
                    partition,
                    tx,
                    claimed,
                    claim_timeout,
                    claim_deadline,
                }));
            }
        }
        let outputs = Arc::new(Self {
            schema,
            compression,
            partitions: outputs,
            failure: OnceLock::new(),
        });
        (outputs, sinks)
    }

    /// Records why the producer failed, so consumers report the cause rather
    /// than only that their stream ended early.
    pub(crate) fn fail(&self, error: &DataFusionError) {
        self.failure.get_or_init(|| error.to_string());
        for output in self.partitions.values() {
            if let PartitionOutput::Replay(buffer) = output {
                buffer.end(ReplayEnd::Failed);
            }
        }
    }

    /// The error a consumer of `partition` sees when the producer stopped
    /// before finishing it.
    fn stopped_early(&self, key: &TaskKey, partition: usize) -> DataFusionError {
        let reason = self.failure.get().map_or(
            "the task stopped before finishing the partition",
            String::as_str,
        );
        DataFusionError::Execution(format!(
            "pipelined producer {key} failed while serving partition {partition}: {reason}"
        ))
    }

    /// Closes every queue no consumer has claimed, so a producer blocked on
    /// one sees its consumer as gone rather than waiting out the claim
    /// timeout. The producer keeps these outputs alive until its task ends,
    /// so dropping the registry's reference alone would not close them.
    fn close_unclaimed_queues(&self) {
        for output in self.partitions.values() {
            if let PartitionOutput::Queue { rx, .. } = output
                && let Some(rx) = rx.lock().unwrap().as_mut()
            {
                rx.close();
            }
        }
    }
}

/// A producer's outputs once it registers. Consumers that ask before then
/// subscribe and wait for the value to be set.
type ProducerSlot = watch::Sender<Option<Arc<ProducerOutputs>>>;

/// What has been cleaned up of one job on this executor.
#[derive(Default)]
struct ClosedJob {
    /// The whole job.
    all: bool,
    /// Stages cleaned up ahead of the job.
    stages: HashSet<usize>,
}

/// The cleaned-up parts of the [`CLOSED_JOB_MEMORY`] most recently touched
/// jobs.
#[derive(Default)]
struct ClosedJobs {
    jobs: HashMap<JobId, ClosedJob>,
    /// Jobs in the order they were first cleaned up, oldest first.
    order: VecDeque<JobId>,
}

impl ClosedJobs {
    /// The entry for `job_id`, forgetting the oldest job if that makes room.
    fn entry(&mut self, job_id: &JobId) -> &mut ClosedJob {
        if !self.jobs.contains_key(job_id) {
            self.order.push_back(job_id.clone());
            if self.order.len() > CLOSED_JOB_MEMORY
                && let Some(oldest) = self.order.pop_front()
            {
                self.jobs.remove(&oldest);
            }
        }
        self.jobs.entry(job_id.clone()).or_default()
    }

    /// Why `key`'s producer can no longer serve, if its job or stage has been
    /// cleaned up on this executor.
    fn reason(&self, key: &TaskKey) -> Option<String> {
        let job = self.jobs.get(&key.job_id)?;
        if job.all {
            Some(format!("job {} has finished on this executor", key.job_id))
        } else if job.stages.contains(&key.stage_id) {
            Some(format!(
                "stage {} of job {} has finished on this executor",
                key.stage_id, key.job_id
            ))
        } else {
            None
        }
    }
}

#[derive(Default)]
struct RegistryState {
    producers: HashMap<TaskKey, ProducerSlot>,
    closed: ClosedJobs,
}

impl RegistryState {
    /// Drops every producer for which `gone` holds, closing the queues no
    /// consumer had claimed. Consumers still waiting for one of them see
    /// their slot dropped.
    fn remove_producers(&mut self, gone: impl Fn(&TaskKey) -> bool) {
        self.producers.retain(|key, slot| {
            if !gone(key) {
                return true;
            }
            if let Some(outputs) = &*slot.borrow() {
                outputs.close_unclaimed_queues();
            }
            false
        });
    }
}

/// Executor-wide registry of the output partitions that running pipelined
/// producer tasks serve. See the [module documentation](self).
pub struct PipelinedRegistry {
    /// The executor this registry belongs to. A reader whose producer runs on
    /// the same executor reads from the registry directly instead of over
    /// Flight.
    executor_id: String,
    state: Mutex<RegistryState>,
}

/// An output partition opened for streaming.
pub struct OpenPartition {
    /// The partition's batches.
    pub stream: SendableRecordBatchStream,
    /// IPC compression the producer asked for when the batches cross the
    /// network.
    pub compression: Option<CompressionType>,
}

impl std::fmt::Debug for PipelinedRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().unwrap();
        f.debug_struct("PipelinedRegistry")
            .field("executor_id", &self.executor_id)
            .field("producers", &state.producers.len())
            .field("closed_jobs", &state.closed.jobs.len())
            .finish()
    }
}

impl PipelinedRegistry {
    /// Creates an empty registry for the executor `executor_id`.
    pub fn new(executor_id: impl Into<String>) -> Self {
        Self {
            executor_id: executor_id.into(),
            state: Mutex::new(RegistryState::default()),
        }
    }

    /// The executor this registry belongs to.
    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }

    /// Registers a producer task's outputs and hands them to any consumers
    /// already waiting for them.
    ///
    /// Fails if the job has already been cleaned up on this executor, so a
    /// task that starts late fails rather than waiting for consumers that are
    /// gone.
    pub fn register(&self, key: TaskKey, outputs: Arc<ProducerOutputs>) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(reason) = state.closed.reason(&key) {
            return Err(DataFusionError::Execution(format!(
                "pipelined producer {key} cannot start: {reason}"
            )));
        }
        let slot = state
            .producers
            .entry(key.clone())
            .or_insert_with(|| watch::Sender::new(None));
        if slot.borrow().is_some() {
            return Err(DataFusionError::Internal(format!(
                "pipelined producer {key} registered twice"
            )));
        }
        // Wakes any consumer already waiting.
        slot.send_replace(Some(outputs));
        Ok(())
    }

    /// Opens one output partition of a producer task for streaming, waiting up
    /// to `timeout` for the task to register if it has not yet.
    ///
    /// A queue partition can be opened once, and a second attempt fails at
    /// once; a broadcast partition can be opened any number of times. The
    /// stream ends with an error, never silently, if the producer fails or
    /// stops before finishing the partition.
    pub async fn open(
        &self,
        key: &TaskKey,
        partition: usize,
        timeout: Duration,
    ) -> Result<OpenPartition> {
        let outputs = self.wait_for(key, timeout).await?;
        let output = outputs.partitions.get(&partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "pipelined producer {key} does not serve partition {partition}"
            ))
        })?;
        let schema = outputs.schema.clone();
        let stream: SendableRecordBatchStream = match output {
            PartitionOutput::Queue { rx: slot, claimed } => {
                let rx = slot.lock().unwrap().take().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "partition {partition} of pipelined producer {key} is already being read"
                    ))
                })?;
                claimed.store(true, Ordering::Release);
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    queue_stream(rx, outputs.clone(), key.clone(), partition),
                ))
            }
            PartitionOutput::Replay(buffer) => Box::pin(RecordBatchStreamAdapter::new(
                schema,
                replay_stream(buffer.clone(), outputs.clone(), key.clone(), partition),
            )),
        };
        Ok(OpenPartition {
            stream,
            compression: outputs.compression,
        })
    }

    /// Drops every producer of `job_id` and remembers the job as finished.
    ///
    /// Consumers waiting for one of its producers fail, and queues nobody
    /// claimed are closed, which also releases a producer still blocked on
    /// one.
    pub fn remove_job(&self, job_id: &JobId) {
        let mut state = self.state.lock().unwrap();
        state.remove_producers(|key| key.job_id == *job_id);
        state.closed.entry(job_id).all = true;
    }

    /// Drops every producer of one stage of `job_id`, as when the scheduler
    /// reclaims a finished job's intermediate stages ahead of the job itself.
    /// Unlike [`Self::remove_job`] it does not mark the job finished.
    pub fn remove_stage(&self, job_id: &JobId, stage_id: usize) {
        let mut state = self.state.lock().unwrap();
        state.remove_producers(|key| key.job_id == *job_id && key.stage_id == stage_id);
        state.closed.entry(job_id).stages.insert(stage_id);
    }

    /// Number of producers registered or waited for.
    #[cfg(test)]
    fn producer_count(&self) -> usize {
        self.state.lock().unwrap().producers.len()
    }

    async fn wait_for(
        &self,
        key: &TaskKey,
        timeout: Duration,
    ) -> Result<Arc<ProducerOutputs>> {
        let mut rx = {
            let mut state = self.state.lock().unwrap();
            if let Some(reason) = state.closed.reason(key) {
                return Err(DataFusionError::Execution(format!(
                    "{reason}; pipelined producer {key} is gone"
                )));
            }
            state
                .producers
                .entry(key.clone())
                .or_insert_with(|| watch::Sender::new(None))
                .subscribe()
        };
        let waited = tokio::time::timeout(timeout, async {
            rx.wait_for(Option::is_some)
                .await
                .ok()
                .and_then(|outputs| outputs.clone())
        })
        .await;
        drop(rx);
        match waited {
            Ok(Some(outputs)) => Ok(outputs),
            // The slot was dropped: its stage or job was cleaned up.
            Ok(None) => Err(DataFusionError::Execution(format!(
                "the stage of pipelined producer {key} finished on this executor \
                 before the producer started"
            ))),
            Err(_) => {
                // Forget the slot if no other consumer is still waiting, so a
                // producer that never starts leaves nothing behind.
                let mut state = self.state.lock().unwrap();
                if state.producers.get(key).is_some_and(|slot| {
                    slot.borrow().is_none() && slot.receiver_count() == 0
                }) {
                    state.producers.remove(key);
                }
                Err(DataFusionError::Execution(format!(
                    "timed out after {timeout:?} waiting for pipelined producer {key} to start"
                )))
            }
        }
    }
}

fn queue_stream(
    rx: mpsc::Receiver<Message>,
    outputs: Arc<ProducerOutputs>,
    key: TaskKey,
    partition: usize,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    futures::stream::unfold(Some((rx, outputs, key)), move |state| async move {
        let (mut rx, outputs, key) = state?;
        match rx.recv().await {
            Some(Message::Batch(batch)) => Some((Ok(batch), Some((rx, outputs, key)))),
            Some(Message::End) => None,
            None => Some((Err(outputs.stopped_early(&key, partition)), None)),
        }
    })
}

/// A broadcast output partition: every batch the producer wrote, kept for
/// consumers that read it from the start.
pub(crate) struct ReplayBuffer {
    state: watch::Sender<ReplayState>,
}

impl Default for ReplayBuffer {
    fn default() -> Self {
        Self {
            state: watch::Sender::new(ReplayState::default()),
        }
    }
}

#[derive(Default)]
struct ReplayState {
    batches: Vec<RecordBatch>,
    /// `None` while the producer is still writing.
    end: Option<ReplayEnd>,
}

/// How a broadcast partition ended. The reason for a failure is held by the
/// producer's [`ProducerOutputs`].
#[derive(Clone, Copy)]
enum ReplayEnd {
    Finished,
    Failed,
}

impl ReplayBuffer {
    fn push(&self, batch: RecordBatch) {
        self.state.send_modify(|state| state.batches.push(batch));
    }

    fn end(&self, end: ReplayEnd) {
        self.state.send_if_modified(|state| {
            let first = state.end.is_none();
            state.end.get_or_insert(end);
            first
        });
    }
}

/// The producer's handle on a broadcast output. Dropping it unfinished — the
/// task failed or was cancelled — fails the partition for its consumers.
pub(crate) struct ReplaySink {
    buffer: Arc<ReplayBuffer>,
}

impl Drop for ReplaySink {
    fn drop(&mut self) {
        // A no-op if the producer already finished the partition.
        self.buffer.end(ReplayEnd::Failed);
    }
}

fn replay_stream(
    buffer: Arc<ReplayBuffer>,
    outputs: Arc<ProducerOutputs>,
    key: TaskKey,
    partition: usize,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    let rx = buffer.state.subscribe();
    futures::stream::unfold(Some((rx, 0usize)), move |state| {
        let outputs = outputs.clone();
        let key = key.clone();
        async move {
            let (mut rx, next) = state?;
            loop {
                {
                    let state = rx.borrow_and_update();
                    if let Some(batch) = state.batches.get(next) {
                        let batch = batch.clone();
                        drop(state);
                        return Some((Ok(batch), Some((rx, next + 1))));
                    }
                    match state.end {
                        Some(ReplayEnd::Finished) => return None,
                        Some(ReplayEnd::Failed) => {
                            return Some((
                                Err(outputs.stopped_early(&key, partition)),
                                None,
                            ));
                        }
                        None => {}
                    }
                }
                // The outputs this stream holds keep the sender alive, so
                // this only waits for the producer's next batch or its end.
                if rx.changed().await.is_err() {
                    return Some((Err(outputs.stopped_early(&key, partition)), None));
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn batch(values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(values.to_vec()))])
            .unwrap()
    }

    fn job() -> JobId {
        "job".into()
    }

    fn key(task_id: usize) -> TaskKey {
        TaskKey {
            job_id: job(),
            stage_id: 1,
            task_id,
        }
    }

    fn values(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    const WAIT: Duration = Duration::from_secs(5);

    /// The outputs of a producer serving the single queue partition 0, with
    /// its sink.
    fn queue(
        capacity: usize,
        claim_timeout: Duration,
    ) -> (Arc<ProducerOutputs>, PartitionSink) {
        let (outputs, mut sinks) =
            ProducerOutputs::new(schema(), [0], false, capacity, None, claim_timeout);
        (outputs, sinks.remove(0))
    }

    /// The outputs of a producer serving the single broadcast partition 0,
    /// with its sink.
    fn broadcast() -> (Arc<ProducerOutputs>, PartitionSink) {
        let (outputs, mut sinks) =
            ProducerOutputs::new(schema(), [0], true, 1, None, WAIT);
        (outputs, sinks.remove(0))
    }

    #[tokio::test]
    async fn consumer_that_arrives_first_waits_for_the_producer() {
        let registry = Arc::new(PipelinedRegistry::new("local"));
        let consumer = {
            let registry = registry.clone();
            tokio::spawn(async move {
                let opened = registry.open(&key(0), 3, WAIT).await?;
                opened.stream.try_collect::<Vec<_>>().await
            })
        };
        // Let the consumer start waiting before the producer registers.
        tokio::task::yield_now().await;

        let (outputs, mut sinks) =
            ProducerOutputs::new(schema(), [3], false, 2, None, WAIT);
        registry.register(key(0), outputs).unwrap();
        let sink = sinks.remove(0);
        assert!(sink.send(batch(&[1, 2])).await.unwrap());
        assert!(sink.send(batch(&[3])).await.unwrap());
        sink.finish().await.unwrap();

        let batches = consumer.await.unwrap().unwrap();
        assert_eq!(values(&batches), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn registering_twice_fails_and_keeps_the_first_outputs() {
        let registry = PipelinedRegistry::new("local");
        let (first, sink) = queue(2, WAIT);
        registry.register(key(0), first).unwrap();
        let (second, _second_sink) = queue(2, WAIT);
        let err = registry.register(key(0), second).unwrap_err();
        assert!(err.to_string().contains("registered twice"), "{err}");

        let stream = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        assert!(sink.send(batch(&[1])).await.unwrap());
        sink.finish().await.unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(values(&batches), vec![1]);
    }

    #[tokio::test]
    async fn queue_applies_backpressure_until_the_consumer_reads() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, sink) = queue(1, WAIT);
        registry.register(key(0), outputs).unwrap();

        assert!(sink.send(batch(&[1])).await.unwrap());
        // The queue holds one batch, so the second send waits for a reader.
        let blocked =
            tokio::time::timeout(Duration::from_millis(50), sink.send(batch(&[2]))).await;
        assert!(blocked.is_err(), "send should wait for the consumer");

        let mut stream = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        assert_eq!(
            values(&[stream.try_next().await.unwrap().unwrap()]),
            vec![1]
        );
    }

    #[tokio::test]
    async fn producer_that_stops_early_fails_the_consumer_instead_of_truncating() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, sink) = queue(4, WAIT);
        registry.register(key(0), outputs.clone()).unwrap();
        assert!(sink.send(batch(&[1])).await.unwrap());
        outputs.fail(&DataFusionError::Execution("disk on fire".to_owned()));
        drop(sink);

        let stream = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(err.to_string().contains("disk on fire"), "{err}");
    }

    #[tokio::test]
    async fn queue_can_be_opened_only_once() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, _sink) = queue(4, WAIT);
        registry.register(key(0), outputs).unwrap();

        let _stream = registry.open(&key(0), 0, WAIT).await.unwrap();
        // A second open fails at once, not after the registration timeout.
        let err =
            tokio::time::timeout(Duration::from_secs(1), registry.open(&key(0), 0, WAIT))
                .await
                .expect("the second open should fail at once")
                .map(|_| ())
                .unwrap_err();
        assert!(err.to_string().contains("already being read"), "{err}");
        // The producer stays registered until its stage is removed.
        assert_eq!(registry.producer_count(), 1);
        registry.remove_stage(&job(), 1);
        assert_eq!(registry.producer_count(), 0);
    }

    #[tokio::test]
    async fn broadcast_output_replays_to_every_consumer() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, sink) = broadcast();
        registry.register(key(0), outputs).unwrap();

        let early = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        assert!(sink.send(batch(&[1, 2])).await.unwrap());
        assert!(sink.send(batch(&[3])).await.unwrap());
        sink.finish().await.unwrap();
        let late = registry.open(&key(0), 0, WAIT).await.unwrap().stream;

        for stream in [early, late] {
            let batches = stream.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(values(&batches), vec![1, 2, 3]);
        }
    }

    #[tokio::test]
    async fn dropped_broadcast_sink_fails_its_consumers() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, sink) = broadcast();
        registry.register(key(0), outputs).unwrap();
        let stream = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        drop(sink);

        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(
            err.to_string().contains("stopped before finishing"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn failed_broadcast_producer_reports_its_cause() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, sink) = broadcast();
        registry.register(key(0), outputs.clone()).unwrap();
        let stream = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        outputs.fail(&DataFusionError::Execution("disk on fire".to_owned()));
        drop(sink);

        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(err.to_string().contains("disk on fire"), "{err}");
    }

    #[tokio::test]
    async fn removing_the_job_fails_waiters_and_rejects_late_producers() {
        let registry = Arc::new(PipelinedRegistry::new("local"));
        let waiter = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.open(&key(0), 0, WAIT).await.map(|_| ()) })
        };
        tokio::task::yield_now().await;
        registry.remove_job(&job());

        let err = waiter.await.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("finished on this executor"),
            "{err}"
        );

        let (outputs, _sink) = queue(1, WAIT);
        let err = registry.register(key(1), outputs).unwrap_err();
        assert!(err.to_string().contains("cannot start"), "{err}");
    }

    #[tokio::test]
    async fn removing_a_stage_turns_away_its_late_producers_and_consumers() {
        let registry = PipelinedRegistry::new("local");
        registry.remove_stage(&job(), 1);

        let (outputs, _sink) = queue(1, WAIT);
        let err = registry.register(key(0), outputs).unwrap_err();
        assert!(err.to_string().contains("cannot start"), "{err}");
        // A consumer fails at once rather than waiting out its timeout.
        let err =
            tokio::time::timeout(Duration::from_secs(1), registry.open(&key(0), 0, WAIT))
                .await
                .expect("the consumer should fail at once")
                .map(|_| ())
                .unwrap_err();
        assert!(err.to_string().contains("stage 1 of job job"), "{err}");

        // The job's other stages carry on.
        let other = TaskKey {
            job_id: job(),
            stage_id: 2,
            task_id: 0,
        };
        let (outputs, _sink) = queue(1, WAIT);
        registry.register(other, outputs).unwrap();
    }

    #[tokio::test]
    async fn removing_the_job_releases_a_producer_blocked_on_an_unclaimed_queue() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, sink) = queue(1, WAIT);
        registry.register(key(0), outputs.clone()).unwrap();
        assert!(sink.send(batch(&[1])).await.unwrap());
        registry.remove_job(&job());
        // The producer still holds `outputs`, as the writer does for the life
        // of its task, so the queue must be closed rather than merely
        // dropped by the registry.
        assert!(!sink.send(batch(&[2])).await.unwrap());
        drop(outputs);
    }

    #[tokio::test]
    async fn producer_gives_up_on_a_full_queue_nobody_opens() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, sink) = queue(1, Duration::from_millis(20));
        registry.register(key(0), outputs).unwrap();
        // Room in the queue: sending does not need a consumer yet.
        assert!(sink.send(batch(&[1])).await.unwrap());
        let err = sink.send(batch(&[2])).await.unwrap_err();
        assert!(err.to_string().contains("no consumer opened"), "{err}");
        let err = sink.finish().await.unwrap_err();
        assert!(err.to_string().contains("no consumer opened"), "{err}");
    }

    #[tokio::test]
    async fn opened_queue_waits_for_its_consumer_past_the_claim_timeout() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, sink) = queue(1, Duration::from_millis(20));
        registry.register(key(0), outputs).unwrap();
        let mut stream = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        assert!(sink.send(batch(&[1])).await.unwrap());

        // A slow consumer is backpressure, not a missing one.
        let blocked =
            tokio::time::timeout(Duration::from_millis(100), sink.send(batch(&[2])))
                .await;
        assert!(blocked.is_err(), "send should still be waiting");
        assert_eq!(
            values(&[stream.try_next().await.unwrap().unwrap()]),
            vec![1]
        );
        assert!(sink.send(batch(&[2])).await.unwrap());
    }

    #[tokio::test]
    async fn consumer_times_out_when_the_producer_never_starts() {
        let registry = PipelinedRegistry::new("local");
        let err = registry
            .open(&key(0), 0, Duration::from_millis(20))
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("timed out"), "{err}");
        assert_eq!(
            registry.producer_count(),
            0,
            "a timed-out consumer leaves no slot behind"
        );
    }
}
