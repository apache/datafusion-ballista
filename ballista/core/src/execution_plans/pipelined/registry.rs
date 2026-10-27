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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt};
use tokio::sync::{Notify, mpsc, oneshot};

/// How many recently cleaned-up jobs the registry remembers. A producer task
/// that starts after its job was cleaned up — launched just before the job was
/// cancelled, say — would otherwise register outputs no consumer will read and
/// wait on them forever. Remembering the job lets it fail instead.
const CLOSED_JOB_MEMORY: usize = 1024;

/// How many recently cleaned-up stages of still-running jobs the registry
/// remembers, for the same reason. When a pipelined job succeeds, the
/// scheduler cleans up its other stages at once, and cancels their leftover
/// tasks; one that had not started yet would otherwise start afterwards.
const CLOSED_STAGE_MEMORY: usize = 4096;

/// Identifies one producer task of a pipelined stage.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProducerKey {
    /// The job the task belongs to.
    pub job_id: String,
    /// The producer stage.
    pub stage_id: usize,
    /// The task within its stage.
    pub task_id: usize,
}

impl std::fmt::Display for ProducerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.job_id, self.stage_id, self.task_id)
    }
}

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
    /// The producer's error, if it failed. Reported to a consumer whose queue
    /// closes without an end-of-stream marker.
    failure: Mutex<Option<String>>,
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
            PartitionSink::Replay(sink) => sink.buffer.finish(Ok(())),
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
    /// sink per partition for the producer to write into.
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
    ) -> (Arc<Self>, HashMap<usize, PartitionSink>) {
        let claim_deadline = tokio::time::Instant::now() + claim_timeout;
        let mut outputs = HashMap::new();
        let mut sinks = HashMap::new();
        for partition in partitions {
            if broadcast {
                let buffer = Arc::new(ReplayBuffer::default());
                outputs.insert(partition, PartitionOutput::Replay(buffer.clone()));
                sinks.insert(partition, PartitionSink::Replay(ReplaySink { buffer }));
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
                sinks.insert(
                    partition,
                    PartitionSink::Queue(QueueSink {
                        partition,
                        tx,
                        claimed,
                        claim_timeout,
                        claim_deadline,
                    }),
                );
            }
        }
        let outputs = Arc::new(Self {
            schema,
            compression,
            partitions: outputs,
            failure: Mutex::new(None),
        });
        (outputs, sinks)
    }

    /// Records why the producer failed, so consumers report the cause rather
    /// than only that their stream ended early.
    pub(crate) fn fail(&self, error: &DataFusionError) {
        let message = error.to_string();
        let mut failure = self.failure.lock().unwrap();
        if failure.is_none() {
            *failure = Some(message.clone());
        }
        drop(failure);
        for output in self.partitions.values() {
            if let PartitionOutput::Replay(buffer) = output {
                buffer.finish(Err(message.clone()));
            }
        }
    }

    /// The schema of every output partition.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn failure(&self) -> Option<String> {
        self.failure.lock().unwrap().clone()
    }

    /// Whether every output has been handed to its consumer, so the registry
    /// no longer needs to hold this producer.
    fn fully_claimed(&self) -> bool {
        self.partitions.values().all(|output| match output {
            PartitionOutput::Queue { rx, .. } => rx.lock().unwrap().is_none(),
            PartitionOutput::Replay(_) => false,
        })
    }
}

enum ProducerSlot {
    /// Consumers that asked for this producer before it registered.
    Waiting(Vec<oneshot::Sender<Arc<ProducerOutputs>>>),
    Ready(Arc<ProducerOutputs>),
}

#[derive(Default)]
struct RegistryState {
    producers: HashMap<ProducerKey, ProducerSlot>,
    closed_jobs: HashSet<String>,
    closed_order: VecDeque<String>,
    closed_stages: HashSet<(String, usize)>,
    closed_stage_order: VecDeque<(String, usize)>,
}

impl RegistryState {
    /// Why `key`'s producer can no longer serve, if its job or stage has been
    /// cleaned up on this executor.
    fn closed(&self, key: &ProducerKey) -> Option<String> {
        if self.closed_jobs.contains(&key.job_id) {
            Some(format!("job {} has finished on this executor", key.job_id))
        } else if self
            .closed_stages
            .contains(&(key.job_id.clone(), key.stage_id))
        {
            Some(format!(
                "stage {} of job {} has finished on this executor",
                key.stage_id, key.job_id
            ))
        } else {
            None
        }
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
            .field("closed_jobs", &state.closed_jobs.len())
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
    pub fn register(
        &self,
        key: ProducerKey,
        outputs: Arc<ProducerOutputs>,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(reason) = state.closed(&key) {
            return Err(DataFusionError::Execution(format!(
                "pipelined producer {key} cannot start: {reason}"
            )));
        }
        match state
            .producers
            .insert(key.clone(), ProducerSlot::Ready(outputs.clone()))
        {
            Some(ProducerSlot::Waiting(waiters)) => {
                for waiter in waiters {
                    // A waiter that timed out has dropped its receiver.
                    let _ = waiter.send(outputs.clone());
                }
                Ok(())
            }
            Some(ProducerSlot::Ready(previous)) => {
                state
                    .producers
                    .insert(key.clone(), ProducerSlot::Ready(previous));
                Err(DataFusionError::Internal(format!(
                    "pipelined producer {key} registered twice"
                )))
            }
            None => Ok(()),
        }
    }

    /// Opens one output partition of a producer task for streaming, waiting up
    /// to `timeout` for the task to register if it has not yet.
    ///
    /// A queue partition can be opened once; a broadcast partition any number
    /// of times. The stream ends with an error, never silently, if the
    /// producer fails or stops before finishing the partition.
    pub async fn open(
        &self,
        key: &ProducerKey,
        partition: usize,
        timeout: Duration,
    ) -> Result<OpenPartition> {
        let outputs = self.wait_for(key, timeout).await?;
        let output = outputs.partitions.get(&partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "pipelined producer {key} does not serve partition {partition}"
            ))
        })?;
        let schema = outputs.schema();
        let stream: SendableRecordBatchStream = match output {
            PartitionOutput::Queue { rx: slot, claimed } => {
                let rx = slot.lock().unwrap().take().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "partition {partition} of pipelined producer {key} is already being read"
                    ))
                })?;
                claimed.store(true, Ordering::Release);
                if outputs.fully_claimed() {
                    self.release(key);
                }
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    queue_stream(rx, outputs.clone(), key.clone(), partition),
                ))
            }
            PartitionOutput::Replay(buffer) => Box::pin(RecordBatchStreamAdapter::new(
                schema,
                replay_stream(buffer.clone()),
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
    pub fn remove_job(&self, job_id: &str) {
        let mut state = self.state.lock().unwrap();
        state.producers.retain(|key, _| key.job_id != job_id);
        if state.closed_jobs.insert(job_id.to_owned()) {
            state.closed_order.push_back(job_id.to_owned());
            while state.closed_order.len() > CLOSED_JOB_MEMORY {
                if let Some(oldest) = state.closed_order.pop_front() {
                    state.closed_jobs.remove(&oldest);
                }
            }
        }
    }

    /// Drops every producer of one stage of `job_id`, as when the scheduler
    /// reclaims a finished job's intermediate stages ahead of the job itself.
    /// Unlike [`Self::remove_job`] it does not mark the job finished.
    pub fn remove_stage(&self, job_id: &str, stage_id: usize) {
        let mut state = self.state.lock().unwrap();
        state
            .producers
            .retain(|key, _| key.job_id != job_id || key.stage_id != stage_id);
        let stage = (job_id.to_owned(), stage_id);
        if state.closed_stages.insert(stage.clone()) {
            state.closed_stage_order.push_back(stage);
            while state.closed_stage_order.len() > CLOSED_STAGE_MEMORY {
                if let Some(oldest) = state.closed_stage_order.pop_front() {
                    state.closed_stages.remove(&oldest);
                }
            }
        }
    }

    /// Number of producers registered or waited for. For tests and metrics.
    pub fn producer_count(&self) -> usize {
        self.state.lock().unwrap().producers.len()
    }

    async fn wait_for(
        &self,
        key: &ProducerKey,
        timeout: Duration,
    ) -> Result<Arc<ProducerOutputs>> {
        let rx = {
            let mut state = self.state.lock().unwrap();
            if let Some(reason) = state.closed(key) {
                return Err(DataFusionError::Execution(format!(
                    "{reason}; pipelined producer {key} is gone"
                )));
            }
            let slot = state
                .producers
                .entry(key.clone())
                .or_insert_with(|| ProducerSlot::Waiting(vec![]));
            match slot {
                ProducerSlot::Ready(outputs) => return Ok(outputs.clone()),
                ProducerSlot::Waiting(waiters) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    rx
                }
            }
        };
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(outputs)) => Ok(outputs),
            Ok(Err(_)) => Err(DataFusionError::Execution(format!(
                "the stage of pipelined producer {key} finished on this executor \
                 before the producer started"
            ))),
            Err(_) => Err(DataFusionError::Execution(format!(
                "timed out after {timeout:?} waiting for pipelined producer {key} to start"
            ))),
        }
    }

    fn release(&self, key: &ProducerKey) {
        let mut state = self.state.lock().unwrap();
        if let Some(ProducerSlot::Ready(outputs)) = state.producers.get(key)
            && outputs.fully_claimed()
        {
            state.producers.remove(key);
        }
    }
}

fn queue_stream(
    rx: mpsc::Receiver<Message>,
    outputs: Arc<ProducerOutputs>,
    key: ProducerKey,
    partition: usize,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    futures::stream::unfold(Some(rx), move |rx| {
        let outputs = outputs.clone();
        let key = key.clone();
        async move {
            let mut rx = rx?;
            match rx.recv().await {
                Some(Message::Batch(batch)) => Some((Ok(batch), Some(rx))),
                Some(Message::End) => None,
                None => {
                    let reason = outputs.failure().unwrap_or_else(|| {
                        "the task stopped before finishing the partition".to_owned()
                    });
                    Some((
                        Err(DataFusionError::Execution(format!(
                            "pipelined producer {key} failed while serving partition {partition}: {reason}"
                        ))),
                        None,
                    ))
                }
            }
        }
    })
}

/// A broadcast output partition: every batch the producer wrote, kept for
/// consumers that read it from the start.
#[derive(Default)]
pub(crate) struct ReplayBuffer {
    state: Mutex<ReplayState>,
    notify: Notify,
}

#[derive(Default)]
struct ReplayState {
    batches: Vec<RecordBatch>,
    /// `None` while the producer is still writing.
    outcome: Option<std::result::Result<(), String>>,
}

impl ReplayBuffer {
    fn push(&self, batch: RecordBatch) {
        self.state.lock().unwrap().batches.push(batch);
        self.notify.notify_waiters();
    }

    fn finish(&self, outcome: std::result::Result<(), String>) {
        let mut state = self.state.lock().unwrap();
        if state.outcome.is_none() {
            state.outcome = Some(outcome);
        }
        drop(state);
        self.notify.notify_waiters();
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
        self.buffer.finish(Err(
            "the task stopped before finishing the partition".to_owned()
        ));
    }
}

fn replay_stream(
    buffer: Arc<ReplayBuffer>,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    futures::stream::unfold((buffer, 0usize, false), |(buffer, next, done)| async move {
        if done {
            return None;
        }
        loop {
            // Wait through a second handle, so `buffer` itself stays free to
            // move into the next state.
            let waiter = buffer.clone();
            let notified = waiter.notify.notified();
            futures::pin_mut!(notified);
            // Register for a wakeup before looking, so a batch pushed between
            // the look and the wait is not missed.
            notified.as_mut().enable();
            {
                let state = buffer.state.lock().unwrap();
                if let Some(batch) = state.batches.get(next) {
                    let batch = batch.clone();
                    drop(state);
                    return Some((Ok(batch), (buffer, next + 1, false)));
                }
                match &state.outcome {
                    Some(Ok(())) => return None,
                    Some(Err(reason)) => {
                        let error = DataFusionError::Execution(format!(
                            "pipelined broadcast producer failed: {reason}"
                        ));
                        drop(state);
                        return Some((Err(error), (buffer, next, true)));
                    }
                    None => {}
                }
            }
            notified.await;
        }
    })
    .boxed()
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

    fn key(task_id: usize) -> ProducerKey {
        ProducerKey {
            job_id: "job".to_owned(),
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
        let sink = sinks.remove(&3).unwrap();
        assert!(sink.send(batch(&[1, 2])).await.unwrap());
        assert!(sink.send(batch(&[3])).await.unwrap());
        sink.finish().await.unwrap();

        let batches = consumer.await.unwrap().unwrap();
        assert_eq!(values(&batches), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn queue_applies_backpressure_until_the_consumer_reads() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, mut sinks) =
            ProducerOutputs::new(schema(), [0], false, 1, None, WAIT);
        registry.register(key(0), outputs).unwrap();
        let sink = sinks.remove(&0).unwrap();

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
        let (outputs, mut sinks) =
            ProducerOutputs::new(schema(), [0], false, 4, None, WAIT);
        registry.register(key(0), outputs.clone()).unwrap();
        let sink = sinks.remove(&0).unwrap();
        assert!(sink.send(batch(&[1])).await.unwrap());
        outputs.fail(&DataFusionError::Execution("disk on fire".to_owned()));
        drop(sink);

        let stream = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(err.to_string().contains("disk on fire"), "{err}");
    }

    #[tokio::test]
    async fn queue_can_be_opened_once_and_is_released_once_claimed() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, _sinks) = ProducerOutputs::new(schema(), [0], false, 4, None, WAIT);
        registry.register(key(0), outputs).unwrap();
        assert_eq!(registry.producer_count(), 1);

        let _stream = registry.open(&key(0), 0, WAIT).await.unwrap();
        // Every queue is claimed, so the registry lets go of the producer.
        assert_eq!(registry.producer_count(), 0);
    }

    #[tokio::test]
    async fn broadcast_output_replays_to_every_consumer() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, mut sinks) =
            ProducerOutputs::new(schema(), [0], true, 1, None, WAIT);
        registry.register(key(0), outputs).unwrap();
        let sink = sinks.remove(&0).unwrap();

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
        let (outputs, mut sinks) =
            ProducerOutputs::new(schema(), [0], true, 1, None, WAIT);
        registry.register(key(0), outputs).unwrap();
        let stream = registry.open(&key(0), 0, WAIT).await.unwrap().stream;
        drop(sinks.remove(&0));

        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(
            err.to_string().contains("stopped before finishing"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn removing_the_job_fails_waiters_and_rejects_late_producers() {
        let registry = Arc::new(PipelinedRegistry::new("local"));
        let waiter = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.open(&key(0), 0, WAIT).await.map(|_| ()) })
        };
        tokio::task::yield_now().await;
        registry.remove_job("job");

        let err = waiter.await.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("finished on this executor"),
            "{err}"
        );

        let (outputs, _sinks) = ProducerOutputs::new(schema(), [0], false, 1, None, WAIT);
        let err = registry.register(key(1), outputs).unwrap_err();
        assert!(err.to_string().contains("cannot start"), "{err}");
    }

    #[tokio::test]
    async fn removing_a_stage_turns_away_its_late_producers_and_consumers() {
        let registry = PipelinedRegistry::new("local");
        registry.remove_stage("job", 1);

        let (outputs, _sinks) = ProducerOutputs::new(schema(), [0], false, 1, None, WAIT);
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
        let other = ProducerKey {
            job_id: "job".to_owned(),
            stage_id: 2,
            task_id: 0,
        };
        let (outputs, _sinks) = ProducerOutputs::new(schema(), [0], false, 1, None, WAIT);
        registry.register(other, outputs).unwrap();
    }

    #[tokio::test]
    async fn removing_the_job_releases_a_producer_blocked_on_an_unclaimed_queue() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, mut sinks) =
            ProducerOutputs::new(schema(), [0], false, 1, None, WAIT);
        registry.register(key(0), outputs).unwrap();
        let sink = sinks.remove(&0).unwrap();
        assert!(sink.send(batch(&[1])).await.unwrap());
        registry.remove_job("job");
        // The registry held the only receiver; dropping it closes the queue.
        assert!(!sink.send(batch(&[2])).await.unwrap());
    }

    #[tokio::test]
    async fn producer_gives_up_on_a_full_queue_nobody_opens() {
        let registry = PipelinedRegistry::new("local");
        let (outputs, mut sinks) = ProducerOutputs::new(
            schema(),
            [0],
            false,
            1,
            None,
            Duration::from_millis(20),
        );
        registry.register(key(0), outputs).unwrap();
        let sink = sinks.remove(&0).unwrap();
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
        let (outputs, mut sinks) = ProducerOutputs::new(
            schema(),
            [0],
            false,
            1,
            None,
            Duration::from_millis(20),
        );
        registry.register(key(0), outputs).unwrap();
        let sink = sinks.remove(&0).unwrap();
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
    }
}
