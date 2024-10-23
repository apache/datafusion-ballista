use async_trait::async_trait;
use datafusion::{
    arrow::record_batch::RecordBatch, execution::RecordBatchStream,
    physical_plan::SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use log::error;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::mpsc;

use crate::{error::BallistaError, serde::scheduler::PartitionStats};

use super::PartitionStore;

#[derive(Clone)]
struct BatchItem {
    batch: Arc<RecordBatch>,
    is_last: bool,
}

// Track consumer state
struct Consumer {
    next_batch_index: usize,
    sender: mpsc::Sender<Arc<BatchItem>>,
}

// Struct to manage stream state
struct StreamState {
    buffer: Vec<Arc<BatchItem>>,
    consumers: Vec<Consumer>,
    completed: bool,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl StreamState {
    fn new(schema: datafusion::arrow::datatypes::SchemaRef) -> Self {
        Self {
            buffer: Vec::new(),
            consumers: Vec::new(),
            completed: false,
            schema,
        }
    }

    fn add_batch(&mut self, batch: RecordBatch, is_last: bool) {
        let item = Arc::new(BatchItem {
            batch: Arc::new(batch),
            is_last,
        });

        // Send to all current consumers who haven't seen this batch yet
        self.consumers.retain_mut(|consumer| {
            if consumer.next_batch_index == self.buffer.len() {
                match consumer.sender.try_send(item.clone()) {
                    Ok(_) => {
                        consumer.next_batch_index += 1;
                        true
                    }
                    Err(_) => false, // Remove consumer if send fails
                }
            } else {
                true
            }
        });

        // Store in buffer
        self.buffer.push(item);

        if is_last {
            self.completed = true;
        }
    }

    fn add_consumer(&mut self) -> mpsc::Receiver<Arc<BatchItem>> {
        let (sender, receiver) = mpsc::channel(100);

        // Create new consumer
        let mut consumer = Consumer {
            next_batch_index: 0,
            sender,
        };

        // Send all buffered batches to the new consumer
        for item in &self.buffer[consumer.next_batch_index..] {
            if consumer.sender.try_send(item.clone()).is_ok() {
                consumer.next_batch_index += 1;
            } else {
                break;
            }
        }

        // If not completed and consumer is still active, add to consumers list
        if !self.completed && consumer.next_batch_index == self.buffer.len() {
            self.consumers.push(consumer);
        }

        receiver
    }
}

// Stream implementation that reads from a channel
struct BufferedStream {
    schema: datafusion::arrow::datatypes::SchemaRef,
    receiver: mpsc::Receiver<Arc<BatchItem>>,
}

impl Stream for BufferedStream {
    type Item = Result<RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                if item.is_last {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok((*item.batch).clone())))
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for BufferedStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

pub struct InMemoryPartitionStore {
    store: Arc<Mutex<HashMap<String, Arc<Mutex<StreamState>>>>>,
}

impl InMemoryPartitionStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl PartitionStore for InMemoryPartitionStore {
    async fn store_partition(
        &self,
        path: &str,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError> {
        let schema = stream.schema();

        // Create new stream state
        let stream_state = Arc::new(Mutex::new(StreamState::new(schema.clone())));

        // Store the state
        self.store
            .lock()
            .unwrap()
            .insert(path.to_string(), stream_state.clone());

        // Spawn a task to read from the input stream and manage the state
        tokio::spawn(async move {
            let mut num_rows = 0;
            let mut num_batches = 0;
            let mut num_bytes = 0;

            while let Some(batch_result) = stream.next().await {
                match batch_result {
                    Ok(batch) => {
                        num_batches += 1;
                        num_rows += batch.num_rows();
                        num_bytes += batch.get_array_memory_size();

                        // Add batch to state
                        stream_state.lock().unwrap().add_batch(batch, false);
                    }
                    Err(e) => {
                        error!("Error processing stream: {:?}", e);
                        break;
                    }
                }
            }

            // Send final message
            stream_state
                .lock()
                .unwrap()
                .add_batch(RecordBatch::new_empty(schema), true);
        });

        Ok(None)
    }

    async fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        let store = self.store.lock().unwrap();
        let state = store.get(path).ok_or_else(|| {
            BallistaError::General(format!(
                "Partition not found in in-memory store: {}",
                path
            ))
        })?;

        let mut state_guard = state.lock().unwrap();
        let receiver = state_guard.add_consumer();
        let schema = state_guard.schema.clone();

        let stream = BufferedStream { schema, receiver };

        Ok(Box::pin(stream))
    }

    async fn delete_partition(&self, path: &str) -> Result<(), BallistaError> {
        self.store.lock().unwrap().remove(path);
        Ok(())
    }

    async fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        let stream = self.fetch_partition(path).await?;
        self.delete_partition(path).await?;
        Ok(stream)
    }
}
