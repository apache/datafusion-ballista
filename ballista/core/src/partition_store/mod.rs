use std::{
    fs::File,
    io::BufReader,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef, ipc::reader::StreamReader},
    error::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::Stream;

use crate::{error::BallistaError, serde::scheduler::PartitionStats};

mod disk;
mod hybrid;
mod memory;

#[async_trait]
pub trait PartitionStore: Send + Sync {
    async fn store_partition(
        &self,
        path: &str,
        stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError>;

    async fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError>;

    async fn delete_partition(&self, path: &str) -> Result<(), BallistaError>;

    async fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError>;
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
