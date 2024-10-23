use std::fs::remove_file;
use std::fs::File;
use std::io::BufReader;

use datafusion::arrow::ipc::reader::StreamReader;
use futures::StreamExt;
use log::error;

use crate::{error::BallistaError, serde::scheduler::PartitionStats};

use super::LocalShuffleStream;
use super::PartitionStore;
use async_trait::async_trait;
use datafusion::{
    arrow::ipc::{
        writer::{IpcWriteOptions, StreamWriter},
        CompressionType,
    },
    execution::SendableRecordBatchStream,
};

pub struct DiskBasedPartitionStore {}

#[async_trait]
impl PartitionStore for DiskBasedPartitionStore {
    async fn store_partition(
        &self,
        path: &str,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError> {
        let file = File::create(path).map_err(|e| {
            error!("Failed to create partition file at {}: {:?}", path, e);
            BallistaError::IoError(e)
        })?;

        let mut num_rows = 0;
        let mut num_batches = 0;
        let mut num_bytes = 0;

        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;

        let mut writer =
            StreamWriter::try_new_with_options(file, stream.schema().as_ref(), options)?;

        while let Some(result) = stream.next().await {
            let batch = result?;
            let batch_size_bytes = batch.get_array_memory_size();

            num_batches += 1;
            num_rows += batch.num_rows();
            num_bytes += batch_size_bytes;

            writer.write(&batch)?;
        }
        writer.finish()?;

        Ok(Some(PartitionStats::new(
            Some(num_rows as u64),
            Some(num_batches),
            Some(num_bytes as u64),
        )))
    }

    async fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        let file = File::open(path).map_err(|e| {
            BallistaError::General(format!(
                "Failed to open partition file at {path}: {e:?}"
            ))
        })?;
        let file = BufReader::new(file);
        let reader = StreamReader::try_new(file, None).map_err(|e| {
            BallistaError::General(format!(
                "Failed to new arrow FileReader at {path}: {e:?}"
            ))
        })?;

        Ok(Box::pin(LocalShuffleStream::new(reader)))
    }

    async fn delete_partition(&self, path: &str) -> Result<(), BallistaError> {
        remove_file(path).map_err(|e| {
            error!("Failed to delete partition file at {}: {:?}", path, e);
            BallistaError::IoError(e)
        })
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
