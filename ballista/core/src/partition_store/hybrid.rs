use async_trait::async_trait;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use datafusion::execution::SendableRecordBatchStream;

use crate::{error::BallistaError, serde::scheduler::PartitionStats};

use super::{
    disk::DiskBasedPartitionStore, memory::InMemoryPartitionStore, PartitionStore,
};

pub struct HybridPartitionStore {
    in_memory_store: InMemoryPartitionStore,
    disk_store: DiskBasedPartitionStore,
    writing_to_disk: Arc<Mutex<HashMap<String, bool>>>,
    memory_threshold_bytes: Option<u64>,
}

impl HybridPartitionStore {
    pub fn new(memory_threshold_bytes: Option<u64>) -> Self {
        Self {
            in_memory_store: InMemoryPartitionStore::new(),
            disk_store: DiskBasedPartitionStore {},
            writing_to_disk: Arc::new(Mutex::new(HashMap::new())),
            memory_threshold_bytes,
        }
    }
}

#[async_trait]
impl PartitionStore for HybridPartitionStore {
    async fn store_partition(
        &self,
        path: &str,
        stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError> {
        // Store in memory first to get the size
        let stats = self.in_memory_store.store_partition(path, stream).await?;

        if let Some(stats) = &stats {
            if stats.num_bytes.unwrap_or(0)
                >= self.memory_threshold_bytes.unwrap_or(u64::MIN)
            {
                // Mark as writing to disk
                self.writing_to_disk
                    .lock()
                    .unwrap()
                    .insert(path.to_string(), true);

                // Move to disk
                let stream = self.in_memory_store.take_partition(path).await?;
                let disk_stats = self.disk_store.store_partition(path, stream).await?;

                // Remove writing to disk mark
                self.writing_to_disk.lock().unwrap().remove(path);

                return Ok(disk_stats);
            }
        }

        Ok(stats)
    }

    async fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        // Check if it's currently being written to disk
        if self.writing_to_disk.lock().unwrap().contains_key(path) {
            log::warn!(
                "Partition {} is currently being written to disk, waiting...",
                path
            );
            // In a production system, we might want to implement a proper wait mechanism here
            return Err(BallistaError::General(
                "Partition is currently being written to disk".to_string(),
            ));
        }

        // Try memory first
        match self.in_memory_store.fetch_partition(path).await {
            Ok(stream) => Ok(stream),
            Err(_) => self.disk_store.fetch_partition(path).await,
        }
    }

    async fn delete_partition(&self, path: &str) -> Result<(), BallistaError> {
        // Try to delete from both stores
        let _ = self.in_memory_store.delete_partition(path).await;
        let _ = self.disk_store.delete_partition(path).await;
        Ok(())
    }

    async fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        // Check if it's currently being written to disk
        if self.writing_to_disk.lock().unwrap().contains_key(path) {
            return Err(BallistaError::General(
                "Partition is currently being written to disk".to_string(),
            ));
        }

        // Try memory first
        match self.in_memory_store.take_partition(path).await {
            Ok(stream) => Ok(stream),
            Err(_) => self.disk_store.take_partition(path).await,
        }
    }
}
