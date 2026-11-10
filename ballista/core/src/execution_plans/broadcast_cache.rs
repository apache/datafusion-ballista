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

//! Executor-wide cache of decoded broadcast shuffle reads.
//!
//! A broadcast hash join's build side is a broadcast [`ShuffleReaderExec`]
//! that reads every output partition of the upstream stage. Each task of the
//! joining stage runs its own copy of the plan, so without a cache every task
//! on an executor fetches and decodes the same data: at 128 partitions on two
//! executors, a broadcast stage of 16 tasks fetched 16,384 blocks and decoded
//! the same build side 16 times. Spark broadcasts a relation once per
//! executor.
//!
//! [`BroadcastReadCache`] keeps the decoded batches of each broadcast read so
//! that the first task on an executor fetches them and the others reuse them.
//! Tasks that ask while the first fetch is running wait for it. A failed fetch
//! is not cached: every task that asks retries it and reports its own error,
//! so fetch-failure recovery works as before.
//!
//! Entries are dropped when unused for [`BroadcastReadCache::idle_ttl`], when
//! their job's data is removed, and are not kept at all above
//! [`BroadcastReadCache::max_entry_bytes`]. The batches are not charged to a
//! task's memory pool, as the fetched batches of a single read never were: the
//! pool sees each task's hash table, which is built from them.
//!
//! [`ShuffleReaderExec`]: super::ShuffleReaderExec

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::utils::memory::RecordBatchMemoryCounter;
use datafusion::error::Result;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use tokio::sync::OnceCell;

use crate::serde::scheduler::PartitionLocation;

/// Identifies one broadcast read: the job, the upstream stage, and the exact
/// locations read, so a re-run upstream stage (new locations) is a new entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BroadcastKey {
    job_id: String,
    stage_id: usize,
    locations: u64,
}

impl BroadcastKey {
    /// Builds the key for reading `locations` of `stage_id` in `job_id`.
    pub fn new(job_id: &str, stage_id: usize, locations: &[PartitionLocation]) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        locations.len().hash(&mut hasher);
        for loc in locations {
            loc.executor_meta.id.hash(&mut hasher);
            loc.map_partition_id.hash(&mut hasher);
            loc.partition_id.stage_id.hash(&mut hasher);
            loc.partition_id.partition_id.hash(&mut hasher);
            loc.file_id.hash(&mut hasher);
            loc.is_sort_shuffle.hash(&mut hasher);
        }
        Self {
            job_id: job_id.to_string(),
            stage_id,
            locations: hasher.finish(),
        }
    }
}

#[derive(Debug)]
struct Entry {
    batches: OnceCell<Arc<Vec<RecordBatch>>>,
    last_used: Mutex<Instant>,
}

/// Executor-wide cache of decoded broadcast shuffle reads. See the
/// [module documentation](self).
#[derive(Debug)]
pub struct BroadcastReadCache {
    entries: Mutex<HashMap<BroadcastKey, Arc<Entry>>>,
    idle_ttl: Duration,
    max_entry_bytes: usize,
}

impl Default for BroadcastReadCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), 1024 * 1024 * 1024)
    }
}

impl BroadcastReadCache {
    /// Creates a cache that drops entries unused for `idle_ttl` and does not
    /// keep reads larger than `max_entry_bytes`.
    pub fn new(idle_ttl: Duration, max_entry_bytes: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            idle_ttl,
            max_entry_bytes,
        }
    }

    /// How long an unused entry is kept.
    pub fn idle_ttl(&self) -> Duration {
        self.idle_ttl
    }

    /// The largest read, in bytes, that is kept for reuse.
    pub fn max_entry_bytes(&self) -> usize {
        self.max_entry_bytes
    }

    /// Number of reads currently held.
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Whether no reads are held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops every read of `job_id`.
    pub fn remove_job(&self, job_id: &str) {
        self.entries
            .lock()
            .unwrap()
            .retain(|key, _| key.job_id != job_id);
    }

    /// Returns a stream over the batches of the read `key`, fetching them with
    /// `fetch` if no other task on this executor has, or waiting for the task
    /// that is fetching them.
    pub fn stream<F>(
        self: &Arc<Self>,
        key: BroadcastKey,
        schema: SchemaRef,
        fetch: F,
    ) -> SendableRecordBatchStream
    where
        F: FnOnce() -> Result<SendableRecordBatchStream> + Send + 'static,
    {
        let entry = self.entry(&key);
        let cache = Arc::clone(self);
        let batches = async move {
            let batches = entry
                .batches
                .get_or_try_init(|| async {
                    let batches: Vec<RecordBatch> = fetch()?.try_collect().await?;
                    let mut counter = RecordBatchMemoryCounter::new();
                    let bytes: usize =
                        batches.iter().map(|b| counter.count_batch(b)).sum();
                    debug!(
                        "Fetched broadcast read of stage {} ({} batches, {bytes} bytes)",
                        key.stage_id,
                        batches.len()
                    );
                    if bytes > cache.max_entry_bytes {
                        // Too large to hold between tasks: this task and any
                        // waiting for it still share it, later ones fetch again.
                        cache.entries.lock().unwrap().remove(&key);
                    }
                    Ok::<_, datafusion::error::DataFusionError>(Arc::new(batches))
                })
                .await?;
            // Measured from when a task last got the batches, so a slow fetch
            // does not make its entry look idle as soon as it finishes.
            *entry.last_used.lock().unwrap() = Instant::now();
            Ok::<_, datafusion::error::DataFusionError>(futures::stream::iter(
                batches.iter().cloned().map(Ok).collect::<Vec<_>>(),
            ))
        };
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(batches).try_flatten().boxed(),
        ))
    }

    /// The entry for `key`, created if missing, after dropping idle entries.
    fn entry(&self, key: &BroadcastKey) -> Arc<Entry> {
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|k, e| {
            k == key || now - *e.last_used.lock().unwrap() < self.idle_ttl
        });
        entries
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(Entry {
                    batches: OnceCell::new(),
                    last_used: Mutex::new(now),
                })
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
    use futures::TryStreamExt;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn key(job: &str, stage: usize) -> BroadcastKey {
        BroadcastKey {
            job_id: job.to_string(),
            stage_id: stage,
            locations: 7,
        }
    }

    fn source(
        fetches: Arc<AtomicUsize>,
    ) -> impl FnOnce() -> Result<SendableRecordBatchStream> + Send + 'static {
        move || {
            fetches.fetch_add(1, Ordering::SeqCst);
            let batch = RecordBatch::try_new(
                schema(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema(),
                futures::stream::iter(vec![Ok(batch.clone()), Ok(batch)]),
            )))
        }
    }

    async fn rows(stream: SendableRecordBatchStream) -> Result<usize> {
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches.iter().map(|b| b.num_rows()).sum())
    }

    #[tokio::test]
    async fn concurrent_and_later_reads_fetch_once() -> Result<()> {
        let cache = Arc::new(BroadcastReadCache::default());
        let fetches = Arc::new(AtomicUsize::new(0));
        let a = cache.stream(key("j", 1), schema(), source(fetches.clone()));
        let b = cache.stream(key("j", 1), schema(), source(fetches.clone()));
        let (ra, rb) = tokio::join!(rows(a), rows(b));
        assert_eq!((ra?, rb?), (6, 6));
        let c = cache.stream(key("j", 1), schema(), source(fetches.clone()));
        assert_eq!(rows(c).await?, 6);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn different_reads_fetch_separately_and_jobs_are_removed() -> Result<()> {
        let cache = Arc::new(BroadcastReadCache::default());
        let fetches = Arc::new(AtomicUsize::new(0));
        rows(cache.stream(key("j", 1), schema(), source(fetches.clone()))).await?;
        rows(cache.stream(key("j", 2), schema(), source(fetches.clone()))).await?;
        rows(cache.stream(key("k", 1), schema(), source(fetches.clone()))).await?;
        assert_eq!(fetches.load(Ordering::SeqCst), 3);
        assert_eq!(cache.len(), 3);
        cache.remove_job("j");
        assert_eq!(cache.len(), 1);
        rows(cache.stream(key("j", 1), schema(), source(fetches.clone()))).await?;
        assert_eq!(fetches.load(Ordering::SeqCst), 4);
        Ok(())
    }

    #[tokio::test]
    async fn failed_fetch_is_retried() -> Result<()> {
        let cache = Arc::new(BroadcastReadCache::default());
        let failing = cache.stream(key("j", 1), schema(), || {
            Err(DataFusionError::Execution("fetch failed".to_string()))
        });
        assert!(rows(failing).await.is_err());
        let fetches = Arc::new(AtomicUsize::new(0));
        assert_eq!(
            rows(cache.stream(key("j", 1), schema(), source(fetches.clone()))).await?,
            6
        );
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_reads_are_not_kept() -> Result<()> {
        let cache = Arc::new(BroadcastReadCache::new(Duration::from_secs(30), 1));
        let fetches = Arc::new(AtomicUsize::new(0));
        rows(cache.stream(key("j", 1), schema(), source(fetches.clone()))).await?;
        rows(cache.stream(key("j", 1), schema(), source(fetches.clone()))).await?;
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
        assert!(cache.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn idle_entries_expire() -> Result<()> {
        let cache = Arc::new(BroadcastReadCache::new(Duration::from_millis(1), 1 << 30));
        let fetches = Arc::new(AtomicUsize::new(0));
        rows(cache.stream(key("j", 1), schema(), source(fetches.clone()))).await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Another read sweeps the idle entry.
        rows(cache.stream(key("j", 2), schema(), source(fetches.clone()))).await?;
        assert_eq!(cache.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_slow_fetch_is_not_idle_when_it_finishes() -> Result<()> {
        let cache = Arc::new(BroadcastReadCache::new(Duration::from_millis(50), 1 << 30));
        let fetches = Arc::new(AtomicUsize::new(0));
        let counter = fetches.clone();
        let slow = cache.stream(key("j", 1), schema(), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            let batch = RecordBatch::try_new(
                schema(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema(),
                futures::stream::once(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(batch)
                }),
            )) as SendableRecordBatchStream)
        });
        assert_eq!(rows(slow).await?, 3);
        // Another read sweeps idle entries, which must not include this one.
        rows(cache.stream(key("j", 2), schema(), source(fetches.clone()))).await?;
        rows(cache.stream(key("j", 1), schema(), source(fetches.clone()))).await?;
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn keys_depend_on_the_locations() {
        use crate::serde::scheduler::{ExecutorEndpoint, PartitionId, PartitionStats};
        let loc = |map: usize| PartitionLocation {
            map_partition_id: map,
            partition_id: PartitionId {
                job_id: "j".into(),
                stage_id: 1,
                partition_id: 0,
            },
            executor_meta: Arc::new(ExecutorEndpoint {
                id: "e".to_string(),
                host: "h".to_string(),
                port: 1,
            }),
            partition_stats: PartitionStats::default(),
            file_id: None,
            is_sort_shuffle: true,
        };
        let a = BroadcastKey::new("j", 1, &[loc(0), loc(1)]);
        assert_eq!(a, BroadcastKey::new("j", 1, &[loc(0), loc(1)]));
        assert_ne!(a, BroadcastKey::new("j", 1, &[loc(0), loc(2)]));
        assert_ne!(a, BroadcastKey::new("j", 2, &[loc(0), loc(1)]));
    }
}
