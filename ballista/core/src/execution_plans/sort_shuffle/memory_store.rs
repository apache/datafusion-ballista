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

//! Executor-wide store for sort-shuffle output kept in memory instead of
//! written to the work directory.
//!
//! Writing shuffle bytes to disk and reading them back is the single largest
//! cost in a distributed query on this engine: pointing `--work-dir` at a RAM
//! disk cuts TPC-H SF10 wall time by roughly 40%, which is the whole of the
//! remaining gap to single-process DataFusion. This store captures that win
//! for outputs small enough to hold, and leaves everything else on the
//! existing disk path.
//!
//! # Entries are never evicted
//!
//! A task that skipped its file write has nowhere else to serve its output
//! from, so evicting an entry would lose data that downstream stages still
//! need. The budget is therefore enforced at **admission** time: a task whose
//! output does not fit writes to disk as before. An admitted entry lives
//! until its job's data is cleaned up.
//!
//! `ballista.shuffle.memory_store_limit_bytes` bounds the whole executor
//! (1 GiB by default). Once it is full, further tasks write to disk as
//! before, so the store degrades into the old behaviour rather than failing.

use crate::JobId;
use datafusion::arrow::datatypes::SchemaRef;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

/// Identifies one task's shuffle output within an executor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShuffleKey {
    pub job_id: JobId,
    pub stage_id: usize,
    pub file_id: u64,
}

/// One task's shuffle output, held as the same IPC bytes that would otherwise
/// have been written to `data.arrow`.
///
/// `partitions[k]` holds output partition `k`'s bytes: zero or more
/// concatenated Arrow IPC streams, one per local input partition that
/// contributed rows. That is byte-identical to the range the on-disk reader
/// would have addressed through the index, so both paths decode the same way.
#[derive(Debug)]
pub struct InMemoryShuffle {
    pub schema: SchemaRef,
    /// The schema-header IPC stream that leads the on-disk file. Block
    /// transport prepends it to a partition's bytes so the receiver recovers
    /// the schema even when the partition is empty, so the store has to carry
    /// it as well.
    pub header: Vec<u8>,
    pub partitions: Vec<Vec<u8>>,
}

impl InMemoryShuffle {
    /// Total bytes held, which is what the store charges against its budget.
    pub fn size_bytes(&self) -> usize {
        self.header.len() + self.partitions.iter().map(|p| p.len()).sum::<usize>()
    }
}

#[derive(Default)]
struct StoreState {
    entries: HashMap<ShuffleKey, Arc<InMemoryShuffle>>,
    used_bytes: usize,
}

/// Executor-wide in-memory shuffle store.
pub struct ShuffleMemoryStore {
    state: Mutex<StoreState>,
}

static GLOBAL: LazyLock<ShuffleMemoryStore> = LazyLock::new(|| ShuffleMemoryStore {
    state: Mutex::new(StoreState::default()),
});

/// The process-wide store. Shuffle writers and the read paths that serve
/// their output run in the same executor process, so a single instance is
/// what lets a write skip the filesystem and a later read still find it.
pub fn global() -> &'static ShuffleMemoryStore {
    &GLOBAL
}

impl ShuffleMemoryStore {
    /// Admit `shuffle` under `key` if it fits the remaining budget, returning
    /// whether it was stored. `false` means the caller must write to disk
    /// instead — it is a normal outcome, not an error.
    pub fn try_insert(
        &self,
        key: ShuffleKey,
        shuffle: InMemoryShuffle,
        total_limit_bytes: usize,
    ) -> bool {
        if total_limit_bytes == 0 {
            return false;
        }
        let size = shuffle.size_bytes();
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.used_bytes.saturating_add(size) > total_limit_bytes {
            return false;
        }
        state.used_bytes += size;
        state.entries.insert(key, Arc::new(shuffle));
        true
    }

    /// Admit an entry whose bytes are only built if it fits.
    ///
    /// `size_bytes` is what the caller intends to store; `build` is called
    /// under the lock only after the budget check passes, so a task that does
    /// not fit never pays to assemble its buffers and never has to undo the
    /// move. `build` must not block — it exists to move already-encoded
    /// buffers into place.
    pub fn try_insert_with<F>(
        &self,
        key: ShuffleKey,
        total_limit_bytes: usize,
        size_bytes: usize,
        build: F,
    ) -> bool
    where
        F: FnOnce() -> InMemoryShuffle,
    {
        if total_limit_bytes == 0 {
            return false;
        }
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.used_bytes.saturating_add(size_bytes) > total_limit_bytes {
            return false;
        }
        state.used_bytes += size_bytes;
        state.entries.insert(key, Arc::new(build()));
        true
    }

    /// Look up a task's stored output, if it was admitted.
    pub fn get(&self, key: &ShuffleKey) -> Option<Arc<InMemoryShuffle>> {
        self.state.lock().ok()?.entries.get(key).cloned()
    }

    /// Release a job's entries. With an empty `stage_ids` every stage of the
    /// job is released, matching `remove_job_data`'s whole-job form.
    pub fn remove_job(&self, job_id: &JobId, stage_ids: &[u32]) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let mut freed = 0usize;
        state.entries.retain(|key, value| {
            let job_matches = &key.job_id == job_id;
            let stage_matches =
                stage_ids.is_empty() || stage_ids.contains(&(key.stage_id as u32));
            if job_matches && stage_matches {
                freed += value.size_bytes();
                false
            } else {
                true
            }
        });
        state.used_bytes = state.used_bytes.saturating_sub(freed);
    }

    /// Bytes currently held.
    pub fn used_bytes(&self) -> usize {
        self.state.lock().map(|s| s.used_bytes).unwrap_or(0)
    }

    /// Number of task outputs currently held.
    pub fn len(&self) -> usize {
        self.state.lock().map(|s| s.entries.len()).unwrap_or(0)
    }

    /// Whether the store holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    fn clear(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.entries.clear();
            state.used_bytes = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn shuffle(sizes: &[usize]) -> InMemoryShuffle {
        InMemoryShuffle {
            schema: schema(),
            header: Vec::new(),
            partitions: sizes.iter().map(|n| vec![0u8; *n]).collect(),
        }
    }

    fn key(job: &str, stage: usize, file: u64) -> ShuffleKey {
        ShuffleKey {
            job_id: job.into(),
            stage_id: stage,
            file_id: file,
        }
    }

    #[test]
    fn admits_and_serves_within_budget() {
        let store = ShuffleMemoryStore {
            state: Mutex::new(StoreState::default()),
        };
        assert!(store.try_insert(key("j", 1, 0), shuffle(&[100, 200]), 1024));
        let got = store.get(&key("j", 1, 0)).expect("entry must be readable");
        assert_eq!(got.partitions.len(), 2);
        assert_eq!(store.used_bytes(), 300);
    }

    #[test]
    fn rejects_a_task_larger_than_the_whole_budget() {
        let store = ShuffleMemoryStore {
            state: Mutex::new(StoreState::default()),
        };
        assert!(!store.try_insert(key("j", 1, 0), shuffle(&[500]), 100));
        assert!(store.get(&key("j", 1, 0)).is_none());
        assert_eq!(store.used_bytes(), 0, "a rejected task must not be charged");
    }

    #[test]
    fn rejects_once_the_total_budget_is_reached() {
        let store = ShuffleMemoryStore {
            state: Mutex::new(StoreState::default()),
        };
        assert!(store.try_insert(key("j", 1, 0), shuffle(&[600]), 1000));
        // Would take the store to 1200 > 1000, so it has to go to disk.
        assert!(!store.try_insert(key("j", 1, 1), shuffle(&[600]), 1000));
        assert_eq!(store.used_bytes(), 600);
        assert!(store.get(&key("j", 1, 0)).is_some(), "admitted entry stays");
    }

    #[test]
    fn zero_limit_disables_the_store() {
        let store = ShuffleMemoryStore {
            state: Mutex::new(StoreState::default()),
        };
        assert!(!store.try_insert(key("j", 1, 0), shuffle(&[1]), 0));
    }

    #[test]
    fn remove_job_frees_only_the_named_stages() {
        let store = ShuffleMemoryStore {
            state: Mutex::new(StoreState::default()),
        };
        store.try_insert(key("j", 1, 0), shuffle(&[100]), 10_000);
        store.try_insert(key("j", 2, 0), shuffle(&[100]), 10_000);
        store.try_insert(key("other", 1, 0), shuffle(&[100]), 10_000);

        store.remove_job(&"j".into(), &[1]);
        assert!(store.get(&key("j", 1, 0)).is_none());
        assert!(store.get(&key("j", 2, 0)).is_some());
        assert!(store.get(&key("other", 1, 0)).is_some());
        assert_eq!(store.used_bytes(), 200);

        // Empty stage list releases the whole job.
        store.remove_job(&"j".into(), &[]);
        assert!(store.get(&key("j", 2, 0)).is_none());
        assert!(store.get(&key("other", 1, 0)).is_some());
        assert_eq!(store.used_bytes(), 100);
    }

    #[test]
    fn global_store_is_shared() {
        global().clear();
        assert!(global().is_empty());
        assert!(global().try_insert(key("g", 1, 0), shuffle(&[10]), 1000));
        assert!(global().get(&key("g", 1, 0)).is_some());
        global().remove_job(&"g".into(), &[]);
        assert!(global().is_empty());
    }
}
