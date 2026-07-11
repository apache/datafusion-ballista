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

//! Session-scoped reuse of executor runtime state.

use std::num::NonZeroUsize;
use std::sync::Arc;

use ballista_core::RuntimeProducer;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use lru::LruCache;
use parking_lot::Mutex;

/// Derives a task's runtime from a shared base [`RuntimeEnv`] by installing a
/// fresh per-task memory pool. The base's object-store registry and the cache
/// manager's underlying file-metadata (footer) cache are preserved via
/// [`RuntimeEnvBuilder::from_runtime_env`](datafusion::execution::runtime_env::RuntimeEnvBuilder::from_runtime_env)
/// — the outer `CacheManager` is rebuilt around that same inner cache — so
/// read-side state is shared while the memory pool stays per task.
pub type MemoryPoolPolicy = Arc<
    dyn Fn(Arc<RuntimeEnv>, &SessionConfig) -> datafusion::error::Result<Arc<RuntimeEnv>>
        + Send
        + Sync,
>;

/// Produces a task's [`RuntimeEnv`], optionally reusing read-side state across
/// the tasks of a session.
///
/// The executor calls [`produce_runtime`](Self::produce_runtime) for every task;
/// implementors decide whether and how to share base runtime state (object-store
/// clients, file-metadata cache) between a session's tasks.
/// [`DefaultSessionRuntimeCache`] is the built-in implementation — provide a
/// custom one to change the caching/sharing strategy.
pub trait SessionRuntimeCache: Send + Sync {
    /// Returns the per-task runtime for `session_id`.
    fn produce_runtime(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>>;
}

/// A bounded, session-keyed cache of shared *base* [`RuntimeEnv`]s.
///
/// A base env carries the read-side state safe to share across all tasks of a
/// session: the object-store registry and the cache manager (whose Parquet
/// footer cache is thereby reused across the session's tasks and queries).
/// `RuntimeEnvBuilder::from_runtime_env` also carries over the disk manager
/// (rooted at the executor's `work_dir`), so a session's tasks share one
/// `DiskManager` too; this is safe because spill temp files are uniquely
/// named, matching the standard one-`RuntimeEnv`-per-`SessionContext` model.
/// Each task's real runtime is produced by applying [`MemoryPoolPolicy`] to
/// the shared base, which installs a fresh per-task memory pool — so memory
/// isolation is unchanged.
///
/// The cache is bounded by an LRU of `capacity` sessions. A capacity of `0`
/// disables caching entirely: every call builds a fresh base env, matching the
/// behavior of building a runtime per task.
pub struct DefaultSessionRuntimeCache {
    base_producer: RuntimeProducer,
    pool_policy: MemoryPoolPolicy,
    cache: Option<Mutex<LruCache<String, Arc<RuntimeEnv>>>>,
}

impl DefaultSessionRuntimeCache {
    /// Creates a new cache that produces per-task runtimes from `base_producer`
    /// (invoked at most once per cached session) and `pool_policy` (invoked on
    /// every call). `capacity` bounds the number of distinct sessions kept in
    /// the LRU; `0` disables caching.
    pub fn new(
        base_producer: RuntimeProducer,
        pool_policy: MemoryPoolPolicy,
        capacity: usize,
    ) -> Self {
        let cache = NonZeroUsize::new(capacity).map(|cap| Mutex::new(LruCache::new(cap)));
        Self {
            base_producer,
            pool_policy,
            cache,
        }
    }
}

impl SessionRuntimeCache for DefaultSessionRuntimeCache {
    /// Returns the per-task runtime for `session_id`, reusing a cached base env
    /// when present and building + caching one on miss.
    fn produce_runtime(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        let base = match &self.cache {
            None => (self.base_producer)(config)?,
            Some(cache) => {
                if let Some(base) = cache.lock().get(session_id) {
                    base.clone()
                } else {
                    // Build the base env without holding the lock, so a miss
                    // never stalls other sessions' lookups. A rare concurrent
                    // first-miss for the same session may build twice; that is
                    // harmless (idempotent, cheap) — the last writer wins and
                    // the extra env is dropped.
                    let base = (self.base_producer)(config)?;
                    cache.lock().put(session_id.to_string(), base.clone());
                    base
                }
            }
        };
        (self.pool_policy)(base, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    fn base_producer() -> RuntimeProducer {
        Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())))
    }

    /// Identity policy: the task shares the base env unchanged (no pool swap).
    fn identity_policy() -> MemoryPoolPolicy {
        Arc::new(|base, _| Ok(base))
    }

    /// Rebuilding policy: mimics production — a fresh per-task pool layered onto
    /// the shared base, preserving the base's read-side state.
    fn per_task_pool_policy() -> MemoryPoolPolicy {
        Arc::new(|base, _| {
            RuntimeEnvBuilder::from_runtime_env(&base)
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(1024)))
                .build_arc()
        })
    }

    #[test]
    fn same_session_shares_cache_manager() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 4);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg).unwrap();
        assert!(Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn different_sessions_get_different_base() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 4);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg).unwrap();
        let e2 = cache.produce_runtime("s2", &cfg).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn per_task_pool_shares_footer_cache_but_not_env() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), per_task_pool_policy(), 4);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg).unwrap();
        // Different runtime envs (fresh per-task pool)...
        assert!(!Arc::ptr_eq(&e1, &e2));
        // ...but the shared read-side state is reused: object-store registry
        // passes through unchanged, and the inner footer (file-metadata) cache
        // is the same instance even though the outer CacheManager wrapper is
        // rebuilt. Do NOT compare the outer Arc<CacheManager>.
        assert!(Arc::ptr_eq(
            &e1.object_store_registry,
            &e2.object_store_registry
        ));
        assert!(Arc::ptr_eq(
            &e1.cache_manager.get_file_metadata_cache(),
            &e2.cache_manager.get_file_metadata_cache(),
        ));
    }

    #[test]
    fn capacity_zero_disables_cache() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 0);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn evicts_least_recently_used() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 2);
        let cfg = SessionConfig::new();
        let s1_a = cache.produce_runtime("s1", &cfg).unwrap();
        cache.produce_runtime("s2", &cfg).unwrap();
        // Inserting s3 (capacity 2) evicts the least-recently-used, s1.
        cache.produce_runtime("s3", &cfg).unwrap();
        let s1_b = cache.produce_runtime("s1", &cfg).unwrap();
        assert!(!Arc::ptr_eq(&s1_a.cache_manager, &s1_b.cache_manager));
    }
}
