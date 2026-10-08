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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use ballista_core::RuntimeProducer;
use ballista_core::config::BallistaConfig;
use datafusion::config::ConfigExtension;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use lru::LruCache;
use parking_lot::Mutex;

/// Derives a task's runtime from a shared base [`RuntimeEnv`] by installing a
/// fresh per-task memory pool sized to the task's vcore claim. The base's
/// object-store registry and the cache manager's underlying file-metadata
/// (footer) cache are preserved via
/// [`RuntimeEnvBuilder::from_runtime_env`](datafusion::execution::runtime_env::RuntimeEnvBuilder::from_runtime_env)
/// — the outer `CacheManager` is rebuilt around that same inner cache — so
/// read-side state is shared while the memory pool stays per task.
///
/// `vcores_consumed` is the number of vcores this task claims from the
/// executor's budget (`min(input_partition_ids.len(), budget)` for non-collapse
/// stages, `1` for collapse). The policy scales the pool proportionally so a
/// task claiming N/total vcores gets N/total of the executor's memory budget.
pub type MemoryPoolPolicy = Arc<
    dyn Fn(
            Arc<RuntimeEnv>,
            &SessionConfig,
            u32,
        ) -> datafusion::error::Result<Arc<RuntimeEnv>>
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
    /// Returns the per-task runtime for `session_id`, sized to `vcores_consumed`
    /// vcores of the executor's memory budget.
    fn produce_runtime(
        &self,
        session_id: &str,
        config: &SessionConfig,
        vcores_consumed: u32,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>>;
}

/// Shared base envs keyed by (session id, `config_fingerprint`) — see
/// [`DefaultSessionRuntimeCache`].
type BaseRuntimeLru = LruCache<(String, u64), Arc<RuntimeEnv>>;

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
/// The cache key pairs the session id with a fingerprint of the session
/// config's extension entries (see `config_fingerprint`). The base producer
/// captures config state when it builds the env — the S3-aware producer
/// clones the session's `S3Options` into the object-store registry — so a
/// `SET` that lands between two tasks of the same session must miss the
/// cache and build a fresh base; otherwise the new task keeps reading with
/// the settings the session's first task carried.
///
/// The cache is bounded by an LRU of `capacity` sessions. A capacity of `0`
/// disables caching entirely: every call builds a fresh base env, matching
/// the behavior of building a runtime per task.
pub struct DefaultSessionRuntimeCache {
    base_producer: RuntimeProducer,
    pool_policy: MemoryPoolPolicy,
    cache: Option<Mutex<BaseRuntimeLru>>,
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
        vcores_consumed: u32,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        let base = match &self.cache {
            None => (self.base_producer)(config)?,
            Some(cache) => {
                let key = (session_id.to_string(), config_fingerprint(config));
                if let Some(base) = cache.lock().get(&key) {
                    base.clone()
                } else {
                    // Build the base env without holding the lock, so a miss
                    // never stalls other sessions' lookups. A rare concurrent
                    // first-miss for the same session may build twice; that is
                    // harmless (idempotent, cheap) — the last writer wins and
                    // the extra env is dropped.
                    let base = (self.base_producer)(config)?;
                    cache.lock().put(key, base.clone());
                    base
                }
            }
        };
        (self.pool_policy)(base, config, vcores_consumed)
    }
}

/// Fingerprint of the session config's extension entries.
///
/// A base [`RuntimeEnv`] bakes in whatever config the producer reads at build
/// time (e.g. the `S3Options` cloned into the object-store registry), and
/// every extension entry flows through `SET`-driven task properties, so this
/// is the part of the config that can legitimately change mid-session.
/// Built-in `datafusion.*` options are not included: they do not feed the
/// base env's shared state.
///
/// The `ballista` extension is skipped for the same reason: none of the
/// shipped producers read it, and the client re-sends `ballista.job.name`
/// with every job, so keying on it would give a session that `SET`s a job
/// name a fresh fingerprint per query — each one building a new base (and
/// dropping the footer cache and cross-query reuse) and taking another LRU
/// slot.
///
/// Extension iteration is ordered by prefix, and each extension's entries
/// are sorted by key, so equal configs always produce equal fingerprints.
fn config_fingerprint(config: &SessionConfig) -> u64 {
    let mut hasher = DefaultHasher::new();
    for (prefix, extension) in config.options().extensions.iter() {
        if prefix == BallistaConfig::PREFIX {
            continue;
        }
        prefix.hash(&mut hasher);
        let mut entries = extension.entries();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        for entry in entries {
            entry.key.hash(&mut hasher);
            entry.value.hash(&mut hasher);
        }
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::config::BALLISTA_JOB_NAME;
    use ballista_core::extension::{SessionConfigExt, SessionConfigHelperExt};
    use ballista_core::object_store::{
        runtime_env_with_s3_support, session_config_with_s3_support,
    };
    use ballista_core::serde::protobuf::KeyValuePair;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    fn base_producer() -> RuntimeProducer {
        Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())))
    }

    /// Identity policy: the task shares the base env unchanged (no pool swap).
    fn identity_policy() -> MemoryPoolPolicy {
        Arc::new(|base, _, _| Ok(base))
    }

    /// Rebuilding policy: mimics production — a fresh per-task pool layered onto
    /// the shared base, preserving the base's read-side state.
    fn per_task_pool_policy() -> MemoryPoolPolicy {
        Arc::new(|base, _, _| {
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
        let e1 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        assert!(Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn different_sessions_get_different_base() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 4);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        let e2 = cache.produce_runtime("s2", &cfg, 1).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn per_task_pool_shares_footer_cache_but_not_env() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), per_task_pool_policy(), 4);
        let cfg = SessionConfig::new();
        let e1 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg, 1).unwrap();
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
        let e1 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        let e2 = cache.produce_runtime("s1", &cfg, 1).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }

    #[test]
    fn evicts_least_recently_used() {
        let cache =
            DefaultSessionRuntimeCache::new(base_producer(), identity_policy(), 2);
        let cfg = SessionConfig::new();
        let s1_a = cache.produce_runtime("s1", &cfg, 1).unwrap();
        cache.produce_runtime("s2", &cfg, 1).unwrap();
        // Inserting s3 (capacity 2) evicts the least-recently-used, s1.
        cache.produce_runtime("s3", &cfg, 1).unwrap();
        let s1_b = cache.produce_runtime("s1", &cfg, 1).unwrap();
        assert!(!Arc::ptr_eq(&s1_a.cache_manager, &s1_b.cache_manager));
    }

    fn kv(key: &str, value: &str) -> KeyValuePair {
        KeyValuePair {
            key: key.to_string(),
            value: Some(value.to_string()),
        }
    }

    /// Mirrors what `execution_loop` does per task: a default config with the
    /// task's `s3.*` properties applied on top.
    fn s3_task_config(key_id: &str) -> SessionConfig {
        session_config_with_s3_support().update_from_key_value_pair(&[
            kv("s3.access_key_id", key_id),
            kv("s3.secret_access_key", "secret"),
            kv("s3.region", "us-east-1"),
        ])
    }

    fn s3_cache(capacity: usize) -> DefaultSessionRuntimeCache {
        DefaultSessionRuntimeCache::new(
            Arc::new(runtime_env_with_s3_support),
            identity_policy(),
            capacity,
        )
    }

    #[test]
    fn changed_s3_options_build_new_base_runtime() {
        let url = ObjectStoreUrl::parse("s3://bucket").unwrap();
        let cache = s3_cache(16);

        let rt1 = cache
            .produce_runtime("s1", &s3_task_config("KEY_ONE"), 1)
            .unwrap();
        let store1 = format!("{:?}", rt1.object_store(&url).unwrap());
        assert!(store1.contains("KEY_ONE"));

        // A SET between two tasks of the same session must reach the second
        // task; keyed by session id alone the second task kept the first
        // task's S3Options.
        let rt2 = cache
            .produce_runtime("s1", &s3_task_config("KEY_TWO"), 1)
            .unwrap();
        let store2 = format!("{:?}", rt2.object_store(&url).unwrap());
        assert!(store2.contains("KEY_TWO"));
        assert!(!store2.contains("KEY_ONE"));
    }

    #[test]
    fn unchanged_s3_options_share_base_runtime() {
        let cache = s3_cache(16);
        // Two separately built configs with equal extension entries must hit
        // the same cached base.
        let e1 = cache
            .produce_runtime("s1", &s3_task_config("KEY_ONE"), 1)
            .unwrap();
        let e2 = cache
            .produce_runtime("s1", &s3_task_config("KEY_ONE"), 1)
            .unwrap();
        assert!(Arc::ptr_eq(&e1, &e2));
    }

    #[test]
    fn reverted_s3_options_hit_original_base_runtime() {
        let cache = s3_cache(16);
        let e1 = cache
            .produce_runtime("s1", &s3_task_config("KEY_ONE"), 1)
            .unwrap();
        let e2 = cache
            .produce_runtime("s1", &s3_task_config("KEY_TWO"), 1)
            .unwrap();
        let e3 = cache
            .produce_runtime("s1", &s3_task_config("KEY_ONE"), 1)
            .unwrap();
        assert!(!Arc::ptr_eq(&e1, &e2));
        assert!(Arc::ptr_eq(&e1, &e3));
    }

    #[test]
    fn different_sessions_same_s3_options_get_different_base() {
        let cache = s3_cache(16);
        let e1 = cache
            .produce_runtime("s1", &s3_task_config("KEY_ONE"), 1)
            .unwrap();
        let e2 = cache
            .produce_runtime("s2", &s3_task_config("KEY_ONE"), 1)
            .unwrap();
        assert!(!Arc::ptr_eq(&e1, &e2));
    }

    /// Same as `s3_task_config` plus a `ballista.job.name`, mimicking the
    /// client re-sending a per-query job name with every job.
    fn s3_task_config_with_job_name(key_id: &str, job_name: &str) -> SessionConfig {
        session_config_with_s3_support().update_from_key_value_pair(&[
            kv("s3.access_key_id", key_id),
            kv("s3.secret_access_key", "secret"),
            kv("s3.region", "us-east-1"),
            kv(BALLISTA_JOB_NAME, job_name),
        ])
    }

    #[test]
    fn changed_ballista_job_name_hits_same_base_runtime() {
        let with_name = |key_id: &str, job_name: &str| {
            let cfg = s3_task_config_with_job_name(key_id, job_name);
            // sanity: the SET actually landed on the ballista extension
            assert_eq!(
                cfg.ballista_config()
                    .settings()
                    .get(BALLISTA_JOB_NAME)
                    .map(String::as_str),
                Some(job_name)
            );
            cfg
        };
        let cache = s3_cache(16);
        let e1 = cache
            .produce_runtime("s1", &with_name("KEY_ONE", "job-a"), 1)
            .unwrap();
        let e2 = cache
            .produce_runtime("s1", &with_name("KEY_ONE", "job-b"), 1)
            .unwrap();
        assert!(Arc::ptr_eq(&e1, &e2));
    }

    #[test]
    fn fingerprint_ignores_ballista_extension() {
        let a = config_fingerprint(&s3_task_config_with_job_name("KEY_ONE", "job-a"));
        let b = config_fingerprint(&s3_task_config_with_job_name("KEY_ONE", "job-b"));
        assert_eq!(a, b);
        // s3.* entries still rekey the fingerprint
        let c = config_fingerprint(&s3_task_config("KEY_TWO"));
        assert_ne!(a, c);
    }

    #[test]
    fn fingerprint_tracks_extension_entries() {
        let a = config_fingerprint(&s3_task_config("KEY_ONE"));
        let a_again = config_fingerprint(&s3_task_config("KEY_ONE"));
        let b = config_fingerprint(&s3_task_config("KEY_TWO"));
        let no_ext = config_fingerprint(&SessionConfig::new());
        assert_eq!(a, a_again);
        assert_ne!(a, b);
        assert_ne!(a, no_ext);
    }
}
