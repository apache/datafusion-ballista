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

use crate::loading_cache::CacheGetStatus;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use crate::listener::cache_policy::CachePolicyListener;
use crate::listener::loading_cache::LoadingCacheListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Struct containing all the metrics
#[derive(Debug)]
pub struct Metrics<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    get_hit_count: U64Counter,
    get_miss_count: U64Counter,
    get_miss_already_loading_count: U64Counter,
    get_cancelled_count: U64Counter,
    put_count: U64Counter,
    eviction_count: U64Counter,
    _key_marker: PhantomData<K>,
    _value_marker: PhantomData<V>,
}

impl<K, V> Default for Metrics<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Metrics<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            get_hit_count: Default::default(),
            get_miss_count: Default::default(),
            get_miss_already_loading_count: Default::default(),
            get_cancelled_count: Default::default(),
            put_count: Default::default(),
            eviction_count: Default::default(),
            _key_marker: Default::default(),
            _value_marker: Default::default(),
        }
    }

    pub fn get_hit_count(&self) -> u64 {
        self.get_hit_count.fetch()
    }

    pub fn get_miss_count(&self) -> u64 {
        self.get_miss_count.fetch()
    }

    pub fn get_miss_already_loading_count(&self) -> u64 {
        self.get_miss_already_loading_count.fetch()
    }

    pub fn get_cancelled_count(&self) -> u64 {
        self.get_cancelled_count.fetch()
    }

    pub fn put_count(&self) -> u64 {
        self.put_count.fetch()
    }

    pub fn eviction_count(&self) -> u64 {
        self.eviction_count.fetch()
    }
}

// Since we don't store K and V directly, it will be safe.
unsafe impl<K, V> Sync for Metrics<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
}

impl<K, V> LoadingCacheListener for Metrics<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn listen_on_get_if_present(&self, _k: Self::K, v: Option<Self::V>) {
        if v.is_some() {
            &self.get_hit_count
        } else {
            &self.get_miss_count
        }
        .inc(1);
    }

    fn listen_on_get(&self, _k: Self::K, _v: Self::V, status: CacheGetStatus) {
        match status {
            CacheGetStatus::Hit => &self.get_hit_count,

            CacheGetStatus::Miss => &self.get_miss_count,

            CacheGetStatus::MissAlreadyLoading => &self.get_miss_already_loading_count,
        }
        .inc(1);
    }

    fn listen_on_put(&self, _k: Self::K, _v: Self::V) {
        // Do nothing
    }

    fn listen_on_invalidate(&self, _k: Self::K) {
        // Do nothing
    }

    fn listen_on_get_cancelling(&self, _k: Self::K) {
        self.get_cancelled_count.inc(1);
    }
}

impl<K, V> CachePolicyListener for Metrics<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn listen_on_get(&self, _k: Self::K, _v: Option<Self::V>) {
        // Do nothing
    }

    fn listen_on_peek(&self, _k: Self::K, _v: Option<Self::V>) {
        // Do nothing
    }

    fn listen_on_put(&self, _k: Self::K, _v: Self::V, _old_v: Option<Self::V>) {
        self.put_count.inc(1);
    }

    fn listen_on_remove(&self, _k: Self::K, _v: Option<Self::V>) {
        self.eviction_count.inc(1);
    }

    fn listen_on_pop(&self, _entry: (Self::K, Self::V)) {
        self.eviction_count.inc(1);
    }
}

/// A monotonic counter
#[derive(Debug, Clone, Default)]
pub struct U64Counter {
    counter: Arc<AtomicU64>,
}

impl U64Counter {
    pub fn inc(&self, count: u64) {
        self.counter.fetch_add(count, Ordering::Relaxed);
    }

    pub fn fetch(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::policy::lru::hashlink::lru_cache::LruCache;
    use crate::backend::policy::lru::DefaultResourceCounter;
    use crate::create_loading_cache_with_metrics;
    use crate::loading_cache::loader::CacheLoader;
    use crate::loading_cache::LoadingCache;
    use async_trait::async_trait;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_metrics() {
        let cache_policy =
            LruCache::with_resource_counter(DefaultResourceCounter::new(3));
        let loader = TestStringCacheLoader {
            prefix: "file".to_string(),
        };
        let (loading_cache, metrics) =
            create_loading_cache_with_metrics(cache_policy, Arc::new(loader));

        assert_eq!(
            "file1".to_string(),
            loading_cache.get("1".to_string(), ()).await
        );
        assert_eq!(
            "file2".to_string(),
            loading_cache.get("2".to_string(), ()).await
        );
        assert_eq!(
            "file3".to_string(),
            loading_cache.get("3".to_string(), ()).await
        );
        assert_eq!(3, metrics.get_miss_count());

        assert_eq!(
            "file4".to_string(),
            loading_cache.get("4".to_string(), ()).await
        );
        assert_eq!(0, metrics.get_hit_count());
        assert_eq!(4, metrics.get_miss_count());
        assert_eq!(4, metrics.put_count());
        assert_eq!(1, metrics.eviction_count());

        assert!(loading_cache.get_if_present("1".to_string()).is_none());
        assert_eq!(0, metrics.get_hit_count());
        assert_eq!(5, metrics.get_miss_count());
        assert_eq!(4, metrics.put_count());
        assert_eq!(1, metrics.eviction_count());

        loading_cache
            .put("2".to_string(), "file2-bak".to_string())
            .await;
        assert_eq!(0, metrics.get_hit_count());
        assert_eq!(5, metrics.get_miss_count());
        assert_eq!(5, metrics.put_count());
        assert_eq!(1, metrics.eviction_count());

        assert_eq!(
            "file5".to_string(),
            loading_cache.get("5".to_string(), ()).await
        );
        assert_eq!(0, metrics.get_hit_count());
        assert_eq!(6, metrics.get_miss_count());
        assert_eq!(6, metrics.put_count());
        assert_eq!(2, metrics.eviction_count());

        assert!(loading_cache.get_if_present("3".to_string()).is_none());
        assert_eq!(0, metrics.get_hit_count());
        assert_eq!(7, metrics.get_miss_count());
        assert_eq!(6, metrics.put_count());
        assert_eq!(2, metrics.eviction_count());

        assert!(loading_cache.get_if_present("2".to_string()).is_some());
        assert_eq!(1, metrics.get_hit_count());
        assert_eq!(7, metrics.get_miss_count());
        assert_eq!(6, metrics.put_count());
        assert_eq!(2, metrics.eviction_count());

        loading_cache.invalidate("2".to_string());
        assert_eq!(1, metrics.get_hit_count());
        assert_eq!(7, metrics.get_miss_count());
        assert_eq!(6, metrics.put_count());
        assert_eq!(3, metrics.eviction_count());
    }

    #[derive(Debug)]
    struct TestStringCacheLoader {
        prefix: String,
    }

    #[async_trait]
    impl CacheLoader for TestStringCacheLoader {
        type K = String;
        type V = String;
        type Extra = ();

        async fn load(&self, k: Self::K, _extra: Self::Extra) -> Self::V {
            format!("{}{k}", self.prefix)
        }
    }
}
