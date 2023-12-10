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

use crate::cache_layer::medium::CacheMedium;
use crate::cache_layer::object_store::ObjectStoreWithKey;
use crate::error::{BallistaError, Result};
use async_trait::async_trait;
use ballista_cache::backend::policy::lru::lru_cache::LruCache;
use ballista_cache::backend::policy::lru::ResourceCounter;
use ballista_cache::listener::cache_policy::{
    CachePolicyListener, CachePolicyWithListener,
};
use ballista_cache::loading_cache::loader::CacheLoader;
use ballista_cache::{
    create_loading_cache_with_metrics, DefaultLoadingCache, LoadingCacheMetrics,
};
use log::{error, info, warn};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use std::ops::Range;
use std::sync::Arc;
use tokio::runtime::Runtime;

type DefaultFileLoadingCache<M> =
    DefaultLoadingCache<Path, ObjectMeta, FileCacheLoader<M>>;
type FileCacheMetrics = LoadingCacheMetrics<Path, ObjectMeta>;

#[derive(Debug)]
pub struct FileCacheLayer<M>
where
    M: CacheMedium,
{
    cache_store: Arc<dyn ObjectStore>,
    loading_cache: DefaultFileLoadingCache<M>,
    io_runtime: Runtime,
    metrics: Arc<FileCacheMetrics>,
}

impl<M> FileCacheLayer<M>
where
    M: CacheMedium,
{
    pub fn new(capacity: usize, cache_io_concurrency: u32, cache_medium: M) -> Self {
        let cache_store = cache_medium.get_object_store();

        let cache_counter = FileCacheCounter::new(capacity);
        let lru_cache = LruCache::with_resource_counter(cache_counter);
        let file_cache_loader = Arc::new(FileCacheLoader::new(cache_medium));
        let cache_with_removal_listener =
            CachePolicyWithListener::new(lru_cache, vec![file_cache_loader.clone()]);
        let (loading_cache, metrics) = create_loading_cache_with_metrics(
            cache_with_removal_listener,
            file_cache_loader,
        );
        let io_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("loading_cache")
            .worker_threads(cache_io_concurrency as usize)
            .build()
            .expect("Creating tokio runtime");

        Self {
            cache_store,
            loading_cache,
            io_runtime,
            metrics,
        }
    }

    pub fn cache_store(&self) -> Arc<dyn ObjectStore> {
        self.cache_store.clone()
    }

    pub fn cache(&self) -> &DefaultFileLoadingCache<M> {
        &self.loading_cache
    }

    pub fn io_runtime(&self) -> &Runtime {
        &self.io_runtime
    }

    pub fn metrics(&self) -> &FileCacheMetrics {
        self.metrics.as_ref()
    }
}

#[derive(Debug)]
pub struct FileCacheLoader<M>
where
    M: CacheMedium,
{
    cache_medium: Arc<M>,
}

impl<M> FileCacheLoader<M>
where
    M: CacheMedium,
{
    fn new(cache_medium: M) -> Self {
        Self {
            cache_medium: Arc::new(cache_medium),
        }
    }

    fn remove_object(&self, source_path: Path, object_meta: ObjectMeta) {
        let cache_store = self.cache_medium.get_object_store();
        let location = object_meta.location;
        tokio::runtime::Handle::try_current().unwrap().block_on( async {
            if let Err(e) = cache_store.delete(&location).await {
                error!("Fail to delete file {location} on the cache ObjectStore for source {source_path} due to {e}");
            }
        });
    }
}

/// Will return the location of the cached file on the cache object store.
///
/// The last_modified of the ObjectMeta will be from the source file, which will be useful
/// for checking whether the source file changed or not.
///
/// The size will be the one of cached file rather than the one of the source file in case of changing the data format
async fn load_object<M>(
    cache_medium: Arc<M>,
    source_location: Path,
    source_store: &ObjectStoreWithKey,
) -> Result<ObjectMeta>
where
    M: CacheMedium,
{
    let source_meta = source_store.head(&source_location).await.map_err(|e| {
        BallistaError::General(format!(
            "Fail to read head info for {source_location} due to {e}"
        ))
    })?;

    let cache_store = cache_medium.get_object_store();
    let cache_location =
        cache_medium.get_mapping_location(&source_location, source_store);

    // Check whether the cache location exist or not. If exists, delete it first.
    if cache_store.head(&cache_location).await.is_ok() {
        if let Err(e) = cache_store.delete(&cache_location).await {
            error!(
                "Fail to delete file {cache_location} on the cache ObjectStore due to {e}"
            );
        }
    }

    info!(
        "Going to cache object from {} to {}",
        source_location, cache_location
    );
    let range = Range {
        start: 0,
        end: source_meta.size,
    };
    let data = source_store
        .get_range(&source_location, range)
        .await
        .map_err(|e| {
            BallistaError::General(format!(
                "Fail to get file data from {source_location} due to {e}"
            ))
        })?;
    info!(
        "{} bytes will be cached for {}",
        data.len(),
        source_location
    );
    cache_store.put(&cache_location, data).await.map_err(|e| {
        BallistaError::General(format!(
            "Fail to write out data to {cache_location} due to {e}"
        ))
    })?;
    info!(
        "Object {} has already been cached to {}",
        source_location, cache_location
    );

    let cache_meta = cache_store.head(&cache_location).await.map_err(|e| {
        BallistaError::General(format!(
            "Fail to read head info for {cache_location} due to {e}"
        ))
    })?;

    Ok(ObjectMeta {
        location: cache_location,
        last_modified: source_meta.last_modified,
        size: cache_meta.size,
        e_tag: source_meta.e_tag,
        version: None,
    })
}

#[async_trait]
impl<M> CacheLoader for FileCacheLoader<M>
where
    M: CacheMedium,
{
    type K = Path;
    type V = ObjectMeta;
    type Extra = Arc<ObjectStoreWithKey>;

    async fn load(&self, source_location: Self::K, source_store: Self::Extra) -> Self::V {
        match load_object(self.cache_medium.clone(), source_location, &source_store).await
        {
            Ok(object_meta) => object_meta,
            Err(e) => panic!("{}", e),
        }
    }
}

impl<M> CachePolicyListener for FileCacheLoader<M>
where
    M: CacheMedium,
{
    type K = Path;
    type V = ObjectMeta;

    fn listen_on_get(&self, _k: Self::K, _v: Option<Self::V>) {
        // Do nothing
    }

    fn listen_on_peek(&self, _k: Self::K, _v: Option<Self::V>) {
        // Do nothing
    }

    fn listen_on_put(&self, _k: Self::K, _v: Self::V, _old_v: Option<Self::V>) {
        // Do nothing
    }

    fn listen_on_remove(&self, k: Self::K, v: Option<Self::V>) {
        if let Some(v) = v {
            self.remove_object(k, v);
        } else {
            warn!("The entry does not exist for key {k}");
        }
    }

    fn listen_on_pop(&self, entry: (Self::K, Self::V)) {
        self.remove_object(entry.0, entry.1);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FileCacheCounter {
    /// The maximum data size to be cached
    capacity: usize,
    /// The data size already be cached
    cached_size: usize,
}

impl FileCacheCounter {
    pub fn new(capacity: usize) -> Self {
        FileCacheCounter {
            capacity,
            cached_size: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn cached_size(&self) -> usize {
        self.cached_size
    }
}

impl ResourceCounter for FileCacheCounter {
    type K = Path;
    type V = ObjectMeta;

    fn consume(&mut self, _k: &Self::K, v: &Self::V) {
        self.cached_size += v.size;
    }

    fn restore(&mut self, _k: &Self::K, v: &Self::V) {
        self.cached_size -= v.size;
    }

    fn exceed_capacity(&self) -> bool {
        self.cached_size > self.capacity
    }
}
