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

use crate::cache_layer::object_store::file::FileCacheObjectStore;
use crate::cache_layer::object_store::ObjectStoreWithKey;
use crate::cache_layer::CacheLayer;
use crate::object_store_registry::BallistaObjectStoreRegistry;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::RuntimeConfig;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

/// Get a RuntimeConfig with CachedBasedObjectStoreRegistry
pub fn with_cache_layer(config: RuntimeConfig, cache_layer: CacheLayer) -> RuntimeConfig {
    let registry = Arc::new(BallistaObjectStoreRegistry::default());
    let registry = Arc::new(CachedBasedObjectStoreRegistry::new(registry, cache_layer));
    config.with_object_store_registry(registry)
}

/// An object store registry wrapped an existing one with a cache layer.
///
/// During [`get_store`], after getting the source [`ObjectStore`], based on the url,
/// it will firstly be wrapped with a key which will be used as the cache prefix path.
/// And then it will be wrapped with the [`cache_layer`].
#[derive(Debug)]
pub struct CachedBasedObjectStoreRegistry {
    inner: Arc<dyn ObjectStoreRegistry>,
    cache_layer: CacheLayer,
}

impl CachedBasedObjectStoreRegistry {
    pub fn new(inner: Arc<dyn ObjectStoreRegistry>, cache_layer: CacheLayer) -> Self {
        Self { inner, cache_layer }
    }
}

impl ObjectStoreRegistry for CachedBasedObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        let source_object_store = self.inner.get_store(url)?;
        let object_store_with_key = Arc::new(ObjectStoreWithKey::new(
            get_url_key(url),
            source_object_store,
        ));
        Ok(match &self.cache_layer {
            CacheLayer::LocalDiskFile(cache_layer) => Arc::new(
                FileCacheObjectStore::new(cache_layer.clone(), object_store_with_key),
            ),
            CacheLayer::LocalMemoryFile(cache_layer) => Arc::new(
                FileCacheObjectStore::new(cache_layer.clone(), object_store_with_key),
            ),
        })
    }
}

/// Get the key of a url for object store cache prefix path.
/// The credential info will be removed.
fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}
