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
use crate::cache_layer::policy::file::FileCacheLayer;
use crate::error::BallistaError;
use async_trait::async_trait;
use ballista_cache::loading_cache::LoadingCache;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use log::info;
use object_store::path::Path;
use object_store::{
    Error, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutOptions, PutResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct FileCacheObjectStore<M>
where
    M: CacheMedium,
{
    cache_layer: Arc<FileCacheLayer<M>>,
    inner: Arc<ObjectStoreWithKey>,
}

impl<M> FileCacheObjectStore<M>
where
    M: CacheMedium,
{
    pub fn new(
        cache_layer: Arc<FileCacheLayer<M>>,
        inner: Arc<ObjectStoreWithKey>,
    ) -> Self {
        Self { cache_layer, inner }
    }
}

impl<M> Display for FileCacheObjectStore<M>
where
    M: CacheMedium,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Object store {} with file level cache on {}",
            self.inner,
            self.cache_layer.cache_store()
        )
    }
}

#[async_trait]
impl<M> ObjectStore for FileCacheObjectStore<M>
where
    M: CacheMedium,
{
    async fn put(
        &self,
        _location: &Path,
        _bytes: Bytes,
    ) -> object_store::Result<PutResult> {
        Err(Error::NotSupported {
            source: Box::new(BallistaError::General(
                "Write path is not supported".to_string(),
            )),
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: Bytes,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(Error::NotSupported {
            source: Box::new(BallistaError::General(
                "Write path is not supported".to_string(),
            )),
        })
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(Error::NotSupported {
            source: Box::new(BallistaError::General(
                "Write path is not supported".to_string(),
            )),
        })
    }

    async fn abort_multipart(
        &self,
        _location: &Path,
        _multipart_id: &MultipartId,
    ) -> object_store::Result<()> {
        Err(Error::NotSupported {
            source: Box::new(BallistaError::General(
                "Write path is not supported".to_string(),
            )),
        })
    }

    /// If it already exists in cache, use the cached result.
    /// Otherwise, trigger a task to load the data into cache; At the meanwhile,
    /// get the result from the data source
    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        if let Some(cache_object_mata) =
            self.cache_layer.cache().get_if_present(location.clone())
        {
            info!("Data for {} is cached", location);
            let cache_location = &cache_object_mata.location;
            self.cache_layer.cache_store().get(cache_location).await
        } else {
            let io_runtime = self.cache_layer.io_runtime();
            let cache_layer = self.cache_layer.clone();
            let key = location.clone();
            let extra = self.inner.clone();
            io_runtime.spawn(async move {
                info!("Going to cache data for {}", key);
                cache_layer.cache().get(key.clone(), extra).await;
                info!("Data for {} has been cached", key);
            });
            self.inner.get(location).await
        }
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if let Some(cache_object_mata) =
            self.cache_layer.cache().get_if_present(location.clone())
        {
            info!("Data for {} is cached", location);
            let cache_location = &cache_object_mata.location;
            self.cache_layer
                .cache_store()
                .get_opts(cache_location, options)
                .await
        } else {
            let io_runtime = self.cache_layer.io_runtime();
            let cache_layer = self.cache_layer.clone();
            let key = location.clone();
            let extra = self.inner.clone();
            io_runtime.spawn(async move {
                info!("Going to cache data for {}", key);
                cache_layer.cache().get(key.clone(), extra).await;
                info!("Data for {} has been cached", key);
            });
            self.inner.get_opts(location, options).await
        }
    }

    /// If it already exists in cache, use the cached result.
    /// Otherwise, trigger a task to load the data into cache; At the meanwhile,
    /// get the result from the data source
    async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        if let Some(cache_object_mata) =
            self.cache_layer.cache().get_if_present(location.clone())
        {
            info!("Data for {} is cached", location);
            let cache_location = &cache_object_mata.location;
            self.cache_layer
                .cache_store()
                .get_range(cache_location, range)
                .await
        } else {
            let io_runtime = self.cache_layer.io_runtime();
            let cache_layer = self.cache_layer.clone();
            let key = location.clone();
            let extra = self.inner.clone();
            io_runtime.spawn(async move {
                info!("Going to cache data for {}", key);
                cache_layer.cache().get(key.clone(), extra).await;
                info!("Data for {} has been cached", key);
            });
            self.inner.get_range(location, range).await
        }
    }

    /// If it already exists in cache, use the cached result.
    /// Otherwise, get the result from the data source.
    /// It will not trigger the task to load data into cache.
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        if let Some(cache_object_mata) =
            self.cache_layer.cache().get_if_present(location.clone())
        {
            let cache_location = &cache_object_mata.location;
            self.cache_layer.cache_store().head(cache_location).await
        } else {
            self.inner.head(location).await
        }
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        Err(Error::NotSupported {
            source: Box::new(BallistaError::General(
                "Delete is not supported".to_string(),
            )),
        })
    }

    fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        stream::once(async {
            Err(Error::NotSupported {
                source: Box::new(BallistaError::General(
                    "List is not supported".to_string(),
                )),
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(Error::NotSupported {
            source: Box::new(BallistaError::General("List is not supported".to_string())),
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(Error::NotSupported {
            source: Box::new(BallistaError::General("Copy is not supported".to_string())),
        })
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> object_store::Result<()> {
        Err(Error::NotSupported {
            source: Box::new(BallistaError::General("Copy is not supported".to_string())),
        })
    }
}
