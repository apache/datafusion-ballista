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

pub mod file;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, PutOptions,
    PutResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

/// An [`ObjectStore`] wrapper with a specific key which is used for registration in [`ObjectStoreRegistry`].
///
/// The [`key`] can be used for the cache path prefix.
#[derive(Debug)]
pub struct ObjectStoreWithKey {
    key: String,
    inner: Arc<dyn ObjectStore>,
}

impl ObjectStoreWithKey {
    pub fn new(key: String, inner: Arc<dyn ObjectStore>) -> Self {
        Self { key, inner }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Display for ObjectStoreWithKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Registered object store {} with key {}",
            self.inner, self.key,
        )
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreWithKey {
    async fn put(
        &self,
        location: &Path,
        bytes: Bytes,
    ) -> object_store::Result<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: Bytes,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> object_store::Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}
