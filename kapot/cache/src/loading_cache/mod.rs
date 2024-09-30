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

/// It's a fork version from the influxdb_iox::cache_system[https://github.com/influxdata/influxdb_iox].
/// Later we will propose to influxdb team to make this cache part more general.
mod cancellation_safe_future;
pub mod driver;
pub mod loader;

use async_trait::async_trait;
use std::fmt::Debug;
use std::hash::Hash;

/// High-level loading cache interface.
///
/// Cache entries are manually added using get(Key, GetExtra) or put(Key, Value),
/// and are stored in the cache until either evicted or manually invalidated.
///
/// # Concurrency
///
/// Multiple cache requests for different keys can run at the same time. When data is requested for
/// the same key, the underlying loader will only be polled once, even when the requests are made
/// while the loader is still running.
///
/// # Cancellation
///
/// Canceling a [`get`](Self::get) request will NOT cancel the underlying loader. The data will
/// still be cached.
#[async_trait]
pub trait LoadingCache: Debug + Send + Sync + 'static {
    /// Cache key.
    type K: Clone + Eq + Hash + Debug + Ord + Send + 'static;

    /// Cache value.
    type V: Clone + Debug + Send + 'static;

    /// Extra data that is provided during [`GET`](Self::get) but that is NOT part of the cache key.
    type GetExtra: Debug + Send + 'static;

    /// Get value from cache.
    ///
    /// In contrast to [`get`](Self::get) this will only return a value if there is a stored value.
    /// This will NOT start a new loading task.
    fn get_if_present(&self, k: Self::K) -> Option<Self::V>;

    /// Get value from cache.
    ///
    /// Note that `extra` is only used if the key is missing from the storage backend
    /// and no value loader for this key is running yet.
    async fn get(&self, k: Self::K, extra: Self::GetExtra) -> Self::V {
        self.get_with_status(k, extra).await.0
    }

    /// Get value from cache and the [status](CacheGetStatus).
    ///
    /// Note that `extra` is only used if the key is missing from the storage backend
    /// and no value loader for this key is running yet.
    async fn get_with_status(
        &self,
        k: Self::K,
        extra: Self::GetExtra,
    ) -> (Self::V, CacheGetStatus);

    /// Side-load an entry into the cache. If the cache previously contained a value associated with key,
    /// the old value is replaced by value.
    ///
    /// This will also complete a currently running loader for this key.
    async fn put(&self, k: Self::K, v: Self::V);

    /// Discard any cached value for the key.
    ///
    /// This will also interrupt a currently running loader for this key.
    fn invalidate(&self, k: Self::K);
}

/// Status of a [`Cache`] [GET](LoadingCache::get_with_status) request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheGetStatus {
    /// The requested entry was present in the storage backend.
    Hit,

    /// The requested entry was NOT present in the storage backend and there's no running value loader.
    Miss,

    /// The requested entry was NOT present in the storage backend, but there was already a running value loader for
    /// this particular key.
    MissAlreadyLoading,
}

impl CacheGetStatus {
    /// Get human and machine readable name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::MissAlreadyLoading => "miss_already_loading",
        }
    }
}
