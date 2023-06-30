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

use crate::backend::policy::CachePolicy;
use crate::backend::CacheBackend;
use crate::listener::{
    cache_policy::CachePolicyWithListener, loading_cache::LoadingCacheWithListener,
};
use crate::loading_cache::{driver::CacheDriver, loader::CacheLoader};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

pub mod backend;
pub mod listener;
pub mod loading_cache;
pub mod metrics;

pub type DefaultLoadingCache<K, V, L> = LoadingCacheWithListener<CacheDriver<K, V, L>>;
pub type LoadingCacheMetrics<K, V> = metrics::loading_cache::Metrics<K, V>;

pub fn create_loading_cache_with_metrics<K, V, L>(
    policy: impl CachePolicy<K = K, V = V>,
    loader: Arc<L>,
) -> (DefaultLoadingCache<K, V, L>, Arc<LoadingCacheMetrics<K, V>>)
where
    K: Clone + Eq + Hash + Debug + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    L: CacheLoader<K = K, V = V>,
{
    let metrics = Arc::new(metrics::loading_cache::Metrics::new());

    let policy_with_metrics = CachePolicyWithListener::new(policy, vec![metrics.clone()]);
    let cache_backend = CacheBackend::new(policy_with_metrics);
    let loading_cache = CacheDriver::new(cache_backend, loader);
    let loading_cache_with_metrics =
        LoadingCacheWithListener::new(loading_cache, vec![metrics.clone()]);

    (loading_cache_with_metrics, metrics)
}
