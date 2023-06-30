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

use crate::loading_cache::{CacheGetStatus, LoadingCache};
use async_trait::async_trait;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

pub trait LoadingCacheListener: Debug + Send + Sync + 'static {
    /// Cache key.
    type K: Clone + Eq + Hash + Debug + Ord + Send + 'static;

    /// Cache value.
    type V: Clone + Debug + Send + 'static;

    fn listen_on_get_if_present(&self, k: Self::K, v: Option<Self::V>);

    fn listen_on_get(&self, k: Self::K, v: Self::V, status: CacheGetStatus);

    fn listen_on_put(&self, k: Self::K, v: Self::V);

    fn listen_on_invalidate(&self, k: Self::K);

    fn listen_on_get_cancelling(&self, k: Self::K);
}

#[derive(Debug)]
pub struct LoadingCacheWithListener<L>
where
    L: LoadingCache,
{
    inner: L,
    listeners: Vec<Arc<dyn LoadingCacheListener<K = L::K, V = L::V>>>,
}

impl<L> LoadingCacheWithListener<L>
where
    L: LoadingCache,
{
    pub fn new(
        inner: L,
        listeners: Vec<Arc<dyn LoadingCacheListener<K = L::K, V = L::V>>>,
    ) -> Self {
        Self { inner, listeners }
    }
}

#[async_trait]
impl<L> LoadingCache for LoadingCacheWithListener<L>
where
    L: LoadingCache,
{
    type K = L::K;
    type V = L::V;
    type GetExtra = L::GetExtra;

    fn get_if_present(&self, k: Self::K) -> Option<Self::V> {
        let v = self.inner.get_if_present(k.clone());
        self.listen_on_get_if_present(k, v.as_ref().cloned());
        v
    }

    async fn get_with_status(
        &self,
        k: Self::K,
        extra: Self::GetExtra,
    ) -> (Self::V, CacheGetStatus) {
        let mut set_on_drop = SetGetListenerOnDrop::new(self, k.clone());
        let (v, status) = self.inner.get_with_status(k, extra).await;
        set_on_drop.get_result = Some((v.clone(), status));
        (v, status)
    }

    async fn put(&self, k: Self::K, v: Self::V) {
        let k_captured = k.clone();
        let v_captured = v.clone();
        self.inner.put(k_captured, v_captured).await;
        self.listen_on_put(k, v);
    }

    fn invalidate(&self, k: Self::K) {
        self.inner.invalidate(k.clone());
        self.listen_on_invalidate(k);
    }
}

struct SetGetListenerOnDrop<'a, L>
where
    L: LoadingCache,
{
    listener: &'a LoadingCacheWithListener<L>,
    key: L::K,
    get_result: Option<(L::V, CacheGetStatus)>,
}

impl<'a, L> SetGetListenerOnDrop<'a, L>
where
    L: LoadingCache,
{
    fn new(listener: &'a LoadingCacheWithListener<L>, key: L::K) -> Self {
        Self {
            listener,
            key,
            get_result: None,
        }
    }
}

impl<'a, L> Drop for SetGetListenerOnDrop<'a, L>
where
    L: LoadingCache,
{
    fn drop(&mut self) {
        if let Some((value, status)) = &self.get_result {
            self.listener
                .listen_on_get(self.key.clone(), value.clone(), *status)
        } else {
            self.listener.listen_on_get_cancelling(self.key.clone());
        }
    }
}

impl<L> LoadingCacheListener for LoadingCacheWithListener<L>
where
    L: LoadingCache,
{
    type K = L::K;
    type V = L::V;

    fn listen_on_get_if_present(&self, k: Self::K, v: Option<Self::V>) {
        if self.listeners.len() == 1 {
            self.listeners
                .get(0)
                .unwrap()
                .listen_on_get_if_present(k, v);
        } else {
            self.listeners.iter().for_each(|listener| {
                listener.listen_on_get_if_present(k.clone(), v.as_ref().cloned())
            });
        }
    }

    fn listen_on_get(&self, k: Self::K, v: Self::V, status: CacheGetStatus) {
        if self.listeners.len() == 1 {
            self.listeners.get(0).unwrap().listen_on_get(k, v, status);
        } else {
            self.listeners.iter().for_each(|listener| {
                listener.listen_on_get(k.clone(), v.clone(), status)
            });
        }
    }

    fn listen_on_put(&self, k: Self::K, v: Self::V) {
        if self.listeners.len() == 1 {
            self.listeners.get(0).unwrap().listen_on_put(k, v);
        } else {
            self.listeners
                .iter()
                .for_each(|listener| listener.listen_on_put(k.clone(), v.clone()));
        }
    }

    fn listen_on_invalidate(&self, k: Self::K) {
        if self.listeners.len() == 1 {
            self.listeners.get(0).unwrap().listen_on_invalidate(k);
        } else {
            self.listeners
                .iter()
                .for_each(|listener| listener.listen_on_invalidate(k.clone()));
        }
    }

    fn listen_on_get_cancelling(&self, k: Self::K) {
        if self.listeners.len() == 1 {
            self.listeners.get(0).unwrap().listen_on_get_cancelling(k);
        } else {
            self.listeners
                .iter()
                .for_each(|listener| listener.listen_on_get_cancelling(k.clone()));
        }
    }
}
