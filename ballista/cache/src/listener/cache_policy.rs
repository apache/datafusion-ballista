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

use crate::backend::policy::{CachePolicy, CachePolicyPutResult};
use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

pub trait CachePolicyListener: Debug + Send + Sync + 'static {
    /// Cache key.
    type K: Clone + Eq + Hash + Debug + Ord + Send + 'static;

    /// Cache value.
    type V: Clone + Debug + Send + 'static;

    fn listen_on_get(&self, k: Self::K, v: Option<Self::V>);

    fn listen_on_peek(&self, k: Self::K, v: Option<Self::V>);

    fn listen_on_put(&self, k: Self::K, v: Self::V, old_v: Option<Self::V>);

    fn listen_on_remove(&self, k: Self::K, v: Option<Self::V>);

    fn listen_on_pop(&self, entry: (Self::K, Self::V));
}

#[derive(Debug)]
pub struct CachePolicyWithListener<P>
where
    P: CachePolicy,
{
    inner: P,
    listeners: Vec<Arc<dyn CachePolicyListener<K = P::K, V = P::V>>>,
}

impl<P> CachePolicyWithListener<P>
where
    P: CachePolicy,
{
    pub fn new(
        inner: P,
        listeners: Vec<Arc<dyn CachePolicyListener<K = P::K, V = P::V>>>,
    ) -> Self {
        Self { inner, listeners }
    }
}

impl<P> CachePolicy for CachePolicyWithListener<P>
where
    P: CachePolicy,
{
    type K = P::K;
    type V = P::V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        let v = self.inner.get(k);

        // For listeners
        self.listeners
            .iter()
            .for_each(|listener| listener.listen_on_get(k.clone(), v.as_ref().cloned()));

        v
    }

    fn peek(&mut self, k: &Self::K) -> Option<Self::V> {
        let v = self.inner.peek(k);

        // For listeners
        self.listeners
            .iter()
            .for_each(|listener| listener.listen_on_peek(k.clone(), v.as_ref().cloned()));

        v
    }

    fn put(&mut self, k: Self::K, v: Self::V) -> CachePolicyPutResult<Self::K, Self::V> {
        let ret = self.inner.put(k.clone(), v.clone());

        // For listeners
        self.listeners.iter().for_each(|listener| {
            listener.listen_on_put(k.clone(), v.clone(), ret.0.as_ref().cloned());
            ret.1
                .iter()
                .for_each(|entry| listener.listen_on_pop(entry.clone()));
        });

        ret
    }

    fn remove(&mut self, k: &Self::K) -> Option<Self::V> {
        let v = self.inner.remove(k);

        // For listeners
        self.listeners.iter().for_each(|listener| {
            listener.listen_on_remove(k.clone(), v.as_ref().cloned())
        });

        v
    }

    fn pop(&mut self) -> Option<(Self::K, Self::V)> {
        let entry = self.inner.pop();

        // For listeners
        if let Some(entry) = &entry {
            self.listeners
                .iter()
                .for_each(|listener| listener.listen_on_pop(entry.clone()));
        }

        entry
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
