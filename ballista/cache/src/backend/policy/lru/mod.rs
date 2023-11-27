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

pub mod lru_cache;

use crate::backend::policy::CachePolicyPutResult;
use crate::backend::CachePolicy;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

pub trait LruCachePolicy: CachePolicy {
    /// Retrieve the value for the given key,
    /// marking it as recently used and moving it to the back of the LRU list.
    fn get_lru(&mut self, k: &Self::K) -> Option<Self::V>;

    /// Put value for given key.
    ///
    /// If a key already exists, its old value will be returned.
    ///
    /// If necessary, will remove the value at the front of the LRU list to make room.
    fn put_lru(
        &mut self,
        k: Self::K,
        v: Self::V,
    ) -> CachePolicyPutResult<Self::K, Self::V>;

    /// Remove the least recently used entry and return it.
    ///
    /// If the `LruCache` is empty this will return None.
    fn pop_lru(&mut self) -> Option<(Self::K, Self::V)>;
}

pub trait ResourceCounter: Debug + Send + 'static {
    /// Resource key.
    type K: Clone + Eq + Hash + Ord + Debug + Send + 'static;

    /// Resource value.
    type V: Clone + Debug + Send + 'static;

    /// Consume resource for a given key-value pair.
    fn consume(&mut self, k: &Self::K, v: &Self::V);

    /// Return resource for a given key-value pair.
    fn restore(&mut self, k: &Self::K, v: &Self::V);

    /// Check whether the current used resource exceeds the capacity
    fn exceed_capacity(&self) -> bool;
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultResourceCounter<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    max_num: usize,
    current_num: usize,
    _key_marker: PhantomData<K>,
    _value_marker: PhantomData<V>,
}

impl<K, V> DefaultResourceCounter<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            max_num: capacity,
            current_num: 0,
            _key_marker: PhantomData,
            _value_marker: PhantomData,
        }
    }
}

impl<K, V> ResourceCounter for DefaultResourceCounter<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn consume(&mut self, _k: &Self::K, _v: &Self::V) {
        self.current_num += 1;
    }

    fn restore(&mut self, _k: &Self::K, _v: &Self::V) {
        self.current_num -= 1;
    }

    fn exceed_capacity(&self) -> bool {
        self.current_num > self.max_num
    }
}
