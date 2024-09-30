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

use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;

pub mod lru;

pub type CachePolicyPutResult<K, V> = (Option<V>, Vec<(K, V)>);

pub trait CachePolicy: Debug + Send + 'static {
    /// Cache key.
    type K: Clone + Eq + Hash + Ord + Debug + Send + 'static;

    /// Cached value.
    type V: Clone + Debug + Send + 'static;

    /// Get value for given key if it exists.
    fn get(&mut self, k: &Self::K) -> Option<Self::V>;

    /// Peek value for given key if it exists.
    ///
    /// In contrast to [`get`](Self::get) this will only return a value if there is a stored value.
    /// This will not change the cache entries.
    fn peek(&mut self, k: &Self::K) -> Option<Self::V>;

    /// Put value for given key.
    ///
    /// If a key already exists, its old value will be returned.
    ///
    /// At the meanwhile, entries popped due to memory pressure will be returned
    fn put(&mut self, k: Self::K, v: Self::V) -> CachePolicyPutResult<Self::K, Self::V>;

    /// Remove value for given key.
    ///
    /// If a key does not exist, none will be returned.
    fn remove(&mut self, k: &Self::K) -> Option<Self::V>;

    /// Remove an entry from the cache due to memory pressure or expiration.
    ///
    /// If the cache is empty, none will be returned.
    fn pop(&mut self) -> Option<(Self::K, Self::V)>;

    /// Return backend as [`Any`] which can be used to downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}
