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

pub mod policy;

use crate::backend::policy::CachePolicy;
use std::fmt::Debug;
use std::hash::Hash;

/// Backend to keep and manage stored entries.
///
/// A backend might remove entries at any point, e.g. due to memory pressure or expiration.
#[derive(Debug)]
pub struct CacheBackend<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    policy: Box<dyn CachePolicy<K = K, V = V>>,
}

impl<K, V> CacheBackend<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    pub fn new(policy: impl CachePolicy<K = K, V = V>) -> Self {
        Self {
            policy: Box::new(policy),
        }
    }

    /// Get value for given key if it exists.
    pub fn get(&mut self, k: &K) -> Option<V> {
        self.policy.get(k)
    }

    /// Peek value for given key if it exists.
    ///
    /// In contrast to [`get`](Self::get) this will only return a value if there is a stored value.
    /// This will not change the cache contents.
    pub fn peek(&mut self, k: &K) -> Option<V> {
        self.policy.peek(k)
    }

    /// Put value for given key.
    ///
    /// If a key already exists, its old value will be returned.
    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        self.policy.put(k, v).0
    }

    /// Remove value for given key.
    ///
    /// If a key does not exist, none will be returned.
    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.policy.remove(k)
    }
}
