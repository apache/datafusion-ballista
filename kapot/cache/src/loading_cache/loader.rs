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

use async_trait::async_trait;
use std::fmt::Debug;
use std::hash::Hash;

/// Loader for missing [`Cache`](crate::cache::Cache) entries.
#[async_trait]
pub trait CacheLoader: Debug + Send + Sync + 'static {
    /// Cache key.
    type K: Debug + Hash + Send + 'static;

    /// Cache value.
    type V: Debug + Send + 'static;

    /// Extra data needed when loading a missing entry. Specify `()` if not needed.
    type Extra: Debug + Send + 'static;

    /// Load value for given key, using the extra data if needed.
    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V;
}

#[async_trait]
impl<K, V, Extra> CacheLoader for Box<dyn CacheLoader<K = K, V = V, Extra = Extra>>
where
    K: Debug + Hash + Send + 'static,
    V: Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    type K = K;
    type V = V;
    type Extra = Extra;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        self.as_ref().load(k, extra).await
    }
}
