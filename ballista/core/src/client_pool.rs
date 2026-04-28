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

//! Connection pool for `BallistaClient` instances.

use crate::client::BallistaClient;
use crate::error::Result;
use crate::extension::BallistaConfigGrpcEndpoint;
use crate::utils::GrpcClientConfig;
use async_trait::async_trait;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Manages a pool of reusable [BallistaClient] connections.
#[async_trait]
pub trait BallistaClientPool: Send + Sync + Debug {
    /// Acquire an idle client for `(host, port)`, or create a new one if the
    /// pool is empty for that key. The returned [PooledClient] returns itself
    /// to the pool on drop.
    async fn acquire(
        &self,
        host: &str,
        port: u16,
        config: &GrpcClientConfig,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    ) -> Result<PooledClient>;

    /// Remove all idle clients that have been sitting unused longer than the
    /// configured idle timeout.
    async fn evict_idle(&self);
}

// ---------------------------------------------------------------------------
// PooledClient guard
// ---------------------------------------------------------------------------

/// A [BallistaClient] checked out from a pool.
///
/// Implements [Deref] / [DerefMut] so it can be used exactly like a
/// [BallistaClient]. On drop, the inner client is returned to the pool
/// automatically. Call [PooledClient::discard] before dropping if the
/// connection should **not** be reused (e.g. after a transport error).
pub struct PooledClient {
    client: BallistaClient,
    /// Invoked in `Drop::drop` to push the client back into the idle deque.
    /// `None` after `discard()` is called.
    return_fn: Option<Box<dyn FnOnce(BallistaClient) + Send>>,
}

impl PooledClient {
    /// Creates new PooledClient
    pub fn new(
        client: BallistaClient,
        return_fn: Box<dyn FnOnce(BallistaClient) + Send>,
    ) -> Self {
        Self {
            client,
            return_fn: Some(return_fn),
        }
    }

    /// Close the connection instead of returning it to the pool.
    pub fn discard(mut self) {
        self.return_fn = None;
    }
}

impl Deref for PooledClient {
    type Target = BallistaClient;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(f) = self.return_fn.take() {
            let client = self.client.clone();
            f(client);
        }
    }
}
