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

//! Etcd config backend.

use std::collections::HashSet;

use std::task::Poll;

use ballista_core::error::{ballista_error, Result};
use std::time::Instant;

use etcd_client::{
    GetOptions, LockOptions, LockResponse, Txn, TxnOp, WatchOptions, WatchStream, Watcher,
};
use futures::{Stream, StreamExt};
use log::{debug, error, warn};

use crate::state::backend::{
    Keyspace, Lock, Operation, StateBackendClient, Watch, WatchEvent,
};

/// A [`StateBackendClient`] implementation that uses etcd to save cluster configuration.
#[derive(Clone)]
pub struct EtcdClient {
    namespace: String,
    etcd: etcd_client::Client,
}

impl EtcdClient {
    pub fn new(namespace: String, etcd: etcd_client::Client) -> Self {
        Self { namespace, etcd }
    }
}

#[tonic::async_trait]
impl StateBackendClient for EtcdClient {
    async fn get(&self, keyspace: Keyspace, key: &str) -> Result<Vec<u8>> {
        let key = format!("/{}/{:?}/{}", self.namespace, keyspace, key);

        Ok(self
            .etcd
            .clone()
            .get(key, None)
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .get(0)
            .map(|kv| kv.value().to_owned())
            .unwrap_or_default())
    }

    async fn get_from_prefix(
        &self,
        keyspace: Keyspace,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!("/{}/{:?}/{}", self.namespace, keyspace, prefix);

        Ok(self
            .etcd
            .clone()
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .iter()
            .map(|kv| (kv.key_str().unwrap().to_owned(), kv.value().to_owned()))
            .collect())
    }

    async fn scan(
        &self,
        keyspace: Keyspace,
        limit: Option<usize>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!("/{}/{:?}/", self.namespace, keyspace);

        let options = if let Some(limit) = limit {
            GetOptions::new().with_prefix().with_limit(limit as i64)
        } else {
            GetOptions::new().with_prefix()
        };

        Ok(self
            .etcd
            .clone()
            .get(prefix, Some(options))
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .iter()
            .map(|kv| (kv.key_str().unwrap().to_owned(), kv.value().to_owned()))
            .collect())
    }

    async fn scan_keys(&self, keyspace: Keyspace) -> Result<HashSet<String>> {
        let prefix = format!("/{}/{:?}/", self.namespace, keyspace);

        let options = GetOptions::new().with_prefix().with_keys_only();

        Ok(self
            .etcd
            .clone()
            .get(prefix.clone(), Some(options))
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .iter()
            .map(|kv| {
                kv.key_str()
                    .unwrap()
                    .strip_prefix(&prefix)
                    .unwrap()
                    .to_owned()
            })
            .collect())
    }

    async fn put(&self, keyspace: Keyspace, key: String, value: Vec<u8>) -> Result<()> {
        let key = format!("/{}/{:?}/{}", self.namespace, keyspace, key);

        let mut etcd = self.etcd.clone();
        etcd.put(key, value.clone(), None)
            .await
            .map_err(|e| {
                warn!("etcd put failed: {}", e);
                ballista_error(&*format!("etcd put failed: {}", e))
            })
            .map(|_| ())
    }

    /// Apply multiple operations in a single transaction.
    async fn apply_txn(&self, ops: Vec<(Operation, Keyspace, String)>) -> Result<()> {
        let mut etcd = self.etcd.clone();

        let txn_ops: Vec<TxnOp> = ops
            .into_iter()
            .map(|(operation, ks, key)| {
                let key = format!("/{}/{:?}/{}", self.namespace, ks, key);
                match operation {
                    Operation::Put(value) => TxnOp::put(key, value, None),
                    Operation::Delete => TxnOp::delete(key, None),
                }
            })
            .collect();

        etcd.txn(Txn::new().and_then(txn_ops))
            .await
            .map_err(|e| {
                error!("etcd operation failed: {}", e);
                ballista_error(&*format!("etcd operation failed: {}", e))
            })
            .map(|_| ())
    }

    async fn mv(
        &self,
        from_keyspace: Keyspace,
        to_keyspace: Keyspace,
        key: &str,
    ) -> Result<()> {
        let mut etcd = self.etcd.clone();
        let from_key = format!("/{}/{:?}/{}", self.namespace, from_keyspace, key);
        let to_key = format!("/{}/{:?}/{}", self.namespace, to_keyspace, key);

        let current_value = etcd
            .get(from_key.as_str(), None)
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .get(0)
            .map(|kv| kv.value().to_owned());

        if let Some(value) = current_value {
            let txn = Txn::new().and_then(vec![
                TxnOp::delete(from_key.as_str(), None),
                TxnOp::put(to_key.as_str(), value, None),
            ]);
            etcd.txn(txn).await.map_err(|e| {
                error!("etcd put failed: {}", e);
                ballista_error("etcd move failed")
            })?;
        } else {
            warn!("Cannot move value at {}, does not exist", from_key);
        }

        Ok(())
    }

    async fn lock(&self, keyspace: Keyspace, key: &str) -> Result<Box<dyn Lock>> {
        let start = Instant::now();
        let mut etcd = self.etcd.clone();

        let lock_id = format!("/{}/mutex/{:?}/{}", self.namespace, keyspace, key);

        // Create a lease which expires after 30 seconds. We then associate this lease with the lock
        // acquired below. This protects against a scheduler dying unexpectedly while holding locks
        // on shared resources. In that case, those locks would expire once the lease expires.
        // TODO This is not great to do for every lock. We should have a single lease per scheduler instance
        let lease_id = etcd
            .lease_client()
            .grant(30, None)
            .await
            .map_err(|e| {
                warn!("etcd lease failed: {}", e);
                ballista_error("etcd lease failed")
            })?
            .id();

        let lock_options = LockOptions::new().with_lease(lease_id);

        let lock = etcd
            .lock(lock_id.as_str(), Some(lock_options))
            .await
            .map_err(|e| {
                warn!("etcd lock failed: {}", e);
                ballista_error("etcd lock failed")
            })?;

        let elapsed = start.elapsed();
        debug!("Acquired lock {} in {:?}", lock_id, elapsed);
        Ok(Box::new(EtcdLockGuard { etcd, lock }))
    }

    async fn watch(&self, keyspace: Keyspace, prefix: String) -> Result<Box<dyn Watch>> {
        let prefix = format!("/{}/{:?}/{}", self.namespace, keyspace, prefix);

        let mut etcd = self.etcd.clone();
        let options = WatchOptions::new().with_prefix();
        let (watcher, stream) = etcd.watch(prefix, Some(options)).await.map_err(|e| {
            warn!("etcd watch failed: {}", e);
            ballista_error("etcd watch failed")
        })?;
        Ok(Box::new(EtcdWatch {
            watcher,
            stream,
            buffered_events: Vec::new(),
        }))
    }

    async fn delete(&self, keyspace: Keyspace, key: &str) -> Result<()> {
        let key = format!("/{}/{:?}/{}", self.namespace, keyspace, key);

        let mut etcd = self.etcd.clone();

        etcd.delete(key, None).await.map_err(|e| {
            warn!("etcd delete failed: {:?}", e);
            ballista_error("etcd delete failed")
        })?;

        Ok(())
    }
}

struct EtcdWatch {
    watcher: Watcher,
    stream: WatchStream,
    buffered_events: Vec<WatchEvent>,
}

#[tonic::async_trait]
impl Watch for EtcdWatch {
    async fn cancel(&mut self) -> Result<()> {
        self.watcher.cancel().await.map_err(|e| {
            warn!("etcd watch cancel failed: {}", e);
            ballista_error("etcd watch cancel failed")
        })
    }
}

impl Stream for EtcdWatch {
    type Item = WatchEvent;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();
        if let Some(event) = self_mut.buffered_events.pop() {
            Poll::Ready(Some(event))
        } else {
            loop {
                match self_mut.stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Err(e))) => {
                        warn!("Error when watching etcd prefix: {}", e);
                        continue;
                    }
                    Poll::Ready(Some(Ok(v))) => {
                        self_mut.buffered_events.extend(v.events().iter().map(|ev| {
                            match ev.event_type() {
                                etcd_client::EventType::Put => {
                                    let kv = ev.kv().unwrap();
                                    WatchEvent::Put(
                                        kv.key_str().unwrap().to_string(),
                                        kv.value().to_owned(),
                                    )
                                }
                                etcd_client::EventType::Delete => {
                                    let kv = ev.kv().unwrap();
                                    WatchEvent::Delete(kv.key_str().unwrap().to_string())
                                }
                            }
                        }));
                        if let Some(event) = self_mut.buffered_events.pop() {
                            return Poll::Ready(Some(event));
                        } else {
                            continue;
                        }
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

struct EtcdLockGuard {
    etcd: etcd_client::Client,
    lock: LockResponse,
}

// Cannot use Drop because we need this to be async
#[tonic::async_trait]
impl Lock for EtcdLockGuard {
    async fn unlock(&mut self) {
        self.etcd.unlock(self.lock.key()).await.unwrap();
    }
}
