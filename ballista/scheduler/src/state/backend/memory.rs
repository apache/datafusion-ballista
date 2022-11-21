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

use crate::state::backend::utils::subscriber::{Subscriber, Subscribers};
use crate::state::backend::{
    Keyspace, Lock, Operation, StateBackendClient, Watch, WatchEvent,
};
use ballista_core::error::Result;
use dashmap::DashMap;
use futures::{FutureExt, Stream};
use log::warn;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

type KeySpaceState = BTreeMap<String, Vec<u8>>;
type KeyLock = Arc<Mutex<()>>;

/// A [`StateBackendClient`] implementation that uses in memory map to save cluster state.
#[derive(Clone, Default)]
pub struct MemoryBackendClient {
    /// The key is the KeySpace. For every KeySpace, there will be a tree map which is better for prefix filtering
    states: DashMap<String, KeySpaceState>,
    /// The key is the full key formatted like "/KeySpace/key". It's a flatted map
    locks: DashMap<String, KeyLock>,
    subscribers: Arc<Subscribers>,
}

impl MemoryBackendClient {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_space_key(keyspace: &Keyspace) -> String {
        format!("/{:?}", keyspace)
    }

    fn get_flat_key(keyspace: &Keyspace, key: &str) -> String {
        format!("/{:?}/{}", keyspace, key)
    }
}

#[tonic::async_trait]
impl StateBackendClient for MemoryBackendClient {
    async fn get(&self, keyspace: Keyspace, key: &str) -> Result<Vec<u8>> {
        let space_key = Self::get_space_key(&keyspace);
        Ok(self
            .states
            .get(&space_key)
            .map(|space_state| space_state.value().get(key).cloned().unwrap_or_default())
            .unwrap_or_default())
    }

    async fn get_from_prefix(
        &self,
        keyspace: Keyspace,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let space_key = Self::get_space_key(&keyspace);
        Ok(self
            .states
            .get(&space_key)
            .map(|space_state| {
                space_state
                    .value()
                    .range(prefix.to_owned()..)
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .map(|e| (format!("{}/{}", space_key, e.0), e.1.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn scan(
        &self,
        keyspace: Keyspace,
        limit: Option<usize>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let space_key = Self::get_space_key(&keyspace);
        Ok(self
            .states
            .get(&space_key)
            .map(|space_state| {
                if let Some(limit) = limit {
                    space_state
                        .value()
                        .iter()
                        .take(limit)
                        .map(|e| (format!("{}/{}", space_key, e.0), e.1.clone()))
                        .collect::<Vec<(String, Vec<u8>)>>()
                } else {
                    space_state
                        .value()
                        .iter()
                        .map(|e| (format!("{}/{}", space_key, e.0), e.1.clone()))
                        .collect::<Vec<(String, Vec<u8>)>>()
                }
            })
            .unwrap_or_default())
    }

    async fn scan_keys(&self, keyspace: Keyspace) -> Result<HashSet<String>> {
        let space_key = Self::get_space_key(&keyspace);
        Ok(self
            .states
            .get(&space_key)
            .map(|space_state| {
                space_state
                    .value()
                    .iter()
                    .map(|e| format!("{}/{}", space_key, e.0))
                    .collect::<HashSet<String>>()
            })
            .unwrap_or_default())
    }

    async fn put(&self, keyspace: Keyspace, key: String, value: Vec<u8>) -> Result<()> {
        let space_key = Self::get_space_key(&keyspace);
        if !self.states.contains_key(&space_key) {
            self.states.insert(space_key.clone(), BTreeMap::default());
        }
        self.states
            .get_mut(&space_key)
            .unwrap()
            .value_mut()
            .insert(key.clone(), value.clone());

        // Notify subscribers
        let full_key = format!("{}/{}", space_key, key);
        if let Some(res) = self.subscribers.reserve(&full_key) {
            let event = WatchEvent::Put(full_key, value);
            res.complete(&event);
        }

        Ok(())
    }

    /// Currently the locks should be acquired before invoking this method.
    /// Later need to be refined by acquiring all of the related locks inside this method
    async fn apply_txn(&self, ops: Vec<(Operation, Keyspace, String)>) -> Result<()> {
        for (op, keyspace, key) in ops.into_iter() {
            match op {
                Operation::Delete => {
                    self.delete(keyspace, &key).await?;
                }
                Operation::Put(value) => {
                    self.put(keyspace, key, value).await?;
                }
            };
        }

        Ok(())
    }

    /// Currently it's not used. Later will refine the caller side by leveraging this method
    async fn mv(
        &self,
        from_keyspace: Keyspace,
        to_keyspace: Keyspace,
        key: &str,
    ) -> Result<()> {
        let from_space_key = Self::get_space_key(&from_keyspace);

        let ops = if let Some(from_space_state) = self.states.get(&from_space_key) {
            if let Some(state) = from_space_state.value().get(key) {
                Some(vec![
                    (Operation::Delete, from_keyspace, key.to_owned()),
                    (Operation::Put(state.clone()), to_keyspace, key.to_owned()),
                ])
            } else {
                // TODO should this return an error?
                warn!(
                    "Cannot move value at {}/{}, does not exist",
                    from_space_key, key
                );
                None
            }
        } else {
            // TODO should this return an error?
            warn!(
                "Cannot move value at {}/{}, does not exist",
                from_space_key, key
            );
            None
        };

        if let Some(ops) = ops {
            self.apply_txn(ops).await?;
        }

        Ok(())
    }

    async fn lock(&self, keyspace: Keyspace, key: &str) -> Result<Box<dyn Lock>> {
        let flat_key = Self::get_flat_key(&keyspace, key);
        let lock = self
            .locks
            .entry(flat_key)
            .or_insert_with(|| Arc::new(Mutex::new(())));
        Ok(Box::new(lock.value().clone().lock_owned().await))
    }

    async fn watch(&self, keyspace: Keyspace, prefix: String) -> Result<Box<dyn Watch>> {
        let prefix = format!("/{:?}/{}", keyspace, prefix);

        Ok(Box::new(MemoryWatch {
            subscriber: self.subscribers.register(prefix.as_bytes()),
        }))
    }

    async fn delete(&self, keyspace: Keyspace, key: &str) -> Result<()> {
        let space_key = Self::get_space_key(&keyspace);
        if let Some(mut space_state) = self.states.get_mut(&space_key) {
            if space_state.value_mut().remove(key).is_some() {
                // Notify subscribers
                let full_key = format!("{}/{}", space_key, key);
                if let Some(res) = self.subscribers.reserve(&full_key) {
                    let event = WatchEvent::Delete(full_key);
                    res.complete(&event);
                }
            }
        }

        Ok(())
    }
}

struct MemoryWatch {
    subscriber: Subscriber,
}

#[tonic::async_trait]
impl Watch for MemoryWatch {
    async fn cancel(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Stream for MemoryWatch {
    type Item = WatchEvent;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().subscriber.poll_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.subscriber.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::{StateBackendClient, Watch, WatchEvent};

    use crate::state::backend::memory::MemoryBackendClient;
    use crate::state::backend::{Keyspace, Operation};
    use crate::state::with_locks;
    use futures::StreamExt;
    use std::result::Result;

    #[tokio::test]
    async fn put_read() -> Result<(), Box<dyn std::error::Error>> {
        let client = MemoryBackendClient::new();
        let key = "key";
        let value = "value".as_bytes();
        client
            .put(Keyspace::Slots, key.to_owned(), value.to_vec())
            .await?;
        assert_eq!(client.get(Keyspace::Slots, key).await?, value);
        Ok(())
    }

    #[tokio::test]
    async fn put_move() -> Result<(), Box<dyn std::error::Error>> {
        let client = MemoryBackendClient::new();
        let key = "key";
        let value = "value".as_bytes();
        client
            .put(Keyspace::ActiveJobs, key.to_owned(), value.to_vec())
            .await?;
        client
            .mv(Keyspace::ActiveJobs, Keyspace::FailedJobs, key)
            .await?;
        assert_eq!(client.get(Keyspace::FailedJobs, key).await?, value);
        Ok(())
    }

    #[tokio::test]
    async fn multiple_operation() -> Result<(), Box<dyn std::error::Error>> {
        let client = MemoryBackendClient::new();
        let key = "key".to_string();
        let value = "value".as_bytes().to_vec();
        let locks = client
            .acquire_locks(vec![(Keyspace::ActiveJobs, ""), (Keyspace::Slots, "")])
            .await?;

        let _r: ballista_core::error::Result<()> = with_locks(locks, async {
            let txn_ops = vec![
                (Operation::Put(value.clone()), Keyspace::Slots, key.clone()),
                (
                    Operation::Put(value.clone()),
                    Keyspace::ActiveJobs,
                    key.clone(),
                ),
            ];
            client.apply_txn(txn_ops).await?;
            Ok(())
        })
        .await;

        assert_eq!(client.get(Keyspace::Slots, key.as_str()).await?, value);
        assert_eq!(client.get(Keyspace::ActiveJobs, key.as_str()).await?, value);
        Ok(())
    }

    #[tokio::test]
    async fn read_empty() -> Result<(), Box<dyn std::error::Error>> {
        let client = MemoryBackendClient::new();
        let key = "key";
        let empty: &[u8] = &[];
        assert_eq!(client.get(Keyspace::Slots, key).await?, empty);
        Ok(())
    }

    #[tokio::test]
    async fn read_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let client = MemoryBackendClient::new();
        let key = "key";
        let value = "value".as_bytes();
        client
            .put(Keyspace::Slots, format!("{}/1", key), value.to_vec())
            .await?;
        client
            .put(Keyspace::Slots, format!("{}/2", key), value.to_vec())
            .await?;
        assert_eq!(
            client.get_from_prefix(Keyspace::Slots, key).await?,
            vec![
                ("/Slots/key/1".to_owned(), value.to_vec()),
                ("/Slots/key/2".to_owned(), value.to_vec())
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_watch() -> Result<(), Box<dyn std::error::Error>> {
        let client = MemoryBackendClient::new();
        let key = "key";
        let value = "value".as_bytes();
        let mut watch_keyspace: Box<dyn Watch> =
            client.watch(Keyspace::Slots, "".to_owned()).await?;
        let mut watch_key: Box<dyn Watch> =
            client.watch(Keyspace::Slots, key.to_owned()).await?;
        client
            .put(Keyspace::Slots, key.to_owned(), value.to_vec())
            .await?;
        assert_eq!(
            watch_keyspace.next().await,
            Some(WatchEvent::Put(
                format!("/{:?}/{}", Keyspace::Slots, key.to_owned()),
                value.to_owned()
            ))
        );
        assert_eq!(
            watch_key.next().await,
            Some(WatchEvent::Put(
                format!("/{:?}/{}", Keyspace::Slots, key.to_owned()),
                value.to_owned()
            ))
        );
        let value2 = "value2".as_bytes();
        client
            .put(Keyspace::Slots, key.to_owned(), value2.to_vec())
            .await?;
        assert_eq!(
            watch_keyspace.next().await,
            Some(WatchEvent::Put(
                format!("/{:?}/{}", Keyspace::Slots, key.to_owned()),
                value2.to_owned()
            ))
        );
        assert_eq!(
            watch_key.next().await,
            Some(WatchEvent::Put(
                format!("/{:?}/{}", Keyspace::Slots, key.to_owned()),
                value2.to_owned()
            ))
        );
        watch_keyspace.cancel().await?;
        watch_key.cancel().await?;
        Ok(())
    }
}
