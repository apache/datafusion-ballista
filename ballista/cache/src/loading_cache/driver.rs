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

//! Main data structure, see [`CacheDriver`].

use crate::backend::CacheBackend;
use crate::loading_cache::{
    cancellation_safe_future::CancellationSafeFuture,
    loader::CacheLoader,
    {CacheGetStatus, LoadingCache},
};
use async_trait::async_trait;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, TryFutureExt};
use log::debug;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use tokio::{
    sync::oneshot::{error::RecvError, Sender},
    task::JoinHandle,
};

/// Combine a [`CacheBackend`] and a [`Loader`] into a single [`Cache`]
#[derive(Debug)]
pub struct CacheDriver<K, V, L>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
    L: CacheLoader<K = K, V = V>,
{
    state: Arc<Mutex<CacheState<K, V>>>,
    loader: Arc<L>,
}

impl<K, V, L> CacheDriver<K, V, L>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
    L: CacheLoader<K = K, V = V>,
{
    /// Create new, empty cache with given loader function.
    pub fn new(backend: CacheBackend<K, V>, loader: Arc<L>) -> Self {
        Self {
            state: Arc::new(Mutex::new(CacheState {
                cached_entries: backend,
                loaders: HashMap::new(),
                next_loader_tag: 0,
            })),
            loader,
        }
    }
}

#[async_trait]
impl<K, V, L> LoadingCache for CacheDriver<K, V, L>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
    L: CacheLoader<K = K, V = V>,
{
    type K = K;
    type V = V;
    type GetExtra = L::Extra;

    fn get_if_present(&self, k: Self::K) -> Option<Self::V> {
        self.state.lock().cached_entries.get(&k)
    }

    async fn get_with_status(
        &self,
        k: Self::K,
        extra: Self::GetExtra,
    ) -> (Self::V, CacheGetStatus) {
        // place state locking into its own scope so it doesn't leak into the generator (async
        // function)
        let (fut, receiver, status) = {
            let mut state = self.state.lock();

            // check if the entry has already been cached
            if let Some(v) = state.cached_entries.get(&k) {
                return (v, CacheGetStatus::Hit);
            }

            // check if there is already a running loader for this key
            if let Some(loader) = state.loaders.get(&k) {
                (
                    None,
                    loader.recv.clone(),
                    CacheGetStatus::MissAlreadyLoading,
                )
            } else {
                // generate unique tag
                let loader_tag = state.next_loader_tag();

                // requires new loader
                let (fut, loader) = create_value_loader(
                    self.state.clone(),
                    self.loader.clone(),
                    loader_tag,
                    k.clone(),
                    extra,
                );

                let receiver = loader.recv.clone();
                state.loaders.insert(k, loader);

                (Some(fut), receiver, CacheGetStatus::Miss)
            }
        };

        // try to run the loader future in this very task context to avoid spawning tokio tasks (which adds latency and
        // overhead)
        if let Some(fut) = fut {
            fut.await;
        }

        let v = retrieve_from_shared(receiver).await;

        (v, status)
    }

    async fn put(&self, k: Self::K, v: Self::V) {
        let maybe_join_handle = {
            let mut state = self.state.lock();

            let maybe_recv = if let Some(loader) = state.loaders.remove(&k) {
                // it's OK when the receiver side is gone (likely panicked)
                loader.set.send(v.clone()).ok();

                // When we side-load data into the running task, the task does NOT modify the
                // backend, so we have to do that. The reason for not letting the task feed the
                // side-loaded data back into `cached_entries` is that we would need to drop the
                // state lock here before the task could acquire it, leading to a lock gap.
                Some(loader.recv)
            } else {
                None
            };

            state.cached_entries.put(k, v);

            maybe_recv
        };

        // drive running loader (if any) to completion
        if let Some(recv) = maybe_join_handle {
            // we do not care if the loader died (e.g. due to a panic)
            recv.await.ok();
        }
    }

    fn invalidate(&self, k: Self::K) {
        let mut state = self.state.lock();

        if state.loaders.remove(&k).is_some() {
            debug!("Running loader for key {:?} is removed", k);
        }

        state.cached_entries.remove(&k);
    }
}

impl<K, V, L> Drop for CacheDriver<K, V, L>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
    L: CacheLoader<K = K, V = V>,
{
    fn drop(&mut self) {
        for (_k, loader) in self.state.lock().loaders.drain() {
            // It's unlikely that anyone is still using the shared receiver at this point, because
            // `Cache::get` borrows the `self`. If it is still in use, aborting the task will
            // cancel the contained future which in turn will drop the sender of the oneshot
            // channel. The receivers will be notified.
            let handle = loader.join_handle.lock();
            if let Some(handle) = handle.as_ref() {
                handle.abort();
            }
        }
    }
}

fn create_value_loader<K, V, Extra>(
    state: Arc<Mutex<CacheState<K, V>>>,
    loader: Arc<dyn CacheLoader<K = K, V = V, Extra = Extra>>,
    loader_tag: u64,
    k: K,
    extra: Extra,
) -> (
    CancellationSafeFuture<impl Future<Output = ()>>,
    ValueLoader<V>,
)
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    let (tx_main, rx_main) = tokio::sync::oneshot::channel();
    let receiver = rx_main
        .map_ok(|v| Arc::new(Mutex::new(v)))
        .map_err(Arc::new)
        .boxed()
        .shared();
    let (tx_set, rx_set) = tokio::sync::oneshot::channel();

    // need to wrap the loader into a `CancellationSafeFuture` so that it doesn't get cancelled when
    // this very request is cancelled
    let join_handle_receiver = Arc::new(Mutex::new(None));
    let fut = async move {
        let loader_fut = async move {
            let mut submitter = ResultSubmitter::new(state, k.clone(), loader_tag);

            // execute the loader
            // If we panic here then `tx` will be dropped and the receivers will be
            // notified.
            let v = loader.load(k, extra).await;

            // remove "running" state and store result
            let was_running = submitter.submit(v.clone());

            if !was_running {
                // value was side-loaded, so we cannot populate `v`. Instead block this
                // execution branch and wait for `rx_set` to deliver the side-loaded
                // result.
                loop {
                    tokio::task::yield_now().await;
                }
            }

            v
        };

        // prefer the side-loader
        let v = futures::select_biased! {
            maybe_v = rx_set.fuse() => {
                match maybe_v {
                    Ok(v) => {
                        // data get side-loaded via `Cache::set`. In this case, we do
                        // NOT modify the state because there would be a lock-gap. The
                        // `set` function will do that for us instead.
                        v
                    }
                    Err(_) => {
                        // sender side is gone, very likely the cache is shutting down
                        debug!(
                            "Sender for side-loading data into running loader gone.",
                        );
                        return;
                    }
                }
            }
            v = loader_fut.fuse() => v,
        };

        // broadcast result
        // It's OK if the receiver side is gone. This might happen during shutdown
        tx_main.send(v).ok();
    };
    let fut = CancellationSafeFuture::new(fut, Arc::clone(&join_handle_receiver));

    (
        fut,
        ValueLoader {
            recv: receiver,
            set: tx_set,
            join_handle: join_handle_receiver,
            tag: loader_tag,
        },
    )
}

/// Inner cache state that is usually guarded by a lock.
///
/// The state parts must be updated in a consistent manner, i.e. while using the same lock guard.
#[derive(Debug)]
struct CacheState<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Cached entries (i.e. queries completed).
    cached_entries: CacheBackend<K, V>,

    /// Currently value loaders indexed by cache key.
    loaders: HashMap<K, ValueLoader<V>>,

    /// Tag used for the next value loader to distinguish loaders for the same key
    /// (e.g. when starting, side-loading, starting again)
    next_loader_tag: u64,
}

impl<K, V> CacheState<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// To avoid overflow issue, it will begin from 0. It will rarely happen that
    /// two value loaders share the same key and tag while for different purposes
    #[inline]
    fn next_loader_tag(&mut self) -> u64 {
        let ret = self.next_loader_tag;
        if self.next_loader_tag != u64::MAX {
            self.next_loader_tag += 1;
        } else {
            self.next_loader_tag = 0;
        }
        ret
    }
}

/// State for coordinating the execution of a single value loader.
#[derive(Debug)]
struct ValueLoader<V> {
    /// A receiver that can await the result.
    recv: SharedReceiver<V>,

    /// A sender that enables setting entries while the query is running.
    set: Sender<V>,

    /// A handle for the task that is currently loading the value.
    ///
    /// The handle can be used to abort the loading, e.g. when dropping the cache.
    join_handle: Arc<Mutex<Option<JoinHandle<()>>>>,

    /// Tag so that loaders for the same key (e.g. when starting, side-loading, starting again) can
    /// be told apart.
    tag: u64,
}

/// A [`tokio::sync::oneshot::Receiver`] that can be cloned.
///
/// The types are:
///
/// - `Arc<Mutex<V>>`: Ensures that we can clone `V` without requiring `V: Sync`. At the same time
///   the reference to `V` (i.e. the `Arc`) must be cloneable for `Shared`
/// - `Arc<RecvError>`: Is required because `RecvError` is not `Clone` but `Shared` requires that.
/// - `BoxFuture`: The transformation from `Result<V, RecvError>` to `Result<Arc<Mutex<V>>,
///   Arc<RecvError>>` results in a kinda messy type and we wanna erase that.
/// - `Shared`: Allow the receiver to be cloned and be awaited from multiple places.
type SharedReceiver<V> =
    Shared<BoxFuture<'static, Result<Arc<Mutex<V>>, Arc<RecvError>>>>;

/// Retrieve data from shared receiver.
async fn retrieve_from_shared<V>(receiver: SharedReceiver<V>) -> V
where
    V: Clone + Send,
{
    receiver
        .await
        .expect("cache loader panicked, see logs")
        .lock()
        .clone()
}

/// Helper to submit results of running queries.
///
/// Ensures that running loader is removed when dropped (e.g. during panic).
struct ResultSubmitter<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    state: Arc<Mutex<CacheState<K, V>>>,
    tag: u64,
    k: Option<K>,
    v: Option<V>,
}

impl<K, V> ResultSubmitter<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn new(state: Arc<Mutex<CacheState<K, V>>>, k: K, tag: u64) -> Self {
        Self {
            state,
            tag,
            k: Some(k),
            v: None,
        }
    }

    /// Submit value.
    ///
    /// Returns `true` if this very loader was running.
    fn submit(&mut self, v: V) -> bool {
        assert!(self.v.is_none());
        self.v = Some(v);
        self.finalize()
    }

    /// Finalize request.
    ///
    /// Returns `true` if this very loader was running.
    fn finalize(&mut self) -> bool {
        let k = self.k.take().expect("finalized twice");
        let mut state = self.state.lock();

        match state.loaders.get(&k) {
            Some(loader) if loader.tag == self.tag => {
                state.loaders.remove(&k);

                if let Some(v) = self.v.take() {
                    // this very loader is in charge of the key, so store in in the
                    // underlying cache
                    state.cached_entries.put(k, v);
                }

                true
            }
            _ => {
                // This loader is actually not really running any longer but got
                // shut down, e.g. due to side loading. Do NOT store the
                // generated value in the underlying cache.

                false
            }
        }
    }
}

impl<K, V> Drop for ResultSubmitter<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn drop(&mut self) {
        if self.k.is_some() {
            // not finalized yet
            self.finalize();
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::backend::policy::lru::hashlink::lru_cache::LruCache;
    use crate::listener::cache_policy::CachePolicyListener;
    use crate::{CacheBackend, CacheDriver, CacheLoader, CachePolicyWithListener};

    use crate::backend::policy::lru::DefaultResourceCounter;
    use crate::loading_cache::LoadingCache;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::sync::mpsc::{channel, Sender};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_removal_entries() {
        let cache_policy =
            LruCache::with_resource_counter(DefaultResourceCounter::new(3));
        let loader = TestStringCacheLoader {
            prefix: "file".to_string(),
        };
        let (sender, receiver) = channel::<(String, String)>();
        let listener = Arc::new(EntryRemovalListener::new(sender));
        let policy_with_listener =
            CachePolicyWithListener::new(cache_policy, vec![listener.clone()]);
        let cache_backend = CacheBackend::new(policy_with_listener);
        let loading_cache = CacheDriver::new(cache_backend, Arc::new(loader));

        assert_eq!(
            "file1".to_string(),
            loading_cache.get("1".to_string(), ()).await
        );
        assert_eq!(
            "file2".to_string(),
            loading_cache.get("2".to_string(), ()).await
        );
        assert_eq!(
            "file3".to_string(),
            loading_cache.get("3".to_string(), ()).await
        );
        assert_eq!(
            "file4".to_string(),
            loading_cache.get("4".to_string(), ()).await
        );
        assert_eq!(Ok(("1".to_string(), "file1".to_string())), receiver.recv());
        assert!(loading_cache.get_if_present("1".to_string()).is_none());

        loading_cache
            .put("2".to_string(), "file2-bak".to_string())
            .await;
        assert_eq!(
            "file5".to_string(),
            loading_cache.get("5".to_string(), ()).await
        );
        assert_eq!(Ok(("3".to_string(), "file3".to_string())), receiver.recv());
        assert!(loading_cache.get_if_present("3".to_string()).is_none());
        assert!(loading_cache.get_if_present("2".to_string()).is_some());

        loading_cache.invalidate("2".to_string());
        assert_eq!(
            Ok(("2".to_string(), "file2-bak".to_string())),
            receiver.recv()
        );
        assert!(loading_cache.get_if_present("2".to_string()).is_none());
    }

    #[derive(Debug)]
    struct EntryRemovalListener {
        sender: Arc<Mutex<Sender<(String, String)>>>,
    }

    impl EntryRemovalListener {
        pub fn new(sender: Sender<(String, String)>) -> Self {
            Self {
                sender: Arc::new(Mutex::new(sender)),
            }
        }
    }

    impl CachePolicyListener for EntryRemovalListener {
        type K = String;
        type V = String;

        fn listen_on_get(&self, _k: Self::K, _v: Option<Self::V>) {
            // Do nothing
        }

        fn listen_on_peek(&self, _k: Self::K, _v: Option<Self::V>) {
            // Do nothing
        }

        fn listen_on_put(&self, _k: Self::K, _v: Self::V, _old_v: Option<Self::V>) {
            // Do nothing
        }

        fn listen_on_remove(&self, k: Self::K, v: Option<Self::V>) {
            if let Some(v) = v {
                self.sender.lock().send((k, v)).unwrap();
            }
        }

        fn listen_on_pop(&self, entry: (Self::K, Self::V)) {
            self.sender.lock().send(entry).unwrap();
        }
    }

    #[derive(Debug)]
    struct TestStringCacheLoader {
        prefix: String,
    }

    #[async_trait]
    impl CacheLoader for TestStringCacheLoader {
        type K = String;
        type V = String;
        type Extra = ();

        async fn load(&self, k: Self::K, _extra: Self::Extra) -> Self::V {
            format!("{}{k}", self.prefix)
        }
    }
}
