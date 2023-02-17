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

//! It's mainly a modified version of sled::subscriber

use crate::state::backend::utils::oneshot::{OneShot, OneShotFiller};
use crate::state::backend::WatchEvent;

use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

static ID_GEN: AtomicUsize = AtomicUsize::new(0);

type Senders = HashMap<usize, (Option<Waker>, SyncSender<OneShot<Option<WatchEvent>>>)>;

/// Aynchronous, non-blocking subscriber:
///
/// `Subscription` implements `Future<Output=Option<Event>>`.
///
/// `while let Some(event) = (&mut subscriber).await { /* use it */ }`
pub struct Subscriber {
    id: usize,
    rx: Receiver<OneShot<Option<WatchEvent>>>,
    existing: Option<OneShot<Option<WatchEvent>>>,
    home: Arc<RwLock<Senders>>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let mut w_senders = self.home.write();
        w_senders.remove(&self.id);
    }
}

impl Subscriber {
    /// Attempts to wait for a value on this `Subscriber`, returning
    /// an error if no event arrives within the provided `Duration`
    /// or if the backing `Db` shuts down.
    pub fn next_timeout(
        &mut self,
        mut timeout: Duration,
    ) -> std::result::Result<WatchEvent, std::sync::mpsc::RecvTimeoutError> {
        loop {
            let start = Instant::now();
            let mut future_rx = if let Some(future_rx) = self.existing.take() {
                future_rx
            } else {
                self.rx.recv_timeout(timeout)?
            };
            timeout = if let Some(timeout) = timeout.checked_sub(start.elapsed()) {
                timeout
            } else {
                Duration::from_nanos(0)
            };

            let start = Instant::now();
            match future_rx.wait_timeout(timeout) {
                Ok(Some(event)) => return Ok(event),
                Ok(None) => (),
                Err(timeout_error) => {
                    self.existing = Some(future_rx);
                    return Err(timeout_error);
                }
            }
            timeout = if let Some(timeout) = timeout.checked_sub(start.elapsed()) {
                timeout
            } else {
                Duration::from_nanos(0)
            };
        }
    }
}

impl Future for Subscriber {
    type Output = Option<WatchEvent>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut future_rx = if let Some(future_rx) = self.existing.take() {
                future_rx
            } else {
                match self.rx.try_recv() {
                    Ok(future_rx) => future_rx,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Poll::Ready(None),
                }
            };

            match Future::poll(Pin::new(&mut future_rx), cx) {
                Poll::Ready(Some(event)) => return Poll::Ready(event),
                Poll::Ready(None) => continue,
                Poll::Pending => {
                    self.existing = Some(future_rx);
                    return Poll::Pending;
                }
            }
        }
        let mut home = self.home.write();
        let entry = home.get_mut(&self.id).unwrap();
        entry.0 = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Iterator for Subscriber {
    type Item = WatchEvent;

    fn next(&mut self) -> Option<WatchEvent> {
        loop {
            let future_rx = self.rx.recv().ok()?;
            match future_rx.wait() {
                Some(Some(event)) => return Some(event),
                Some(None) => return None,
                None => continue,
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Subscribers {
    watched: RwLock<BTreeMap<Vec<u8>, Arc<RwLock<Senders>>>>,
    ever_used: AtomicBool,
}

impl Drop for Subscribers {
    fn drop(&mut self) {
        let watched = self.watched.read();

        for senders in watched.values() {
            let senders = std::mem::take(&mut *senders.write());
            for (_, (waker, sender)) in senders {
                drop(sender);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
        }
    }
}

impl Subscribers {
    pub(crate) fn register(&self, prefix: &[u8]) -> Subscriber {
        self.ever_used.store(true, Relaxed);
        let r_mu = {
            let r_mu = self.watched.read();
            if r_mu.contains_key(prefix) {
                r_mu
            } else {
                drop(r_mu);
                let mut w_mu = self.watched.write();
                if !w_mu.contains_key(prefix) {
                    let old = w_mu.insert(
                        prefix.to_vec(),
                        Arc::new(RwLock::new(HashMap::default())),
                    );
                    assert!(old.is_none());
                }
                drop(w_mu);
                self.watched.read()
            }
        };

        let (tx, rx) = sync_channel(1024);

        let arc_senders = &r_mu[prefix];
        let mut w_senders = arc_senders.write();

        let id = ID_GEN.fetch_add(1, Relaxed);

        w_senders.insert(id, (None, tx));

        Subscriber {
            id,
            rx,
            existing: None,
            home: arc_senders.clone(),
        }
    }

    pub(crate) fn reserve<R: AsRef<[u8]>>(&self, key: R) -> Option<ReservedBroadcast> {
        if !self.ever_used.load(Relaxed) {
            return None;
        }

        let r_mu = self.watched.read();
        let prefixes = r_mu.iter().filter(|(k, _)| key.as_ref().starts_with(k));

        let mut subscribers = vec![];

        for (_, subs_rwl) in prefixes {
            let subs = subs_rwl.read();

            for (_id, (waker, sender)) in subs.iter() {
                let (tx, rx) = OneShot::pair();
                if sender.send(rx).is_err() {
                    continue;
                }
                subscribers.push((waker.clone(), tx));
            }
        }

        if subscribers.is_empty() {
            None
        } else {
            Some(ReservedBroadcast { subscribers })
        }
    }
}

pub(crate) struct ReservedBroadcast {
    subscribers: Vec<(Option<Waker>, OneShotFiller<Option<WatchEvent>>)>,
}

impl ReservedBroadcast {
    pub fn complete(self, event: &WatchEvent) {
        let iter = self.subscribers.into_iter();

        for (waker, tx) in iter {
            tx.fill(Some(event.clone()));
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
