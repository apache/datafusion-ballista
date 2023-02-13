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

use futures::Stream;
use log::debug;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::TryRecvError;

// TODO make configurable
const EVENT_BUFFER_SIZE: usize = 256;

static ID_GEN: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct Shared {
    subscriptions: AtomicUsize,
    wakers: RwLock<BTreeMap<usize, Waker>>,
}

impl Shared {
    pub fn register(&self, subscriber_id: usize, waker: Waker) {
        self.wakers.write().insert(subscriber_id, waker);
    }

    pub fn deregister(&self, subscriber_id: usize) {
        self.wakers.write().remove(&subscriber_id);
    }

    pub fn notify(&self) {
        let guard = self.wakers.read();
        for waker in guard.values() {
            waker.wake_by_ref();
        }
    }
}

pub(crate) struct ClusterEventSender<T: Clone> {
    sender: broadcast::Sender<T>,
    shared: Arc<Shared>,
}

impl<T: Clone> ClusterEventSender<T> {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);

        Self {
            sender,
            shared: Arc::new(Shared::default()),
        }
    }

    pub fn send(&self, event: &T) {
        if self.shared.subscriptions.load(Ordering::Acquire) > 0 {
            if let Err(e) = self.sender.send(event.clone()) {
                debug!("Failed to send event to channel: {}", e);
                return;
            }

            self.shared.notify();
        }
    }

    pub fn subscribe(&self) -> EventSubscriber<T> {
        self.shared.subscriptions.fetch_add(1, Ordering::AcqRel);
        let id = ID_GEN.fetch_add(1, Ordering::AcqRel);

        EventSubscriber {
            id,
            receiver: self.sender.subscribe(),
            shared: self.shared.clone(),
            registered: false,
        }
    }

    #[cfg(test)]
    pub fn registered_wakers(&self) -> usize {
        self.shared.wakers.read().len()
    }
}

impl<T: Clone> Default for ClusterEventSender<T> {
    fn default() -> Self {
        Self::new(EVENT_BUFFER_SIZE)
    }
}

pub struct EventSubscriber<T: Clone> {
    id: usize,
    receiver: broadcast::Receiver<T>,
    shared: Arc<Shared>,
    registered: bool,
}

impl<T: Clone> EventSubscriber<T> {
    pub fn register(&mut self, waker: Waker) {
        if !self.registered {
            self.shared.register(self.id, waker);
            self.registered = true;
        }
    }
}

impl<T: Clone> Drop for EventSubscriber<T> {
    fn drop(&mut self) {
        self.shared.deregister(self.id);
    }
}

impl<T: Clone> Stream for EventSubscriber<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => {
                    self.register(cx.waker().clone());
                    return Poll::Ready(Some(event));
                }
                Err(TryRecvError::Closed) => return Poll::Ready(None),
                Err(TryRecvError::Lagged(n)) => {
                    debug!("Subscriber lagged by {} message", n);
                    self.register(cx.waker().clone());
                    continue;
                }
                Err(TryRecvError::Empty) => {
                    self.register(cx.waker().clone());
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::cluster::event::{ClusterEventSender, EventSubscriber};
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;

    async fn collect_events<T: Clone>(mut rx: EventSubscriber<T>) -> Vec<T> {
        let mut events = vec![];
        while let Some(event) = rx.next().await {
            events.push(event);
        }

        events
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let sender = ClusterEventSender::new(100);

        let rx = vec![sender.subscribe(), sender.subscribe(), sender.subscribe()];

        let mut tasks: FuturesUnordered<_> = rx
            .into_iter()
            .map(|rx| async move { collect_events(rx).await })
            .collect();

        let handle = tokio::spawn(async move {
            let mut results = vec![];
            while let Some(result) = tasks.next().await {
                results.push(result)
            }
            results
        });

        tokio::spawn(async move {
            for i in 0..100 {
                sender.send(&i);
            }
        });

        let expected: Vec<i32> = (0..100).into_iter().collect();

        let results = handle.await.unwrap();
        assert_eq!(results.len(), 3);

        for res in results {
            assert_eq!(res, expected);
        }
    }

    #[tokio::test]
    async fn test_event_lagged() {
        // Created sender with a buffer for only 8 events
        let sender = ClusterEventSender::new(8);

        let rx = vec![sender.subscribe(), sender.subscribe(), sender.subscribe()];

        let mut tasks: FuturesUnordered<_> = rx
            .into_iter()
            .map(|rx| async move { collect_events(rx).await })
            .collect();

        let handle = tokio::spawn(async move {
            let mut results = vec![];
            while let Some(result) = tasks.next().await {
                results.push(result)
            }
            results
        });

        // Send events faster than they can be consumed by subscribers
        tokio::spawn(async move {
            for i in 0..100 {
                sender.send(&i);
            }
        });

        // When we reach capacity older events should be dropped so we only see
        // the last 8 events in our subscribers
        let expected: Vec<i32> = (92..100).into_iter().collect();

        let results = handle.await.unwrap();
        assert_eq!(results.len(), 3);

        for res in results {
            assert_eq!(res, expected);
        }
    }

    #[tokio::test]
    async fn test_event_skip_unsubscribed() {
        let sender = ClusterEventSender::new(100);

        // There are no subscribers yet so this event should be ignored
        sender.send(&0);

        let rx = vec![sender.subscribe(), sender.subscribe(), sender.subscribe()];

        let mut tasks: FuturesUnordered<_> = rx
            .into_iter()
            .map(|rx| async move { collect_events(rx).await })
            .collect();

        let handle = tokio::spawn(async move {
            let mut results = vec![];
            while let Some(result) = tasks.next().await {
                results.push(result)
            }
            results
        });

        tokio::spawn(async move {
            for i in 1..=100 {
                sender.send(&i);
            }
        });

        let expected: Vec<i32> = (1..=100).into_iter().collect();

        let results = handle.await.unwrap();
        assert_eq!(results.len(), 3);

        for res in results {
            assert_eq!(res, expected);
        }
    }

    #[tokio::test]
    async fn test_event_register_wakers() {
        let sender = ClusterEventSender::new(100);

        let mut rx_1 = sender.subscribe();
        let mut rx_2 = sender.subscribe();
        let mut rx_3 = sender.subscribe();

        sender.send(&0);

        // Subscribers haven't been polled yet so expect not registered wakers
        assert_eq!(sender.registered_wakers(), 0);

        let event = rx_1.next().await;
        assert_eq!(event, Some(0));
        assert_eq!(sender.registered_wakers(), 1);

        let event = rx_2.next().await;
        assert_eq!(event, Some(0));
        assert_eq!(sender.registered_wakers(), 2);

        let event = rx_3.next().await;
        assert_eq!(event, Some(0));
        assert_eq!(sender.registered_wakers(), 3);

        drop(rx_1);
        assert_eq!(sender.registered_wakers(), 2);

        drop(rx_2);
        assert_eq!(sender.registered_wakers(), 1);

        drop(rx_3);
        assert_eq!(sender.registered_wakers(), 0);
    }
}
