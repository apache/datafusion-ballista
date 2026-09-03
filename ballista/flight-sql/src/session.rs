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

//! Server-side state the Flight SQL frontend keeps between requests.
//!
//! Everything in here is keyed by an opaque handle the client is given and
//! hands back, and everything expires. A client that disconnects without
//! closing its prepared statements (or that never redeems a ticket) must not
//! pin memory forever, which is what the previous Flight SQL implementation
//! got wrong: its plan cache only shed entries on an explicit close.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use dashmap::DashMap;
use datafusion::logical_expr::LogicalPlan;

/// A prepared statement's server-side state.
#[derive(Clone)]
pub(crate) struct Prepared {
    /// Session the statement was prepared in; it is planned and executed
    /// against that session's catalog.
    pub session_id: String,
    /// The plan as prepared. Re-planned on execution only if absent.
    pub plan: LogicalPlan,
}

/// Result of a statement the frontend ran locally rather than distributing
/// (DDL, and DML whose only output is an affected-row count).
#[derive(Clone)]
pub(crate) struct LocalResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

struct Tracked<T> {
    value: T,
    last_used: Instant,
}

impl<T> Tracked<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            last_used: Instant::now(),
        }
    }
}

/// Handle stores for sessions, prepared statements, and locally-computed
/// results, all with a shared idle TTL.
pub(crate) struct SessionStore {
    ttl: Duration,
    /// Bearer token -> Ballista session id.
    sessions: DashMap<String, Tracked<String>>,
    /// Prepared statement handle -> prepared plan.
    prepared: DashMap<String, Tracked<Prepared>>,
    /// Local result handle -> materialized batches.
    results: DashMap<String, Tracked<LocalResult>>,
}

impl SessionStore {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            sessions: DashMap::new(),
            prepared: DashMap::new(),
            results: DashMap::new(),
        }
    }

    pub(crate) fn insert_session(&self, token: String, session_id: String) {
        self.sessions.insert(token, Tracked::new(session_id));
    }

    /// Resolves a bearer token to its session id, refreshing its idle timer.
    pub(crate) fn session(&self, token: &str) -> Option<String> {
        self.sessions.get_mut(token).map(|mut entry| {
            entry.last_used = Instant::now();
            entry.value.clone()
        })
    }

    /// Drops a token, returning the session it pointed at if no other token
    /// still references that session.
    pub(crate) fn remove_session(&self, token: &str) -> Option<String> {
        let (_, entry) = self.sessions.remove(token)?;
        let session_id = entry.value;
        let still_referenced = self
            .sessions
            .iter()
            .any(|other| other.value().value == session_id);
        (!still_referenced).then_some(session_id)
    }

    pub(crate) fn insert_prepared(&self, handle: String, prepared: Prepared) {
        self.prepared.insert(handle, Tracked::new(prepared));
    }

    pub(crate) fn prepared(&self, handle: &str) -> Option<Prepared> {
        self.prepared.get_mut(handle).map(|mut entry| {
            entry.last_used = Instant::now();
            entry.value.clone()
        })
    }

    pub(crate) fn remove_prepared(&self, handle: &str) {
        self.prepared.remove(handle);
    }

    pub(crate) fn insert_result(&self, handle: String, result: LocalResult) {
        self.results.insert(handle, Tracked::new(result));
    }

    /// Takes a local result. Results are single-use: a ticket is redeemed once.
    pub(crate) fn take_result(&self, handle: &str) -> Option<LocalResult> {
        self.results.remove(handle).map(|(_, entry)| entry.value)
    }

    /// Evicts everything idle for longer than the TTL.
    ///
    /// Returns the session ids that no longer have any live token, so the
    /// caller can release them in the backend.
    pub(crate) fn sweep(&self) -> Vec<String> {
        let ttl = self.ttl;
        let expired: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| entry.value().last_used.elapsed() > ttl)
            .map(|entry| entry.key().clone())
            .collect();

        let mut released = Vec::new();
        for token in expired {
            if let Some(session_id) = self.remove_session(&token) {
                released.push(session_id);
            }
        }

        self.prepared
            .retain(|_, entry| entry.last_used.elapsed() <= ttl);
        self.results
            .retain(|_, entry| entry.last_used.elapsed() <= ttl);

        released
    }

    /// Starts a background task that sweeps at `interval`, closing sessions it
    /// evicts. The task ends when the last reference to the store is dropped.
    pub(crate) fn spawn_reaper<F, Fut>(self: &Arc<Self>, interval: Duration, close: F)
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let store = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick completes immediately; skip it so we do not sweep
            // a store that was created a moment ago.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(store) = store.upgrade() else {
                    return;
                };
                for session_id in store.sweep() {
                    log::debug!("flight-sql: expiring idle session {session_id}");
                    close(session_id).await;
                }
            }
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn session_survives_while_touched_and_expires_when_idle() {
        let store = SessionStore::new(Duration::from_millis(50));
        store.insert_session("token".to_string(), "session".to_string());

        assert_eq!(store.session("token"), Some("session".to_string()));
        assert!(store.sweep().is_empty());

        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(store.sweep(), vec!["session".to_string()]);
        assert_eq!(store.session("token"), None);
    }

    #[test]
    fn session_is_released_only_when_its_last_token_goes() {
        let store = SessionStore::new(Duration::from_secs(60));
        store.insert_session("a".to_string(), "session".to_string());
        store.insert_session("b".to_string(), "session".to_string());

        assert_eq!(store.remove_session("a"), None);
        assert_eq!(store.remove_session("b"), Some("session".to_string()));
    }

    #[test]
    fn local_results_are_single_use() {
        let store = SessionStore::new(Duration::from_secs(60));
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        store.insert_result(
            "handle".to_string(),
            LocalResult {
                schema,
                batches: vec![],
            },
        );

        assert!(store.take_result("handle").is_some());
        assert!(store.take_result("handle").is_none());
    }
}
