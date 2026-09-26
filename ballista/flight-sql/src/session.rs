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
//! pin memory forever.
//!
//! Prepared statements and local results belong to the session that created
//! them, and are only visible to requests from that session.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use dashmap::DashMap;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

/// A prepared statement's server-side state.
#[derive(Clone)]
pub(crate) struct Prepared {
    /// Session the statement was prepared in; it is planned and executed
    /// against that session's catalog.
    pub session_id: String,
    /// The plan as prepared; execution never re-plans it.
    pub plan: LogicalPlan,
}

/// Result of a statement the frontend ran on the scheduler rather than
/// distributing: DDL, session statements, and catalog queries.
#[derive(Clone)]
pub(crate) struct LocalResult {
    /// Session the statement ran in; only that session may redeem it.
    pub session_id: String,
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
    /// Ballista session id -> its context.
    contexts: DashMap<String, Tracked<Arc<SessionContext>>>,
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
            contexts: DashMap::new(),
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

    /// Returns the cached context for a session, refreshing its idle timer.
    pub(crate) fn context(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        self.contexts.get_mut(session_id).map(|mut entry| {
            entry.last_used = Instant::now();
            entry.value.clone()
        })
    }

    /// Caches a context, or returns the one another caller cached first.
    ///
    /// Two requests for a cold session can both build one; letting the first
    /// insertion win keeps them looking at the same catalog.
    pub(crate) fn insert_context(
        &self,
        session_id: String,
        ctx: Arc<SessionContext>,
    ) -> Arc<SessionContext> {
        self.contexts
            .entry(session_id)
            .or_insert_with(|| Tracked::new(ctx))
            .value
            .clone()
    }

    pub(crate) fn insert_prepared(&self, handle: String, prepared: Prepared) {
        self.prepared.insert(handle, Tracked::new(prepared));
    }

    /// Looks up a prepared statement for `session_id`, refreshing its idle
    /// timer. A handle from another session is treated as unknown.
    pub(crate) fn prepared(&self, handle: &str, session_id: &str) -> Option<Prepared> {
        let mut entry = self.prepared.get_mut(handle)?;
        if entry.value.session_id != session_id {
            return None;
        }
        entry.last_used = Instant::now();
        Some(entry.value.clone())
    }

    pub(crate) fn remove_prepared(&self, handle: &str, session_id: &str) {
        self.prepared
            .remove_if(handle, |_, entry| entry.value.session_id == session_id);
    }

    pub(crate) fn insert_result(&self, handle: String, result: LocalResult) {
        self.results.insert(handle, Tracked::new(result));
    }

    /// Takes a local result for `session_id`. Results are single-use: a
    /// ticket is redeemed once. A handle from another session is treated as
    /// unknown and left in place.
    pub(crate) fn take_result(
        &self,
        handle: &str,
        session_id: &str,
    ) -> Option<LocalResult> {
        self.results
            .remove_if(handle, |_, entry| entry.value.session_id == session_id)
            .map(|(_, entry)| entry.value)
    }

    /// Evicts everything idle for longer than the TTL.
    ///
    /// Returns the session ids that no longer have any live token, so the
    /// caller can release them in the backend.
    pub(crate) fn sweep(&self) -> Vec<String> {
        let ttl = self.ttl;
        let mut released = Vec::new();
        self.sessions.retain(|_, entry| {
            let live = entry.last_used.elapsed() <= ttl;
            if !live {
                released.push(entry.value.clone());
            }
            live
        });

        // Each token gets its own session, so an expired token releases its
        // context along with it, however recently the context was used.
        for session_id in &released {
            self.contexts.remove(session_id);
        }
        self.contexts
            .retain(|_, entry| entry.last_used.elapsed() <= ttl);
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

    /// Moves an entry's idle timer back by `age`, so expiry can be tested
    /// without sleeping.
    fn backdate<T>(entry: &mut Tracked<T>, age: Duration) {
        entry.last_used = Instant::now() - age;
    }

    #[test]
    fn session_survives_while_touched_and_expires_when_idle() {
        let ttl = Duration::from_secs(60);
        let store = SessionStore::new(ttl);
        store.insert_session("token".to_string(), "session".to_string());
        store.insert_context("session".to_string(), Arc::new(SessionContext::new()));

        assert_eq!(store.session("token"), Some("session".to_string()));
        assert!(store.sweep().is_empty());

        backdate(&mut store.sessions.get_mut("token").unwrap(), ttl * 2);
        assert_eq!(store.sweep(), vec!["session".to_string()]);
        assert_eq!(store.session("token"), None);
        assert!(
            store.context("session").is_none(),
            "an expired token releases its context"
        );
    }

    #[test]
    fn handles_are_only_visible_to_their_session() {
        let store = SessionStore::new(Duration::from_secs(60));
        store.insert_result(
            "result".to_string(),
            LocalResult {
                session_id: "a".to_string(),
                schema: Arc::new(arrow::datatypes::Schema::empty()),
                batches: vec![],
            },
        );
        store.insert_prepared(
            "prepared".to_string(),
            Prepared {
                session_id: "a".to_string(),
                plan: LogicalPlan::EmptyRelation(
                    datafusion::logical_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(datafusion::common::DFSchema::empty()),
                    },
                ),
            },
        );

        assert!(store.take_result("result", "b").is_none());
        assert!(store.prepared("prepared", "b").is_none());
        store.remove_prepared("prepared", "b");

        assert!(store.prepared("prepared", "a").is_some());
        assert!(store.take_result("result", "a").is_some());
        assert!(
            store.take_result("result", "a").is_none(),
            "results are single-use"
        );
    }

    #[test]
    fn the_first_cached_context_wins() {
        let store = SessionStore::new(Duration::from_secs(60));
        let first = Arc::new(SessionContext::new());
        let second = Arc::new(SessionContext::new());

        let kept = store.insert_context("session".to_string(), first.clone());
        assert!(Arc::ptr_eq(&kept, &first));

        let kept = store.insert_context("session".to_string(), second);
        assert!(
            Arc::ptr_eq(&kept, &first),
            "a later caller must not swap it"
        );
    }
}
