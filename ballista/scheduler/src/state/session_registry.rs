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

use dashmap::DashMap;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// A Registry holds all the datafusion session contexts
pub struct SessionContextRegistry {
    /// A map from session_id to SessionContext
    pub running_sessions: DashMap<String, Arc<SessionContext>>,
}

impl Default for SessionContextRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionContextRegistry {
    /// Create the registry that session contexts can registered into.
    /// ['LocalFileSystem'] store is registered in by default to support read local files natively.
    pub fn new() -> Self {
        Self {
            running_sessions: DashMap::new(),
        }
    }

    /// Adds a new session to this registry.
    pub async fn register_session(
        &self,
        session_ctx: Arc<SessionContext>,
    ) -> Option<Arc<SessionContext>> {
        let session_id = session_ctx.session_id();
        self.running_sessions.insert(session_id, session_ctx)
    }

    /// Lookup the session context registered
    pub async fn lookup_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        self.running_sessions
            .get(session_id)
            .map(|value| value.clone())
    }

    /// Remove a session from this registry.
    pub async fn unregister_session(
        &self,
        session_id: &str,
    ) -> Option<Arc<SessionContext>> {
        match self.running_sessions.remove(session_id) {
            None => None,
            Some(value) => Some(value.1),
        }
    }
}
