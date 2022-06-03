use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A Registry holds all the datafusion session contexts
pub struct SessionContextRegistry {
    /// A map from session_id to SessionContext
    pub running_sessions: RwLock<HashMap<String, Arc<SessionContext>>>,
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
            running_sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a new session to this registry.
    pub async fn register_session(
        &self,
        session_ctx: Arc<SessionContext>,
    ) -> Option<Arc<SessionContext>> {
        let session_id = session_ctx.session_id();
        let mut sessions = self.running_sessions.write().await;
        sessions.insert(session_id, session_ctx)
    }

    /// Lookup the session context registered
    pub async fn lookup_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        let sessions = self.running_sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// Remove a session from this registry.
    pub async fn unregister_session(
        &self,
        session_id: &str,
    ) -> Option<Arc<SessionContext>> {
        let mut sessions = self.running_sessions.write().await;
        sessions.remove(session_id)
    }
}
