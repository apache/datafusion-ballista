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

use crate::scheduler_server::SessionBuilder;
use ballista_core::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::cluster::JobState;
use std::sync::{Arc, OnceLock};

/// Manages DataFusion session contexts for the Ballista scheduler.
///
/// Sessions hold configuration and state for query execution.
#[derive(Clone)]
pub struct SessionManager {
    /// Job state storage for persisting session information.
    state: Arc<dyn JobState>,
}

impl SessionManager {
    /// Creates a new `SessionManager` with the given job state backend.
    pub fn new(state: Arc<dyn JobState>) -> Self {
        Self { state }
    }

    /// Removes a session from the state store.
    pub async fn remove_session(&self, session_id: &str) -> Result<()> {
        self.state.remove_session(session_id).await
    }

    /// Creates a new session or updates an existing one with the given configuration.
    ///
    /// Returns the session context that can be used for query execution.
    pub async fn create_or_update_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>> {
        self.state
            .create_or_update_session(session_id, config)
            .await
    }

    pub(crate) fn produce_config(&self) -> SessionConfig {
        self.state.produce_config()
    }
}

/// Creates a DataFusion session context that is compatible with Ballista configuration.
///
/// This function disables round-robin repartitioning if it was enabled, as Ballista
/// handles partitioning differently.
pub fn create_datafusion_context(
    session_config: &SessionConfig,
    session_builder: SessionBuilder,
) -> datafusion::common::Result<Arc<SessionContext>> {
    let session_state = if session_config.round_robin_repartition() {
        let session_config = session_config
            .clone()
            // should we disable catalog on the scheduler side
            .with_round_robin_repartition(false);

        log::warn!(
            "session manager will override `datafusion.optimizer.enable_round_robin_repartition` to `false` "
        );
        session_builder(session_config)?
    } else {
        session_builder(session_config.clone())?
    };

    Ok(Arc::new(SessionContext::new_with_state(session_state)))
}

/// Wraps `session_builder` so that every session it builds shares one file
/// statistics cache.
///
/// The scheduler builds a new session, with its own runtime, for every query.
/// Planning a scan of a listing table collects statistics by reading the
/// footer of every file in it, so with a cache per session every job pays for
/// that again, which on large tables takes seconds. Cached statistics are
/// checked against the size and modification time from each job's own file
/// listing, so a file that has changed is read again. That check is why the
/// listing cache must stay per session: sharing it too would serve stale
/// statistics, and `COUNT(*)` is answered from them.
///
/// The shared cache is the one the first session was built with, so the
/// builder's configured limit applies, and a builder that disables the cache
/// also disables sharing.
pub(crate) fn share_file_statistics_cache(
    session_builder: SessionBuilder,
) -> SessionBuilder {
    let shared = OnceLock::new();
    Arc::new(move |config| {
        let state = session_builder(config)?;
        let runtime = state.runtime_env();
        let Some(cache) = shared
            .get_or_init(|| runtime.cache_manager.get_file_statistic_cache())
            .clone()
        else {
            return Ok(state);
        };

        let mut runtime = RuntimeEnvBuilder::from_runtime_env(runtime);
        // Building the runtime sets the cache's limit to the configured one,
        // so pass the cache's own limit rather than this session's.
        runtime.cache_manager = runtime
            .cache_manager
            .with_file_statistics_cache_limit(cache.cache_limit())
            .with_file_statistics_cache(Some(cache));

        // `new_from_existing` would otherwise give the session a new ID.
        let session_id = state.session_id().to_string();
        Ok(SessionStateBuilder::new_from_existing(state)
            .with_session_id(session_id)
            .with_runtime_env(runtime.build_arc()?)
            .build())
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::execution::SessionState;

    #[test]
    fn test_share_file_statistics_cache_keeps_session_id() -> Result<()> {
        let session_builder: SessionBuilder = Arc::new(|config| {
            Ok(SessionStateBuilder::new()
                .with_config(config)
                .with_session_id("session_0".to_string())
                .build())
        });
        let session_builder = share_file_statistics_cache(session_builder);

        let state: SessionState = session_builder(SessionConfig::new())?;
        assert_eq!("session_0", state.session_id());

        Ok(())
    }
}
