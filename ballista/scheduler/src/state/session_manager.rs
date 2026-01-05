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
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::cluster::JobState;
use std::sync::Arc;

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
