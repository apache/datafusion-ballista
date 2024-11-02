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
use ballista_core::config::BallistaConfig;
use ballista_core::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::cluster::JobState;
use std::sync::Arc;

#[derive(Clone)]
pub struct SessionManager {
    state: Arc<dyn JobState>,
}

impl SessionManager {
    pub fn new(state: Arc<dyn JobState>) -> Self {
        Self { state }
    }

    pub async fn remove_session(
        &self,
        session_id: &str,
    ) -> Result<Option<Arc<SessionContext>>> {
        self.state.remove_session(session_id).await
    }

    pub async fn update_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>> {
        self.state.update_session(session_id, config).await
    }

    pub async fn create_session(
        &self,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>> {
        self.state.create_session(config).await
    }

    pub async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        self.state.get_session(session_id).await
    }
}

/// Create a DataFusion session context that is compatible with Ballista Configuration
pub fn create_datafusion_context(
    session_config: &SessionConfig,
    session_builder: SessionBuilder,
) -> Arc<SessionContext> {
    let session_state = if session_config.round_robin_repartition() {
        let session_config = session_config
            .clone()
            .set_bool("datafusion.optimizer.enable_round_robin_repartition", false);

        log::warn!("session manager will override `datafusion.optimizer.enable_round_robin_repartition` to `false` ");
        session_builder(session_config)
    } else {
        session_builder(session_config.clone())
    };

    Arc::new(SessionContext::new_with_state(session_state))
}
