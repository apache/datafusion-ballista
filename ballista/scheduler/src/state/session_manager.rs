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
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        self.state.update_session(session_id, config).await
    }

    pub async fn create_session(
        &self,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        self.state.create_session(config).await
    }

    pub async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        self.state.get_session(session_id).await
    }
}

/// Create a DataFusion session context that is compatible with Ballista Configuration
pub fn create_datafusion_context(
    ballista_config: &BallistaConfig,
    session_builder: SessionBuilder,
) -> Arc<SessionContext> {
    let config =
        SessionConfig::from_string_hash_map(ballista_config.settings().clone()).unwrap();
    let config = config
        .with_target_partitions(ballista_config.default_shuffle_partitions())
        .with_batch_size(ballista_config.default_batch_size())
        .with_repartition_joins(ballista_config.repartition_joins())
        .with_repartition_aggregations(ballista_config.repartition_aggregations())
        .with_repartition_windows(ballista_config.repartition_windows())
        .with_parquet_pruning(ballista_config.parquet_pruning())
        .set_bool("datafusion.optimizer.enable_round_robin_repartition", false);
    let session_state = session_builder(config);
    Arc::new(SessionContext::with_state(session_state))
}
