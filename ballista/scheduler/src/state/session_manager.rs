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
use datafusion::common::ScalarValue;
use log::warn;
use std::sync::Arc;

#[derive(Clone)]
pub struct SessionManager {
    state: Arc<dyn JobState>,
}

impl SessionManager {
    pub fn new(state: Arc<dyn JobState>) -> Self {
        Self { state }
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
    let config = SessionConfig::new()
        .with_target_partitions(ballista_config.default_shuffle_partitions())
        .with_batch_size(ballista_config.default_batch_size())
        .with_repartition_joins(ballista_config.repartition_joins())
        .with_repartition_aggregations(ballista_config.repartition_aggregations())
        .with_repartition_windows(ballista_config.repartition_windows())
        .with_parquet_pruning(ballista_config.parquet_pruning());
    let config = propagate_ballista_configs(config, ballista_config);

    let session_state = session_builder(config);
    Arc::new(SessionContext::with_state(session_state))
}

fn propagate_ballista_configs(
    config: SessionConfig,
    ballista_config: &BallistaConfig,
) -> SessionConfig {
    let mut config = config;
    // TODO we cannot just pass string values along to DataFusion configs
    // and we will need to improve that in the next release of DataFusion
    // see https://github.com/apache/arrow-datafusion/issues/3500
    for (k, v) in ballista_config.settings() {
        // see https://arrow.apache.org/datafusion/user-guide/configs.html for explanation of these configs
        match k.as_str() {
            "datafusion.optimizer.filter_null_join_keys" => {
                config = config.set(
                    k,
                    ScalarValue::Boolean(Some(v.parse::<bool>().unwrap_or(false))),
                )
            }
            "datafusion.execution.coalesce_batches" => {
                config = config.set(
                    k,
                    ScalarValue::Boolean(Some(v.parse::<bool>().unwrap_or(true))),
                )
            }
            "datafusion.execution.coalesce_target_batch_size" => {
                config = config.set(
                    k,
                    ScalarValue::UInt64(Some(v.parse::<u64>().unwrap_or(4096))),
                )
            }
            "datafusion.optimizer.skip_failed_rules" => {
                config = config.set(
                    k,
                    ScalarValue::Boolean(Some(v.parse::<bool>().unwrap_or(true))),
                )
            }
            _ => {
                warn!("Ignoring unknown configuration option {} = {}", k, v);
            }
        }
    }
    config
}
