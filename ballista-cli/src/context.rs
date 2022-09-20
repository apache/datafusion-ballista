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

//! Context (remote or local)

use std::sync::Arc;

use ballista::prelude::{BallistaConfig, BallistaContext, BallistaError, Result};
use datafusion::dataframe::DataFrame;

/// The CLI supports using a Ballista Standalone context or a Distributed BallistaContext
pub enum Context {
    /// In-process execution with Ballista Standalone
    Local(BallistaContext),
    /// Distributed execution with Ballista (if available)
    Remote(BallistaContext),
}

impl Context {
    /// create a new remote context with given host and port
    pub async fn new_remote(
        host: &str,
        port: u16,
        config: &BallistaConfig,
    ) -> Result<Context> {
        Ok(Context::Remote(
            BallistaContext::remote(host, port, config).await?,
        ))
    }

    /// create a local context using the given config
    pub async fn new_local(
        config: &BallistaConfig,
        concurrent_tasks: usize,
    ) -> Result<Context> {
        Ok(Context::Local(
            BallistaContext::standalone(config, concurrent_tasks).await?,
        ))
    }

    /// execute an SQL statement against the context
    pub async fn sql(&mut self, sql: &str) -> Result<Arc<DataFrame>> {
        match self {
            Context::Local(ballista) => ballista
                .sql(sql)
                .await
                .map_err(|e| BallistaError::DataFusionError(e)),
            Context::Remote(ballista) => ballista
                .sql(sql)
                .await
                .map_err(|e| BallistaError::DataFusionError(e)),
        }
    }
}
