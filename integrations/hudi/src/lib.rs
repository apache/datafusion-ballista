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

use std::sync::Arc;

use ballista::datafusion::arrow::datatypes::SchemaRef;
use ballista::datafusion::catalog::TableProvider;
use ballista::datafusion::common::Result;
use ballista::datafusion::execution::TaskContext;
use ballista::datafusion::execution::context::SessionConfig;
use ballista::datafusion::prelude::SessionContext;
use ballista::hudi::{
    HudiProvider, HudiProviderCodec, register_hudi_codec as register_codec,
};
use hudi_datafusion::HudiDataSource;

#[derive(Debug)]
struct HudiDataSourceCodec;

impl HudiProviderCodec for HudiDataSourceCodec {
    fn try_encode(&self, provider: &dyn TableProvider) -> Result<Option<HudiProvider>> {
        Ok(provider.downcast_ref::<HudiDataSource>().map(|provider| {
            HudiProvider {
                base_uri: provider.base_uri(),
                options: provider.options().clone(),
            }
        }))
    }

    fn decode(
        &self,
        provider: HudiProvider,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let provider = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(HudiDataSource::new_with_options(
                &provider.base_uri,
                provider.options,
            ))
        })?;
        Ok(Arc::new(provider))
    }
}

/// Installs the Hudi provider codec for a Ballista session.
pub fn register_hudi_codec(config: SessionConfig) -> SessionConfig {
    register_codec(config, Arc::new(HudiDataSourceCodec))
}

/// Creates and registers a Hudi table provider.
pub async fn register_hudi_table<I, K, V>(
    ctx: &SessionContext,
    name: &str,
    base_uri: &str,
    options: I,
) -> Result<()>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let provider = HudiDataSource::new_with_options(base_uri, options).await?;
    ctx.register_table(name, Arc::new(provider))?;
    Ok(())
}
