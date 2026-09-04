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

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock};

use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaTableProviderCodec};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::execution::{TaskContext, context::SessionConfig};
use datafusion::prelude::SessionContext;
use hudi_datafusion::HudiDataSource;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct HudiProviderWire {
    base_uri: String,
    options: HashMap<String, String>,
}

static HUDI_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("hudi-ballista-codec")
        .build()
        .expect("failed to build Hudi codec runtime")
});

fn block_on<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let wait = move || {
        std::thread::scope(|scope| {
            scope
                .spawn(|| HUDI_RUNTIME.block_on(future))
                .join()
                .expect("Hudi provider reconstruction panicked")
        })
    };

    if tokio::runtime::Handle::try_current().is_ok_and(|handle| {
        handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    }) {
        tokio::task::block_in_place(wait)
    } else {
        wait()
    }
}

/// Serializes the recipe required to reconstruct a Hudi table provider.
#[derive(Debug, Default)]
pub struct HudiTableProviderCodec;

impl BallistaTableProviderCodec for HudiTableProviderCodec {
    fn name(&self) -> &str {
        "hudi"
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Vec<u8>>> {
        let Some(provider) = provider.downcast_ref::<HudiDataSource>() else {
            return Ok(None);
        };
        serde_json::to_vec(&HudiProviderWire {
            base_uri: provider.base_uri(),
            options: provider.options().clone(),
        })
        .map(Some)
        .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    fn decode_table_provider(
        &self,
        payload: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let wire: HudiProviderWire = serde_json::from_slice(payload)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let provider = block_on(HudiDataSource::new_with_options(
            &wire.base_uri,
            wire.options,
        ))?;
        Ok(Arc::new(provider))
    }
}

/// Installs the Hudi provider codec for a standalone Ballista session.
pub fn register_hudi_codec(config: SessionConfig) -> SessionConfig {
    let codec = BallistaLogicalExtensionCodec::default()
        .with_table_provider_codec(Arc::new(HudiTableProviderCodec));
    config.with_ballista_logical_extension_codec(Arc::new(codec))
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
