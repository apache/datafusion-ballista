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

//! Build gate for optional Apache Hudi support.
//!
//! The gate defines the Hudi wire contract and delegates provider construction
//! to an implementation supplied by an integration crate.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionConfig;
use datafusion::logical_expr::Extension;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use prost::Message;

const HUDI_PROVIDER_PREFIX: &[u8] = b"ballista-contrib-hudi-v1\0";

/// Data needed to reconstruct a Hudi table provider on another Ballista node.
#[derive(Clone, PartialEq, Message)]
pub struct HudiProvider {
    /// Hudi table location.
    #[prost(string, tag = 1)]
    pub base_uri: String,
    /// Caller-supplied Hudi and object-store options.
    #[prost(map = "string, string", tag = 2)]
    pub options: HashMap<String, String>,
}

/// Hudi-specific provider serialization implemented outside Ballista's release graph.
pub trait HudiProviderCodec: Debug + Send + Sync {
    /// Returns the Hudi wire descriptor when this codec owns the provider.
    fn try_encode(&self, provider: &dyn TableProvider) -> Result<Option<HudiProvider>>;

    /// Reconstructs a Hudi table provider from its wire descriptor.
    fn decode(
        &self,
        provider: HudiProvider,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>>;
}

/// Logical codec that combines Ballista's defaults with a Hudi provider codec.
#[derive(Debug)]
pub struct HudiLogicalExtensionCodec {
    default_codec: BallistaLogicalExtensionCodec,
    provider_codec: Arc<dyn HudiProviderCodec>,
}

impl HudiLogicalExtensionCodec {
    /// Creates a logical codec backed by the supplied Hudi provider codec.
    pub fn new(provider_codec: Arc<dyn HudiProviderCodec>) -> Self {
        Self {
            default_codec: BallistaLogicalExtensionCodec::default(),
            provider_codec,
        }
    }
}

impl LogicalExtensionCodec for HudiLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.default_codec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.default_codec.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let Some(payload) = buf.strip_prefix(HUDI_PROVIDER_PREFIX) else {
            return self
                .default_codec
                .try_decode_table_provider(buf, table_ref, schema, ctx);
        };
        let provider = HudiProvider::decode(payload).map_err(|error| {
            DataFusionError::Internal(format!("failed to decode Hudi provider: {error}"))
        })?;
        self.provider_codec.decode(provider, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(provider) = self.provider_codec.try_encode(node.as_ref())? else {
            return self
                .default_codec
                .try_encode_table_provider(table_ref, node, buf);
        };
        buf.extend_from_slice(HUDI_PROVIDER_PREFIX);
        provider.encode(buf).map_err(|error| {
            DataFusionError::Internal(format!("failed to encode Hudi provider: {error}"))
        })
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn datafusion::datasource::file_format::FileFormatFactory>> {
        self.default_codec.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    ) -> Result<()> {
        self.default_codec.try_encode_file_format(buf, node)
    }
}

/// Installs a Hudi provider codec while retaining Ballista's defaults.
pub fn register_hudi_codec(
    config: SessionConfig,
    provider_codec: Arc<dyn HudiProviderCodec>,
) -> SessionConfig {
    config.with_ballista_logical_extension_codec(Arc::new(
        HudiLogicalExtensionCodec::new(provider_codec),
    ))
}
