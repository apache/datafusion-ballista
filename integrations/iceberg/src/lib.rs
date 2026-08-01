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

//! # Ballista Iceberg driver
//!
//! Adds distributed Apache Iceberg reads and writes to Ballista.
//!
//! Iceberg's DataFusion integration already produces a complete physical plan;
//! the only thing missing for Ballista is serialization. Ballista ships plans to
//! remote nodes, but the Iceberg plan nodes hold live catalog/storage handles
//! that can't be serialized. This crate's logical and physical extension codecs
//! serialize the minimal config needed to rebuild those handles per node.
//!
//! ## Usage (standalone)
//!
//! ```ignore
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! use iceberg_ballista::{register_iceberg_codecs, register_iceberg_table, IcebergCatalogConfig};
//! use ballista_core::extension::SessionConfigExt;
//! use datafusion::prelude::{SessionConfig, SessionContext};
//! use iceberg::NamespaceIdent;
//!
//! # async fn run() -> datafusion::error::Result<()> {
//! // 1. Register the Iceberg codecs on the session config, then start standalone Ballista.
//! let config = register_iceberg_codecs(SessionConfig::new_with_ballista());
//! let ctx = SessionContext::standalone_with_config(config).await?;
//!
//! // 2. Register a catalog-backed Iceberg table for reads and writes.
//! let props = HashMap::from([("uri".to_string(), "http://localhost:8181".to_string())]);
//! let cfg = IcebergCatalogConfig::new("rest", "rest", props);
//! register_iceberg_table(&ctx, "t", cfg, NamespaceIdent::new("ns".into()), "tbl").await?;
//!
//! // 3. INSERT runs distributed across the cluster.
//! ctx.sql("INSERT INTO t SELECT * FROM source").await?.collect().await?;
//! # Ok(())
//! # }
//! ```

mod bridge;
mod logical_codec;
mod physical_codec;

use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use datafusion::common::DataFusionError;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::NamespaceIdent;
pub use iceberg_datafusion::IcebergCatalogConfig;
use iceberg_datafusion::to_datafusion_error;

pub use crate::logical_codec::IcebergLogicalCodec;
pub use crate::physical_codec::IcebergPhysicalCodec;

/// Installs the Iceberg logical and physical extension codecs onto a
/// [`SessionConfig`].
///
/// In a standalone cluster the scheduler and executor both derive their codecs
/// from this config, so one call suffices. For a separately deployed scheduler
/// and executor, set the same codecs on their process configs
/// (`override_logical_codec` / `override_physical_codec`).
pub fn register_iceberg_codecs(config: SessionConfig) -> SessionConfig {
    config
        .with_ballista_logical_extension_codec(Arc::new(IcebergLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(IcebergPhysicalCodec::default()))
}

/// Builds a catalog-backed [`IcebergTableProvider`](iceberg_datafusion::IcebergTableProvider)
/// from `config` and registers it on `ctx` under `register_name`.
///
/// The provider carries `config` so its plan nodes can be reconstructed on
/// remote Ballista nodes.
pub async fn register_iceberg_table(
    ctx: &SessionContext,
    register_name: &str,
    config: IcebergCatalogConfig,
    namespace: NamespaceIdent,
    table: impl Into<String>,
) -> Result<(), DataFusionError> {
    let catalog = bridge::build_catalog(&config).await?;
    let provider = iceberg_datafusion::IcebergTableProvider::try_new_with_config(
        catalog, config, namespace, table,
    )
    .await
    .map_err(to_datafusion_error)?;
    ctx.register_table(register_name, Arc::new(provider))?;
    Ok(())
}

/// Builds an [`IcebergCatalogProvider`](iceberg_datafusion::IcebergCatalogProvider)
/// from `config` and registers it on `ctx` under `register_name`, mounting the
/// whole Iceberg catalog at once.
///
/// Every table then resolves as `<register_name>.<namespace>.<table>` in SQL,
/// and each provider carries `config` so its plan nodes can be reconstructed on
/// remote nodes — including metadata tables such as `<table>$snapshots`.
pub async fn register_iceberg_catalog(
    ctx: &SessionContext,
    register_name: &str,
    config: IcebergCatalogConfig,
) -> Result<(), DataFusionError> {
    let catalog = bridge::build_catalog(&config).await?;
    let provider =
        iceberg_datafusion::IcebergCatalogProvider::try_new_with_config(catalog, config)
            .await
            .map_err(to_datafusion_error)?;
    ctx.register_catalog(register_name, Arc::new(provider));
    Ok(())
}
