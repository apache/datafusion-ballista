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
//! send what each node needs to rebuild them:
//!
//! - Scans, writes and metadata-table scans send their table's metadata file
//!   and its serialized storage access (`FileIO`). An executor rebuilds exactly
//!   the table version the plan was made against, without a catalog.
//! - Commits, and catalog-backed tables, send the [`IcebergCatalogConfig`] so
//!   the catalog can be rebuilt where it is needed.
//!
//! Both carry credentials in plain text, so the links between client, scheduler
//! and executors must be trusted or encrypted.
//!
//! ## Usage (standalone)
//!
//! ```ignore
//! use std::collections::HashMap;
//!
//! use iceberg_ballista::{
//!     IcebergCatalogConfig, register_iceberg_codecs, register_iceberg_table,
//!     register_iceberg_table_at_snapshot,
//! };
//! use ballista_core::extension::SessionConfigExt;
//! use datafusion::prelude::{SessionConfig, SessionContext};
//! use iceberg::NamespaceIdent;
//!
//! # async fn run(snapshot_id: i64) -> datafusion::error::Result<()> {
//! // 1. Register the Iceberg codecs on the session config, then start standalone Ballista.
//! let config = register_iceberg_codecs(SessionConfig::new_with_ballista());
//! let ctx = SessionContext::standalone_with_config(config).await?;
//!
//! // 2. Register a catalog-backed Iceberg table for reads and writes.
//! let props = HashMap::from([("uri".to_string(), "http://localhost:8181".to_string())]);
//! let cfg = IcebergCatalogConfig::new("rest", "rest", props.clone());
//! register_iceberg_table(&ctx, "t", cfg, NamespaceIdent::new("ns".into()), "tbl").await?;
//!
//! // 3. INSERT runs distributed across the cluster.
//! ctx.sql("INSERT INTO t SELECT * FROM source").await?.collect().await?;
//!
//! // 4. Time travel: a read-only view pinned to a snapshot.
//! let cfg = IcebergCatalogConfig::new("rest", "rest", props);
//! let ns = NamespaceIdent::new("ns".into());
//! register_iceberg_table_at_snapshot(&ctx, "t_v1", cfg, ns, "tbl", Some(snapshot_id)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Prefer these `register_*` helpers to building providers and calling
//! `with_catalog_config` yourself. Each helper builds the catalog from the
//! config it records, so the scheduler and the executors always use the same
//! catalog. A hand-built provider whose catalog differs from its config raises
//! no error: the plan is made against one catalog while the executors commit
//! through the other.

mod bridge;
mod logical_codec;
mod physical_codec;
#[cfg(test)]
mod test_util;

use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use datafusion::common::DataFusionError;
use datafusion::prelude::{SessionConfig, SessionContext};
pub use datafusion_iceberg::IcebergCatalogConfig;
use datafusion_iceberg::to_datafusion_error;
use iceberg::{NamespaceIdent, TableIdent};

pub use crate::logical_codec::IcebergLogicalCodec;
pub use crate::physical_codec::IcebergPhysicalCodec;

/// Installs the Iceberg logical and physical extension codecs onto a
/// [`SessionConfig`].
///
/// In a standalone cluster the scheduler and executor both derive their codecs
/// from this config, so one call suffices. For a separately deployed scheduler
/// and executor, set the same codecs on their process configs
/// (`override_logical_codec` / `override_physical_codec`); the
/// `cluster-iceberg-write` example shows both custom binaries.
pub fn register_iceberg_codecs(config: SessionConfig) -> SessionConfig {
    let logical = config.ballista_logical_extension_codec();
    let physical = config.ballista_physical_extension_codec();

    config
        .with_ballista_logical_extension_codec(Arc::new(IcebergLogicalCodec::new(
            logical,
        )))
        .with_ballista_physical_extension_codec(Arc::new(IcebergPhysicalCodec::new(
            physical,
        )))
}

/// Builds a catalog-backed [`IcebergTableProvider`](datafusion_iceberg::IcebergTableProvider)
/// from `config` and registers it on `ctx` under `register_name`.
///
/// The provider carries `config`, so the scheduler can rebuild it and executors
/// can commit writes through the same catalog.
pub async fn register_iceberg_table(
    ctx: &SessionContext,
    register_name: &str,
    config: IcebergCatalogConfig,
    namespace: NamespaceIdent,
    table: impl Into<String>,
) -> Result<(), DataFusionError> {
    let catalog = bridge::build_catalog(&config).await?;
    let provider =
        datafusion_iceberg::IcebergTableProvider::try_new(catalog, namespace, table)
            .await?
            .with_catalog_config(config);
    ctx.register_table(register_name, Arc::new(provider))?;
    Ok(())
}

/// Registers a read-only view of an Iceberg table on `ctx` under
/// `register_name`, pinned to `snapshot_id` (time travel), or to the table's
/// current snapshot when `None`.
///
/// The view is an
/// [`IcebergStaticTableProvider`](datafusion_iceberg::IcebergStaticTableProvider):
/// it always reads the same snapshot, with the schema that snapshot was written
/// under, and rejects writes. Use [`register_iceberg_table`] to write to the
/// table or to read its latest state.
///
/// `config` is only used here, to load the table. The view keeps the table as
/// loaded, so remote nodes read that same version without a catalog.
pub async fn register_iceberg_table_at_snapshot(
    ctx: &SessionContext,
    register_name: &str,
    config: IcebergCatalogConfig,
    namespace: NamespaceIdent,
    table: impl Into<String>,
    snapshot_id: Option<i64>,
) -> Result<(), DataFusionError> {
    let catalog = bridge::build_catalog(&config).await?;
    let table = catalog
        .load_table(&TableIdent::new(namespace, table.into()))
        .await
        .map_err(to_datafusion_error)?;
    let provider = bridge::static_provider(table, snapshot_id).await?;
    ctx.register_table(register_name, Arc::new(provider))?;
    Ok(())
}

/// Builds an [`IcebergCatalogProvider`](datafusion_iceberg::IcebergCatalogProvider)
/// from `config` and registers it on `ctx` under `register_name`, mounting the
/// whole Iceberg catalog at once.
///
/// Every table then resolves as `<register_name>.<namespace>.<table>` in SQL,
/// including metadata tables such as `<table>$snapshots`, and each table
/// provider carries `config` as [`register_iceberg_table`] describes.
pub async fn register_iceberg_catalog(
    ctx: &SessionContext,
    register_name: &str,
    config: IcebergCatalogConfig,
) -> Result<(), DataFusionError> {
    let catalog = bridge::build_catalog(&config).await?;
    let provider =
        datafusion_iceberg::IcebergCatalogProvider::try_new_with_config(catalog, config)
            .await?;
    ctx.register_catalog(register_name, Arc::new(provider));
    Ok(())
}
