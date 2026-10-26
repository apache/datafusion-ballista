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

//! Apache Iceberg support for Ballista.
//!
//! Ballista sends plans between nodes as bytes, so these codecs send each Iceberg
//! table and scan as one version of the table, which any node can rebuild
//! without a catalog. A catalog-backed table is sent as the version its catalog
//! served when the query was planned, a static one as the version it holds.
//! Only the client talks to a catalog.
//!
//! Storage comes from [`iceberg_storage_opendal`]: S3 and local files by
//! default, other stores with its `opendal-*` features.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use ballista::prelude::{SessionConfigExt, SessionContextExt};
//! # use datafusion::execution::SessionStateBuilder;
//! # use datafusion::prelude::{SessionConfig, SessionContext};
//! # use ballista_iceberg::register_iceberg;
//! # use iceberg_datafusion::IcebergCatalogProvider;
//! # async fn example(catalog: Arc<dyn iceberg::Catalog>) -> datafusion::error::Result<()> {
//! let state = SessionStateBuilder::new()
//!     .with_config(SessionConfig::new_with_ballista())
//!     .with_default_features()
//!     .build();
//! let ctx = SessionContext::standalone_with_state(register_iceberg(state)).await?;
//!
//! ctx.register_catalog("rest", Arc::new(IcebergCatalogProvider::try_new(catalog).await?));
//! ctx.sql("SELECT * FROM rest.ns.t").await?.show().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Limitations
//!
//! - Reads only.
//! - Each scan runs as a single task.
//! - No metadata tables such as `t$snapshots`.
//! - Storage credentials travel in plain text.

mod catalog;
mod logical_codec;
mod physical_codec;
mod wire;

use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionConfig;
// Linked for the storage it registers; nothing here calls it.
use iceberg_storage_opendal as _;

use crate::catalog::PinningCatalogList;

pub use crate::logical_codec::IcebergLogicalCodec;
pub use crate::physical_codec::IcebergPhysicalCodec;

/// Readies a client's session for Iceberg: registers the codecs, and pins
/// each catalog-backed table a query names to the version its catalog serves
/// when the query is planned.
pub fn register_iceberg(mut state: SessionState) -> SessionState {
    *state.config_mut() = register_iceberg_codecs(state.config().clone());
    let catalogs = PinningCatalogList(state.catalog_list().clone());
    SessionStateBuilder::new_from_existing(state)
        .with_catalog_list(Arc::new(catalogs))
        .build()
}

/// Wraps `config`'s Ballista codecs in the Iceberg ones.
///
/// The client registers them with [`register_iceberg`]. In standalone mode the
/// scheduler and executor share the client's config; a separately deployed
/// scheduler and executors need this on their configs.
pub fn register_iceberg_codecs(config: SessionConfig) -> SessionConfig {
    let logical = IcebergLogicalCodec::new(config.ballista_logical_extension_codec());
    let physical = IcebergPhysicalCodec::new(config.ballista_physical_extension_codec());
    config
        .with_ballista_logical_extension_codec(Arc::new(logical))
        .with_ballista_physical_extension_codec(Arc::new(physical))
}
