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
//! Only the node that plans a query talks to a catalog: the client, or, for
//! Flight SQL clients, a scheduler built with [`iceberg_session_builder`].
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
//! - A view, or a `DataFrame` or prepared statement reused across runs, reads
//!   the version from when it was planned.
//! - Storage credentials travel in plain text.

mod catalog;
mod logical_codec;
mod physical_codec;
mod wire;

use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use datafusion::catalog::CatalogProvider;
use datafusion::common::Result;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionConfig;
// Linked for the storage it registers; nothing here calls it.
use iceberg_storage_opendal as _;

use crate::catalog::PinningCatalogList;

pub use crate::logical_codec::IcebergLogicalCodec;
pub use crate::physical_codec::IcebergPhysicalCodec;

/// Readies a client's session for Iceberg: registers the codecs, and pins
/// each catalog-backed table a query names to the version its catalog serves
/// when the query is planned. A session that is already ready is returned
/// unchanged.
pub fn register_iceberg(mut state: SessionState) -> SessionState {
    if state.catalog_list().is::<PinningCatalogList>() {
        return state;
    }
    let config = state.config_mut();
    let logical = IcebergLogicalCodec::new(config.ballista_logical_extension_codec());
    let physical = IcebergPhysicalCodec::new(config.ballista_physical_extension_codec());
    *config = config
        .clone()
        .with_ballista_logical_extension_codec(Arc::new(logical))
        .with_ballista_physical_extension_codec(Arc::new(physical));
    let catalogs = PinningCatalogList(state.catalog_list().clone());
    SessionStateBuilder::new_from_existing(state)
        .with_catalog_list(Arc::new(catalogs))
        .build()
}

/// A scheduler's session builder, the shape `SchedulerConfig` stores.
pub type SessionBuilder =
    Arc<dyn Fn(SessionConfig) -> Result<SessionState> + Send + Sync>;

/// Wraps a scheduler's session builder, `base`, so its sessions have
/// `catalogs` mounted and are readied with [`register_iceberg`], for queries
/// the scheduler plans itself: those from Flight SQL clients, which hold no
/// catalog of their own. Pass `Arc::new(default_session_builder)` as `base` if
/// the scheduler has no session builder of its own.
///
/// Pass the result to `SchedulerConfig::with_override_session_builder`. The
/// scheduler and executors also need the Iceberg codecs, set as their
/// `override_logical_codec` and `override_physical_codec`.
pub fn iceberg_session_builder(
    base: SessionBuilder,
    catalogs: impl IntoIterator<Item = (String, Arc<dyn CatalogProvider>)>,
) -> SessionBuilder {
    let catalogs: Vec<_> = catalogs.into_iter().collect();
    Arc::new(move |config| {
        let state = register_iceberg(base(config)?);
        for (name, catalog) in &catalogs {
            state
                .catalog_list()
                .register_catalog(name.clone(), catalog.clone());
        }
        Ok(state)
    })
}
