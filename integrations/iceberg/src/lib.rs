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
//! Flight SQL clients, a scheduler readied with `SchedulerConfigExt` (the
//! `scheduler` feature). Executors are readied with `ExecutorProcessConfigExt`
//! (the `executor` feature).
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
//! - No encrypted tables.
//! - Storage credentials travel in plain text.

mod catalog;
#[cfg(feature = "executor")]
mod executor;
mod logical_codec;
mod physical_codec;
#[cfg(feature = "scheduler")]
mod scheduler;
mod wire;

use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use datafusion::execution::{SessionState, SessionStateBuilder};
// Linked for the storage it registers; nothing here calls it.
use iceberg_storage_opendal as _;

use crate::catalog::PinningCatalogList;

#[cfg(feature = "executor")]
pub use crate::executor::ExecutorProcessConfigExt;
pub use crate::logical_codec::IcebergLogicalCodec;
pub use crate::physical_codec::IcebergPhysicalCodec;
#[cfg(feature = "scheduler")]
pub use crate::scheduler::SchedulerConfigExt;

/// Readies a client's session for Iceberg: adds the Iceberg codecs after the
/// session's codecs, and pins each catalog-backed table a query names to the
/// version its catalog serves when the query is planned. A session that is
/// already ready is returned unchanged.
pub fn register_iceberg(mut state: SessionState) -> SessionState {
    if state.catalog_list().is::<PinningCatalogList>() {
        return state;
    }
    let config = state.config_mut();
    let logical =
        IcebergLogicalCodec::after(Some(config.ballista_logical_extension_codec()));
    let physical =
        IcebergPhysicalCodec::after(Some(config.ballista_physical_extension_codec()));
    *config = config
        .clone()
        .with_ballista_logical_extension_codec(logical)
        .with_ballista_physical_extension_codec(physical);
    let catalogs = PinningCatalogList(state.catalog_list().clone());
    // `new_from_existing` would otherwise give the session a new ID.
    let session_id = state.session_id().to_string();
    SessionStateBuilder::new_from_existing(state)
        .with_session_id(session_id)
        .with_catalog_list(Arc::new(catalogs))
        .build()
}
