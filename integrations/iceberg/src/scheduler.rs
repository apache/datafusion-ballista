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

//! Readies a separately deployed scheduler for Iceberg.

use std::sync::Arc;

use ballista_core::utils::default_session_builder;
use ballista_scheduler::config::SchedulerConfig;
use datafusion::catalog::CatalogProvider;

use crate::{IcebergLogicalCodec, IcebergPhysicalCodec, register_iceberg};

/// Readies a [`SchedulerConfig`] for Iceberg.
pub trait SchedulerConfigExt {
    /// Sets the Iceberg codecs, wrapping any codecs already set, and mounts
    /// `catalogs` in each session, readied with [`register_iceberg`]. The
    /// scheduler plans queries from Flight SQL clients itself, so they can
    /// then query those catalogs.
    fn with_iceberg<N: Into<String>>(
        self,
        catalogs: impl IntoIterator<Item = (N, Arc<dyn CatalogProvider>)>,
    ) -> Self;
}

impl SchedulerConfigExt for SchedulerConfig {
    fn with_iceberg<N: Into<String>>(
        mut self,
        catalogs: impl IntoIterator<Item = (N, Arc<dyn CatalogProvider>)>,
    ) -> Self {
        let logical = self.override_logical_codec.take();
        let physical = self.override_physical_codec.take();
        self.override_logical_codec = Some(Arc::new(
            logical.map_or_else(IcebergLogicalCodec::default, IcebergLogicalCodec::new),
        ));
        self.override_physical_codec = Some(Arc::new(
            physical
                .map_or_else(IcebergPhysicalCodec::default, IcebergPhysicalCodec::new),
        ));
        let base = self
            .override_session_builder
            .take()
            .unwrap_or_else(|| Arc::new(default_session_builder));
        let catalogs: Vec<(String, _)> = catalogs
            .into_iter()
            .map(|(name, catalog)| (name.into(), catalog))
            .collect();
        self.with_override_session_builder(Arc::new(move |config| {
            let state = register_iceberg(base(config)?);
            for (name, catalog) in &catalogs {
                state
                    .catalog_list()
                    .register_catalog(name.clone(), catalog.clone());
            }
            Ok(state)
        }))
    }
}
