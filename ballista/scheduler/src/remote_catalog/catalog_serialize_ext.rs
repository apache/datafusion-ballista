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

use ballista_core::serde::protobuf::{CatalogInfo, SchemaInfo, TableInfo};
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::SessionContext;
use futures::stream;
use futures::StreamExt;
use std::sync::Arc;

/// Used to serialize catalog schemas and names to ship to Ballista clients
#[async_trait::async_trait]
pub trait CatalogSerializeExt {
    async fn serialize_catalogs(&self) -> Vec<CatalogInfo>;

    async fn serialize_catalog(&self, name: &str) -> Option<CatalogInfo>;

    async fn serialize_schema(
        &self,
        name: &str,
        catalog: &Arc<dyn CatalogProvider>,
    ) -> Option<SchemaInfo>;
}

#[async_trait::async_trait]
impl CatalogSerializeExt for SessionContext {
    async fn serialize_catalogs(&self) -> Vec<CatalogInfo> {
        let catalog_names = self.state().catalog_list().catalog_names();

        stream::iter(catalog_names.iter())
            .filter_map(|catalog_name| self.serialize_catalog(&catalog_name))
            .collect::<Vec<_>>()
            .await
    }

    async fn serialize_catalog(&self, name: &str) -> Option<CatalogInfo> {
        let Some(catalog) = self.catalog(name) else {
            return None;
        };

        let schemas = stream::iter(catalog.schema_names().iter())
            .filter_map(|schema_name| self.serialize_schema(schema_name, &catalog))
            .collect::<Vec<_>>()
            .await;

        Some(CatalogInfo {
            catalog_name: name.to_string(),
            schemas,
        })
    }

    async fn serialize_schema(
        &self,
        name: &str,
        catalog: &Arc<dyn CatalogProvider>,
    ) -> Option<SchemaInfo> {
        let Some(schema) = catalog.schema(name) else {
            return None;
        };

        let tables = stream::iter(schema.table_names())
            .filter_map(|table_name| async {
                let table_lookup = schema.table(&table_name).await;
                table_lookup.ok().and_then(|maybe_table| {
                    maybe_table.map(|provider| (table_name, provider))
                })
            })
            .map(|(table_name, provider)| TableInfo {
                table_name,
                schema: Some(
                    provider
                        .schema()
                        .as_ref()
                        .try_into()
                        .expect("Must serialize schema"),
                ),
            })
            .collect::<Vec<_>>()
            .await;

        Some(SchemaInfo {
            schema_name: name.to_string(),
            tables,
        })
    }
}
