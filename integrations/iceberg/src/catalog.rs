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

//! Pins catalog-backed tables to one version while a query is planned.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, SchemaProvider, Session, TableProvider,
};
use datafusion::common::{Result, not_impl_err};
use datafusion::datasource::TableType;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::table::Table;
use iceberg_datafusion::read::IcebergDataSource;
use iceberg_datafusion::{IcebergCatalogTableProvider, to_datafusion_error};

/// A session's catalogs, serving each catalog-backed Iceberg table as a
/// [`PinnedTable`].
///
/// DataFusion looks up each table a query names once, while planning it, so
/// every scan of a table in the query reads the same version, and encoding the
/// plan needs no I/O.
#[derive(Debug)]
pub(crate) struct PinningCatalogList(pub(crate) Arc<dyn CatalogProviderList>);

impl CatalogProviderList for PinningCatalogList {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.0.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.0.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let catalog = self.0.catalog(name)?;
        Some(Arc::new(PinningCatalog(catalog)))
    }
}

#[derive(Debug)]
struct PinningCatalog(Arc<dyn CatalogProvider>);

impl CatalogProvider for PinningCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.0.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schema = self.0.schema(name)?;
        Some(Arc::new(PinningSchema(schema)))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.0.register_schema(name, schema)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.0.deregister_schema(name, cascade)
    }
}

#[derive(Debug)]
struct PinningSchema(Arc<dyn SchemaProvider>);

#[async_trait]
impl SchemaProvider for PinningSchema {
    fn owner_name(&self) -> Option<&str> {
        self.0.owner_name()
    }

    fn table_names(&self) -> Vec<String> {
        self.0.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Some(table) = self.0.table(name).await? else {
            return Ok(None);
        };
        let Some(provider) = table.downcast_ref::<IcebergCatalogTableProvider>() else {
            return Ok(Some(table));
        };
        let pinned = provider
            .catalog()
            .load_table(provider.table_ident())
            .await
            .map_err(to_datafusion_error)?;
        Ok(Some(Arc::new(PinnedTable {
            table: pinned,
            snapshot_id: None,
            schema: provider.schema(),
        })))
    }

    async fn table_type(&self, name: &str) -> Result<Option<TableType>> {
        self.0.table_type(name).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.0.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.0.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.0.table_exist(name)
    }
}

/// One version of an Iceberg table, read with the schema the client planned
/// against.
#[derive(Debug)]
pub(crate) struct PinnedTable {
    pub(crate) table: Table,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) schema: SchemaRef,
}

#[async_trait]
impl TableProvider for PinnedTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source = IcebergDataSource::builder(self.table.clone(), self.schema.clone())
            .with_snapshot_id(self.snapshot_id)
            .with_projection(projection)
            .with_filters(filters)
            .with_fetch(limit)
            .build()?;
        Ok(DataSourceExec::from_data_source(source))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("INSERT INTO an Iceberg table isn't supported on Ballista yet")
    }
}
