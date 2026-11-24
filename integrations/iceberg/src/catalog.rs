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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::view::ViewTable;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, SchemaProvider, Session, TableProvider,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{
    Constraints, DataFusionError, Result, internal_datafusion_err, not_impl_err,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{DefaultTableSource, TableType, provider_as_source};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    Expr, LogicalPlan, TableProviderFilterPushDown, TableScan,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_plan::ExecutionPlan;
use futures::future::join_all;
use iceberg::TableIdent;
use iceberg::table::Table;
use iceberg_datafusion::read::IcebergDataSource;
use iceberg_datafusion::{IcebergCatalogTableProvider, to_datafusion_error};

/// A session's catalogs, serving each catalog-backed Iceberg table as a
/// [`PinnedTable`], and each view with its Iceberg tables pinned again, as a
/// [`RepinnedView`].
///
/// DataFusion looks up each table a query names once, while planning it, so
/// every scan of a table in the query reads the same version, and encoding the
/// plan needs no I/O. A view stores the plan it was created with, so its tables
/// are pinned again each time a query names it, and it reads the latest
/// versions, as it would in DataFusion.
///
/// A lookup never fails because a table can't be loaded: the [`PinnedTable`]
/// holds the error instead, and [`RequireLoadedTables`] fails a query with it
/// only when it reads the table, so the table, or a view over it, can still be
/// described, dropped or replaced.
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
        if let Some(provider) = table.downcast_ref::<IcebergCatalogTableProvider>() {
            let table = load(provider).await;
            return Ok(Some(Arc::new(PinnedTable::pin(provider, table))));
        }
        let Some(plan) = table.get_logical_plan() else {
            return Ok(Some(table));
        };
        let Some(plan) = repin(&plan).await? else {
            return Ok(Some(table));
        };
        Ok(Some(Arc::new(RepinnedView {
            original: table,
            repinned: ViewTable::new(plan, None),
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

/// A catalog-backed table: its catalog, by address, and its identifier.
type TableKey = (usize, TableIdent);

fn key(provider: &IcebergCatalogTableProvider) -> TableKey {
    let catalog = Arc::as_ptr(provider.catalog()).cast::<()>() as usize;
    (catalog, provider.table_ident().clone())
}

/// A loaded table, or the error loading it returned, shared by every scan of it.
type Loaded = std::result::Result<Table, Arc<DataFusionError>>;

/// The version of `provider`'s table its catalog serves now, or why it couldn't
/// be loaded.
async fn load(provider: &IcebergCatalogTableProvider) -> Loaded {
    provider
        .catalog()
        .load_table(provider.table_ident())
        .await
        .map_err(|error| Arc::new(to_datafusion_error(error)))
}

/// The pinned table a scan reads, if any.
fn pinned(scan: &TableScan) -> Option<&PinnedTable> {
    let source = scan.source.downcast_ref::<DefaultTableSource>()?;
    source.table_provider.downcast_ref::<PinnedTable>()
}

/// The catalog-backed table a scan's pinned table was pinned from, if any.
fn origin(scan: &TableScan) -> Option<&IcebergCatalogTableProvider> {
    pinned(scan)?.origin.as_ref()
}

/// Fails a query that reads a table its catalog couldn't load with the error
/// loading it returned, while the query is planned, as reading a missing table
/// does in DataFusion. Otherwise the query would fail only when the client
/// encodes it, with that error buried in the encoding error.
///
/// DDL and `DESCRIBE` aren't analyzed, so they still work while a table is gone.
///
/// TODO: remove once the integration's ballista-core keeps the cause of a
/// plan-encoding error (#2911) and `ComposedLogicalExtensionCodec` returns the
/// owning codec's error unchanged, both on DataFusion 56. Encoding the plan then
/// fails with the load error as its cause.
#[derive(Debug)]
pub(crate) struct RequireLoadedTables;

impl AnalyzerRule for RequireLoadedTables {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Some(pinned) = pinned(scan)
            {
                pinned.table()?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "require_loaded_iceberg_tables"
    }
}

/// Returns a view's `plan` with each catalog-backed Iceberg table it reads
/// pinned to the version its catalog serves now, loading each table once, or
/// `None` if it reads none. A table that can't be loaded is pinned to the error,
/// so the plan still knows which table to load next time. DataFusion inlines a
/// view when it plans a scan of it, so the plan of a view over other views
/// already holds their tables, and needs no recursion.
async fn repin(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let mut providers = HashMap::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(origin) = origin(scan)
        {
            providers
                .entry(key(origin))
                .or_insert_with(|| origin.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if providers.is_empty() {
        return Ok(None);
    }
    let loads = providers
        .into_iter()
        .map(|(key, provider)| async move { (key, load(&provider).await) });
    let tables: HashMap<_, _> = join_all(loads).await.into_iter().collect();
    let plan = plan
        .clone()
        .transform_up_with_subqueries(|node| {
            let LogicalPlan::TableScan(mut scan) = node else {
                return Ok(Transformed::no(node));
            };
            let Some(origin) = origin(&scan) else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            // Both traversals visit the same scans, so this is always loaded.
            // If they ever drift apart, reading the scan fails rather than
            // silently reading the version the view was created with.
            let table = tables.get(&key(origin)).cloned().unwrap_or_else(|| {
                Err(Arc::new(internal_datafusion_err!(
                    "Iceberg table {} wasn't loaded to re-pin a view",
                    origin.table_ident()
                )))
            });
            scan.source = provider_as_source(Arc::new(PinnedTable::pin(origin, table)));
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        })?
        .data;
    Ok(Some(plan))
}

/// A view with each catalog-backed Iceberg table it reads pinned again.
///
/// A view is any table DataFusion inlines: one whose provider has a logical
/// plan, such as a [`ViewTable`] from `CREATE VIEW` or a `DataFrame`'s
/// `into_view`. It keeps the original view's type, schema, definition,
/// constraints and column defaults, and replaces only its plan.
#[derive(Debug)]
struct RepinnedView {
    original: Arc<dyn TableProvider>,
    /// The re-pinned plan, held as a [`ViewTable`] to plan a scan of it.
    repinned: ViewTable,
}

#[async_trait]
impl TableProvider for RepinnedView {
    fn schema(&self) -> SchemaRef {
        self.original.schema()
    }

    fn table_type(&self) -> TableType {
        self.original.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.original.get_table_definition()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.original.constraints()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.original.get_column_default(column)
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.repinned.get_logical_plan()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.repinned.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.repinned.scan(state, projection, filters, limit).await
    }
}

/// One version of an Iceberg table, read with the schema the client planned
/// against, or the error loading it from its catalog returned.
///
/// A table that couldn't be loaded still knows the catalog-backed table it was
/// pinned from, so a view over it reads it once its catalog can load it again.
/// [`RequireLoadedTables`] fails a query that reads it with the error, and
/// scanning or encoding it returns the error too.
///
/// Not an [`IcebergStaticTableProvider`]: that derives its schema from the
/// table, which may have evolved since planning, and is built asynchronously,
/// which a codec's decode can't await.
///
/// [`IcebergStaticTableProvider`]: iceberg_datafusion::IcebergStaticTableProvider
#[derive(Debug)]
pub(crate) struct PinnedTable {
    pub(crate) table: Loaded,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) schema: SchemaRef,
    /// The catalog-backed table it was pinned from, to pin it again. Not sent
    /// between nodes.
    pub(crate) origin: Option<IcebergCatalogTableProvider>,
}

impl PinnedTable {
    /// `table`, a version of `provider` loaded from its catalog or the error
    /// loading it returned, read with the provider's schema, which it caches
    /// when it is created.
    fn pin(provider: &IcebergCatalogTableProvider, table: Loaded) -> Self {
        Self {
            table,
            snapshot_id: None,
            schema: provider.schema(),
            origin: Some(provider.clone()),
        }
    }

    /// The pinned version, or the error loading it returned.
    pub(crate) fn table(&self) -> Result<&Table> {
        self.table
            .as_ref()
            .map_err(|error| DataFusionError::Shared(Arc::clone(error)))
    }
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
        let source =
            IcebergDataSource::builder(self.table()?.clone(), self.schema.clone())
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
