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

//! Sends Iceberg tables from the client to the scheduler.

use std::sync::Arc;

use async_trait::async_trait;
use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, TableReference, exec_err, not_impl_err};
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    AggregateUDF, Expr, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF,
    TableProviderFilterPushDown, WindowUDF,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use iceberg::table::Table;
use iceberg_datafusion::read::IcebergDataSource;
use iceberg_datafusion::{
    IcebergCatalogTableProvider, IcebergStaticTableProvider, to_datafusion_error,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Handle, RuntimeFlavor};

use crate::wire::{self, Frame, TAG_DELEGATED, TableWire};

#[derive(Debug, Serialize, Deserialize)]
struct ProviderWire {
    table: TableWire,
    snapshot_id: Option<i64>,
}

/// The scheduler's side of an Iceberg table: one version of it, read with the
/// schema the client planned against.
#[derive(Debug)]
struct PinnedTable {
    table: Table,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
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

/// A [`LogicalExtensionCodec`] for [`IcebergStaticTableProvider`] and
/// [`IcebergCatalogTableProvider`] that delegates everything else to an inner
/// codec.
#[derive(Debug)]
pub struct IcebergLogicalCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl IcebergLogicalCodec {
    /// Creates a codec that delegates everything but Iceberg tables to `inner`.
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self { inner }
    }
}

impl Default for IcebergLogicalCodec {
    fn default() -> Self {
        Self::new(Arc::new(BallistaLogicalExtensionCodec::default()))
    }
}

impl LogicalExtensionCodec for IcebergLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let ProviderWire { table, snapshot_id } = match wire::decode(buf)? {
            Frame::Iceberg(provider) => provider,
            Frame::Delegated(rest) => {
                return self
                    .inner
                    .try_decode_table_provider(rest, table_ref, schema, ctx);
            }
        };
        Ok(Arc::new(PinnedTable {
            table: table.load()?,
            snapshot_id,
            schema,
        }))
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let payload = if let Some(provider) =
            node.downcast_ref::<IcebergStaticTableProvider>()
        {
            ProviderWire {
                table: TableWire::with_metadata(provider.table())?,
                snapshot_id: provider.snapshot_id(),
            }
        } else if let Some(provider) = node.downcast_ref::<IcebergCatalogTableProvider>()
        {
            // Loading blocks this thread, and a REST catalog's pooled
            // connections may need this thread's runtime to answer.
            if let Ok(RuntimeFlavor::CurrentThread) =
                Handle::try_current().map(|handle| handle.runtime_flavor())
            {
                return exec_err!(
                    "Iceberg table {table_ref} is catalog-backed, so sending the query \
                     loads it, which can hang on a single-threaded tokio runtime; run \
                     the client on a multi-threaded runtime, or register the table as \
                     an IcebergStaticTableProvider"
                );
            }
            let catalog = provider.catalog().clone();
            let ident = provider.table_ident().clone();
            let table = wire::block_on(async move { catalog.load_table(&ident).await })?
                .map_err(to_datafusion_error)?;
            ProviderWire {
                table: TableWire::with_metadata(&table)?,
                snapshot_id: None,
            }
        } else {
            buf.push(TAG_DELEGATED);
            return self.inner.try_encode_table_provider(table_ref, node, buf);
        };
        wire::encode(buf, &payload)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<()> {
        self.inner.try_encode_file_format(buf, node)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        self.inner.try_decode_higher_order_function(name, buf)
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_higher_order_function(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}
