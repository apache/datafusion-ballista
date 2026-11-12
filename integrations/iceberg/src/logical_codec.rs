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

use ballista_core::serde::{
    BallistaLogicalExtensionCodec, ComposedLogicalExtensionCodec,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Result, TableReference, exec_err, not_impl_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use iceberg_datafusion::metadata::IcebergMetadataTableProvider;
use iceberg_datafusion::{IcebergCatalogTableProvider, IcebergStaticTableProvider};
use serde::{Deserialize, Serialize};

use crate::catalog::PinnedTable;
use crate::wire::{self, TableWire};

#[derive(Debug, Serialize, Deserialize)]
struct ProviderWire {
    table: TableWire,
    snapshot_id: Option<i64>,
}

/// A [`LogicalExtensionCodec`] for [`IcebergStaticTableProvider`] and
/// catalog-backed tables pinned by [`register_iceberg`]. It fails on anything
/// else, so compose it after other codecs with
/// [`ComposedLogicalExtensionCodec`], as [`register_iceberg`] does.
///
/// [`register_iceberg`]: crate::register_iceberg
#[derive(Debug, Default)]
pub struct IcebergLogicalCodec;

impl IcebergLogicalCodec {
    /// Composes `codec`, or Ballista's when `None`, with this codec after it.
    pub(crate) fn after(
        codec: Option<Arc<dyn LogicalExtensionCodec>>,
    ) -> Arc<dyn LogicalExtensionCodec> {
        let codec =
            codec.unwrap_or_else(|| Arc::new(BallistaLogicalExtensionCodec::default()));
        Arc::new(ComposedLogicalExtensionCodec::new(vec![
            codec,
            Arc::new(Self),
        ]))
    }
}

impl LogicalExtensionCodec for IcebergLogicalCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        not_impl_err!("ballista-iceberg has no extension nodes")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("ballista-iceberg has no extension nodes")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let ProviderWire { table, snapshot_id } = wire::decode(buf)?;
        Ok(Arc::new(PinnedTable {
            table: table.load()?,
            snapshot_id,
            schema,
            origin: None,
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
                table: TableWire::new(provider.table())?,
                snapshot_id: provider.snapshot_id(),
            }
        } else if let Some(pinned) = node.downcast_ref::<PinnedTable>() {
            ProviderWire {
                table: TableWire::new(&pinned.table)?,
                snapshot_id: pinned.snapshot_id,
            }
        } else if node.is::<IcebergCatalogTableProvider>() {
            return exec_err!(
                "Iceberg table {table_ref} is catalog-backed but wasn't pinned to a \
                 version: build the client's session with ballista_iceberg::register_iceberg"
            );
        } else if node.is::<IcebergMetadataTableProvider>() {
            return not_impl_err!(
                "Iceberg metadata table {table_ref} isn't supported on Ballista yet"
            );
        } else {
            return not_impl_err!("{table_ref} isn't an Iceberg table");
        };
        wire::encode(buf, &payload)
    }
}
