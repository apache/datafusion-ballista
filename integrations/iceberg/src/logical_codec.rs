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

//! Logical extension codec that serializes Iceberg table providers so that the
//! Ballista scheduler can rebuild them from a logical plan and perform physical
//! planning for Iceberg tables.
//!
//! Each provider decodes back to its own type:
//!
//! - [`IcebergTableProvider`] is catalog-backed: it reads the table's current
//!   state and supports `INSERT`. It travels as its
//!   [`IcebergCatalogConfig`](crate::IcebergCatalogConfig) and table identifier.
//! - [`IcebergStaticTableProvider`] is read-only and fixed to the table version
//!   the client loaded, optionally at an older snapshot. Use it for time travel.
//! - [`IcebergMetadataTableProvider`] serves metadata tables such as
//!   `tbl$snapshots`.
//!
//! The last two travel as a [`TableWire`], so the scheduler reads exactly what
//! the client planned against and needs no catalog to rebuild them.
//!
//! All other logical-plan serialization (extension nodes, file formats, other
//! table providers) is delegated to an inner codec (by default Ballista's
//! [`BallistaLogicalExtensionCodec`]).

use std::sync::Arc;

use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, TableReference};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_iceberg::{
    IcebergMetadataTableProvider, IcebergStaticTableProvider, IcebergTableProvider,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use serde::{Deserialize, Serialize};

use crate::bridge::{
    Frame, TAG_DELEGATED, TableRefWire, TableWire, block_on, encode_blob, get_catalog,
    json_err, metadata_provider, missing_table_config_err, split_frame, static_provider,
};

/// Wire representation of an Iceberg table provider. Carries enough to rebuild
/// either the catalog-backed data provider or a metadata-table provider on a
/// remote node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum IcebergProviderWire {
    /// The catalog-backed [`IcebergTableProvider`].
    Table {
        #[serde(flatten)]
        table_ref: TableRefWire,
    },
    /// A read-only [`IcebergStaticTableProvider`].
    Static {
        table: TableWire,
        /// The snapshot a time-travel view reads; `None` reads the current
        /// snapshot of `table`.
        snapshot_id: Option<i64>,
    },
    /// An [`IcebergMetadataTableProvider`] (e.g. `tbl$snapshots`).
    Metadata {
        table: TableWire,
        /// The metadata table kind, as its lowercase string name.
        metadata_type: String,
    },
}

/// A [`LogicalExtensionCodec`] that understands the Iceberg table providers and
/// delegates everything else to an inner codec.
#[derive(Debug)]
pub struct IcebergLogicalCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl Default for IcebergLogicalCodec {
    fn default() -> Self {
        Self {
            inner: Arc::new(BallistaLogicalExtensionCodec::default()),
        }
    }
}

impl IcebergLogicalCodec {
    /// Creates a codec that delegates non-Iceberg work to `inner`.
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self { inner }
    }
}

impl LogicalExtensionCodec for IcebergLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension, DataFusionError> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &Extension,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        match split_frame(buf, "iceberg logical table-provider")? {
            Frame::Delegated(rest) => self
                .inner
                .try_decode_table_provider(rest, table_ref, schema, ctx),
            Frame::Iceberg(rest) => {
                let wire: IcebergProviderWire =
                    serde_json::from_slice(rest).map_err(json_err)?;
                match wire {
                    IcebergProviderWire::Table { table_ref } => {
                        // Rebuilt with the schema the plan was encoded with, the
                        // client's provider's, rather than the table's current
                        // one: the plan refers to the provider's columns by their
                        // index in that schema. The provider then plans exactly as
                        // the client's does, without loading the table here.
                        let (config, table) = table_ref.into_parts();
                        let provider = IcebergTableProvider::new_with_schema(
                            get_catalog(&config)?,
                            table,
                            schema,
                        )
                        .with_catalog_config(config);
                        Ok(Arc::new(provider))
                    }
                    IcebergProviderWire::Static { table, snapshot_id } => Ok(Arc::new(
                        block_on(static_provider(table.load()?, snapshot_id))?,
                    )),
                    IcebergProviderWire::Metadata {
                        table,
                        metadata_type,
                    } => Ok(Arc::new(metadata_provider(table.load()?, &metadata_type)?)),
                }
            }
        }
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(provider) = node.downcast_ref::<IcebergTableProvider>() {
            let config = provider
                .catalog_config()
                .ok_or_else(|| missing_table_config_err("IcebergTableProvider"))?;
            let wire = IcebergProviderWire::Table {
                table_ref: TableRefWire::new(config, provider.table_ident()),
            };
            return encode_blob(buf, &wire);
        }
        if let Some(provider) = node.downcast_ref::<IcebergStaticTableProvider>() {
            let wire = IcebergProviderWire::Static {
                table: TableWire::new(provider.table())?,
                snapshot_id: provider.snapshot_id(),
            };
            return encode_blob(buf, &wire);
        }
        if let Some(provider) = node.downcast_ref::<IcebergMetadataTableProvider>() {
            let wire = IcebergProviderWire::Metadata {
                table: TableWire::new(provider.table())?,
                metadata_type: provider.metadata_type().as_str().to_string(),
            };
            return encode_blob(buf, &wire);
        }
        buf.push(TAG_DELEGATED);
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>, DataFusionError> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<(), DataFusionError> {
        self.inner.try_encode_file_format(buf, node)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;

    use crate::bridge::TAG_ICEBERG;
    use crate::test_util;

    use super::*;

    fn roundtrip(provider: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        let codec = IcebergLogicalCodec::default();
        let table_ref = TableReference::bare("t");
        let schema = provider.schema();
        let mut buf = Vec::new();
        codec
            .try_encode_table_provider(&table_ref, provider, &mut buf)
            .expect("encode");
        assert_eq!(buf[0], TAG_ICEBERG);
        let ctx = SessionContext::new();
        codec
            .try_decode_table_provider(&buf, &table_ref, schema, &ctx.task_ctx())
            .expect("decode")
    }

    #[tokio::test]
    async fn static_provider_decodes_to_the_planned_table_version() {
        // The scheduler rebuilds the view from the planned metadata file and
        // FileIO alone: there is no catalog here to ask.
        let dir = tempfile::tempdir().unwrap();
        let table = test_util::stored_table(dir.path(), &[1, 2]);

        for snapshot_id in [None, Some(1)] {
            let provider = static_provider(table.clone(), snapshot_id).await.unwrap();
            let schema = provider.schema();
            let decoded = roundtrip(Arc::new(provider));
            let decoded = decoded
                .downcast_ref::<IcebergStaticTableProvider>()
                .unwrap();
            assert_eq!(
                decoded.table().metadata_location(),
                table.metadata_location()
            );
            assert_eq!(decoded.snapshot_id(), snapshot_id);
            assert_eq!(decoded.schema(), schema);
        }
    }

    #[test]
    fn metadata_provider_decodes_to_the_planned_table_version() {
        let dir = tempfile::tempdir().unwrap();
        let table = test_util::stored_table(dir.path(), &[1]);
        let provider = metadata_provider(table.clone(), "snapshots").unwrap();

        let decoded = roundtrip(Arc::new(provider));
        let decoded = decoded
            .downcast_ref::<IcebergMetadataTableProvider>()
            .unwrap();
        assert_eq!(
            decoded.table().metadata_location(),
            table.metadata_location()
        );
        assert_eq!(decoded.metadata_type().as_str(), "snapshots");
    }

    /// Stand-in inner codec for the delegation test. The real Ballista codec can't
    /// serve here — its `try_encode_table_provider` is a permanent stub — so this
    /// mock echoes a marker to prove framing reached it and forwarded the payload.
    #[derive(Debug)]
    struct MarkerInnerCodec;

    impl LogicalExtensionCodec for MarkerInnerCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[LogicalPlan],
            _ctx: &TaskContext,
        ) -> Result<Extension, DataFusionError> {
            unreachable!()
        }

        fn try_encode(
            &self,
            _node: &Extension,
            _buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            unreachable!()
        }

        fn try_encode_table_provider(
            &self,
            _table_ref: &TableReference,
            _node: Arc<dyn TableProvider>,
            buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            buf.extend_from_slice(b"INNER-PROVIDER");
            Ok(())
        }

        fn try_decode_table_provider(
            &self,
            buf: &[u8],
            _table_ref: &TableReference,
            schema: SchemaRef,
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
            assert_eq!(
                buf, b"INNER-PROVIDER",
                "inner codec must get its bytes, tag stripped"
            );
            Ok(Arc::new(EmptyTable::new(schema)))
        }
    }

    #[test]
    fn non_iceberg_table_provider_is_framed_and_delegated_to_inner() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let codec = IcebergLogicalCodec::new(Arc::new(MarkerInnerCodec));
        let table_ref = TableReference::bare("t");
        let provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(schema.clone()));

        let mut buf = Vec::new();
        codec
            .try_encode_table_provider(&table_ref, provider, &mut buf)
            .expect("encode");
        assert_eq!(
            buf[0], TAG_DELEGATED,
            "non-Iceberg provider must be delegated"
        );
        assert_eq!(
            &buf[1..],
            b"INNER-PROVIDER",
            "inner payload follows the tag"
        );

        let ctx = SessionContext::new();
        let decoded = codec
            .try_decode_table_provider(&buf, &table_ref, schema, &ctx.task_ctx())
            .expect("decode");
        assert!(decoded.downcast_ref::<EmptyTable>().is_some());
    }

    fn one_col_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )]))
    }

    #[test]
    fn try_decode_table_provider_rejects_unframed_buffers() {
        // Missing or unrecognized framing must be a hard error, never a misparse
        // of whatever bytes follow.
        let codec = IcebergLogicalCodec::default();
        let ctx = SessionContext::new();
        let decode = |buf: &[u8]| {
            codec.try_decode_table_provider(
                buf,
                &TableReference::bare("t"),
                one_col_schema(),
                &ctx.task_ctx(),
            )
        };

        let err = decode(&[]).unwrap_err();
        assert!(err.to_string().contains("empty"), "{err}");

        let err = decode(&[99]).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown iceberg logical table-provider tag 99"),
            "{err}"
        );
    }
}
