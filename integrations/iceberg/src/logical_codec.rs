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

//! Logical extension codec that serializes Iceberg table providers (their
//! [`IcebergCatalogConfig`](crate::IcebergCatalogConfig) + table identifier) so
//! that the Ballista scheduler can rebuild them from a logical plan and perform
//! physical planning for Iceberg tables.
//!
//! Each provider decodes back to its own type:
//!
//! - [`IcebergTableProvider`] is catalog-backed: it reads the table's current
//!   state and supports `INSERT`.
//! - [`IcebergStaticTableProvider`] is read-only and pinned to a snapshot, which
//!   is fixed at encode time so the scheduler reads exactly what the client
//!   planned against. Use it for time travel.
//! - [`IcebergMetadataTableProvider`] serves metadata tables such as
//!   `tbl$snapshots`.
//!
//! All other logical-plan serialization (extension nodes, file formats, other
//! table providers) is delegated to an inner codec (by default Ballista's
//! [`BallistaLogicalExtensionCodec`]).

use std::sync::Arc;

use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, TableReference};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_iceberg::{
    IcebergMetadataTableProvider, IcebergStaticTableProvider, IcebergTableProvider,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use iceberg::TableIdent;
use serde::{Deserialize, Serialize};

use crate::bridge::{
    Frame, TAG_DELEGATED, TableRefWire, block_on, build_metadata_provider, encode_blob,
    get_catalog, json_err, load_table_pinned, missing_catalog_config_err,
    missing_static_config_err, missing_table_config_err, split_frame, static_provider,
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
        #[serde(flatten)]
        table_ref: TableRefWire,
        snapshot: ViewSnapshot,
    },
    /// An [`IcebergMetadataTableProvider`] (e.g. `tbl$snapshots`).
    Metadata {
        #[serde(flatten)]
        table_ref: TableRefWire,
        /// The metadata table kind, as its lowercase string name.
        metadata_type: String,
    },
}

/// What a read-only view reads, fixed when the view is encoded.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum ViewSnapshot {
    Snapshot(i64),
    /// The table had no snapshot, so the view is empty. Reloading the table
    /// instead could read rows committed after the view was planned.
    Empty,
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
                        let (config, table) = table_ref.into_parts();
                        let cat = get_catalog(&config)?;
                        let TableIdent { namespace, name } = table;
                        let provider =
                            block_on(IcebergTableProvider::try_new_with_config(
                                cat, config, namespace, name,
                            ))?;
                        Ok(Arc::new(provider))
                    }
                    IcebergProviderWire::Static {
                        snapshot: ViewSnapshot::Empty,
                        ..
                    } => Ok(Arc::new(EmptyTable::new(schema))),
                    IcebergProviderWire::Static {
                        table_ref,
                        snapshot: ViewSnapshot::Snapshot(id),
                    } => {
                        let (config, table) = table_ref.into_parts();
                        let table = load_table_pinned(&config, &table, id)?;
                        let provider =
                            block_on(static_provider(table, Some(id), config))?;
                        Ok(Arc::new(provider))
                    }
                    IcebergProviderWire::Metadata {
                        table_ref,
                        metadata_type,
                    } => Ok(Arc::new(build_metadata_provider(
                        table_ref,
                        &metadata_type,
                    )?)),
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
            let config = provider
                .catalog_config()
                .ok_or_else(|| missing_static_config_err("IcebergStaticTableProvider"))?;
            // An unpinned static provider reads the table as it was loaded, so
            // pin that snapshot. Otherwise the scheduler, which reloads the
            // table, could read a newer one.
            let snapshot = match provider
                .snapshot_id()
                .or_else(|| provider.table().metadata().current_snapshot_id())
            {
                Some(id) => ViewSnapshot::Snapshot(id),
                None => ViewSnapshot::Empty,
            };
            let wire = IcebergProviderWire::Static {
                table_ref: TableRefWire::new(config, provider.table_ident()),
                snapshot,
            };
            return encode_blob(buf, &wire);
        }
        if let Some(provider) = node.downcast_ref::<IcebergMetadataTableProvider>() {
            let config = provider.catalog_config().ok_or_else(|| {
                missing_catalog_config_err("IcebergMetadataTableProvider")
            })?;
            let wire = IcebergProviderWire::Metadata {
                table_ref: TableRefWire::new(config, provider.table().identifier()),
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
    use std::collections::BTreeMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;

    use crate::bridge::{CatalogConfigWire, TAG_ICEBERG};
    use crate::test_util;

    use super::*;

    fn sample_table_ref() -> TableRefWire {
        TableRefWire {
            catalog: CatalogConfigWire {
                r#type: "rest".to_string(),
                name: "rest".to_string(),
                props: BTreeMap::from([(
                    "uri".to_string(),
                    "http://localhost:8181".to_string(),
                )]),
            },
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
        }
    }

    fn roundtrip(wire: &IcebergProviderWire) -> IcebergProviderWire {
        let mut buf = Vec::new();
        encode_blob(&mut buf, wire).expect("encode");
        assert_eq!(buf[0], TAG_ICEBERG, "blob must carry the iceberg tag");
        serde_json::from_slice(&buf[1..]).expect("decode")
    }

    #[test]
    fn table_provider_wire_roundtrips() {
        let wire = IcebergProviderWire::Table {
            table_ref: sample_table_ref(),
        };
        assert_eq!(wire, roundtrip(&wire));
    }

    #[test]
    fn static_provider_wire_roundtrips() {
        let wire = IcebergProviderWire::Static {
            table_ref: sample_table_ref(),
            snapshot: ViewSnapshot::Snapshot(42),
        };
        assert_eq!(wire, roundtrip(&wire));
    }

    #[test]
    fn metadata_provider_wire_roundtrips() {
        let wire = IcebergProviderWire::Metadata {
            table_ref: sample_table_ref(),
            metadata_type: "snapshots".to_string(),
        };
        assert_eq!(wire, roundtrip(&wire));
    }

    #[test]
    fn table_ref_flattens_to_inline_catalog_and_table_keys() {
        // Wire compat: `TableRefWire` must serialize as inline `catalog` and
        // `table` keys, exactly as when the variants spelled the two fields
        // out — never nested under a `table_ref` object.
        let wire = IcebergProviderWire::Static {
            table_ref: sample_table_ref(),
            snapshot: ViewSnapshot::Snapshot(42),
        };
        let value = serde_json::to_value(&wire).unwrap();
        let obj = value["Static"].as_object().unwrap();
        assert!(obj.contains_key("catalog"), "{value}");
        assert!(obj.contains_key("table"), "{value}");
        assert!(!obj.contains_key("table_ref"), "{value}");
    }

    /// Encodes `provider` with the Iceberg codec and returns its wire form.
    fn encode_static(provider: IcebergStaticTableProvider) -> IcebergProviderWire {
        let mut buf = Vec::new();
        IcebergLogicalCodec::default()
            .try_encode_table_provider(
                &TableReference::bare("t"),
                Arc::new(provider),
                &mut buf,
            )
            .expect("encode");
        assert_eq!(buf[0], TAG_ICEBERG);
        serde_json::from_slice(&buf[1..]).expect("decode wire")
    }

    fn encoded_snapshot(provider: IcebergStaticTableProvider) -> ViewSnapshot {
        match encode_static(provider) {
            IcebergProviderWire::Static { snapshot, .. } => snapshot,
            other => panic!("expected a static provider, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn static_provider_is_pinned_when_encoded() {
        let config = test_util::catalog_config();

        // Unpinned: pinned to the snapshot current when it was loaded, since
        // the scheduler reloads the table and could otherwise see a newer one.
        let unpinned =
            IcebergStaticTableProvider::try_new_from_table(test_util::table(&[1, 2]))
                .await
                .unwrap()
                .with_catalog_config(config.clone());
        assert_eq!(encoded_snapshot(unpinned), ViewSnapshot::Snapshot(2));

        // Pinned: keeps its own snapshot, not the current one.
        let pinned = IcebergStaticTableProvider::try_new_from_table_snapshot(
            test_util::table(&[1, 2]),
            1,
        )
        .await
        .unwrap()
        .with_catalog_config(config.clone());
        assert_eq!(encoded_snapshot(pinned), ViewSnapshot::Snapshot(1));

        // No snapshot to pin: the view is empty.
        let empty = IcebergStaticTableProvider::try_new_from_table(test_util::table(&[]))
            .await
            .unwrap()
            .with_catalog_config(config);
        assert_eq!(encoded_snapshot(empty), ViewSnapshot::Empty);
    }

    #[tokio::test]
    async fn empty_static_provider_decodes_to_an_empty_table() {
        let provider =
            IcebergStaticTableProvider::try_new_from_table(test_util::table(&[]))
                .await
                .unwrap()
                .with_catalog_config(test_util::catalog_config());
        let schema = provider.schema();
        let codec = IcebergLogicalCodec::default();
        let table_ref = TableReference::bare("t");

        let mut buf = Vec::new();
        codec
            .try_encode_table_provider(&table_ref, Arc::new(provider), &mut buf)
            .expect("encode");
        // Decoding needs no catalog: the view is known to be empty.
        let ctx = SessionContext::new();
        let decoded = codec
            .try_decode_table_provider(&buf, &table_ref, schema.clone(), &ctx.task_ctx())
            .expect("decode");

        assert!(decoded.downcast_ref::<EmptyTable>().is_some());
        assert_eq!(decoded.schema(), schema);
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
