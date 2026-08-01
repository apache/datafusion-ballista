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

//! Logical extension codec that serializes the catalog-backed
//! [`IcebergTableProvider`] (its [`IcebergCatalogConfig`] + table identifier) so
//! that the Ballista scheduler can rebuild the provider from a logical plan and
//! perform physical planning (including `insert_into`) for Iceberg tables.
//!
//! All other logical-plan serialization (extension nodes, file formats, other
//! table providers) is delegated to an inner codec (by default Ballista's
//! [`BallistaLogicalExtensionCodec`]).

use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::sql::TableReference;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use iceberg::TableIdent;
use iceberg::inspect::MetadataTableType;
use iceberg_datafusion::{
    IcebergMetadataTableProvider, IcebergTableProvider, to_datafusion_error,
};
use serde::{Deserialize, Serialize};

use crate::bridge::{
    CatalogConfigWire, Frame, TAG_DELEGATED, block_on, encode_blob, get_catalog,
    load_table, split_frame, to_df_err,
};

/// Wire representation of an Iceberg table provider. Carries enough to rebuild
/// either the catalog-backed data provider or a metadata-table provider on a
/// remote node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum IcebergProviderWire {
    /// The catalog-backed [`IcebergTableProvider`].
    Table {
        catalog: CatalogConfigWire,
        table: TableIdent,
        /// Pinned snapshot for time-travel reads, if any.
        #[serde(default)]
        snapshot_id: Option<i64>,
    },
    /// An [`IcebergMetadataTableProvider`] (e.g. `tbl$snapshots`).
    Metadata {
        catalog: CatalogConfigWire,
        table: TableIdent,
        /// The metadata table kind, as its lowercase string name.
        metadata_type: String,
    },
}

/// A [`LogicalExtensionCodec`] that understands the catalog-backed
/// [`IcebergTableProvider`] and delegates everything else to an inner codec.
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
                    serde_json::from_slice(rest).map_err(to_df_err)?;
                match wire {
                    IcebergProviderWire::Table {
                        catalog,
                        table,
                        snapshot_id,
                    } => {
                        let config = catalog.into();
                        let cat = get_catalog(&config)?;
                        let TableIdent { namespace, name } = table;
                        // Both steps run on the catalog runtime: `with_snapshot_id`
                        // reloads metadata to validate the pin, so it is async
                        // too. It reloads even for `None`, so only call it when
                        // there is a pin — the provider was just built from a
                        // fresh load, and skipping it saves a catalog round-trip
                        // on every unpinned decode.
                        let provider = block_on(async {
                            let unpinned = IcebergTableProvider::try_new_with_config(
                                cat, config, namespace, name,
                            )
                            .await?;
                            match snapshot_id {
                                Some(_) => unpinned.with_snapshot_id(snapshot_id).await,
                                None => Ok(unpinned),
                            }
                        })
                        .map_err(to_datafusion_error)?;
                        Ok(Arc::new(provider))
                    }
                    IcebergProviderWire::Metadata {
                        catalog,
                        table,
                        metadata_type,
                    } => {
                        let config = catalog.into();
                        let (_, table_obj) = load_table(&config, &table)?;
                        let kind = MetadataTableType::try_from(metadata_type.as_str())
                            .map_err(DataFusionError::Internal)?;
                        let provider = IcebergMetadataTableProvider::new(table_obj, kind)
                            .with_catalog_config(Some(config));
                        Ok(Arc::new(provider))
                    }
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
            let config = provider.config().ok_or_else(|| {
                DataFusionError::Internal(
                    "IcebergTableProvider has no IcebergCatalogConfig and cannot be \
                     distributed; register it with \
                     IcebergTableProvider::try_new_with_config (see \
                     iceberg_ballista::register_iceberg_table)."
                        .to_string(),
                )
            })?;
            let wire = IcebergProviderWire::Table {
                catalog: config.into(),
                table: provider.table_ident().clone(),
                snapshot_id: provider.snapshot_id(),
            };
            return encode_blob(buf, &wire);
        }
        if let Some(provider) = node.downcast_ref::<IcebergMetadataTableProvider>() {
            let config = provider.catalog_config().ok_or_else(|| {
                DataFusionError::Internal(
                    "IcebergMetadataTableProvider has no IcebergCatalogConfig and cannot be \
                     distributed; register the catalog with \
                     IcebergCatalogProvider::try_new_with_config so its tables carry it."
                        .to_string(),
                )
            })?;
            let wire = IcebergProviderWire::Metadata {
                catalog: config.into(),
                table: provider.table().identifier().clone(),
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
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;

    use crate::bridge::TAG_ICEBERG;

    use super::*;

    fn sample_catalog() -> CatalogConfigWire {
        CatalogConfigWire {
            r#type: "rest".to_string(),
            name: "rest".to_string(),
            props: BTreeMap::from([(
                "uri".to_string(),
                "http://localhost:8181".to_string(),
            )]),
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
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
            snapshot_id: Some(42),
        };
        assert_eq!(wire, roundtrip(&wire));
    }

    #[test]
    fn metadata_provider_wire_roundtrips() {
        let wire = IcebergProviderWire::Metadata {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
            metadata_type: "snapshots".to_string(),
        };
        assert_eq!(wire, roundtrip(&wire));
    }

    #[test]
    fn table_provider_without_snapshot_id_decodes_to_none() {
        // `snapshot_id` is `#[serde(default)]`: a payload missing the key still decodes.
        let wire = IcebergProviderWire::Table {
            catalog: sample_catalog(),
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
            snapshot_id: Some(99),
        };
        let mut value = serde_json::to_value(&wire).unwrap();
        value["Table"]
            .as_object_mut()
            .unwrap()
            .remove("snapshot_id");

        let decoded: IcebergProviderWire = serde_json::from_value(value).expect("decode");
        assert!(matches!(
            decoded,
            IcebergProviderWire::Table {
                snapshot_id: None,
                ..
            }
        ));
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
