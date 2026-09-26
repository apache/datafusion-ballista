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

//! Physical extension codec for the Iceberg execution plan nodes.
//!
//! Encodes/decodes [`IcebergTableScan`], [`IcebergWriteExec`],
//! [`IcebergCommitExec`] and [`IcebergMetadataScan`] so Ballista can ship them
//! to remote executors. Any node that is not an Iceberg node is delegated to an
//! inner codec (by default Ballista's own [`BallistaPhysicalExtensionCodec`]),
//! so shuffle and other Ballista plan nodes keep working.
//!
//! Scans, writes and metadata scans carry their table as a [`TableWire`], so an
//! executor rebuilds exactly the version the scheduler planned against without
//! contacting the catalog. Only a commit carries the catalog config.

use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_iceberg::physical_plan::{
    IcebergCommitExec, IcebergMetadataScan, IcebergTableScan, IcebergWriteExec,
    PartitionExpr,
};
use datafusion_iceberg::to_datafusion_error;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::expr::Predicate;
use iceberg::spec::{PartitionSpec, Schema as IcebergSchema};
use iceberg::table::Table;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bridge::{
    Frame, TAG_DELEGATED, TableRefWire, TableWire, encode_blob, json_err, load_table_at,
    metadata_provider, missing_table_config_err, split_frame,
};

/// Wire representation of an Iceberg physical plan node.
// `Predicate` is not `Eq` (it can hold float literals), so this derives only
// `PartialEq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum IcebergPhysicalNode {
    Scan {
        table: TableWire,
        /// The snapshot a time-travel scan reads; `None` reads the current
        /// snapshot of the planned table version.
        snapshot_id: Option<i64>,
        /// The scan's output schema, as planned. The executor reads these
        /// columns, by name, and produces exactly this schema, even if the
        /// table's schema differs from the one the scan was planned with.
        schema: Schema,
        limit: Option<usize>,
        /// Pushed-down filter, restored on the remote node so Iceberg file
        /// pruning is preserved (DataFusion still re-applies it above the scan).
        #[serde(default)]
        predicates: Option<Predicate>,
    },
    Write {
        table: TableWire,
    },
    Commit {
        #[serde(flatten)]
        table_ref: TableRefWire,
        /// The metadata file the write was planned against, which the commit
        /// rebuilds the table from (see [`load_table_at`]).
        metadata_location: String,
        /// Must be the planned node's, so that every attempt at running the
        /// commit (Ballista retries failed tasks) commits at most once; see
        /// [`IcebergCommitExec::commit_id`].
        commit_id: Uuid,
    },
    Metadata {
        table: TableWire,
        /// The metadata table kind, as its lowercase string name.
        metadata_type: String,
    },
}

/// Wire representation of an [`IcebergDataFusion`](datafusion_iceberg) partition
/// expression. The live `PartitionValueCalculator` it wraps is not serializable,
/// but it can be rebuilt on the far node from the (self-contained) partition spec
/// and table schema, so those are all that travels on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartitionExprWire {
    partition_spec: PartitionSpec,
    schema: IcebergSchema,
}

/// A [`PhysicalExtensionCodec`] that understands the Iceberg plan nodes and
/// delegates everything else to an inner codec.
#[derive(Debug)]
pub struct IcebergPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
}

impl Default for IcebergPhysicalCodec {
    fn default() -> Self {
        Self {
            inner: Arc::new(BallistaPhysicalExtensionCodec::default()),
        }
    }
}

impl IcebergPhysicalCodec {
    /// Creates a codec that delegates non-Iceberg nodes to `inner`.
    pub fn new(inner: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { inner }
    }
}

impl PhysicalExtensionCodec for IcebergPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let rest = match split_frame(buf, "iceberg physical codec")? {
            Frame::Delegated(rest) => {
                return self.inner.try_decode(rest, inputs, ctx, proto_converter);
            }
            Frame::Iceberg(rest) => rest,
        };
        let node: IcebergPhysicalNode = serde_json::from_slice(rest).map_err(json_err)?;

        match node {
            IcebergPhysicalNode::Scan {
                table,
                snapshot_id,
                schema,
                limit,
                predicates,
            } => {
                // Every column of the planned output schema, in order.
                let projection = (0..schema.fields().len()).collect();
                let scan = IcebergTableScan::new_with_predicate(
                    table.load()?,
                    snapshot_id,
                    Arc::new(schema),
                    Some(&projection),
                    predicates,
                    limit,
                )?;
                Ok(Arc::new(scan))
            }
            IcebergPhysicalNode::Write { table } => {
                let input = single_input(inputs, "IcebergWriteExec")?;
                Ok(Arc::new(IcebergWriteExec::new(table.load()?, input)))
            }
            IcebergPhysicalNode::Commit {
                table_ref,
                metadata_location,
                commit_id,
            } => {
                let (config, table) = table_ref.into_parts();
                let (cat, table_obj) =
                    load_table_at(&config, &table, &metadata_location)?;
                let arrow_schema = current_arrow_schema(&table_obj)?;
                let input = single_input(inputs, "IcebergCommitExec")?;
                let commit = IcebergCommitExec::new(table_obj, cat, input, arrow_schema)
                    .with_catalog_config(config)
                    .with_commit_id(commit_id);
                Ok(Arc::new(commit))
            }
            IcebergPhysicalNode::Metadata {
                table,
                metadata_type,
            } => Ok(Arc::new(IcebergMetadataScan::new(metadata_provider(
                table.load()?,
                &metadata_type,
            )?))),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<(), DataFusionError> {
        if let Some(scan) = node.downcast_ref::<IcebergTableScan>() {
            let node = IcebergPhysicalNode::Scan {
                table: TableWire::new(scan.table())?,
                snapshot_id: scan.snapshot_id(),
                schema: scan.schema().as_ref().clone(),
                limit: scan.limit(),
                predicates: scan.predicates().cloned(),
            };
            return encode_blob(buf, &node);
        }

        if let Some(write) = node.downcast_ref::<IcebergWriteExec>() {
            let node = IcebergPhysicalNode::Write {
                table: TableWire::new(write.table())?,
            };
            return encode_blob(buf, &node);
        }

        if let Some(commit) = node.downcast_ref::<IcebergCommitExec>() {
            let config = commit
                .catalog_config()
                .ok_or_else(|| missing_table_config_err("IcebergCommitExec"))?;
            let node = IcebergPhysicalNode::Commit {
                table_ref: TableRefWire::new(config, commit.table().identifier()),
                metadata_location: commit
                    .table()
                    .metadata_location_result()
                    .map_err(to_datafusion_error)?
                    .to_string(),
                commit_id: commit.commit_id(),
            };
            return encode_blob(buf, &node);
        }

        if let Some(meta) = node.downcast_ref::<IcebergMetadataScan>() {
            let provider = meta.provider();
            let node = IcebergPhysicalNode::Metadata {
                table: TableWire::new(provider.table())?,
                metadata_type: provider.metadata_type().as_str().to_string(),
            };
            return encode_blob(buf, &node);
        }

        buf.push(TAG_DELEGATED);
        self.inner.try_encode(node, buf, proto_converter)
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<(), DataFusionError> {
        // The partition-value expression a partitioned write injects holds a
        // live calculator; serialize the spec + schema it can be rebuilt from.
        if let Some(expr) = node.downcast_ref::<PartitionExpr>() {
            let wire = PartitionExprWire {
                partition_spec: expr.partition_spec().as_ref().clone(),
                schema: expr.table_schema().as_ref().clone(),
            };
            return encode_blob(buf, &wire);
        }
        buf.push(TAG_DELEGATED);
        self.inner.try_encode_expr(node, buf, ctx)
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        match split_frame(buf, "iceberg physical expr")? {
            Frame::Delegated(rest) => self.inner.try_decode_expr(rest, inputs, ctx),
            Frame::Iceberg(rest) => {
                let wire: PartitionExprWire =
                    serde_json::from_slice(rest).map_err(json_err)?;
                let expr = PartitionExpr::try_new(
                    Arc::new(wire.partition_spec),
                    Arc::new(wire.schema),
                )?;
                Ok(Arc::new(expr))
            }
        }
    }
}

/// The Arrow schema of `table`'s current schema.
fn current_arrow_schema(table: &Table) -> Result<SchemaRef, DataFusionError> {
    Ok(Arc::new(
        schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(to_datafusion_error)?,
    ))
}

fn single_input(
    inputs: &[Arc<dyn ExecutionPlan>],
    node: &str,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if inputs.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "{node} expects exactly one input, got {}",
            inputs.len()
        )));
    }
    Ok(inputs[0].clone())
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::common::collect;
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;

    use crate::bridge::TAG_ICEBERG;
    use crate::test_util;

    use super::*;

    fn sample_table() -> TableWire {
        TableWire::new(&test_util::table(&[1])).unwrap()
    }

    fn roundtrip(node: &IcebergPhysicalNode) -> IcebergPhysicalNode {
        let mut buf = Vec::new();
        encode_blob(&mut buf, node).expect("encode");
        assert_eq!(buf[0], TAG_ICEBERG, "blob must carry the iceberg tag");
        serde_json::from_slice(&buf[1..]).expect("decode")
    }

    #[test]
    fn scan_node_with_predicate_roundtrips() {
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        let node = IcebergPhysicalNode::Scan {
            table: sample_table(),
            snapshot_id: Some(1),
            schema: arrow_schema(),
            limit: None,
            predicates: Some(Reference::new("a").less_than(Datum::long(5))),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn scan_node_with_compound_predicate_roundtrips() {
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        // Exercise AND / OR / IN / IS NULL together — Predicate is the trickiest type.
        let predicate = Reference::new("a")
            .less_than(Datum::long(5))
            .and(Reference::new("b").is_null())
            .or(Reference::new("c").is_in([Datum::string("x"), Datum::string("y")]));
        let node = IcebergPhysicalNode::Scan {
            table: sample_table(),
            snapshot_id: Some(1),
            schema: arrow_schema(),
            limit: None,
            predicates: Some(predicate),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn scan_node_without_predicates_field_decodes_to_none() {
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        // `predicates` is `#[serde(default)]`: a payload missing the key still decodes.
        let node = IcebergPhysicalNode::Scan {
            table: sample_table(),
            snapshot_id: Some(7),
            schema: arrow_schema(),
            limit: None,
            predicates: Some(Reference::new("a").less_than(Datum::long(5))),
        };
        let mut value = serde_json::to_value(&node).unwrap();
        value["Scan"].as_object_mut().unwrap().remove("predicates");

        let decoded: IcebergPhysicalNode = serde_json::from_value(value).expect("decode");
        assert!(matches!(
            decoded,
            IcebergPhysicalNode::Scan {
                predicates: None,
                snapshot_id: Some(7),
                ..
            }
        ));
    }

    #[test]
    fn partition_expr_roundtrips_through_the_codec() {
        use datafusion::arrow::datatypes::Schema as ArrowSchema;
        use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecode;
        use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncode;
        use datafusion_proto::protobuf::PhysicalExprNode;
        use iceberg::spec::{NestedField, PrimitiveType, Transform, Type};

        /// `PartitionExpr` has no child expressions, so neither is called.
        struct Unused;
        impl PhysicalExprEncode for Unused {
            fn encode(
                &self,
                _expr: &Arc<dyn PhysicalExpr>,
            ) -> Result<PhysicalExprNode, DataFusionError> {
                unreachable!()
            }
        }
        impl PhysicalExprDecode for Unused {
            fn decode(
                &self,
                _node: &PhysicalExprNode,
                _schema: &ArrowSchema,
            ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
                unreachable!()
            }
        }

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("region", "region", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let expr: Arc<dyn PhysicalExpr> = Arc::new(
            PartitionExpr::try_new(Arc::new(partition_spec), Arc::new(schema)).unwrap(),
        );

        // The live partition-value calculator is rebuilt from the spec and
        // schema on the far side.
        let codec = IcebergPhysicalCodec::default();
        let mut buf = Vec::new();
        codec
            .try_encode_expr(&expr, &mut buf, &PhysicalExprEncodeCtx::new(&Unused))
            .expect("encode");
        let arrow_schema = ArrowSchema::empty();
        let decoded = codec
            .try_decode_expr(
                &buf,
                &[],
                &PhysicalExprDecodeCtx::new(&arrow_schema, &Unused),
            )
            .expect("decode");

        assert_eq!(
            decoded.downcast_ref::<PartitionExpr>(),
            expr.downcast_ref::<PartitionExpr>()
        );
    }

    #[test]
    fn non_iceberg_node_roundtrips_through_inner_codec() {
        use ballista_core::execution_plans::ShuffleWriterExec;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::prelude::SessionContext;

        // A Ballista shuffle node is not an Iceberg node, so the codec must
        // frame it with TAG_DELEGATED and hand it to the inner Ballista codec —
        // and decode must route it back there, reconstructing the same node.
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let shuffle = ShuffleWriterExec::try_new(
            "job-1".to_string().into(),
            7,
            input.clone(),
            "/tmp/work".to_string(),
        )
        .expect("build shuffle writer");

        let codec = IcebergPhysicalCodec::default();
        let mut buf = Vec::new();
        codec
            .try_encode(
                Arc::new(shuffle),
                &mut buf,
                &DefaultPhysicalProtoConverter {},
            )
            .expect("encode delegated node");
        assert_eq!(buf[0], TAG_DELEGATED, "non-Iceberg node must be delegated");

        let ctx = SessionContext::new();
        let decoded = codec
            .try_decode(
                &buf,
                &[input],
                &ctx.task_ctx(),
                &DefaultPhysicalProtoConverter {},
            )
            .expect("decode delegated node");
        let decoded = decoded
            .downcast_ref::<ShuffleWriterExec>()
            .expect("decoded plan should be a ShuffleWriterExec");
        assert_eq!(decoded.job_id().as_str(), "job-1");
        assert_eq!(decoded.stage_id(), 7);
    }

    fn arrow_schema() -> Schema {
        use datafusion::arrow::datatypes::{DataType, Field};
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ])
    }

    /// The Arrow schema a scan of `table` at `snapshot_id` was planned with, resolved
    /// as the providers resolve it: the snapshot's own schema for a time-travel read,
    /// the table's current schema otherwise.
    fn scan_schema(
        table: &Table,
        snapshot_id: Option<i64>,
    ) -> Result<SchemaRef, DataFusionError> {
        let Some(id) = snapshot_id else {
            return current_arrow_schema(table);
        };
        let snapshot = table.metadata().snapshot_by_id(id).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "snapshot {id} not found in table {}",
                table.identifier()
            ))
        })?;
        let schema = snapshot
            .schema(table.metadata())
            .map_err(to_datafusion_error)?;
        Ok(Arc::new(
            schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?,
        ))
    }

    fn roundtrip_plan(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        use datafusion::prelude::SessionContext;

        let codec = IcebergPhysicalCodec::default();
        let mut buf = Vec::new();
        codec
            .try_encode(plan, &mut buf, &DefaultPhysicalProtoConverter {})
            .expect("encode");
        let ctx = SessionContext::new();
        codec
            .try_decode(
                &buf,
                &[],
                &ctx.task_ctx(),
                &DefaultPhysicalProtoConverter {},
            )
            .expect("decode")
    }

    /// A scan of `table` at `snapshot_id`, projecting column `name` only.
    fn name_scan(table: Table, snapshot_id: Option<i64>) -> Arc<dyn ExecutionPlan> {
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        let schema = scan_schema(&table, snapshot_id).unwrap();
        Arc::new(
            IcebergTableScan::new_with_predicate(
                table,
                snapshot_id,
                schema,
                Some(&vec![1]),
                Some(Reference::new("id").less_than(Datum::int(5))),
                Some(10),
            )
            .unwrap(),
        )
    }

    #[test]
    fn scan_decodes_to_the_planned_table_version() {
        // The executor rebuilds the table from the planned metadata file and
        // FileIO alone: there is no catalog here to ask.
        let dir = tempfile::tempdir().unwrap();
        let table = test_util::stored_table(dir.path(), &[1, 2]);

        for snapshot_id in [None, Some(1)] {
            let original = name_scan(table.clone(), snapshot_id);
            let decoded = roundtrip_plan(Arc::clone(&original));
            let original = original.downcast_ref::<IcebergTableScan>().unwrap();
            let decoded = decoded.downcast_ref::<IcebergTableScan>().unwrap();

            assert_eq!(
                decoded.table().metadata_location(),
                table.metadata_location()
            );
            assert_eq!(decoded.snapshot_id(), snapshot_id);
            assert_eq!(decoded.projection(), original.projection());
            assert_eq!(decoded.predicates(), original.predicates());
            assert_eq!(decoded.limit(), original.limit());
            assert_eq!(decoded.schema(), original.schema());
        }
    }

    /// The executor produces the schema the scan was planned with, not one
    /// derived again from the table. A catalog-backed provider plans with the
    /// schema it was created with, which lags the table's once its schema
    /// changes: here the table has gained `name` since.
    #[test]
    fn scan_decodes_to_the_planned_schema() {
        use datafusion::arrow::datatypes::{DataType, Field};

        let dir = tempfile::tempdir().unwrap();
        let table = test_util::stored_table(dir.path(), &[1]);
        let planned =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let original: Arc<dyn ExecutionPlan> = Arc::new(
            IcebergTableScan::new_with_predicate(table, None, planned, None, None, None)
                .unwrap(),
        );

        let decoded = roundtrip_plan(Arc::clone(&original));
        assert_eq!(decoded.schema(), original.schema());
        let decoded = decoded.downcast_ref::<IcebergTableScan>().unwrap();
        assert_eq!(decoded.projection(), Some(&["id".to_string()][..]));
    }

    #[tokio::test]
    async fn scan_of_an_empty_table_decodes_and_reads_nothing() {
        use datafusion::prelude::SessionContext;

        let dir = tempfile::tempdir().unwrap();
        let decoded =
            roundtrip_plan(name_scan(test_util::stored_table(dir.path(), &[]), None));

        let ctx = SessionContext::new();
        let batches = collect(decoded.execute(0, ctx.task_ctx()).unwrap())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        let fields: Vec<_> = decoded
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, ["name"], "keeps the scan's projection");
    }

    #[test]
    fn metadata_scan_decodes_to_the_planned_table_version() {
        use datafusion_iceberg::IcebergMetadataTableProvider;
        use iceberg::inspect::MetadataTableType;

        let dir = tempfile::tempdir().unwrap();
        let table = test_util::stored_table(dir.path(), &[1, 2]);
        let scan = IcebergMetadataScan::new(IcebergMetadataTableProvider::new(
            table.clone(),
            MetadataTableType::History,
        ));

        let decoded = roundtrip_plan(Arc::new(scan));
        let provider = decoded
            .downcast_ref::<IcebergMetadataScan>()
            .unwrap()
            .provider();
        assert_eq!(
            provider.table().metadata_location(),
            table.metadata_location()
        );
        assert!(matches!(
            provider.metadata_type(),
            MetadataTableType::History
        ));
    }

    #[tokio::test]
    async fn write_and_commit_record_the_planned_metadata_file_and_commit_id() {
        use std::collections::HashMap;

        use iceberg::CatalogBuilder;
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};

        // Executors rebuild the table from this file, so a write runs against
        // the version it was planned against rather than whatever the catalog
        // serves when each task decodes.
        // Only held by the commit node; never contacted.
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "/test".to_string(),
                )]),
            )
            .await
            .unwrap();
        let table = test_util::table(&[1]);
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                current_arrow_schema(&table).unwrap(),
            ));
        let write: Arc<dyn ExecutionPlan> =
            Arc::new(IcebergWriteExec::new(table.clone(), Arc::clone(&input)));
        let commit: Arc<dyn ExecutionPlan> = Arc::new(
            IcebergCommitExec::new(
                table.clone(),
                Arc::new(catalog),
                input,
                current_arrow_schema(&table).unwrap(),
            )
            .with_catalog_config(test_util::catalog_config()),
        );
        let planned_commit_id = commit
            .downcast_ref::<IcebergCommitExec>()
            .unwrap()
            .commit_id();

        for node in [write, commit] {
            let mut buf = Vec::new();
            IcebergPhysicalCodec::default()
                .try_encode(node, &mut buf, &DefaultPhysicalProtoConverter {})
                .expect("encode");
            let location = match serde_json::from_slice(&buf[1..]).expect("decode wire") {
                IcebergPhysicalNode::Write { table } => table.metadata_location,
                IcebergPhysicalNode::Commit {
                    metadata_location,
                    commit_id,
                    ..
                } => {
                    // Every attempt at the commit must share the planned id.
                    assert_eq!(commit_id, planned_commit_id);
                    metadata_location
                }
                other => panic!("expected a write or commit, got {other:?}"),
            };
            assert_eq!(location, "/test/tbl/metadata.json");
        }
    }

    #[test]
    fn try_decode_rejects_unframed_buffers() {
        use datafusion::prelude::SessionContext;

        // Missing or unrecognized framing must be a hard error, never a misparse
        // of whatever bytes follow.
        let ctx = SessionContext::new();
        let codec = IcebergPhysicalCodec::default();

        let converter = DefaultPhysicalProtoConverter {};
        let decode = |buf: &[u8]| {
            codec
                .try_decode(buf, &[], &ctx.task_ctx(), &converter)
                .unwrap_err()
        };

        let err = decode(&[]);
        assert!(err.to_string().contains("empty"), "{err}");

        let err = decode(&[99]);
        assert!(
            err.to_string()
                .contains("unknown iceberg physical codec tag 99"),
            "{err}"
        );
    }

    #[test]
    fn try_decode_expr_rejects_unframed_buffers() {
        use datafusion::arrow::datatypes::Schema as ArrowSchema;
        use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecode;
        use datafusion_proto::protobuf::PhysicalExprNode;

        /// Framing is rejected before any nested expression is decoded.
        struct UnusedDecoder;

        impl PhysicalExprDecode for UnusedDecoder {
            fn decode(
                &self,
                _node: &PhysicalExprNode,
                _schema: &ArrowSchema,
            ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
                unreachable!()
            }
        }

        // The expr path has its own tag dispatch, so it needs its own check.
        let codec = IcebergPhysicalCodec::default();
        let schema = ArrowSchema::empty();
        let ctx = PhysicalExprDecodeCtx::new(&schema, &UnusedDecoder);

        let err = codec.try_decode_expr(&[], &[], &ctx).unwrap_err();
        assert!(err.to_string().contains("empty"), "{err}");

        let err = codec.try_decode_expr(&[99], &[], &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown iceberg physical expr tag 99"),
            "{err}"
        );
    }

    /// A named multi-thread runtime, standing in for an executor's.
    fn named_runtime(name: &str) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(name)
            .enable_all()
            .build()
            .unwrap()
    }

    /// A scan of all of `table`'s rows, encoded by the codec.
    fn encoded_full_scan(table: Table) -> Vec<u8> {
        let schema = scan_schema(&table, None).unwrap();
        let scan =
            IcebergTableScan::new_with_predicate(table, None, schema, None, None, None)
                .unwrap();
        let mut buf = Vec::new();
        IcebergPhysicalCodec::default()
            .try_encode(Arc::new(scan), &mut buf, &DefaultPhysicalProtoConverter {})
            .unwrap();
        buf
    }

    /// Decodes `buf` on `runtime`, as an executor does, runs it there and
    /// returns the number of rows.
    fn decode_and_run(runtime: &tokio::runtime::Runtime, buf: &[u8]) -> usize {
        use datafusion::prelude::SessionContext;

        runtime.block_on(async {
            let ctx = SessionContext::new();
            let plan = IcebergPhysicalCodec::default()
                .try_decode(buf, &[], &ctx.task_ctx(), &DefaultPhysicalProtoConverter {})
                .unwrap();
            datafusion::physical_plan::collect(plan, ctx.task_ctx())
                .await
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows())
                .sum()
        })
    }

    /// A rebuilt table is cached and served to later decodes, so it must not
    /// be bound to the runtime of the decode that built it: once that runtime
    /// shuts down, as a finished test or a dropped standalone context does,
    /// Iceberg's scan planning is spawned onto it and never runs, and the
    /// scan returns no rows instead of failing.
    #[test]
    fn decoded_scan_survives_the_decoding_runtime_shutting_down() {
        let dir = tempfile::tempdir().unwrap();
        let table =
            named_runtime("setup").block_on(test_util::table_with_rows(dir.path(), 3));
        let buf = encoded_full_scan(table);

        let first = named_runtime("first-executor");
        assert_eq!(decode_and_run(&first, &buf), 3);
        drop(first);

        assert_eq!(decode_and_run(&named_runtime("second-executor"), &buf), 3);
    }

    /// Scan planning (reading and parsing manifests) runs on the table
    /// runtime, which has a worker per core, not on the single-threaded
    /// catalog runtime, where every scan in the process would share one thread.
    #[test]
    fn decoded_scan_plans_on_the_table_runtime() {
        use crate::test_util::recording_storage::take_reads;

        let dir = tempfile::tempdir().unwrap();
        let table =
            named_runtime("setup").block_on(test_util::table_with_rows(dir.path(), 3));
        let buf = encoded_full_scan(table);
        take_reads(dir.path());

        assert_eq!(decode_and_run(&named_runtime("executor"), &buf), 3);

        let manifest_threads: Vec<String> = take_reads(dir.path())
            .into_iter()
            .filter(|(path, _)| path.ends_with(".avro") && !path.contains("/snap-"))
            .map(|(_, thread)| thread)
            .collect();
        assert_eq!(manifest_threads.len(), 3, "one manifest per commit");
        assert!(
            manifest_threads
                .iter()
                .all(|t| t.starts_with("iceberg-table")),
            "manifests read on {manifest_threads:?}"
        );
    }
}
