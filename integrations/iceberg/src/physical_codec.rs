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
//! Encodes/decodes [`IcebergTableScan`], [`IcebergWriteExec`], and
//! [`IcebergCommitExec`] so Ballista can ship them to remote executors. Any
//! node that is not an Iceberg node is delegated to an inner codec (by default
//! Ballista's own [`BallistaPhysicalExtensionCodec`]), so shuffle and other
//! Ballista plan nodes keep working.

use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion_iceberg::physical_plan::{
    IcebergCommitExec, IcebergMetadataScan, IcebergTableScan, IcebergWriteExec,
    PartitionExpr,
};
use datafusion_iceberg::{IcebergStaticTableProvider, to_datafusion_error};
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use iceberg::TableIdent;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::expr::Predicate;
use iceberg::spec::{PartitionSpec, Schema};
use iceberg::table::Table;
use serde::{Deserialize, Serialize};

use crate::bridge::{
    Frame, TAG_DELEGATED, TableRefWire, block_on, build_metadata_provider, encode_blob,
    json_err, load_table_at, load_table_pinned, missing_catalog_config_err,
    missing_table_config_err, split_frame,
};

/// Wire representation of an Iceberg physical plan node.
// `Predicate` is not `Eq` (it can hold float literals), so this derives only
// `PartialEq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum IcebergPhysicalNode {
    Scan {
        #[serde(flatten)]
        table_ref: TableRefWire,
        snapshot: ScanSnapshot,
        projection: Option<Vec<String>>,
        limit: Option<usize>,
        /// Pushed-down filter, restored on the remote node so Iceberg file
        /// pruning is preserved (DataFusion still re-applies it above the scan).
        #[serde(default)]
        predicates: Option<Predicate>,
    },
    Write {
        #[serde(flatten)]
        table_ref: TableRefWire,
        /// The metadata file the write was planned against, which every
        /// writer task rebuilds the table from (see [`load_table_at`]).
        metadata_location: String,
    },
    Commit {
        #[serde(flatten)]
        table_ref: TableRefWire,
        /// As for [`IcebergPhysicalNode::Write`].
        metadata_location: String,
    },
    Metadata {
        #[serde(flatten)]
        table_ref: TableRefWire,
        /// The metadata table kind, as its lowercase string name.
        metadata_type: String,
    },
}

/// What a scan reads, fixed when the scan is encoded so every task of the query
/// reads the same state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum ScanSnapshot {
    Snapshot(i64),
    /// The table had no snapshot, so the scan is empty. Carries the table schema
    /// the scan was planned against, since there is no snapshot to take it from.
    Empty {
        schema: Box<Schema>,
    },
}

/// Wire representation of an [`IcebergDataFusion`](datafusion_iceberg) partition
/// expression. The live `PartitionValueCalculator` it wraps is not serializable,
/// but it can be rebuilt on the far node from the (self-contained) partition spec
/// and table schema, so those are all that travels on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartitionExprWire {
    partition_spec: PartitionSpec,
    schema: Schema,
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
                table_ref,
                snapshot: ScanSnapshot::Empty { schema },
                projection,
                ..
            } => {
                let arrow_schema: SchemaRef = Arc::new(
                    schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?,
                );
                let output_schema = match project_indices(
                    &arrow_schema,
                    projection.as_ref(),
                    &table_ref.table,
                    None,
                )? {
                    Some(indices) => Arc::new(arrow_schema.project(&indices)?),
                    None => arrow_schema,
                };
                Ok(Arc::new(EmptyExec::new(output_schema)))
            }
            IcebergPhysicalNode::Scan {
                table_ref,
                snapshot: ScanSnapshot::Snapshot(snapshot_id),
                projection,
                limit,
                predicates,
            } => {
                let (config, table) = table_ref.into_parts();
                // Pinned loads are cached: every task of the stage decodes the
                // same pin, so only the first pays the catalog round trip.
                let table_obj = load_table_pinned(&config, &table, snapshot_id)?;
                // A pinned scan must use the schema that snapshot was written
                // under — the table's schema may have changed since, and the
                // current one would describe historical rows incorrectly. The
                // static provider resolves it exactly as for a time-travel read.
                let arrow_schema =
                    block_on(IcebergStaticTableProvider::try_new_from_table_snapshot(
                        table_obj.clone(),
                        snapshot_id,
                    ))?
                    .schema();
                let proj_indices = project_indices(
                    &arrow_schema,
                    projection.as_ref(),
                    &table,
                    Some(snapshot_id),
                )?;
                let scan = IcebergTableScan::new_with_predicate(
                    table_obj,
                    Some(snapshot_id),
                    arrow_schema,
                    proj_indices.as_ref(),
                    predicates,
                    limit,
                )
                .with_catalog_config(config);
                Ok(Arc::new(scan))
            }
            IcebergPhysicalNode::Write {
                table_ref,
                metadata_location,
            } => {
                let (config, table) = table_ref.into_parts();
                let (_, table_obj) = load_table_at(&config, &table, &metadata_location)?;
                let input = single_input(inputs, "IcebergWriteExec")?;
                let write =
                    IcebergWriteExec::new(table_obj, input).with_catalog_config(config);
                Ok(Arc::new(write))
            }
            IcebergPhysicalNode::Commit {
                table_ref,
                metadata_location,
            } => {
                let (config, table) = table_ref.into_parts();
                let (cat, table_obj) =
                    load_table_at(&config, &table, &metadata_location)?;
                let arrow_schema = current_arrow_schema(&table_obj)?;
                let input = single_input(inputs, "IcebergCommitExec")?;
                let commit = IcebergCommitExec::new(table_obj, cat, input, arrow_schema)
                    .with_catalog_config(config);
                Ok(Arc::new(commit))
            }
            IcebergPhysicalNode::Metadata {
                table_ref,
                metadata_type,
            } => Ok(Arc::new(IcebergMetadataScan::new(build_metadata_provider(
                table_ref,
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
            let config = scan
                .catalog_config()
                .ok_or_else(|| missing_table_config_err("IcebergTableScan"))?;
            // Pin the snapshot at encode (planning) time. The executor reloads
            // table metadata independently, so an unpinned scan would read
            // whatever snapshot is current when each task decodes — concurrent
            // commits could then give two tasks of one query different
            // snapshots. `scan.table()` is the table as loaded at planning, so
            // its current snapshot is the consistent choice for every task.
            let metadata = scan.table().metadata();
            let snapshot = match scan.snapshot_id().or(metadata.current_snapshot_id()) {
                Some(id) => ScanSnapshot::Snapshot(id),
                None => ScanSnapshot::Empty {
                    schema: Box::new(metadata.current_schema().as_ref().clone()),
                },
            };
            let node = IcebergPhysicalNode::Scan {
                table_ref: TableRefWire::new(config, scan.table().identifier()),
                snapshot,
                projection: scan.projection().map(|s| s.to_vec()),
                limit: scan.limit(),
                predicates: scan.predicates().cloned(),
            };
            return encode_blob(buf, &node);
        }

        if let Some(write) = node.downcast_ref::<IcebergWriteExec>() {
            let config = write
                .catalog_config()
                .ok_or_else(|| missing_table_config_err("IcebergWriteExec"))?;
            let node = IcebergPhysicalNode::Write {
                table_ref: TableRefWire::new(config, write.table().identifier()),
                metadata_location: planned_metadata_location(write.table())?,
            };
            return encode_blob(buf, &node);
        }

        if let Some(commit) = node.downcast_ref::<IcebergCommitExec>() {
            let config = commit
                .catalog_config()
                .ok_or_else(|| missing_table_config_err("IcebergCommitExec"))?;
            let node = IcebergPhysicalNode::Commit {
                table_ref: TableRefWire::new(config, commit.table().identifier()),
                metadata_location: planned_metadata_location(commit.table())?,
            };
            return encode_blob(buf, &node);
        }

        if let Some(meta) = node.downcast_ref::<IcebergMetadataScan>() {
            let provider = meta.provider();
            let config = provider
                .catalog_config()
                .ok_or_else(|| missing_catalog_config_err("IcebergMetadataScan"))?;
            let node = IcebergPhysicalNode::Metadata {
                table_ref: TableRefWire::new(config, provider.table().identifier()),
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

/// Maps projected column names back to their indices in `arrow_schema`, the
/// schema of the table at `snapshot_id`.
///
/// A name that doesn't resolve is a hard error: the executor reloads table
/// metadata independently of the scheduler, so silently dropping it would
/// rebuild the scan with fewer columns than the plan expects and surface later
/// as a confusing column-count mismatch instead of a clear failure here.
///
/// The usual cause is a schema change with no write behind it. Evolving a schema
/// creates no snapshot, so a scan planned right after an `ADD COLUMN` projects a
/// column that the latest snapshot's schema does not have yet — hence the
/// message points at the snapshot rather than at cluster state.
fn project_indices(
    arrow_schema: &SchemaRef,
    projection: Option<&Vec<String>>,
    table: &TableIdent,
    snapshot_id: Option<i64>,
) -> Result<Option<Vec<usize>>, DataFusionError> {
    projection
        .map(|names| {
            names
                .iter()
                .map(|n| {
                    arrow_schema.index_of(n).map_err(|_| {
                        let cause = match snapshot_id {
                            Some(id) => format!(
                                "not found in the schema of table {table} at snapshot {id}; \
                                 the table's schema may have changed since that snapshot \
                                 was written"
                            ),
                            None => format!(
                                "not found in the current schema of table {table}; \
                                 scheduler and executor table metadata may be out of sync"
                            ),
                        };
                        DataFusionError::Internal(format!("projected column {n:?} {cause}"))
                    })
                })
                .collect::<Result<Vec<usize>, _>>()
        })
        .transpose()
}

/// The metadata file `table` was loaded from at planning time.
fn planned_metadata_location(table: &Table) -> Result<String, DataFusionError> {
    table
        .metadata_location_result()
        .map(str::to_string)
        .map_err(to_datafusion_error)
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
    use std::collections::BTreeMap;

    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;

    use crate::bridge::{CatalogConfigWire, TAG_ICEBERG};
    use crate::test_util;

    use super::*;

    fn sample_table_ref() -> TableRefWire {
        TableRefWire {
            catalog: CatalogConfigWire {
                catalog_type: "rest".to_string(),
                name: "rest".to_string(),
                props: BTreeMap::from([
                    ("uri".to_string(), "http://localhost:8181".to_string()),
                    ("warehouse".to_string(), "s3://bucket/wh".to_string()),
                ]),
            },
            table: TableIdent::from_strs(["ns", "tbl"]).unwrap(),
        }
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
            table_ref: sample_table_ref(),
            snapshot: ScanSnapshot::Snapshot(1),
            projection: None,
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
            table_ref: sample_table_ref(),
            snapshot: ScanSnapshot::Snapshot(1),
            projection: None,
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
            table_ref: sample_table_ref(),
            snapshot: ScanSnapshot::Snapshot(7),
            projection: None,
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
                snapshot: ScanSnapshot::Snapshot(7),
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

        let schema = Schema::builder()
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

    fn arrow_schema() -> SchemaRef {
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]))
    }

    fn tbl() -> TableIdent {
        TableIdent::from_strs(["ns", "tbl"]).unwrap()
    }

    #[test]
    fn project_indices_resolves_names_in_projection_order() {
        let names = vec!["c".to_string(), "a".to_string()];
        let idx = project_indices(&arrow_schema(), Some(&names), &tbl(), None).unwrap();
        assert_eq!(idx, Some(vec![2, 0]), "resolved in projection order");

        // No projection means "all columns", not "no columns".
        assert_eq!(
            project_indices(&arrow_schema(), None, &tbl(), None).unwrap(),
            None
        );
    }

    #[test]
    fn project_indices_unknown_column_errors() {
        // A projected name absent from the reloaded schema must fail loudly,
        // naming the column and a cause that fits how the schema was resolved.
        let names = vec!["missing".to_string()];

        let err =
            project_indices(&arrow_schema(), Some(&names), &tbl(), Some(42)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing"), "names the column: {msg}");
        assert!(msg.contains("snapshot 42"), "names the snapshot: {msg}");
        assert!(
            msg.contains("schema may have changed"),
            "the likely cause: {msg}"
        );

        // Unpinned scans resolve against the current schema, where a missing
        // column really does mean the two nodes disagree about the table.
        let err =
            project_indices(&arrow_schema(), Some(&names), &tbl(), None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("out of sync"), "explains the cause: {msg}");
    }

    /// A scan of `table` pinned to `snapshot_id`, projecting column `name` only.
    fn name_scan(table: Table, snapshot_id: Option<i64>) -> IcebergTableScan {
        let schema = current_arrow_schema(&table).unwrap();
        IcebergTableScan::new_with_predicate(
            table,
            snapshot_id,
            schema,
            Some(&vec![1]),
            None,
            None,
        )
        .with_catalog_config(test_util::catalog_config())
    }

    fn encode_scan(scan: IcebergTableScan) -> Vec<u8> {
        let mut buf = Vec::new();
        IcebergPhysicalCodec::default()
            .try_encode(Arc::new(scan), &mut buf, &DefaultPhysicalProtoConverter {})
            .expect("encode");
        buf
    }

    fn encoded_snapshot(scan: IcebergTableScan) -> ScanSnapshot {
        match serde_json::from_slice(&encode_scan(scan)[1..]).expect("decode wire") {
            IcebergPhysicalNode::Scan { snapshot, .. } => snapshot,
            other => panic!("expected a scan, got {other:?}"),
        }
    }

    #[test]
    fn scan_is_pinned_when_encoded() {
        // Unpinned: pinned to the snapshot current at planning, so every task
        // reads the same state.
        assert_eq!(
            encoded_snapshot(name_scan(test_util::table(&[1, 2]), None)),
            ScanSnapshot::Snapshot(2)
        );
        assert_eq!(
            encoded_snapshot(name_scan(test_util::table(&[1, 2]), Some(1))),
            ScanSnapshot::Snapshot(1)
        );

        // No snapshot to pin: the scan is empty, and carries the schema it was
        // planned against.
        let table = test_util::table(&[]);
        let schema = Box::new(table.metadata().current_schema().as_ref().clone());
        assert_eq!(
            encoded_snapshot(name_scan(table, None)),
            ScanSnapshot::Empty { schema }
        );
    }

    #[tokio::test]
    async fn write_and_commit_record_the_planned_metadata_file() {
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
            Arc::new(EmptyExec::new(current_arrow_schema(&table).unwrap()));
        let write: Arc<dyn ExecutionPlan> = Arc::new(
            IcebergWriteExec::new(table.clone(), Arc::clone(&input))
                .with_catalog_config(test_util::catalog_config()),
        );
        let commit: Arc<dyn ExecutionPlan> = Arc::new(
            IcebergCommitExec::new(
                table.clone(),
                Arc::new(catalog),
                input,
                current_arrow_schema(&table).unwrap(),
            )
            .with_catalog_config(test_util::catalog_config()),
        );

        for node in [write, commit] {
            let mut buf = Vec::new();
            IcebergPhysicalCodec::default()
                .try_encode(node, &mut buf, &DefaultPhysicalProtoConverter {})
                .expect("encode");
            let location = match serde_json::from_slice(&buf[1..]).expect("decode wire") {
                IcebergPhysicalNode::Write {
                    metadata_location, ..
                }
                | IcebergPhysicalNode::Commit {
                    metadata_location, ..
                } => metadata_location,
                other => panic!("expected a write or commit, got {other:?}"),
            };
            assert_eq!(location, "/test/tbl/metadata.json");
        }
    }

    #[test]
    fn empty_scan_decodes_to_empty_exec() {
        use datafusion::prelude::SessionContext;

        let buf = encode_scan(name_scan(test_util::table(&[]), None));
        // Decoding needs no catalog: the scan is known to be empty.
        let ctx = SessionContext::new();
        let decoded = IcebergPhysicalCodec::default()
            .try_decode(
                &buf,
                &[],
                &ctx.task_ctx(),
                &DefaultPhysicalProtoConverter {},
            )
            .expect("decode");

        assert!(decoded.downcast_ref::<EmptyExec>().is_some());
        let fields: Vec<_> = decoded
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, ["name"], "keeps the scan's projection");
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
}
