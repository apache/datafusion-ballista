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
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use iceberg::TableIdent;
use iceberg::expr::Predicate;
use iceberg::spec::{PartitionSpec, Schema};
use iceberg_datafusion::physical_plan::{
    IcebergCommitExec, IcebergMetadataScan, IcebergTableScan, IcebergWriteExec,
    PartitionExpr,
};
use iceberg_datafusion::{snapshot_arrow_schema, to_datafusion_error};
use serde::{Deserialize, Serialize};

use crate::bridge::{
    Frame, TAG_DELEGATED, TableRefWire, build_metadata_provider, encode_blob, json_err,
    load_table, load_table_pinned, load_table_with_catalog, missing_catalog_config_err,
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
        snapshot_id: Option<i64>,
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
    },
    Commit {
        #[serde(flatten)]
        table_ref: TableRefWire,
    },
    Metadata {
        #[serde(flatten)]
        table_ref: TableRefWire,
        /// The metadata table kind, as its lowercase string name.
        metadata_type: String,
    },
}

/// Wire representation of an [`IcebergDataFusion`](iceberg_datafusion) partition
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
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let rest = match split_frame(buf, "iceberg physical codec")? {
            Frame::Delegated(rest) => return self.inner.try_decode(rest, inputs, ctx),
            Frame::Iceberg(rest) => rest,
        };

        let node: IcebergPhysicalNode = serde_json::from_slice(rest).map_err(json_err)?;

        match node {
            IcebergPhysicalNode::Scan {
                table_ref,
                snapshot_id,
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
                // current one would describe historical rows incorrectly.
                let arrow_schema = snapshot_arrow_schema(&table_obj, snapshot_id)
                    .map_err(to_datafusion_error)?;
                let proj_indices = project_indices(
                    &arrow_schema,
                    projection.as_ref(),
                    &table,
                    snapshot_id,
                )?;
                let scan = IcebergTableScan::new(
                    table_obj,
                    snapshot_id,
                    arrow_schema,
                    proj_indices.as_ref(),
                    &[],
                    limit,
                )
                .with_predicates(predicates)
                .with_catalog_config(Some(config));
                Ok(Arc::new(scan))
            }
            IcebergPhysicalNode::Write { table_ref } => {
                let (config, table) = table_ref.into_parts();
                let table_obj = load_table(&config, &table)?;
                // Writes always target the current schema — a snapshot-pinned
                // provider is read-only.
                let arrow_schema = snapshot_arrow_schema(&table_obj, None)
                    .map_err(to_datafusion_error)?;
                let input = single_input(inputs, "IcebergWriteExec")?;
                let write = IcebergWriteExec::new(table_obj, input, arrow_schema)
                    .with_catalog_config(Some(config));
                Ok(Arc::new(write))
            }
            IcebergPhysicalNode::Commit { table_ref } => {
                let (config, table) = table_ref.into_parts();
                let (cat, table_obj) = load_table_with_catalog(&config, &table)?;
                let arrow_schema = snapshot_arrow_schema(&table_obj, None)
                    .map_err(to_datafusion_error)?;
                let input = single_input(inputs, "IcebergCommitExec")?;
                let commit = IcebergCommitExec::new(table_obj, cat, input, arrow_schema)
                    .with_catalog_config(Some(config));
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
            let snapshot_id = scan
                .snapshot_id()
                .or_else(|| scan.table().metadata().current_snapshot_id());
            let node = IcebergPhysicalNode::Scan {
                table_ref: TableRefWire::new(config, scan.table().identifier()),
                snapshot_id,
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
            };
            return encode_blob(buf, &node);
        }

        if let Some(commit) = node.downcast_ref::<IcebergCommitExec>() {
            let config = commit
                .catalog_config()
                .ok_or_else(|| missing_table_config_err("IcebergCommitExec"))?;
            let node = IcebergPhysicalNode::Commit {
                table_ref: TableRefWire::new(config, commit.table().identifier()),
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
        self.inner.try_encode(node, buf)
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
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
        self.inner.try_encode_expr(node, buf)
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        match split_frame(buf, "iceberg physical expr")? {
            Frame::Delegated(rest) => self.inner.try_decode_expr(rest, inputs),
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

    use crate::bridge::{CatalogConfigWire, TAG_ICEBERG};

    use super::*;

    fn sample_table_ref() -> TableRefWire {
        TableRefWire {
            catalog: CatalogConfigWire {
                r#type: "rest".to_string(),
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
    fn scan_node_roundtrips() {
        let node = IcebergPhysicalNode::Scan {
            table_ref: sample_table_ref(),
            snapshot_id: Some(42),
            projection: Some(vec!["a".to_string(), "b".to_string()]),
            limit: Some(10),
            predicates: None,
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn scan_node_with_predicate_roundtrips() {
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        let node = IcebergPhysicalNode::Scan {
            table_ref: sample_table_ref(),
            snapshot_id: None,
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
            snapshot_id: None,
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
            snapshot_id: Some(7),
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
                snapshot_id: Some(7),
                ..
            }
        ));
    }

    #[test]
    fn partition_expr_wire_roundtrips() {
        use iceberg::spec::{NestedField, PrimitiveType, Transform, Type};

        // Schema + PartitionSpec are the heaviest serde types in the crate.
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

        let wire = PartitionExprWire {
            partition_spec,
            schema,
        };

        let mut buf = Vec::new();
        encode_blob(&mut buf, &wire).expect("encode");
        assert_eq!(buf[0], TAG_ICEBERG, "blob must carry the iceberg tag");
        let decoded: PartitionExprWire =
            serde_json::from_slice(&buf[1..]).expect("decode");

        assert_eq!(decoded.partition_spec, wire.partition_spec);
        assert_eq!(decoded.schema, wire.schema);
    }

    #[test]
    fn metadata_node_roundtrips() {
        let node = IcebergPhysicalNode::Metadata {
            table_ref: sample_table_ref(),
            metadata_type: "snapshots".to_string(),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn write_node_roundtrips() {
        let node = IcebergPhysicalNode::Write {
            table_ref: sample_table_ref(),
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn commit_node_roundtrips() {
        // Multi-level namespace, so the ident round-trip is exercised beyond
        // the single-level `ns.tbl` the other tests use.
        let node = IcebergPhysicalNode::Commit {
            table_ref: TableRefWire {
                table: TableIdent::from_strs(["a", "b", "tbl"]).unwrap(),
                ..sample_table_ref()
            },
        };
        assert_eq!(node, roundtrip(&node));
    }

    #[test]
    fn table_ref_flattens_to_inline_catalog_and_table_keys() {
        // Wire compat: `TableRefWire` must serialize as inline `catalog` and
        // `table` keys, exactly as when the variants spelled the two fields
        // out — never nested under a `table_ref` object.
        let node = IcebergPhysicalNode::Write {
            table_ref: sample_table_ref(),
        };
        let value = serde_json::to_value(&node).unwrap();
        let obj = value["Write"].as_object().unwrap();
        assert!(obj.contains_key("catalog"), "{value}");
        assert!(obj.contains_key("table"), "{value}");
        assert!(!obj.contains_key("table_ref"), "{value}");
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
            .try_encode(Arc::new(shuffle), &mut buf)
            .expect("encode delegated node");
        assert_eq!(buf[0], TAG_DELEGATED, "non-Iceberg node must be delegated");

        let ctx = SessionContext::new();
        let decoded = codec
            .try_decode(&buf, &[input], &ctx.task_ctx())
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

    #[test]
    fn try_decode_rejects_unframed_buffers() {
        use datafusion::prelude::SessionContext;

        // Missing or unrecognized framing must be a hard error, never a misparse
        // of whatever bytes follow.
        let ctx = SessionContext::new();
        let codec = IcebergPhysicalCodec::default();

        let err = codec.try_decode(&[], &[], &ctx.task_ctx()).unwrap_err();
        assert!(err.to_string().contains("empty"), "{err}");

        let err = codec.try_decode(&[99], &[], &ctx.task_ctx()).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown iceberg physical codec tag 99"),
            "{err}"
        );
    }

    #[test]
    fn try_decode_expr_rejects_unframed_buffers() {
        // The expr path has its own tag dispatch, so it needs its own check.
        let codec = IcebergPhysicalCodec::default();

        let err = codec.try_decode_expr(&[], &[]).unwrap_err();
        assert!(err.to_string().contains("empty"), "{err}");

        let err = codec.try_decode_expr(&[99], &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown iceberg physical expr tag 99"),
            "{err}"
        );
    }
}
