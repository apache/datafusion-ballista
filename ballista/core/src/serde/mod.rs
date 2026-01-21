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

//! This crate contains code generated from the Ballista Protocol Buffer Definition as well
//! as convenience code for interacting with the generated code.

use crate::{error::BallistaError, serde::scheduler::Action as BallistaAction};

use arrow_flight::sql::ProstMessageExt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::logical_plan::file_formats::{
    ArrowLogicalExtensionCodec, AvroLogicalExtensionCodec, CsvLogicalExtensionCodec,
    JsonLogicalExtensionCodec, ParquetLogicalExtensionCodec,
};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::protobuf::proto_error;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use datafusion_proto::{
    convert_required,
    logical_plan::{AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec},
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
};

use prost::Message;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{convert::TryInto, io::Cursor};

use crate::execution_plans::sort_shuffle::SortShuffleConfig;
use crate::execution_plans::{
    ShuffleReaderExec, ShuffleWriterExec, SortShuffleWriterExec, UnresolvedShuffleExec,
};
use crate::serde::protobuf::ballista_physical_plan_node::PhysicalPlanType;
use crate::serde::scheduler::PartitionLocation;
pub use generated::ballista as protobuf;

/// Generated protobuf code from Ballista protocol definitions.
pub mod generated;
/// Scheduler-specific serialization types and conversions.
pub mod scheduler;

impl ProstMessageExt for protobuf::Action {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.Action"
    }

    fn as_any(&self) -> arrow_flight::sql::Any {
        arrow_flight::sql::Any {
            type_url: protobuf::Action::type_url().to_string(),
            value: self.encode_to_vec().into(),
        }
    }
}

/// Decodes a Ballista action from protobuf bytes.
pub fn decode_protobuf(bytes: &[u8]) -> Result<BallistaAction, BallistaError> {
    let mut buf = Cursor::new(bytes);

    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::Internal(format!("{e:?}")))
        .and_then(|node| node.try_into())
}

/// Codec for serializing and deserializing Ballista logical and physical plans.
#[derive(Clone, Debug)]
pub struct BallistaCodec<
    T: 'static + AsLogicalPlan = LogicalPlanNode,
    U: 'static + AsExecutionPlan = PhysicalPlanNode,
> {
    logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
    physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    logical_plan_repr: PhantomData<T>,
    physical_plan_repr: PhantomData<U>,
}

impl Default for BallistaCodec {
    fn default() -> Self {
        Self {
            logical_extension_codec: Arc::new(BallistaLogicalExtensionCodec::default()),
            physical_extension_codec: Arc::new(BallistaPhysicalExtensionCodec::default()),
            logical_plan_repr: PhantomData,
            physical_plan_repr: PhantomData,
        }
    }
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> BallistaCodec<T, U> {
    /// Creates a new Ballista codec with custom extension codecs.
    pub fn new(
        logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
        physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        Self {
            logical_extension_codec,
            physical_extension_codec,
            logical_plan_repr: PhantomData,
            physical_plan_repr: PhantomData,
        }
    }

    /// Returns the logical extension codec.
    pub fn logical_extension_codec(&self) -> &dyn LogicalExtensionCodec {
        self.logical_extension_codec.as_ref()
    }

    /// Returns the physical extension codec.
    pub fn physical_extension_codec(&self) -> &dyn PhysicalExtensionCodec {
        self.physical_extension_codec.as_ref()
    }
}

/// Logical extension codec for Ballista-specific plan nodes.
#[derive(Debug)]
pub struct BallistaLogicalExtensionCodec {
    default_codec: Arc<dyn LogicalExtensionCodec>,
    file_format_codecs: Vec<Arc<dyn LogicalExtensionCodec>>,
}

impl BallistaLogicalExtensionCodec {
    /// looks for a codec which can operate on this node
    /// returns a position of codec in the list and result.
    ///
    /// position is important with encoding process
    /// as position of used codecs is needed
    /// so the same codec can be used for decoding
    fn try_any<R>(
        &self,
        mut f: impl FnMut(&dyn LogicalExtensionCodec) -> Result<R>,
    ) -> Result<(u32, R)> {
        let mut last_err = None;
        for (position, codec) in self.file_format_codecs.iter().enumerate() {
            match f(codec.as_ref()) {
                Ok(result) => return Ok((position as u32, result)),
                Err(err) => last_err = Some(err),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            DataFusionError::Internal(
                "List of provided extended logical codecs is empty".to_owned(),
            )
        }))
    }
}

impl Default for BallistaLogicalExtensionCodec {
    fn default() -> Self {
        Self {
            default_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            // Position in this list is important as it will be used for decoding.
            // If new codec is added it should go to last position.
            file_format_codecs: vec![
                Arc::new(ParquetLogicalExtensionCodec {}),
                Arc::new(CsvLogicalExtensionCodec {}),
                Arc::new(JsonLogicalExtensionCodec {}),
                Arc::new(ArrowLogicalExtensionCodec {}),
                Arc::new(AvroLogicalExtensionCodec {}),
            ],
        }
    }
}

impl LogicalExtensionCodec for BallistaLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<datafusion::logical_expr::Extension> {
        self.default_codec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.default_codec.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::sql::TableReference,
        schema: datafusion::arrow::datatypes::SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        self.default_codec
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: Arc<dyn datafusion::catalog::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.default_codec
            .try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn datafusion::datasource::file_format::FileFormatFactory>> {
        let proto = FileFormatProto::decode(buf)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;

        let codec = self
            .file_format_codecs
            .get(proto.encoder_position as usize)
            .ok_or(DataFusionError::Internal(
                "Can't find required codec in file codec list".to_owned(),
            ))?;

        codec.try_decode_file_format(&proto.blob, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    ) -> Result<()> {
        let mut blob = vec![];
        let (encoder_position, _) =
            self.try_any(|codec| codec.try_encode_file_format(&mut blob, node.clone()))?;

        let proto = FileFormatProto {
            encoder_position,
            blob,
        };
        proto
            .encode(buf)
            .map_err(|e| DataFusionError::Internal(e.to_string()))
    }
}

/// Physical extension codec for Ballista-specific execution plan nodes.
#[derive(Debug)]
pub struct BallistaPhysicalExtensionCodec {
    default_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl Default for BallistaPhysicalExtensionCodec {
    fn default() -> Self {
        Self {
            default_codec: Arc::new(DefaultPhysicalExtensionCodec {}),
        }
    }
}

impl PhysicalExtensionCodec for BallistaPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let ballista_plan: protobuf::BallistaPhysicalPlanNode =
            protobuf::BallistaPhysicalPlanNode::decode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Could not deserialize BallistaPhysicalPlanNode: {e}"
                ))
            })?;

        let ballista_plan =
            ballista_plan.physical_plan_type.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "Could not deserialize BallistaPhysicalPlanNode because it's physical_plan_type is none".to_string()
                )
            })?;

        match ballista_plan {
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input = inputs[0].clone();

                let shuffle_output_partitioning = parse_protobuf_hash_partitioning(
                    shuffle_writer.output_partitioning.as_ref(),
                    ctx,
                    input.schema().as_ref(),
                    self.default_codec.as_ref(),
                )?;

                Ok(Arc::new(ShuffleWriterExec::try_new(
                    shuffle_writer.job_id.clone(),
                    shuffle_writer.stage_id as usize,
                    input,
                    "".to_string(), // this is intentional but hacky - the executor will fill this in
                    shuffle_output_partitioning,
                )?))
            }
            PhysicalPlanType::SortShuffleWriter(sort_shuffle_writer) => {
                let input = inputs[0].clone();

                let shuffle_output_partitioning = parse_protobuf_hash_partitioning(
                    sort_shuffle_writer.output_partitioning.as_ref(),
                    ctx,
                    input.schema().as_ref(),
                    self.default_codec.as_ref(),
                )?;

                let partitioning = shuffle_output_partitioning.ok_or_else(|| {
                    DataFusionError::Internal(
                        "SortShuffleWriterExec requires hash partitioning".to_string(),
                    )
                })?;

                let config = SortShuffleConfig::new(
                    true,
                    sort_shuffle_writer.buffer_size as usize,
                    sort_shuffle_writer.memory_limit as usize,
                    sort_shuffle_writer.spill_threshold,
                    datafusion::arrow::ipc::CompressionType::LZ4_FRAME,
                );

                Ok(Arc::new(SortShuffleWriterExec::try_new(
                    sort_shuffle_writer.job_id.clone(),
                    sort_shuffle_writer.stage_id as usize,
                    input,
                    "".to_string(), // executor will fill this in
                    partitioning,
                    config,
                )?))
            }
            PhysicalPlanType::ShuffleReader(shuffle_reader) => {
                let stage_id = shuffle_reader.stage_id as usize;
                let schema: SchemaRef =
                    Arc::new(convert_required!(shuffle_reader.schema)?);
                let partition_location: Vec<Vec<PartitionLocation>> = shuffle_reader
                    .partition
                    .iter()
                    .map(|p| {
                        p.location
                            .iter()
                            .map(|l| {
                                l.clone().try_into().map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Fail to get partition location due to {e:?}"
                                    ))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                let partitioning = parse_protobuf_partitioning(
                    shuffle_reader.partitioning.as_ref(),
                    ctx,
                    schema.as_ref(),
                    self.default_codec.as_ref(),
                )?;
                let partitioning = partitioning
                    .ok_or_else(|| proto_error("missing required partitioning field"))?;
                let shuffle_reader = ShuffleReaderExec::try_new(
                    stage_id,
                    partition_location,
                    schema,
                    partitioning,
                )?;
                Ok(Arc::new(shuffle_reader))
            }
            PhysicalPlanType::UnresolvedShuffle(unresolved_shuffle) => {
                let schema: SchemaRef =
                    Arc::new(convert_required!(unresolved_shuffle.schema)?);
                let partitioning = parse_protobuf_partitioning(
                    unresolved_shuffle.partitioning.as_ref(),
                    ctx,
                    schema.as_ref(),
                    self.default_codec.as_ref(),
                )?;
                let partitioning = partitioning
                    .ok_or_else(|| proto_error("missing required partitioning field"))?;
                Ok(Arc::new(UnresolvedShuffleExec::new(
                    unresolved_shuffle.stage_id as usize,
                    schema,
                    partitioning,
                )))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(exec) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            // note that we use shuffle_output_partitioning() rather than output_partitioning()
            // to get the true output partitioning
            let output_partitioning = match exec.shuffle_output_partitioning() {
                Some(Partitioning::Hash(exprs, partition_count)) => {
                    Some(datafusion_proto::protobuf::PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr|datafusion_proto::physical_plan::to_proto::serialize_physical_expr(&expr.clone(), self.default_codec.as_ref()))
                            .collect::<Result<Vec<_>, DataFusionError>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                None => None,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "physical_plan::to_proto() invalid partitioning for ShuffleWriterExec: {other:?}"
                    )));
                }
            };

            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleWriter(
                    protobuf::ShuffleWriterExecNode {
                        job_id: exec.job_id().to_string(),
                        stage_id: exec.stage_id() as u32,
                        input: None,
                        output_partitioning,
                    },
                )),
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode shuffle writer execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<SortShuffleWriterExec>() {
            let output_partitioning = match exec.shuffle_output_partitioning() {
                Partitioning::Hash(exprs, partition_count) => {
                    Some(datafusion_proto::protobuf::PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr| {
                                datafusion_proto::physical_plan::to_proto::serialize_physical_expr(
                                    &expr.clone(),
                                    self.default_codec.as_ref(),
                                )
                            })
                            .collect::<Result<Vec<_>, DataFusionError>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "SortShuffleWriterExec requires Hash partitioning, got: {other:?}"
                    )));
                }
            };

            let config = exec.config();
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::SortShuffleWriter(
                    protobuf::SortShuffleWriterExecNode {
                        job_id: exec.job_id().to_string(),
                        stage_id: exec.stage_id() as u32,
                        input: None,
                        output_partitioning,
                        buffer_size: config.buffer_size as u64,
                        memory_limit: config.memory_limit as u64,
                        spill_threshold: config.spill_threshold,
                    },
                )),
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode sort shuffle writer execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<ShuffleReaderExec>() {
            let stage_id = exec.stage_id as u32;
            let mut partition = vec![];
            for location in &exec.partition {
                partition.push(protobuf::ShuffleReaderPartition {
                    location: location
                        .iter()
                        .map(|l| {
                            l.clone().try_into().map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Fail to get partition location due to {e:?}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                });
            }
            let partitioning = serialize_partitioning(
                &exec.properties().partitioning,
                self.default_codec.as_ref(),
            )?;
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleReader(
                    protobuf::ShuffleReaderExecNode {
                        stage_id,
                        partition,
                        schema: Some(exec.schema().as_ref().try_into()?),
                        partitioning: Some(partitioning),
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode shuffle reader execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<UnresolvedShuffleExec>() {
            let partitioning = serialize_partitioning(
                &exec.properties().partitioning,
                self.default_codec.as_ref(),
            )?;
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::UnresolvedShuffle(
                    protobuf::UnresolvedShuffleExecNode {
                        stage_id: exec.stage_id as u32,
                        schema: Some(exec.schema().as_ref().try_into()?),
                        partitioning: Some(partitioning),
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode unresolved shuffle execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "unsupported plan type: {node:?}"
            )))
        }
    }
}

/// FileFormatProto captures data encoded by file format codecs
///
/// it captures position of codec used to encode FileFormat
/// and actual encoded value.
///
/// capturing codec position is required, as same codec can decode
/// blobs encoded by different encoders (probability is low but  it
/// happened in the past)
///
#[derive(Clone, PartialEq, prost::Message)]
struct FileFormatProto {
    /// encoder id used to encode blob
    /// (to be used for decoding)
    #[prost(uint32, tag = 1)]
    pub encoder_position: u32,
    #[prost(bytes, tag = 2)]
    pub blob: Vec<u8>,
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::expressions::col;
    use datafusion::{
        common::DFSchema,
        datasource::file_format::{DefaultFileType, parquet::ParquetFormatFactory},
        logical_expr::{EmptyRelation, LogicalPlan, dml::CopyTo},
        prelude::SessionContext,
    };
    use datafusion_proto::{logical_plan::AsLogicalPlan, protobuf::LogicalPlanNode};
    use std::sync::Arc;

    #[tokio::test]
    async fn file_format_serialization_roundtrip() {
        let ctx = SessionContext::new().task_ctx();
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        };
        let file_type =
            Arc::new(DefaultFileType::new(Arc::new(ParquetFormatFactory::new())));
        let original_plan = LogicalPlan::Copy(CopyTo {
            input: Arc::new(LogicalPlan::EmptyRelation(empty)),
            output_url: "/tmp/file".to_string(),
            partition_by: vec![],
            file_type,
            options: Default::default(),
            output_schema: Arc::new(DFSchema::empty()),
        });

        let codec = crate::serde::BallistaLogicalExtensionCodec::default();
        let plan_message =
            LogicalPlanNode::try_from_logical_plan(&original_plan, &codec).unwrap();

        let mut buf: Vec<u8> = vec![];
        plan_message.try_encode(&mut buf).unwrap();
        println!("{}", original_plan.display_indent());

        let decoded_message = LogicalPlanNode::try_decode(&buf).unwrap();
        let decoded_plan = decoded_message.try_into_logical_plan(&ctx, &codec).unwrap();

        println!("{}", decoded_plan.display_indent());
        let o = original_plan.display_indent();
        let d = decoded_plan.display_indent();

        assert_eq!(o.to_string(), d.to_string())
        //logical_plan.
    }

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[tokio::test]
    async fn test_unresolved_shuffle_exec_roundtrip() {
        let schema = create_test_schema();
        let partitioning =
            Partitioning::Hash(vec![col("id", schema.as_ref()).unwrap()], 4);

        let original_exec = UnresolvedShuffleExec::new(
            1, // stage_id
            schema.clone(),
            partitioning.clone(),
        );

        let codec = BallistaPhysicalExtensionCodec::default();
        let mut buf: Vec<u8> = vec![];
        codec
            .try_encode(Arc::new(original_exec.clone()), &mut buf)
            .unwrap();

        let ctx = SessionContext::new().task_ctx();
        let decoded_plan = codec.try_decode(&buf, &[], &ctx).unwrap();

        let decoded_exec = decoded_plan
            .as_any()
            .downcast_ref::<UnresolvedShuffleExec>()
            .expect("Expected UnresolvedShuffleExec");

        assert_eq!(decoded_exec.stage_id, 1);
        assert_eq!(decoded_exec.schema().as_ref(), schema.as_ref());
        assert_eq!(&decoded_exec.properties().partitioning, &partitioning);
    }

    #[tokio::test]
    async fn test_shuffle_reader_exec_roundtrip() {
        let schema = create_test_schema();
        let partitioning =
            Partitioning::Hash(vec![col("id", schema.as_ref()).unwrap()], 4);

        let original_exec = ShuffleReaderExec::try_new(
            1, // stage_id
            Vec::new(),
            schema.clone(),
            partitioning.clone(),
        )
        .unwrap();

        let codec = BallistaPhysicalExtensionCodec::default();
        let mut buf: Vec<u8> = vec![];
        codec
            .try_encode(Arc::new(original_exec.clone()), &mut buf)
            .unwrap();

        let ctx = SessionContext::new().task_ctx();
        let decoded_plan = codec.try_decode(&buf, &[], &ctx).unwrap();

        let decoded_exec = decoded_plan
            .as_any()
            .downcast_ref::<ShuffleReaderExec>()
            .expect("Expected ShuffleReaderExec");

        assert_eq!(decoded_exec.stage_id, 1);
        assert_eq!(decoded_exec.schema().as_ref(), schema.as_ref());
        assert_eq!(&decoded_exec.properties().partitioning, &partitioning);
    }
}
