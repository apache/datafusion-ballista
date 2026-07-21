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

use crate::extension::BallistaCacheNode;
use crate::{error::BallistaError, serde::scheduler::Action as BallistaAction};

use arrow_flight::sql::ProstMessageExt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Extension;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::logical_plan::file_formats::{
    ArrowLogicalExtensionCodec, AvroLogicalExtensionCodec, CsvLogicalExtensionCodec,
    JsonLogicalExtensionCodec, ParquetLogicalExtensionCodec,
};
use datafusion_proto::physical_plan::from_proto::parse_physical_sort_exprs;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_physical_sort_exprs;
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
    PhysicalPlanDecodeContext,
};
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
    ChaosExec, CoalescePlan, PartitionGroup, RuntimeStatsExec, ShuffleReaderExec,
    ShuffleWriterExec, SortShuffleWriterExec, UnresolvedShuffleExec,
};
use crate::serde::protobuf::{
    ballista_logical_plan_node::LogicalPlanType,
    ballista_physical_plan_node::PhysicalPlanType,
};
use crate::serde::scheduler::PartitionLocation;
pub use generated::ballista as protobuf;

/// Generated protobuf code from Ballista protocol definitions.
pub mod generated;
/// Scheduler-specific serialization types and conversions.
pub mod scheduler;

// ============================ CoalescePlan codec ============================
//
// Native ↔ proto conversions for `CoalescePlan` and `PartitionGroup`. Borrow-
// based on the encode side because the call site only has a borrow
// (`exec.coalesce.as_ref()`); the `Vec<u32>` clone is intentional and cheap
// for typical K (small post-coalesce partition counts).

impl From<&protobuf::PartitionGroup> for PartitionGroup {
    fn from(p: &protobuf::PartitionGroup) -> Self {
        Self {
            upstream_indices: p.upstream_indices.clone(),
        }
    }
}

impl From<&PartitionGroup> for protobuf::PartitionGroup {
    fn from(p: &PartitionGroup) -> Self {
        Self {
            upstream_indices: p.upstream_indices.clone(),
        }
    }
}

impl From<&protobuf::CoalescePlan> for CoalescePlan {
    fn from(p: &protobuf::CoalescePlan) -> Self {
        Self {
            upstream_partition_count: p.upstream_partition_count,
            groups: p.groups.iter().map(PartitionGroup::from).collect(),
        }
    }
}

impl From<&CoalescePlan> for protobuf::CoalescePlan {
    fn from(p: &CoalescePlan) -> Self {
        Self {
            upstream_partition_count: p.upstream_partition_count,
            groups: p.groups.iter().map(Into::into).collect(),
        }
    }
}

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
        let plan = protobuf::BallistaLogicalPlanNode::decode(buf)
            .ok()
            .and_then(|node| node.logical_plan_type);

        let Some(plan) = plan else {
            return self.default_codec.try_decode(buf, inputs, ctx);
        };

        match plan {
            LogicalPlanType::CacheNode(plan_cache) => Ok(Extension {
                node: Arc::new(BallistaCacheNode::new(
                    plan_cache.cache_id,
                    plan_cache.session_id,
                    inputs
                        .first()
                        .ok_or(DataFusionError::Plan(
                            "expected input size of 1".to_string(),
                        ))?
                        .clone(),
                )),
            }),
        }
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(node) = node.node.as_any().downcast_ref::<BallistaCacheNode>() {
            let proto = protobuf::BallistaLogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CacheNode(
                    protobuf::LogicalPlanCacheNode {
                        cache_id: node.cache_id().to_owned(),
                        session_id: node.session_id().to_owned(),
                    },
                )),
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode cache node logical plan: {e:?}"
                ))
            })?;

            Ok(())
        } else {
            self.default_codec.try_encode(node, buf)
        }
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
        let converter = DefaultPhysicalProtoConverter {};
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        match ballista_plan {
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input = inputs[0].clone();

                let shuffle_output_partitioning = parse_protobuf_hash_partitioning(
                    shuffle_writer.output_partitioning.as_ref(),
                    &decode_ctx,
                    input.schema().as_ref(),
                    &converter,
                )?;

                Ok(Arc::new(ShuffleWriterExec::try_new(
                    shuffle_writer.job_id.clone().into(),
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
                    &decode_ctx,
                    input.schema().as_ref(),
                    &converter,
                )?;

                let partitioning = shuffle_output_partitioning.ok_or_else(|| {
                    DataFusionError::Internal(
                        "SortShuffleWriterExec requires hash partitioning".to_string(),
                    )
                })?;

                let batch_size = if sort_shuffle_writer.batch_size > 0 {
                    sort_shuffle_writer.batch_size as usize
                } else {
                    8192
                };
                let mut config = SortShuffleConfig::new(true, batch_size);
                // Absent (legacy plan) keeps the built-in default; a present
                // value — including 0, which disables the per-task budget —
                // is applied verbatim so the session override reaches the
                // executor where the writer actually runs.
                if let Some(bytes) = sort_shuffle_writer.memory_limit_per_task_bytes {
                    config = config.with_memory_limit_per_task_bytes(bytes as usize);
                }

                Ok(Arc::new(SortShuffleWriterExec::try_new(
                    sort_shuffle_writer.job_id.clone().into(),
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
                    &decode_ctx,
                    schema.as_ref(),
                    &converter,
                )?;
                let partitioning = partitioning
                    .ok_or_else(|| proto_error("missing required partitioning field"))?;
                let exec = if let Some(c) = shuffle_reader.coalesce.as_ref() {
                    ShuffleReaderExec::try_new_coalesced(
                        stage_id,
                        partition_location,
                        CoalescePlan::from(c),
                        schema,
                        partitioning,
                    )?
                } else if shuffle_reader.broadcast {
                    let all_locations = partition_location
                        .into_iter()
                        .next()
                        .ok_or_else(|| proto_error(
                            "broadcast ShuffleReaderExec: expected exactly one partition in proto"
                        ))?;
                    ShuffleReaderExec::try_new_broadcast(
                        stage_id,
                        all_locations,
                        schema,
                        shuffle_reader.upstream_partition_count as usize,
                    )?
                } else {
                    ShuffleReaderExec::try_new(
                        stage_id,
                        partition_location,
                        schema,
                        partitioning,
                    )?
                };
                Ok(Arc::new(exec))
            }
            PhysicalPlanType::UnresolvedShuffle(unresolved_shuffle) => {
                let schema: SchemaRef =
                    Arc::new(convert_required!(unresolved_shuffle.schema)?);
                let partitioning = parse_protobuf_partitioning(
                    unresolved_shuffle.partitioning.as_ref(),
                    &decode_ctx,
                    schema.as_ref(),
                    &converter,
                )?;
                let partitioning = partitioning
                    .ok_or_else(|| proto_error("missing required partitioning field"))?;
                let exec = if let Some(c) = unresolved_shuffle.coalesce.as_ref() {
                    UnresolvedShuffleExec::new_coalesced(
                        unresolved_shuffle.stage_id as usize,
                        schema,
                        partitioning,
                        CoalescePlan::from(c),
                    )
                } else if unresolved_shuffle.broadcast {
                    UnresolvedShuffleExec::new_broadcast(
                        unresolved_shuffle.stage_id as usize,
                        schema,
                        unresolved_shuffle.upstream_partition_count as usize,
                    )
                } else {
                    UnresolvedShuffleExec::new(
                        unresolved_shuffle.stage_id as usize,
                        schema,
                        partitioning,
                    )
                };
                Ok(Arc::new(exec))
            }
            PhysicalPlanType::ChaosExec(chaos_exec) => {
                let input = match inputs {
                    [input] => input.clone(),
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "ChaosExec expects exactly 1 input, got {}",
                            inputs.len()
                        )));
                    }
                };
                Ok(Arc::new(ChaosExec::new(
                    input,
                    chaos_exec.failure_probability,
                    &chaos_exec.fault_type,
                    Some(chaos_exec.seed),
                )?))
            }
            PhysicalPlanType::RuntimeStats(node) => {
                let [input] = inputs else {
                    return Err(DataFusionError::Internal(format!(
                        "RuntimeStatsExec expects exactly 1 input, got {}",
                        inputs.len()
                    )));
                };
                // Empty proto vec means row-count-only mode; a non-empty
                // vec is the ORDER BY that drives the quantile sketch.
                let order_by = if node.order_by.is_empty() {
                    None
                } else {
                    Some(parse_physical_sort_exprs(
                        &node.order_by,
                        &decode_ctx,
                        input.schema().as_ref(),
                        &converter,
                    )?)
                };
                Ok(Arc::new(RuntimeStatsExec::try_new(
                    input.clone(),
                    order_by,
                )?))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(exec) = node.downcast_ref::<ShuffleWriterExec>() {
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
        } else if let Some(exec) = node.downcast_ref::<SortShuffleWriterExec>() {
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
                        batch_size: config.batch_size as u64,
                        memory_limit_per_task_bytes: Some(
                            config.memory_limit_per_task_bytes as u64,
                        ),
                    },
                )),
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode sort shuffle writer execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.downcast_ref::<ShuffleReaderExec>() {
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
            let converter = DefaultPhysicalProtoConverter {};
            let partitioning = serialize_partitioning(
                &exec.properties().partitioning,
                self.default_codec.as_ref(),
                &converter,
            )?;
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleReader(
                    protobuf::ShuffleReaderExecNode {
                        stage_id,
                        partition,
                        schema: Some(exec.schema().as_ref().try_into()?),
                        partitioning: Some(partitioning),
                        broadcast: exec.broadcast,
                        upstream_partition_count: exec.upstream_partition_count as u32,
                        coalesce: exec.coalesce.as_ref().map(|c| c.into()),
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode shuffle reader execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.downcast_ref::<UnresolvedShuffleExec>() {
            let converter = DefaultPhysicalProtoConverter {};
            let partitioning = serialize_partitioning(
                &exec.properties().partitioning,
                self.default_codec.as_ref(),
                &converter,
            )?;
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::UnresolvedShuffle(
                    protobuf::UnresolvedShuffleExecNode {
                        stage_id: exec.stage_id as u32,
                        schema: Some(exec.schema().as_ref().try_into()?),
                        partitioning: Some(partitioning),
                        broadcast: exec.broadcast,
                        upstream_partition_count: exec.upstream_partition_count as u32,
                        coalesce: exec.coalesce.as_ref().map(|c| c.into()),
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode unresolved shuffle execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.downcast_ref::<ChaosExec>() {
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ChaosExec(
                    protobuf::ChaosExecNode {
                        failure_probability: exec.failure_probability(),
                        fault_type: exec.fault_type().to_string(),
                        seed: exec.seed(),
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode chaos monkey execution plan: {e:?}"
                ))
            })?;
            Ok(())
        } else if let Some(exec) = node.downcast_ref::<RuntimeStatsExec>() {
            let converter = DefaultPhysicalProtoConverter {};
            // Empty vec on the wire when the operator is in
            // row-count-only mode; otherwise serialise the full ORDER BY.
            let order_by = match exec.order_by() {
                Some(exprs) => serialize_physical_sort_exprs(
                    exprs.iter().cloned(),
                    self.default_codec.as_ref(),
                    &converter,
                )?,
                None => Vec::new(),
            };
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::RuntimeStats(
                    protobuf::RuntimeStatsExecNode { order_by },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode RuntimeStatsExec: {e:?}"
                ))
            })?;
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "Unsupported plan node, name: [{}] ",
                node.name()
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
    use crate::execution_plans::PartitionGroup;
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

    // Regression coverage for #1838 and the removed `make_filter_projection_serde_safe`
    // workaround: a `FilterExec` that projects to zero columns must survive physical
    // plan serialization with its empty projection intact. datafusion-proto 53.1.0
    // could not distinguish `Some(vec![])` (empty projection) from `None` (full
    // projection) and decoded the former back as `None`, shifting column indices.
    // DataFusion 54 preserves the distinction, so no Ballista-side rewrite is needed.
    #[tokio::test]
    async fn filter_exec_empty_projection_survives_physical_serde() {
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::expressions::lit;
        use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
        use datafusion_proto::physical_plan::{
            AsExecutionPlan, DefaultPhysicalExtensionCodec,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let filter = FilterExecBuilder::new(lit(true), input)
            .apply_projection(Some(vec![]))
            .unwrap()
            .build()
            .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(filter);
        assert_eq!(
            plan.schema().fields().len(),
            0,
            "precondition: filter projects to zero columns"
        );

        let codec = DefaultPhysicalExtensionCodec {};
        let proto = PhysicalPlanNode::try_from_physical_plan(plan, &codec).unwrap();
        let ctx = SessionContext::new().task_ctx();
        let decoded = proto.try_into_physical_plan(&ctx, &codec).unwrap();

        assert_eq!(
            decoded.schema().fields().len(),
            0,
            "decoded FilterExec must still project zero columns"
        );
        let filter = decoded
            .downcast_ref::<FilterExec>()
            .expect("decoded plan must be a FilterExec");
        assert_eq!(
            filter.projection().as_ref().map(|p| p.is_empty()),
            Some(true),
            "empty projection must round-trip as Some(vec![]), not None"
        );
    }

    // `datafusion-proto` encodes an `EmptyExec` as its schema alone, so a
    // multi-partition `EmptyExec` decodes with a single partition (apache/datafusion
    // #23642). `make_empty_exec_serde_safe` in the scheduler works around this by
    // rewriting such nodes into a round-robin `RepartitionExec` before a stage plan
    // goes on the wire.
    //
    // This test pins the upstream behaviour that makes the workaround necessary: when
    // it starts failing, DataFusion preserves the partition count and the workaround
    // can be deleted.
    #[tokio::test]
    async fn empty_exec_partition_count_is_lost_by_datafusion_proto() {
        use datafusion::physical_plan::ExecutionPlanProperties;
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion_proto::physical_plan::{
            AsExecutionPlan, DefaultPhysicalExtensionCodec,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(schema).with_partitions(4));
        assert_eq!(
            plan.output_partitioning().partition_count(),
            4,
            "precondition: EmptyExec reports 4 partitions"
        );

        let codec = DefaultPhysicalExtensionCodec {};
        let proto = PhysicalPlanNode::try_from_physical_plan(plan, &codec).unwrap();
        let ctx = SessionContext::new().task_ctx();
        let decoded = proto.try_into_physical_plan(&ctx, &codec).unwrap();

        assert_eq!(
            decoded.output_partitioning().partition_count(),
            1,
            "EmptyExec partition count is dropped on the wire; if this now decodes as \
             4, apache/datafusion#23642 is fixed and make_empty_exec_serde_safe can go"
        );
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
            .downcast_ref::<UnresolvedShuffleExec>()
            .expect("Expected UnresolvedShuffleExec");

        assert_eq!(decoded_exec.stage_id, 1);
        assert_eq!(decoded_exec.schema().as_ref(), schema.as_ref());
        assert_eq!(&decoded_exec.properties().partitioning, &partitioning);
        assert!(
            decoded_exec.coalesce.is_none(),
            "absent coalesce field must decode to None (codec inertness)"
        );
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
            .downcast_ref::<ShuffleReaderExec>()
            .expect("Expected ShuffleReaderExec");

        assert_eq!(decoded_exec.stage_id, 1);
        assert_eq!(decoded_exec.schema().as_ref(), schema.as_ref());
        assert_eq!(&decoded_exec.properties().partitioning, &partitioning);
        assert!(
            decoded_exec.coalesce.is_none(),
            "absent coalesce field must decode to None (codec inertness)"
        );
    }

    /// The sort shuffle writer's per-task memory budget must survive the
    /// protobuf round trip so a session override reaches the executor where the
    /// writer actually runs (issue #2089). A non-default value and the special
    /// `0` (per-task budget disabled) are both checked.
    #[tokio::test]
    async fn test_sort_shuffle_writer_exec_roundtrip_preserves_memory_limit() {
        use datafusion::physical_plan::empty::EmptyExec;

        for memory_limit in [7 * 1024 * 1024_usize, 0] {
            let schema = create_test_schema();
            let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
            let partitioning =
                Partitioning::Hash(vec![col("id", schema.as_ref()).unwrap()], 4);

            let original_exec = SortShuffleWriterExec::try_new(
                "job1".into(),
                1,
                input.clone(),
                String::new(),
                partitioning.clone(),
                SortShuffleConfig::new(true, 4096)
                    .with_memory_limit_per_task_bytes(memory_limit),
            )
            .unwrap();

            let codec = BallistaPhysicalExtensionCodec::default();
            let mut buf: Vec<u8> = vec![];
            codec.try_encode(Arc::new(original_exec), &mut buf).unwrap();

            let ctx = SessionContext::new().task_ctx();
            let decoded_plan = codec.try_decode(&buf, &[input], &ctx).unwrap();
            let decoded_exec = decoded_plan
                .downcast_ref::<SortShuffleWriterExec>()
                .expect("Expected SortShuffleWriterExec");

            assert_eq!(
                decoded_exec.config().memory_limit_per_task_bytes,
                memory_limit,
                "memory_limit_per_task_bytes must survive the wire, including 0"
            );
            assert_eq!(decoded_exec.config().batch_size, 4096);
        }
    }

    #[tokio::test]
    async fn test_shuffle_reader_exec_coalesced_roundtrip_single_group() {
        let schema = create_test_schema();
        let partitioning =
            Partitioning::Hash(vec![col("id", schema.as_ref()).unwrap()], 1);
        let coalesce = CoalescePlan {
            upstream_partition_count: 4,
            groups: vec![PartitionGroup {
                upstream_indices: vec![0, 1, 2, 3],
            }],
        };

        let original_exec = ShuffleReaderExec::try_new_coalesced(
            7,
            vec![vec![]; 1], // K-shape: 1 output partition
            coalesce.clone(),
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
            .downcast_ref::<ShuffleReaderExec>()
            .expect("Expected ShuffleReaderExec");

        assert_eq!(decoded_exec.stage_id, 7);
        assert_eq!(&decoded_exec.properties().partitioning, &partitioning);
        let stored = decoded_exec
            .coalesce
            .as_ref()
            .expect("coalesce must round-trip");
        assert_eq!(stored, &coalesce);
        assert_eq!(stored.upstream_partition_count, 4);
        assert_eq!(stored.groups.len(), 1);
        assert_eq!(stored.groups[0].upstream_indices, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_shuffle_reader_exec_coalesced_roundtrip_multi_group_mixed_sizes() {
        let schema = create_test_schema();
        let partitioning =
            Partitioning::Hash(vec![col("id", schema.as_ref()).unwrap()], 3);
        let coalesce = CoalescePlan {
            upstream_partition_count: 8,
            groups: vec![
                PartitionGroup {
                    upstream_indices: vec![0, 1, 2],
                },
                PartitionGroup {
                    upstream_indices: vec![3, 4],
                },
                PartitionGroup {
                    upstream_indices: vec![5, 6, 7],
                },
            ],
        };

        let original_exec = ShuffleReaderExec::try_new_coalesced(
            1,
            vec![vec![]; 3], // K-shape: 3 output partitions
            coalesce.clone(),
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
            .downcast_ref::<ShuffleReaderExec>()
            .expect("Expected ShuffleReaderExec");

        let stored = decoded_exec
            .coalesce
            .as_ref()
            .expect("coalesce must round-trip");
        assert_eq!(stored, &coalesce);
        assert_eq!(stored.upstream_partition_count, 8);
        assert_eq!(stored.groups.len(), 3);
    }

    #[tokio::test]
    async fn test_unresolved_shuffle_exec_coalesced_roundtrip_multi_index() {
        let schema = create_test_schema();
        let partitioning =
            Partitioning::Hash(vec![col("id", schema.as_ref()).unwrap()], 2);
        let coalesce = CoalescePlan {
            upstream_partition_count: 5,
            groups: vec![
                PartitionGroup {
                    upstream_indices: vec![0, 1, 2, 3],
                },
                PartitionGroup {
                    upstream_indices: vec![4],
                },
            ],
        };

        let original_exec = UnresolvedShuffleExec::new_coalesced(
            9,
            schema.clone(),
            partitioning.clone(),
            coalesce.clone(),
        );

        let codec = BallistaPhysicalExtensionCodec::default();
        let mut buf: Vec<u8> = vec![];
        codec
            .try_encode(Arc::new(original_exec.clone()), &mut buf)
            .unwrap();

        let ctx = SessionContext::new().task_ctx();
        let decoded_plan = codec.try_decode(&buf, &[], &ctx).unwrap();
        let decoded_exec = decoded_plan
            .downcast_ref::<UnresolvedShuffleExec>()
            .expect("Expected UnresolvedShuffleExec");

        assert_eq!(decoded_exec.stage_id, 9);
        let stored = decoded_exec
            .coalesce
            .as_ref()
            .expect("coalesce must round-trip");
        assert_eq!(stored, &coalesce);
        assert_eq!(stored.upstream_partition_count, 5);
        assert_eq!(stored.groups[0].upstream_indices.len(), 4);
    }

    #[tokio::test]
    async fn test_shuffle_reader_exec_coalesced_roundtrip_non_contiguous_indices() {
        // Proto allows arbitrary upstream_indices sets even though the default
        // algorithm only emits contiguous ranges.
        let schema = create_test_schema();
        let partitioning =
            Partitioning::Hash(vec![col("id", schema.as_ref()).unwrap()], 2);
        let coalesce = CoalescePlan {
            upstream_partition_count: 6,
            groups: vec![
                PartitionGroup {
                    upstream_indices: vec![0, 2, 4],
                },
                PartitionGroup {
                    upstream_indices: vec![1, 3, 5],
                },
            ],
        };

        let original_exec = ShuffleReaderExec::try_new_coalesced(
            3,
            vec![vec![]; 2],
            coalesce.clone(),
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
            .downcast_ref::<ShuffleReaderExec>()
            .expect("Expected ShuffleReaderExec");

        let stored = decoded_exec
            .coalesce
            .as_ref()
            .expect("coalesce must round-trip");
        assert_eq!(stored, &coalesce);
        // Non-contiguous indices preserved bit-for-bit:
        assert_eq!(stored.groups[0].upstream_indices, vec![0, 2, 4]);
        assert_eq!(stored.groups[1].upstream_indices, vec![1, 3, 5]);
    }

    // ---- CoalescePlan native ↔ proto direct conversion round-trips ----

    #[test]
    fn coalesce_plan_native_to_proto_roundtrip_empty() {
        let native = CoalescePlan {
            upstream_partition_count: 0,
            groups: vec![],
        };
        let proto: protobuf::CoalescePlan = (&native).into();
        let back: CoalescePlan = (&proto).into();
        assert_eq!(native, back);
    }

    #[test]
    fn coalesce_plan_native_to_proto_roundtrip_single_group() {
        let native = CoalescePlan {
            upstream_partition_count: 4,
            groups: vec![PartitionGroup {
                upstream_indices: vec![0, 1, 2, 3],
            }],
        };
        let proto: protobuf::CoalescePlan = (&native).into();
        let back: CoalescePlan = (&proto).into();
        assert_eq!(native, back);
    }

    #[test]
    fn coalesce_plan_native_to_proto_roundtrip_multi_group_mixed_sizes() {
        let native = CoalescePlan {
            upstream_partition_count: 8,
            groups: vec![
                PartitionGroup {
                    upstream_indices: vec![0, 1, 2],
                },
                PartitionGroup {
                    upstream_indices: vec![3, 4],
                },
                PartitionGroup {
                    upstream_indices: vec![5, 6, 7],
                },
            ],
        };
        let proto: protobuf::CoalescePlan = (&native).into();
        let back: CoalescePlan = (&proto).into();
        assert_eq!(native, back);
        assert_eq!(back.groups.len(), 3);
        assert_eq!(back.upstream_partition_count, 8);
    }

    #[test]
    fn coalesce_plan_native_to_proto_roundtrip_non_contiguous_indices() {
        // Proto allows arbitrary index sets even though the default algorithm
        // only produces contiguous ranges.
        let native = CoalescePlan {
            upstream_partition_count: 6,
            groups: vec![
                PartitionGroup {
                    upstream_indices: vec![0, 2, 4],
                },
                PartitionGroup {
                    upstream_indices: vec![1, 3, 5],
                },
            ],
        };
        let proto: protobuf::CoalescePlan = (&native).into();
        let back: CoalescePlan = (&proto).into();
        assert_eq!(native, back);
    }

    #[tokio::test]
    async fn test_broadcast_unresolved_shuffle_exec_roundtrip() {
        let schema = create_test_schema();
        let original_exec = UnresolvedShuffleExec::new_broadcast(7, schema.clone(), 4);

        let codec = BallistaPhysicalExtensionCodec::default();
        let mut buf: Vec<u8> = vec![];
        codec
            .try_encode(Arc::new(original_exec.clone()), &mut buf)
            .unwrap();

        let ctx = SessionContext::new().task_ctx();
        let decoded_plan = codec.try_decode(&buf, &[], &ctx).unwrap();

        let decoded_exec = decoded_plan
            .downcast_ref::<UnresolvedShuffleExec>()
            .expect("Expected UnresolvedShuffleExec");

        assert_eq!(decoded_exec.stage_id, 7);
        assert!(decoded_exec.broadcast);
        assert_eq!(decoded_exec.upstream_partition_count, 4);
        assert_eq!(decoded_exec.output_partition_count, 1);
    }

    #[tokio::test]
    async fn test_broadcast_shuffle_reader_exec_roundtrip() {
        let schema = create_test_schema();
        let original_exec =
            ShuffleReaderExec::try_new_broadcast(7, Vec::new(), schema.clone(), 4)
                .unwrap();

        let codec = BallistaPhysicalExtensionCodec::default();
        let mut buf: Vec<u8> = vec![];
        codec
            .try_encode(Arc::new(original_exec.clone()), &mut buf)
            .unwrap();

        let ctx = SessionContext::new().task_ctx();
        let decoded_plan = codec.try_decode(&buf, &[], &ctx).unwrap();

        let decoded_exec = decoded_plan
            .downcast_ref::<ShuffleReaderExec>()
            .expect("Expected ShuffleReaderExec");

        assert_eq!(decoded_exec.stage_id, 7);
        assert!(decoded_exec.broadcast);
        assert_eq!(decoded_exec.upstream_partition_count, 4);
        assert_eq!(decoded_exec.partition.len(), 1);
    }

    /// `RuntimeStatsExec` in sketching mode round-trips its ORDER BY
    /// and keeps the sketch accessor available. Row-count accessor is
    /// always available (and empty on a fresh operator).
    #[tokio::test]
    async fn test_runtime_stats_exec_roundtrip_with_sketch() {
        use crate::execution_plans::RuntimeStatsExec;
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_expr::PhysicalSortExpr;
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![
            Field::new("v2", DataType::Float64, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let sort_expr = PhysicalSortExpr {
            expr: col("v2", schema.as_ref()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        };
        let original =
            RuntimeStatsExec::try_new(input.clone(), Some(vec![sort_expr.clone()]))
                .unwrap();
        // Fresh operator: row-count zero, sketch present but empty.
        // EmptyExec exposes one partition, so partition-0 is the only slot.
        assert_eq!(original.row_count(0).unwrap(), 0);
        assert_eq!(original.total_row_count(), 0);
        assert_eq!(original.quantile_sketch(0).unwrap().unwrap().count(), 0.0);
        assert_eq!(
            original.merged_quantile_sketch().unwrap().unwrap().count(),
            0.0
        );
        // Out-of-range partition surfaces as an internal error, not a panic.
        assert!(original.row_count(1).is_err());
        assert!(original.quantile_sketch(1).is_err());

        let codec = BallistaPhysicalExtensionCodec::default();
        let mut buf: Vec<u8> = vec![];
        codec.try_encode(Arc::new(original), &mut buf).unwrap();

        let ctx = SessionContext::new().task_ctx();
        let decoded_plan = codec.try_decode(&buf, &[input], &ctx).unwrap();

        let decoded = decoded_plan
            .downcast_ref::<RuntimeStatsExec>()
            .expect("Expected RuntimeStatsExec");
        let order_by = decoded
            .order_by()
            .expect("sketching mode preserves order_by");
        assert_eq!(order_by.len(), 1);
        assert_eq!(order_by[0].expr.to_string(), sort_expr.expr.to_string());
        assert!(!order_by[0].options.descending);
        assert_eq!(decoded.row_count(0).unwrap(), 0);
        assert_eq!(decoded.quantile_sketch(0).unwrap().unwrap().count(), 0.0);
        assert_eq!(
            decoded.merged_quantile_sketch().unwrap().unwrap().count(),
            0.0
        );
    }

    /// `RuntimeStatsExec` in row-count-only mode — `order_by = None`.
    /// Sketch accessors return `None`; row-count accessors work.
    #[tokio::test]
    async fn test_runtime_stats_exec_roundtrip_row_count_only() {
        use crate::execution_plans::RuntimeStatsExec;
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "v2",
            DataType::Float64,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let original = RuntimeStatsExec::try_new(input.clone(), None).unwrap();
        assert!(original.order_by().is_none());
        assert_eq!(original.row_count(0).unwrap(), 0);
        assert!(
            original.quantile_sketch(0).unwrap().is_none(),
            "no sketch was requested at construction"
        );
        assert!(original.merged_quantile_sketch().unwrap().is_none());

        let codec = BallistaPhysicalExtensionCodec::default();
        let mut buf: Vec<u8> = vec![];
        codec.try_encode(Arc::new(original), &mut buf).unwrap();

        let ctx = SessionContext::new().task_ctx();
        let decoded_plan = codec.try_decode(&buf, &[input], &ctx).unwrap();
        let decoded = decoded_plan
            .downcast_ref::<RuntimeStatsExec>()
            .expect("Expected RuntimeStatsExec");
        assert!(decoded.order_by().is_none());
        assert!(decoded.quantile_sketch(0).unwrap().is_none());
        assert!(decoded.merged_quantile_sketch().unwrap().is_none());
    }

    /// `try_new` refuses an empty ORDER BY — no routing key means the
    /// downstream sketcher has nothing to sample.
    #[test]
    fn test_runtime_stats_exec_rejects_empty_order_by() {
        use crate::execution_plans::RuntimeStatsExec;
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "v2",
            DataType::Float64,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let err = RuntimeStatsExec::try_new(input, Some(vec![]))
            .expect_err("empty order_by must be rejected");
        assert!(
            err.to_string().contains("order_by is Some but empty"),
            "got: {err}"
        );
    }

    /// `try_new` refuses a routing expression whose evaluated type is
    /// not `Float64` — TDigest can't ingest anything else, so failing
    /// at construction beats a downcast error mid-stream.
    #[test]
    fn test_runtime_stats_exec_rejects_non_float64_routing_expr() {
        use crate::execution_plans::RuntimeStatsExec;
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_expr::PhysicalSortExpr;
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::expressions::col;

        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Float64, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let sort_expr = PhysicalSortExpr {
            expr: col("id", schema.as_ref()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        };
        let err = RuntimeStatsExec::try_new(input, Some(vec![sort_expr]))
            .expect_err("non-Float64 routing expr must be rejected");
        assert!(
            err.to_string()
                .contains("routing expression must be Float64"),
            "got: {err}"
        );
    }

    /// `try_new` refuses a nullable routing expression — TDigest has no
    /// NULL slot, so allowing nulls would silently exclude them from
    /// the sketch while `row_count` still saw them. The KLL swap lifts
    /// this by positioning nulls per `SortOptions::nulls_first`.
    #[test]
    fn test_runtime_stats_exec_rejects_nullable_routing_expr() {
        use crate::execution_plans::RuntimeStatsExec;
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_expr::PhysicalSortExpr;
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::expressions::col;

        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let sort_expr = PhysicalSortExpr {
            expr: col("v", schema.as_ref()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        };
        let err = RuntimeStatsExec::try_new(input, Some(vec![sort_expr]))
            .expect_err("nullable routing expr must be rejected");
        assert!(
            err.to_string()
                .contains("routing expression must be non-nullable"),
            "got: {err}"
        );
    }
}
