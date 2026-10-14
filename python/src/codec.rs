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

//! Extension codecs used by the Python FFI query planner.
//!
//! They delegate to Ballista's default codecs and add support for
//! [DistributedQueryExec], which is only serialized when plans cross the
//! datafusion-python FFI boundary.
//!
//! They are also where Python-specific serde will live. In the future they
//! will encode and decode things Ballista's default codecs do not know about,
//! such as Python UDFs (scalar, aggregate and window functions), Python table
//! providers and similar Python-defined extensions. Anything that cannot be
//! handled here falls back to Ballista's default codecs.

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::DistributedQueryExec;
use ballista_core::serde::protobuf::{
    self, ballista_physical_plan_node::PhysicalPlanType,
};
use ballista_core::serde::{
    BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::config::{ConfigExtension, ExtensionOptions};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::LogicalPlanNode;
use prost::Message;
use std::sync::Arc;

/// Logical extension codec delegating to [BallistaLogicalExtensionCodec].
///
/// Currently a pass-through; it is the extension point for future
/// Python-specific logical serde, such as Python UDFs and Python table
/// providers.
#[derive(Debug, Default)]
pub(crate) struct PyBallistaLogicalExtensionCodec {
    inner: BallistaLogicalExtensionCodec,
}

impl LogicalExtensionCodec for PyBallistaLogicalExtensionCodec {
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
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
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
}

/// Physical extension codec delegating to [BallistaPhysicalExtensionCodec],
/// additionally handling [DistributedQueryExec].
///
/// It is the extension point for future Python-specific physical serde, such
/// as Python UDFs referenced from physical expressions.
#[derive(Debug)]
pub(crate) struct PyBallistaPhysicalExtensionCodec {
    inner: BallistaPhysicalExtensionCodec,
    /// Codec used for the logical plan embedded in [DistributedQueryExec]
    logical_codec: Arc<dyn LogicalExtensionCodec>,
}

impl Default for PyBallistaPhysicalExtensionCodec {
    fn default() -> Self {
        Self::new(Arc::new(PyBallistaLogicalExtensionCodec::default()))
    }
}

impl PyBallistaPhysicalExtensionCodec {
    pub(crate) fn new(logical_codec: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self {
            inner: BallistaPhysicalExtensionCodec::default(),
            logical_codec,
        }
    }

    fn decode_distributed_query(
        &self,
        node: &protobuf::DistributedQueryExecNode,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = LogicalPlanNode::try_decode(&node.logical_plan)
            .and_then(|p| p.try_into_logical_plan(ctx, self.logical_codec.as_ref()))
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to decode DistributedQueryExec logical plan: {e:?}"
                ))
            })?;
        let mut config = BallistaConfig::default();
        for kv in &node.settings {
            if let Some(value) = &kv.value {
                // Settings are encoded with their full key, `set` expects
                // the key without the Ballista prefix
                let key = kv
                    .key
                    .strip_prefix(BallistaConfig::PREFIX)
                    .and_then(|k| k.strip_prefix('.'))
                    .unwrap_or(&kv.key);
                config.set(key, value)?;
            }
        }
        Ok(Arc::new(
            DistributedQueryExec::<LogicalPlanNode>::with_extension(
                node.scheduler_url.clone(),
                config,
                plan,
                Arc::clone(&self.logical_codec),
                node.session_id.clone(),
            ),
        ))
    }

    fn encode_distributed_query(
        &self,
        exec: &DistributedQueryExec<LogicalPlanNode>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let mut logical_plan = vec![];
        LogicalPlanNode::try_from_logical_plan(exec.plan(), self.logical_codec.as_ref())
            .and_then(|p| p.try_encode(&mut logical_plan))
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode DistributedQueryExec logical plan: {e:?}"
                ))
            })?;
        let settings = exec
            .config()
            .settings()
            .iter()
            .map(|(key, value)| protobuf::KeyValuePair {
                key: key.clone(),
                value: Some(value.clone()),
            })
            .collect();
        let proto = protobuf::BallistaPhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::DistributedQuery(
                protobuf::DistributedQueryExecNode {
                    scheduler_url: exec.scheduler_url().to_string(),
                    settings,
                    logical_plan,
                    session_id: exec.session_id().to_string(),
                },
            )),
        };
        proto.encode(buf).map_err(|e| {
            DataFusionError::Internal(format!(
                "failed to encode DistributedQueryExec: {e:?}"
            ))
        })
    }
}

impl PhysicalExtensionCodec for PyBallistaPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Ok(protobuf::BallistaPhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::DistributedQuery(node)),
        }) = protobuf::BallistaPhysicalPlanNode::decode(buf)
        {
            return self.decode_distributed_query(&node, ctx);
        }
        self.inner.try_decode(buf, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if let Some(exec) = node.downcast_ref::<DistributedQueryExec<LogicalPlanNode>>() {
            return self.encode_distributed_query(exec, buf);
        }
        self.inner.try_encode(node, buf, proto_converter)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use ballista_core::execution_plans::UnresolvedShuffleExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::Partitioning;
    use datafusion::prelude::SessionContext;
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;

    #[tokio::test]
    async fn test_distributed_query_exec_roundtrip() {
        let ctx = SessionContext::new();
        let plan = ctx
            .sql("SELECT 1 AS a, 'x' AS b")
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let mut config = BallistaConfig::default();
        config.set("job.name", "roundtrip").unwrap();

        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(DistributedQueryExec::<LogicalPlanNode>::new(
                "df://localhost:50050".to_string(),
                config.clone(),
                plan.clone(),
                "session-1".to_string(),
            ));

        let codec = PyBallistaPhysicalExtensionCodec::default();
        let converter = DefaultPhysicalProtoConverter {};
        let mut buf = vec![];
        codec.try_encode(exec, &mut buf, &converter).unwrap();
        let decoded = codec
            .try_decode(&buf, &[], &ctx.task_ctx(), &converter)
            .unwrap();

        let decoded = decoded
            .downcast_ref::<DistributedQueryExec<LogicalPlanNode>>()
            .expect("decoded plan should be DistributedQueryExec");
        assert_eq!(decoded.scheduler_url(), "df://localhost:50050");
        assert_eq!(decoded.session_id(), "session-1");
        assert_eq!(decoded.config(), &config);
        assert_eq!(decoded.plan(), &plan);
    }

    #[tokio::test]
    async fn test_delegates_ballista_plans() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let exec: Arc<dyn ExecutionPlan> = Arc::new(UnresolvedShuffleExec::new(
            7,
            schema.clone(),
            Partitioning::UnknownPartitioning(3),
        ));

        let codec = PyBallistaPhysicalExtensionCodec::default();
        let converter = DefaultPhysicalProtoConverter {};
        let mut buf = vec![];
        codec.try_encode(exec, &mut buf, &converter).unwrap();
        let decoded = codec
            .try_decode(&buf, &[], &ctx.task_ctx(), &converter)
            .unwrap();

        let decoded = decoded
            .downcast_ref::<UnresolvedShuffleExec>()
            .expect("decoded plan should be UnresolvedShuffleExec");
        assert_eq!(decoded.stage_id, 7);
        assert_eq!(decoded.schema(), schema);
    }
}
