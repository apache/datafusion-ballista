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

//! Sends Iceberg scans from the scheduler to executors.

use std::sync::Arc;

use super::{IcebergScanCodec, PREFIX};
use crate::serde::BallistaPhysicalExtensionCodec;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};

/// Opt-in Iceberg scan framing with SDK operations supplied by `T`.
/// Unhandled operations are forwarded to the session's existing codec.
#[derive(Debug)]
pub struct IcebergPhysicalCodec<T> {
    integration: T,
    inner: Arc<dyn PhysicalExtensionCodec>,
}

impl<T: IcebergScanCodec> IcebergPhysicalCodec<T> {
    /// Creates a codec that delegates everything but Iceberg scans to `inner`.
    pub fn new(inner: Arc<dyn PhysicalExtensionCodec>, integration: T) -> Self {
        Self { inner, integration }
    }
}

impl<T: IcebergScanCodec + Default> Default for IcebergPhysicalCodec<T> {
    fn default() -> Self {
        Self::new(
            Arc::new(BallistaPhysicalExtensionCodec::default()),
            T::default(),
        )
    }
}

impl<T: IcebergScanCodec> PhysicalExtensionCodec for IcebergPhysicalCodec<T> {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(payload) = buf.strip_prefix(PREFIX) else {
            return self.inner.try_decode(buf, inputs, ctx, proto_converter);
        };
        self.integration.decode(payload, ctx)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(payload) = self.integration.try_encode(node.as_ref())? else {
            return self.inner.try_encode(node, buf, proto_converter);
        };
        buf.extend_from_slice(PREFIX);
        buf.extend_from_slice(&payload);
        Ok(())
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.try_decode_expr(buf, inputs, ctx)
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        self.inner.try_encode_expr(node, buf, ctx)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        self.inner.try_decode_higher_order_function(name, buf)
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_higher_order_function(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}
