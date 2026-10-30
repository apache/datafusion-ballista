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

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use iceberg::expr::Predicate;
use iceberg_datafusion::read::IcebergDataSource;
use serde::{Deserialize, Serialize};

use crate::wire::{self, Frame, TableWire};

#[derive(Debug, Serialize, Deserialize)]
struct ScanWire {
    table: TableWire,
    snapshot_id: Option<i64>,
    /// The scan's output schema, already projected.
    schema: SchemaRef,
    predicate: Option<Predicate>,
    fetch: Option<usize>,
}

/// A [`PhysicalExtensionCodec`] for scans of an [`IcebergDataSource`] that
/// delegates everything else to an inner codec.
#[derive(Debug)]
pub struct IcebergPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
}

impl IcebergPhysicalCodec {
    /// Creates a codec that delegates everything but Iceberg scans to `inner`.
    pub fn new(inner: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { inner }
    }
}

impl Default for IcebergPhysicalCodec {
    fn default() -> Self {
        Self::new(Arc::new(BallistaPhysicalExtensionCodec::default()))
    }
}

impl PhysicalExtensionCodec for IcebergPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan: ScanWire = match wire::decode(buf)? {
            Frame::Iceberg(scan) => scan,
            Frame::Delegated(rest) => {
                return self.inner.try_decode(rest, inputs, ctx, proto_converter);
            }
        };
        let source = IcebergDataSource::builder(scan.table.load()?, scan.schema)
            .with_snapshot_id(scan.snapshot_id)
            .with_predicate(scan.predicate)
            .with_fetch(scan.fetch)
            .build()?;
        Ok(DataSourceExec::from_data_source(source))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let source = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<IcebergDataSource>());
        match source {
            Some(source) => wire::encode(
                buf,
                &ScanWire {
                    table: TableWire::new(source.table())?,
                    snapshot_id: source.snapshot_id(),
                    schema: source.schema(),
                    predicate: source.predicate().cloned(),
                    fetch: source.fetch(),
                },
            ),
            None => self.inner.try_encode(node, buf, proto_converter),
        }
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
