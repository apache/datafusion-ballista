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

//! Sends Iceberg tables from the client to the scheduler.

use std::sync::Arc;

use super::{IcebergTableProviderCodec, PREFIX};
use crate::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Result, TableReference};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    AggregateUDF, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;

/// Opt-in Iceberg provider framing with SDK operations supplied by `T`.
/// Unhandled operations are forwarded to the session's existing codec.
#[derive(Debug)]
pub struct IcebergLogicalCodec<T> {
    integration: T,
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl<T: IcebergTableProviderCodec> IcebergLogicalCodec<T> {
    /// Creates a codec that delegates everything but Iceberg tables to `inner`.
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>, integration: T) -> Self {
        Self { inner, integration }
    }
}

impl<T: IcebergTableProviderCodec + Default> Default for IcebergLogicalCodec<T> {
    fn default() -> Self {
        Self::new(
            Arc::new(BallistaLogicalExtensionCodec::default()),
            T::default(),
        )
    }
}

impl<T: IcebergTableProviderCodec> LogicalExtensionCodec for IcebergLogicalCodec<T> {
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
        let Some(payload) = buf.strip_prefix(PREFIX) else {
            return self
                .inner
                .try_decode_table_provider(buf, table_ref, schema, ctx);
        };
        self.integration.decode(payload, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(payload) = self.integration.try_encode(table_ref, node.as_ref())? else {
            return self.inner.try_encode_table_provider(table_ref, node, buf);
        };
        buf.extend_from_slice(PREFIX);
        buf.extend_from_slice(&payload);
        Ok(())
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
