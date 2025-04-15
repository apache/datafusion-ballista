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

use ballista_core::serde::{
    BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use datafusion::logical_expr::ScalarUDF;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use pyo3::types::{PyAnyMethods, PyBytes, PyBytesMethods};
use pyo3::{PyObject, PyResult, Python};
use std::fmt::Debug;
use std::sync::Arc;

static MODULE: &str = "cloudpickle";
static FUN_LOADS: &str = "loads";
static FUN_DUMPS: &str = "dumps";

/// Serde protocol for UD(a)F
#[derive(Debug)]
struct CloudPickle {
    loads: PyObject,
    dumps: PyObject,
}

impl CloudPickle {
    pub fn try_new(py: Python<'_>) -> PyResult<Self> {
        let module = py.import(MODULE)?;
        let loads = module.getattr(FUN_LOADS)?.unbind();
        let dumps = module.getattr(FUN_DUMPS)?.unbind();

        Ok(Self { loads, dumps })
    }

    pub fn pickle(&self, py: Python<'_>, py_any: &PyObject) -> PyResult<Vec<u8>> {
        let b: PyObject = self.dumps.call1(py, (py_any,))?.extract(py)?;
        let blob = b.downcast_bound::<PyBytes>(py)?.clone();

        Ok(blob.as_bytes().to_owned())
    }

    pub fn unpickle(&self, py: Python<'_>, blob: &[u8]) -> PyResult<PyObject> {
        let t: PyObject = self.loads.call1(py, (blob,))?.extract(py)?;

        Ok(t)
    }
}

pub struct PyLogicalCodec {
    inner: BallistaLogicalExtensionCodec,
    cloudpickle: CloudPickle,
}

impl PyLogicalCodec {
    pub fn try_new(py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            inner: BallistaLogicalExtensionCodec::default(),
            cloudpickle: CloudPickle::try_new(py)?,
        })
    }
}

impl Debug for PyLogicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyLogicalCodec").finish()
    }
}

impl LogicalExtensionCodec for PyLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::sql::TableReference,
        schema: datafusion::arrow::datatypes::SchemaRef,
        ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>>
    {
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: std::sync::Arc<dyn datafusion::catalog::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<
        std::sync::Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    > {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: std::sync::Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_file_format(buf, node)
    }

    fn try_decode_udf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> datafusion::error::Result<std::sync::Arc<datafusion::logical_expr::ScalarUDF>>
    {
        // use cloud pickle to decode udf
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(
        &self,
        node: &datafusion::logical_expr::ScalarUDF,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        // use cloud pickle to decode udf
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udaf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> datafusion::error::Result<std::sync::Arc<datafusion::logical_expr::AggregateUDF>>
    {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(
        &self,
        node: &datafusion::logical_expr::AggregateUDF,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> datafusion::error::Result<std::sync::Arc<datafusion::logical_expr::WindowUDF>>
    {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(
        &self,
        node: &datafusion::logical_expr::WindowUDF,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}

pub struct PyPhysicalCodec {
    inner: BallistaPhysicalExtensionCodec,
    cloudpickle: CloudPickle,
}

impl Debug for PyPhysicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyPhysicalCodec").finish()
    }
}

impl PyPhysicalCodec {
    pub fn try_new(py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            inner: BallistaPhysicalExtensionCodec::default(),
            cloudpickle: CloudPickle::try_new(py)?,
        })
    }
}

impl PhysicalExtensionCodec for PyPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> datafusion::error::Result<
        std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    > {
        self.inner.try_decode(buf, inputs, registry)
    }

    fn try_encode(
        &self,
        node: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_udf(
        &self,
        name: &str,
        _buf: &[u8],
    ) -> datafusion::common::Result<Arc<ScalarUDF>> {
        // use cloudpickle here
        datafusion::common::not_impl_err!(
            "PhysicalExtensionCodec is not provided for scalar function {name}"
        )
    }

    fn try_encode_udf(
        &self,
        _node: &ScalarUDF,
        _buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        // use cloudpickle here
        Ok(())
    }
}
