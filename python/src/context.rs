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

use pyo3::prelude::*;

use ballista::prelude::*;
use datafusion::prelude::*;
use datafusion_python::context::PySessionContext as DataFusionPythonSessionContext;
use datafusion_python::utils::wait_for_future;

/// PyBallista session context. This SessionContext will
/// inherit from a Python Object, specifically the DataFusion
/// Python SessionContext. This will allow us to only need to
/// define whether the Context is operating locally or remotely
/// and all the functions for the SessionContext can be defined
/// in the Python Class.
#[pyclass(name = "SessionContext", module = "pyballista", subclass)]
pub struct PySessionContext {
    /// Inherit the Datafusion Python Session Context from your
    /// Python Runtime
    pub(crate) py_ctx: PyObject,
}

/// We only need to provide the cluster type to the SessionContext
/// since all of the methods will inherit form the DataFusion
/// Python library
#[pymethods]
impl PySessionContext {
    /// Provide the Python DataFusion SessionContext to
    /// the PySessionContext struct
    #[new]
    pub fn new(session_ctx: PyObject) -> PyResult<Self> {
        Ok(Self {
            py_ctx: session_ctx,
        })
    }

    /// Provide Context for local execution
    #[staticmethod]
    pub fn local(py: Python) -> PyResult<DataFusionPythonSessionContext> {
        let session_context = SessionContext::standalone();
        let ctx = wait_for_future(py, session_context)?;
        Ok(ctx.into())
    }

    /// Provide Context for remote execution
    #[staticmethod]
    pub fn remote(py: Python, url: &str) -> PyResult<DataFusionPythonSessionContext> {
        let session_context = SessionContext::remote(url);
        let ctx = wait_for_future(py, session_context)?;
        Ok(ctx.into())
    }
}
