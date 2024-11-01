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

use ballista::prelude::*;
use datafusion::prelude::*;
use datafusion_python::context::PySessionContext as DataFusionPythonSessionContext;
use datafusion_python::utils::wait_for_future;
use pyo3::prelude::*;
mod utils;

#[pymodule]
fn pyballista_internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    // Ballista structs
    m.add_class::<PyStandaloneBallista>()?;
    m.add_class::<PyRemoteBallista>()?;
    // DataFusion structs
    m.add_class::<datafusion_python::dataframe::PyDataFrame>()?;
    Ok(())
}

#[pyclass(name = "StandaloneBallista", module = "pyballista", subclass)]
pub struct PyStandaloneBallista;

#[pymethods]
impl PyStandaloneBallista {
    #[staticmethod]
    pub fn build(py: Python) -> PyResult<DataFusionPythonSessionContext> {
        let session_context = SessionContext::standalone();
        let ctx = wait_for_future(py, session_context)?;
        Ok(ctx.into())
    }
}

#[pyclass(name = "RemoteBallista", module = "pyballista", subclass)]
pub struct PyRemoteBallista;

#[pymethods]
impl PyRemoteBallista {
    #[staticmethod]
    pub fn build(url: &str, py: Python) -> PyResult<DataFusionPythonSessionContext> {
        let session_context = SessionContext::remote(url);
        let ctx = wait_for_future(py, session_context)?;

        Ok(ctx.into())
    }
}
