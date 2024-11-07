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
use pyo3::types::{IntoPyDict, PyDict};
use std::collections::HashMap;

use pyo3::prelude::*;
mod utils;

#[pymodule]
fn ballista_internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    // Ballista structs
    m.add_class::<PyBallista>()?;
    // DataFusion structs
    m.add_class::<datafusion_python::dataframe::PyDataFrame>()?;
    Ok(())
}

#[pyclass(name = "Ballista", module = "ballista", subclass)]
pub struct PyBallista(pub Option<HashMap<String, String>>);

#[pymethods]
impl PyBallista {
    #[staticmethod]
    #[pyo3(signature = (config=None))]
    pub fn config(config: Option<HashMap<String, String>>) -> Self {
        if let Some(conf) = config {
            Self(Some(conf))
        } else {
            Self(None)
        }
    }
    
    #[staticmethod]
    /// Construct the standalone instance from the SessionContext
    pub fn standalone(py: Python) -> PyResult<DataFusionPythonSessionContext> {
        // Define the SessionContext
        let session_context = SessionContext::standalone();
        // SessionContext is an async function
        let ctx = wait_for_future(py, session_context).unwrap();
        
        // Convert the SessionContext into a Python SessionContext
        Ok(ctx.into())
    }
    
    #[staticmethod]
    /// Construct the remote instance from the SessionContext
    pub fn remote(url: &str, py: Python) -> PyResult<DataFusionPythonSessionContext> {
        let session_context = SessionContext::remote(url);
        let ctx = wait_for_future(py, session_context)?;
        
        // Convert the SessionContext into a Python SessionContext
        Ok(ctx.into())
    }
}