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
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_python::context::PySessionContext;
use datafusion_python::utils::wait_for_future;
use pyo3::prelude::*;
use std::collections::HashMap;
mod utils;

#[pymodule]
fn ballista_internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    // BallistaBuilder struct
    m.add_class::<PyBallistaBuilder>()?;
    // DataFusion struct
    // m.add_class::<datafusion_python::dataframe::PyDataFrame>()?;
    Ok(())
}

#[derive(Debug, Default)]
#[pyclass(name = "Ballista", module = "ballista", subclass)]
pub struct PyBallistaBuilder {
    conf: HashMap<String, String>,
}

#[pymethods]
impl PyBallistaBuilder {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }
    //#[staticmethod]
    #[classattr]
    pub fn builder() -> Self {
        Self::default()
    }

    pub fn config(
        mut slf: PyRefMut<'_, Self>,
        k: &str,
        v: &str,
        py: Python,
    ) -> PyResult<PyObject> {
        slf.conf.insert(k.into(), v.into());

        Ok(slf.into_py(py))
    }

    /// Construct the standalone instance from the SessionContext
    pub fn standalone(&self, py: Python) -> PyResult<PySessionContext> {
        // Build the config
        let mut config: SessionConfig = SessionConfig::new_with_ballista();
        for (key, value) in &self.conf {
            let _ = config.options_mut().set(&key, value);
        }

        // Build the state
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        // Build the context
        let standalone_session = SessionContext::standalone_with_state(state);

        // SessionContext is an async function
        let ctx = wait_for_future(py, standalone_session)?;

        // Convert the SessionContext into a Python SessionContext
        Ok(ctx.into())
    }

    /// Construct the remote instance from the SessionContext
    pub fn remote(&self, url: &str, py: Python) -> PyResult<PySessionContext> {
        // Build the config
        let mut config: SessionConfig = SessionConfig::new_with_ballista();
        for (key, value) in &self.conf {
            let _ = config.options_mut().set(&key, value);
        }
        // Build the state
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        // Build the context
        let remote_session = SessionContext::remote_with_state(url, state);

        // SessionContext is an async function
        let ctx = wait_for_future(py, remote_session)?;

        // Convert the SessionContext into a Python SessionContext
        Ok(ctx.into())
    }
}
