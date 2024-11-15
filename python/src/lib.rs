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

use ballista::extension::SessionConfigExt;
use ballista::prelude::*;
use ballista_core::utils::SessionStateExt;
use datafusion::catalog::Session;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::*;
use datafusion_python::context::PySessionContext as DataFusionPythonSessionContext;
use datafusion_python::utils::wait_for_future;

use std::collections::HashMap;
use std::fmt::Formatter;
use std::path::Display;

use pyo3::prelude::*;
mod utils;

#[pymodule]
fn ballista_internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    // Ballista structs
    m.add_class::<PyBallista>()?;
    m.add_class::<PyBallistaBuilder>()?;
    // DataFusion structs
    m.add_class::<datafusion_python::dataframe::PyDataFrame>()?;
    // Ballista Config
    /*
    // Future implementation will include more state and config options
    m.add_class::<PySessionStateBuilder>()?;
    m.add_class::<PySessionState>()?;
    m.add_class::<PySessionConfig>()?;
    */
    Ok(())
}

// Ballista Builder will take a HasMap/Dict Cionfg
#[pyclass(name = "BallistaBuilder", module = "ballista", subclass)]
pub struct PyBallistaBuilder(HashMap<String, String>);

#[pymethods]
impl PyBallistaBuilder {
    #[new]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn set(
        mut slf: PyRefMut<'_, Self>,
        k: &str,
        v: &str,
        py: Python,
    ) -> PyResult<PyObject> {
        slf.0.insert(k.into(), v.into());

        Ok(slf.into_py(py))
    }

    pub fn show_config(&self) {
        println!("Ballista Config:");
        for ele in self.0.iter() {
            println!("    {}: {}", ele.0, ele.1)
        }
    }

    pub fn build(slf: PyRef<'_, Self>) -> PyBallista {
        PyBallista {
            conf: PyBallistaBuilder(slf.0.clone()),
        }
    }
}

#[pyclass(name = "Ballista", module = "ballista", subclass)]
pub struct PyBallista {
    pub conf: PyBallistaBuilder,
}

#[pymethods]
impl PyBallista {
    #[new]
    pub fn new() -> Self {
        Self {
            conf: PyBallistaBuilder::new(),
        }
    }

    pub fn show_config(&self) {
        println!("Ballista Config:");
        for ele in self.conf.0.clone() {
            println!("{:4}: {}", ele.0, ele.1)
        }
    }

    /// Construct the standalone instance from the SessionContext
    pub fn standalone(
        &self,
        concurrent_tasks: usize,
        py: Python,
    ) -> PyResult<DataFusionPythonSessionContext> {
        // Build the config
        let config = BallistaConfig::with_settings(self.conf.0.clone()).unwrap();
        // Define the SessionContext
        let session_context = BallistaContext::standalone(&config, concurrent_tasks);
        // SessionContext is an async function
        let ctx = wait_for_future(py, session_context)
            .unwrap()
            .context()
            .clone();

        // Convert the SessionContext into a Python SessionContext
        Ok(ctx.into())
    }

    /*
    /// Construct the remote instance from the SessionContext
    pub fn remote(url: &str, py: Python) -> PyResult<DataFusionPythonSessionContext> {
        let session_context = SessionContext::remote(url);
        let ctx = wait_for_future(py, session_context)?;

        // Convert the SessionContext into a Python SessionContext
        Ok(ctx.into())
    }
    */
}

/*
Plan to implement Session Config and State in a future issue

/// Ballista Session Extension builder
#[pyclass(name = "SessionConfig", module = "ballista", subclass)]
#[derive(Clone)]
pub struct PySessionConfig {
    pub session_config: SessionConfig,
}

#[pymethods]
impl PySessionConfig {
    #[new]
    pub fn new() -> Self {
        let session_config = SessionConfig::new_with_ballista();

        Self { session_config }
    }

    pub fn set_str(&mut self, key: &str, value: &str) -> Self {
        self.session_config.options_mut().set(key, value);

        self.clone()
    }
}

#[pyclass(name = "SessionStateBuilder", module = "ballista", subclass)]
pub struct PySessionStateBuilder {
    pub state: RefCell<SessionStateBuilder>,
}

#[pymethods]
impl PySessionStateBuilder {
    #[new]
    pub fn new() -> Self {
        Self {
            state: RefCell::new(SessionStateBuilder::new()),
        }
    }

    pub fn with_config(&mut self, config: PySessionConfig) -> PySessionStateBuilder {
        let state = self.state.take().with_config(config.session_config);

        PySessionStateBuilder {
            state: state.into()
        }
    }

    pub fn build(&mut self) -> PySessionStateBuilder {
        PySessionStateBuilder {
            state: RefCell::new(self.state.take())
        }
    }
}
*/
