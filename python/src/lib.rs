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

use crate::utils::{to_pyerr, wait_for_future};
use ballista::prelude::*;
use cluster::{PyExecutor, PyScheduler};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_python::context::PySessionContext;
use pyo3::prelude::*;

mod cluster;
#[allow(dead_code)]
mod codec;
mod utils;

pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[pymodule]
fn ballista_internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    m.add_class::<PyBallistaBuilder>()?;
    m.add_class::<datafusion_python::dataframe::PyDataFrame>()?;
    m.add_class::<PyScheduler>()?;
    m.add_class::<PyExecutor>()?;

    Ok(())
}

#[pyclass(name = "BallistaBuilder", module = "ballista", subclass)]
pub struct PyBallistaBuilder {
    session_config: SessionConfig,
}

#[pymethods]
impl PyBallistaBuilder {
    #[new]
    pub fn new() -> Self {
        Self {
            session_config: SessionConfig::new_with_ballista(),
        }
    }

    pub fn config(
        mut slf: PyRefMut<'_, Self>,
        key: &str,
        value: &str,
        py: Python,
    ) -> PyResult<PyObject> {
        let _ = slf.session_config.options_mut().set(key, value);

        Ok(slf.into_py(py))
    }

    /// Construct the standalone instance from the SessionContext
    pub fn standalone(&self, py: Python) -> PyResult<PySessionContext> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_default_features()
            .build();

        let ctx = wait_for_future(py, SessionContext::standalone_with_state(state))
            .map_err(to_pyerr)?;

        Ok(ctx.into())
    }

    /// Construct the remote instance from the SessionContext
    pub fn remote(&self, url: &str, py: Python) -> PyResult<PySessionContext> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_default_features()
            .build();

        let ctx = wait_for_future(py, SessionContext::remote_with_state(url, state))
            .map_err(to_pyerr)?;

        Ok(ctx.into())
    }

    #[classattr]
    pub fn version() -> &'static str {
        ballista_core::BALLISTA_VERSION
    }
}
