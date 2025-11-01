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

use crate::utils::wait_for_future;
use ballista::prelude::*;
use cluster::{PyExecutor, PyScheduler};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_proto::bytes::logical_plan_from_bytes;
use datafusion_python::context::PySessionContext;
use datafusion_python::dataframe::PyDataFrame;
use pyo3::prelude::*;

mod cluster;
#[allow(dead_code)]
mod codec;
mod utils;

pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[pymodule]
fn _internal_ballista(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    m.add_class::<PyBallistaBuilder>()?;
    m.add_class::<datafusion_python::dataframe::PyDataFrame>()?;
    m.add_class::<PyScheduler>()?;
    m.add_class::<PyExecutor>()?;
    m.add_class::<PyBallistaRemoteExecutor>()?;

    Ok(())
}

// this class is only temporary
// to support proof of concept
#[pyclass(name = "PyBallistaRemoteExecutor", module = "ballista", subclass)]
pub struct PyBallistaRemoteExecutor {}

#[pymethods]
impl PyBallistaRemoteExecutor {
    #[new]
    pub fn new() -> Self {
        Self {}
    }

    #[staticmethod]
    fn create_data_frame(
        py: Python,
        plan_blob: &[u8],
        url: &str,
        session_id: &str,
    ) -> PyResult<PyDataFrame> {
        let state = SessionStateBuilder::new_with_default_features()
            .with_session_id(session_id.to_string())
            .build();
        let ctx = wait_for_future(py, SessionContext::remote_with_state(url, state))?;
        let plan = logical_plan_from_bytes(plan_blob, &ctx)?;
        let df = DataFrame::new(ctx.state(), plan);

        Ok(PyDataFrame::new(df))
    }

    // //#[pyo3(signature = (count, offset=0))]
    // #[staticmethod]
    // fn show(py: Python, plan_blob: &[u8], url: &str) -> PyResult<()> {
    //     let ctx = wait_for_future(py, SessionContext::remote(url))?;
    //     let plan = logical_plan_from_bytes(plan_blob, &ctx)?;
    //     let df = DataFrame::new(ctx.state(), plan);

    //     print_dataframe(py, df)
    // }

    // #[staticmethod]
    // fn collect(py: Python, plan_blob: &[u8], url: &str) -> PyResult<Vec<PyObject>> {
    //     let ctx = wait_for_future(py, SessionContext::remote(url))?;
    //     let plan = logical_plan_from_bytes(plan_blob, &ctx)?;
    //     let df = DataFrame::new(ctx.state(), plan);
    //     let batches = wait_for_future(py, df.collect())?;

    //     // cannot use PyResult<Vec<RecordBatch>> return type due to
    //     // https://github.com/PyO3/pyo3/issues/1813
    //     batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    // }
}

// taken from datafusion python
// fn print_dataframe(py: Python, df: DataFrame) -> PyResult<()> {
//     // Get string representation of record batches
//     let batches = wait_for_future(py, df.collect())?;
//     let result = if batches.is_empty() {
//         "DataFrame has no rows".to_string()
//     } else {
//         match pretty_format_batches(&batches) {
//             Ok(batch) => format!("DataFrame()\n{batch}"),
//             Err(err) => format!("Error: {:?}", err.to_string()),
//         }
//     };

//     // Import the Python 'builtins' module to access the print function
//     // Note that println! does not print to the Python debug console and is not visible in notebooks for instance
//     let print = py.import("builtins")?.getattr("print")?;
//     print.call1((result,))?;
//     Ok(())
// }

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

        Ok(slf.into_pyobject(py)?.into())
    }

    /// Construct the standalone instance from the SessionContext
    pub fn standalone(&self, py: Python) -> PyResult<PySessionContext> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_default_features()
            .build();

        let ctx = wait_for_future(py, SessionContext::standalone_with_state(state))?;

        Ok(ctx.into())
    }

    /// Construct the remote instance from the SessionContext
    pub fn remote(&self, url: &str, py: Python) -> PyResult<PySessionContext> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_default_features()
            .build();

        let ctx = wait_for_future(py, SessionContext::remote_with_state(url, state))?;

        Ok(ctx.into())
    }

    #[classattr]
    pub fn version() -> &'static str {
        ballista_core::BALLISTA_VERSION
    }
}
