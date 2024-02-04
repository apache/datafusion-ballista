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

use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::prelude::DataFrame;
use pyo3::prelude::*;
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::Runtime;

use ballista::prelude::*;

/// PyBallista SessionContext
#[pyclass(name = "SessionContext", module = "pyballista", subclass)]
pub struct PySessionContext {
    ctx: BallistaContext,
}

#[pymethods]
impl PySessionContext {
    #[new]
    pub fn new(host: &str, port: u16, py: Python) -> PyResult<Self> {
        let config = BallistaConfig::new().unwrap();
        let ballista_context = BallistaContext::remote(host, port, &config);
        let ctx = wait_for_future(py, ballista_context).unwrap();
        Ok(Self { ctx })
    }

    pub fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).unwrap();
        Ok(PyDataFrame::new(df))
    }
}

#[pyclass(name = "DataFrame", module = "pyballista", subclass)]
#[derive(Clone)]
pub struct PyDataFrame {
    /// DataFusion DataFrame
    df: Arc<DataFrame>,
}

impl PyDataFrame {
    /// creates a new PyDataFrame
    pub fn new(df: DataFrame) -> Self {
        Self { df: Arc::new(df) }
    }
}

#[pymethods]
impl PyDataFrame {
    /// Executes the plan, returning a list of `RecordBatch`es.
    /// Unless some order is specified in the plan, there is no
    /// guarantee of the order of the result.
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches = wait_for_future(py, self.df.as_ref().clone().collect()).unwrap();
        // cannot use PyResult<Vec<RecordBatch>> return type due to
        // https://github.com/PyO3/pyo3/issues/1813
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }
}

fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime(py).0;
    py.allow_threads(|| runtime.block_on(f))
}

fn get_tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    let ballista = py.import("pyballista._internal").unwrap();
    let tmp = ballista.getattr("runtime").unwrap();
    match tmp.extract::<PyRef<TokioRuntime>>() {
        Ok(runtime) => runtime,
        Err(_e) => {
            let rt = TokioRuntime(tokio::runtime::Runtime::new().unwrap());
            let obj: &PyAny = Py::new(py, rt).unwrap().into_ref(py);
            obj.extract().unwrap()
        }
    }
}

#[pyclass]
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    // Register the Tokio Runtime as a module attribute so we can reuse it
    m.add(
        "runtime",
        TokioRuntime(tokio::runtime::Runtime::new().unwrap()),
    )?;
    m.add_class::<PySessionContext>()?;
    m.add_class::<PyDataFrame>()?;
    Ok(())
}
