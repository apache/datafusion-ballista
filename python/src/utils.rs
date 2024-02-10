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

use crate::TokioRuntime;
use ballista_core::error::BallistaError;
use pyo3::exceptions::PyException;
use pyo3::{Py, PyAny, PyErr, PyRef, Python};
use std::future::Future;
use tokio::runtime::Runtime;

/// Allow async functions to be called from Python as blocking calls
//TODO this is duplicated from ADP
pub(crate) fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime(py).0;
    py.allow_threads(|| runtime.block_on(f))
}

fn get_tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    // TODO should get DataFusion's runtime here?
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

pub(crate) fn to_pyerr(err: BallistaError) -> PyErr {
    PyException::new_err(err.to_string())
}
