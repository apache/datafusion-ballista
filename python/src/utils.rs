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

use std::future::Future;
use std::sync::OnceLock;
use tokio::task::JoinHandle;

use ballista_core::error::BallistaError;
use pyo3::exceptions::PyException;
use pyo3::{PyErr, Python};
use tokio::runtime::Runtime;

use crate::TokioRuntime;

pub(crate) fn to_pyerr(err: BallistaError) -> PyErr {
    PyException::new_err(err.to_string())
}

#[inline]
pub(crate) fn get_tokio_runtime() -> &'static TokioRuntime {
    // NOTE: Other pyo3 python libraries have had issues with using tokio
    // behind a forking app-server like `gunicorn`
    // If we run into that problem, in the future we can look to `delta-rs`
    // which adds a check in that disallows calls from a forked process
    // https://github.com/delta-io/delta-rs/blob/87010461cfe01563d91a4b9cd6fa468e2ad5f283/python/src/utils.rs#L10-L31
    static RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| TokioRuntime(tokio::runtime::Runtime::new().unwrap()))
}

/// Utility to collect rust futures with GIL released
pub(crate) fn wait_for_future<F>(py: Python, f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime().0;
    py.detach(|| runtime.block_on(f))
}

pub(crate) fn spawn_feature<F>(py: Python, f: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime().0;
    // do we need py.allow_threads ?
    py.detach(|| runtime.spawn(f))
}
