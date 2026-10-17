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

use cluster::{PyExecutor, PyScheduler};
use pyo3::prelude::*;

mod cluster;
mod codec;
mod config;
mod ffi_planner_workaround;
mod query_planner;
mod utils;

pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[pymodule]
fn _internal_ballista(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    m.add_class::<PyScheduler>()?;
    m.add_class::<PyExecutor>()?;

    m.add_class::<config::PyBallistaConfigExtension>()?;

    m.add_class::<query_planner::PyBallistaExtension>()?;
    m.add_class::<query_planner::PyBallistaLogicalExtensionCodec>()?;
    m.add_class::<query_planner::PyBallistaPhysicalExtensionCodec>()?;
    m.add_class::<query_planner::PyBallistaQueryPlanner>()?;

    m.add_function(wrap_pyfunction!(
        crate::cluster::setup_test_cluster,
        m.clone()
    )?)?;

    m.add_function(wrap_pyfunction!(
        query_planner::ballista_datafusion_config_defaults,
        m.clone()
    )?)?;
    m.add_function(wrap_pyfunction!(
        query_planner::with_ballista_query_planner,
        m.clone()
    )?)?;

    Ok(())
}
