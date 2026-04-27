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
use ballista_core::extension::SessionConfigHelperExt;
use ballista_core::serde::protobuf::KeyValuePair;
use cluster::{PyExecutor, PyScheduler};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_proto::bytes::logical_plan_from_bytes;
use pyo3::prelude::*;
use std::collections::HashMap;

mod cluster;
mod utils;

pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[pymodule]
fn _internal_ballista(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    m.add_class::<PyScheduler>()?;
    m.add_class::<PyExecutor>()?;

    m.add_class::<datafusion_python::dataframe::PyParquetWriterOptions>()?;
    m.add_class::<datafusion_python::dataframe::PyParquetColumnOptions>()?;
    m.add_class::<datafusion_python::dataframe::PyDataFrame>()?;

    m.add_function(wrap_pyfunction!(create_ballista_data_frame, m.clone())?)?;
    m.add_function(wrap_pyfunction!(
        crate::cluster::setup_test_cluster,
        m.clone()
    )?)?;

    Ok(())
}

/// Creates a DataFrame which runs on ballista session context.
///
/// `config_overrides` is an optional dictionary of DataFusion / Ballista
/// configuration keys (e.g. `datafusion.execution.target_partitions`) that
/// will be applied on top of `SessionConfig::new_with_ballista()` and
/// propagated to the scheduler-side session.
///
/// Returned DataFrame will execute its plan on ballista.
#[pyfunction]
#[pyo3(signature = (plan_blob, url, session_id, config_overrides=None))]
fn create_ballista_data_frame(
    py: Python,
    plan_blob: &[u8],
    url: &str,
    session_id: &str,
    config_overrides: Option<HashMap<String, String>>,
) -> PyResult<datafusion_python::dataframe::PyDataFrame> {
    let mut session_config = SessionConfig::new_with_ballista();
    if let Some(overrides) = config_overrides {
        let pairs: Vec<KeyValuePair> = overrides
            .into_iter()
            .map(|(key, value)| KeyValuePair {
                key,
                value: Some(value),
            })
            .collect();
        session_config.update_from_key_value_pair_mut(&pairs);
    }

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config)
        .with_session_id(session_id.to_string())
        .build();

    let ctx = wait_for_future(py, SessionContext::remote_with_state(url, state))?;
    let plan = logical_plan_from_bytes(plan_blob, &ctx.task_ctx())?;

    Ok(datafusion_python::dataframe::PyDataFrame::new(
        DataFrame::new(ctx.state(), plan),
    ))
}
