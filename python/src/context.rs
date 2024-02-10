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

use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

pub use crate::dataframe::PyDataFrame;
use crate::utils::to_pyerr;
use crate::utils::wait_for_future;
use ballista::prelude::*;
use ballista_core::utils::BallistaQueryPlanner;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion_proto::logical_plan::AsLogicalPlan;

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
        let ctx = wait_for_future(py, ballista_context).map_err(to_pyerr)?;
        Ok(Self { ctx })
    }

    pub fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result)?;
        Ok(PyDataFrame::new(df))
    }

    #[pyo3(signature = (path, has_header = false))]
    fn read_csv(
        &self,
        path: PathBuf,
        has_header: bool,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;
        let result = self
            .ctx
            .read_csv(path, CsvReadOptions::default().has_header(has_header));
        let df = wait_for_future(py, result);
        Ok(PyDataFrame::new(df?))
    }
}

pub fn create_df_ctx_with_ballista_query_planner<T: 'static + AsLogicalPlan>(
    scheduler_url: String,
    session_id: String,
    config: &BallistaConfig,
) -> SessionContext {
    let planner: Arc<BallistaQueryPlanner<T>> =
        Arc::new(BallistaQueryPlanner::new(scheduler_url, config.clone()));

    let session_config = SessionConfig::new()
        .with_target_partitions(config.default_shuffle_partitions())
        .with_information_schema(true);
    let mut session_state = SessionState::new_with_config_rt(
        session_config,
        Arc::new(
            // TODO: this originally called with_object_store_provider
            RuntimeEnv::new(RuntimeConfig::default()).unwrap(),
        ),
    )
    .with_query_planner(planner);
    session_state = session_state.with_session_id(session_id);
    // the SessionContext created here is the client side context, but the session_id is from server side.
    SessionContext::new_with_state(session_state)
}
