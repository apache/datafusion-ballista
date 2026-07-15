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

use ballista_core::extension::{SessionConfigExt, SessionConfigHelperExt};
use ballista_core::planner::BallistaQueryPlanner;
use ballista_core::serde::protobuf::KeyValuePair;
use ballista_core::serde::{
    BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::Session;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::{
    ComposedPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf::LogicalPlanNode;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};
use tokio::runtime::{Handle, Runtime};
use url::Url;

const DEFAULT_SCHEDULER_PORT: u16 = 50050;

#[pyclass(
    name = "BallistaQueryPlanner",
    module = "ballista._internal_ballista",
    skip_from_py_object
)]
pub(crate) struct PyBallistaQueryPlanner {
    address: String,
    config: ballista_core::config::BallistaConfig,
    logical_codec: FFI_LogicalExtensionCodec,
    physical_codec: FFI_PhysicalExtensionCodec,
    task_ctx_provider: FFI_TaskContextProvider,
    _session_ctx: Py<PyAny>,
}

#[pymethods]
impl PyBallistaQueryPlanner {
    #[new]
    #[pyo3(signature = (address, session_ctx, config_overrides=None))]
    fn new(
        address: String,
        session_ctx: Bound<'_, PyAny>,
        config_overrides: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let logical_codec = ffi_logical_codec_from_python(session_ctx.clone())?;
        let physical_codec = ffi_physical_codec_from_python(session_ctx.clone())?;
        let task_ctx_provider = ffi_task_ctx_provider_from_python(session_ctx.clone())?;

        let mut session_config = SessionConfig::new_with_ballista();
        if let Some(overrides) = config_overrides {
            let pairs = overrides
                .into_iter()
                .map(|(key, value)| KeyValuePair {
                    key,
                    value: Some(value),
                })
                .collect::<Vec<_>>();
            session_config.update_from_key_value_pair_mut(&pairs);
        }

        Ok(Self {
            address,
            config: session_config.ballista_config(),
            logical_codec,
            physical_codec,
            task_ctx_provider,
            _session_ctx: session_ctx.unbind(),
        })
    }

    fn __datafusion_query_planner__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let user_logical_codec: Arc<dyn LogicalExtensionCodec> =
            (&self.logical_codec).into();
        let logical_codec: Arc<dyn LogicalExtensionCodec + Send> =
            Arc::new(BallistaLogicalExtensionCodec::new(user_logical_codec));
        let runtime = tokio_runtime_handle();
        let ffi_logical_codec = FFI_LogicalExtensionCodec::new(
            Arc::clone(&logical_codec),
            Some(runtime.clone()),
            self.task_ctx_provider.clone(),
        );
        let user_physical_codec: Arc<dyn PhysicalExtensionCodec> =
            (&self.physical_codec).into();
        let planner_logical_codec: Arc<dyn LogicalExtensionCodec> = logical_codec.clone();
        let physical_codec: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(ComposedPhysicalExtensionCodec::new(vec![
                Arc::clone(&user_physical_codec),
                Arc::new(BallistaPhysicalExtensionCodec::new(Arc::clone(
                    &planner_logical_codec,
                ))),
            ]));
        let physical_codec = FFI_PhysicalExtensionCodec::new(
            physical_codec,
            Some(runtime),
            self.task_ctx_provider.clone(),
        );
        let planner: Arc<dyn QueryPlanner + Send + Sync> =
            Arc::new(LazyBallistaQueryPlanner {
                address: self.address.clone(),
                config: self.config.clone(),
                logical_codec: planner_logical_codec,
                _session_ctx: self._session_ctx.clone_ref(py),
            });
        let ffi = FFI_QueryPlanner::new_with_ffi_codecs(
            planner,
            ffi_logical_codec,
            physical_codec,
        );

        PyCapsule::new(py, ffi, Some(cr"datafusion_query_planner_v1".into()))
    }
}

struct LazyBallistaQueryPlanner {
    address: String,
    config: ballista_core::config::BallistaConfig,
    logical_codec: Arc<dyn LogicalExtensionCodec>,
    _session_ctx: Py<PyAny>,
}

impl std::fmt::Debug for LazyBallistaQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyBallistaQueryPlanner")
            .field("address", &self.address)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl QueryPlanner for LazyBallistaQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if matches!(logical_plan, LogicalPlan::Analyze(_)) {
            return Err(DataFusionError::NotImplemented(
                "EXPLAIN ANALYZE is not yet supported by the Ballista Python FFI planner"
                    .to_string(),
            ));
        }

        let scheduler_url = parse_scheduler_url(&self.address)?;
        let planner = BallistaQueryPlanner::<LogicalPlanNode>::with_extension(
            scheduler_url,
            self.config.clone(),
            Arc::clone(&self.logical_codec),
        );
        planner.create_physical_plan(logical_plan, session).await
    }
}

#[pyfunction]
pub(crate) fn ballista_datafusion_config_defaults() -> HashMap<String, String> {
    let defaults = SessionConfig::new();
    let ballista = SessionConfig::new_with_ballista();
    let default_values = defaults
        .options()
        .entries()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect::<HashMap<_, _>>();

    ballista
        .options()
        .entries()
        .into_iter()
        .filter_map(|entry| {
            if !entry.key.starts_with("datafusion.") {
                return None;
            }
            let value = entry.value?;
            (default_values.get(&entry.key) != Some(&Some(value.clone())))
                .then_some((entry.key, value))
        })
        .collect()
}

#[pyfunction]
#[pyo3(signature = (session_ctx, address, config_overrides=None))]
pub(crate) fn with_ballista_query_planner(
    py: Python<'_>,
    session_ctx: Bound<'_, PyAny>,
    address: String,
    config_overrides: Option<HashMap<String, String>>,
) -> PyResult<Py<PyAny>> {
    let planner =
        PyBallistaQueryPlanner::new(address, session_ctx.clone(), config_overrides)?;
    let planner = Py::new(py, planner)?;
    Ok(session_ctx
        .call_method1("with_query_planner", (planner,))?
        .unbind())
}

fn parse_scheduler_url(address: &str) -> Result<String> {
    let url =
        Url::parse(address).map_err(|e| DataFusionError::Configuration(e.to_string()))?;
    let host = url.host().ok_or_else(|| {
        DataFusionError::Configuration("hostname should be provided".to_string())
    })?;
    let port = url.port().unwrap_or(DEFAULT_SCHEDULER_PORT);
    Ok(format!("http://{host}:{port}"))
}

fn tokio_runtime_handle() -> Handle {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| Runtime::new().expect("tokio runtime for Ballista Python FFI"))
        .handle()
        .clone()
}

fn ffi_logical_codec_from_python(
    obj: Bound<'_, PyAny>,
) -> PyResult<FFI_LogicalExtensionCodec> {
    let capsule = obj
        .getattr("__datafusion_logical_extension_codec__")?
        .call0()?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_LogicalExtensionCodec> = capsule
        .pointer_checked(Some(c"datafusion_logical_extension_codec"))?
        .cast();
    Ok(unsafe { data.as_ref().clone() })
}

fn ffi_task_ctx_provider_from_python(
    obj: Bound<'_, PyAny>,
) -> PyResult<FFI_TaskContextProvider> {
    let capsule = obj
        .getattr("__datafusion_task_context_provider__")?
        .call0()?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_TaskContextProvider> = capsule
        .pointer_checked(Some(c"datafusion_task_context_provider"))?
        .cast();
    Ok(unsafe { data.as_ref().clone() })
}

fn ffi_physical_codec_from_python(
    obj: Bound<'_, PyAny>,
) -> PyResult<FFI_PhysicalExtensionCodec> {
    let capsule = obj
        .getattr("__datafusion_physical_extension_codec__")?
        .call0()?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_PhysicalExtensionCodec> = capsule
        .pointer_checked(Some(c"datafusion_physical_extension_codec"))?
        .cast();
    Ok(unsafe { data.as_ref().clone() })
}
