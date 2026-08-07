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

use ballista_core::extension::SessionConfigExt;
use ballista_core::planner::BallistaQueryPlanner;
use ballista_core::serde::{
    BallistaLogicalExtensionCodec as NativeBallistaLogicalExtensionCodec,
    BallistaPhysicalExtensionCodec as NativeBallistaPhysicalExtensionCodec,
};
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::LogicalPlanNode;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};
use tokio::runtime::{Handle, Runtime};
use url::Url;

const DEFAULT_SCHEDULER_PORT: u16 = 50050;

#[pyclass(
    name = "BallistaExtension",
    module = "ballista._internal_ballista",
    skip_from_py_object
)]
pub(crate) struct PyBallistaExtension {
    address: String,
    config_overrides: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyBallistaExtension {
    #[new]
    #[pyo3(signature = (address, config_overrides=None))]
    fn new(address: String, config_overrides: Option<HashMap<String, String>>) -> Self {
        Self {
            address,
            config_overrides,
        }
    }

    fn __datafusion_session_extension__<'py>(
        &self,
        py: Python<'py>,
        session_ctx: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        ballista_extension_components(
            py,
            session_ctx,
            self.address.clone(),
            self.config_overrides.clone(),
        )
    }
}

#[pyclass(
    name = "BallistaLogicalExtensionCodec",
    module = "ballista._internal_ballista",
    skip_from_py_object
)]
pub(crate) struct PyBallistaLogicalExtensionCodec {
    logical_codec: Arc<dyn LogicalExtensionCodec>,
    ffi_logical_codec: FFI_LogicalExtensionCodec,
    // Retain the source context for the legacy low-level API. Atomic extension
    // installation leaves this empty so the returned context owns the provider.
    _session_ctx: Option<Py<PyAny>>,
}

#[pymethods]
impl PyBallistaLogicalExtensionCodec {
    #[new]
    fn new(session_ctx: Bound<'_, PyAny>) -> PyResult<Self> {
        let user_logical_codec: Arc<dyn LogicalExtensionCodec> =
            (&ffi_logical_codec_from_python(session_ctx.clone())?).into();
        let logical_codec: Arc<dyn LogicalExtensionCodec> =
            Arc::new(NativeBallistaLogicalExtensionCodec::new(user_logical_codec));
        let task_ctx_provider = ffi_task_ctx_provider_from_python(session_ctx.clone())?;
        let ffi_logical_codec = FFI_LogicalExtensionCodec::new(
            Arc::clone(&logical_codec),
            Some(tokio_runtime_handle()),
            task_ctx_provider,
        );

        Ok(Self {
            logical_codec,
            ffi_logical_codec,
            _session_ctx: Some(session_ctx.unbind()),
        })
    }

    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        PyCapsule::new_with_value(
            py,
            self.ffi_logical_codec.clone(),
            cr"datafusion_logical_extension_codec",
        )
    }
}

#[pyclass(
    name = "BallistaPhysicalExtensionCodec",
    module = "ballista._internal_ballista",
    skip_from_py_object
)]
pub(crate) struct PyBallistaPhysicalExtensionCodec {
    ffi_physical_codec: FFI_PhysicalExtensionCodec,
    // Retain the source context for the legacy low-level API. Atomic extension
    // installation leaves this empty so the returned context owns the provider.
    _session_ctx: Option<Py<PyAny>>,
    // Retain the Python logical codec as well as its native Arc.
    _logical_codec: Py<PyAny>,
}

impl PyBallistaPhysicalExtensionCodec {
    fn from_logical_codec(
        session_ctx: Bound<'_, PyAny>,
        logical_codec: Arc<dyn LogicalExtensionCodec>,
        logical_codec_object: Py<PyAny>,
    ) -> PyResult<Self> {
        let ffi_physical_codec =
            ballista_physical_ffi_codec(session_ctx.clone(), logical_codec)?;

        Ok(Self {
            ffi_physical_codec,
            _session_ctx: Some(session_ctx.unbind()),
            _logical_codec: logical_codec_object,
        })
    }
}

#[pymethods]
impl PyBallistaPhysicalExtensionCodec {
    #[new]
    fn new(
        session_ctx: Bound<'_, PyAny>,
        logical_codec: Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let native_logical_codec = {
            let logical_codec =
                logical_codec.extract::<PyRef<'_, PyBallistaLogicalExtensionCodec>>()?;
            Arc::clone(&logical_codec.logical_codec)
        };
        Self::from_logical_codec(
            session_ctx,
            native_logical_codec,
            logical_codec.unbind(),
        )
    }

    fn __datafusion_physical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        PyCapsule::new_with_value(
            py,
            self.ffi_physical_codec.clone(),
            cr"datafusion_physical_extension_codec",
        )
    }
}

#[pyclass(
    name = "BallistaQueryPlanner",
    module = "ballista._internal_ballista",
    skip_from_py_object
)]
pub(crate) struct PyBallistaQueryPlanner {
    address: String,
    config: ballista_core::config::BallistaConfig,
    logical_codec: Arc<dyn LogicalExtensionCodec>,
    fallback_logical_codec: FFI_LogicalExtensionCodec,
    fallback_physical_codec: FFI_PhysicalExtensionCodec,
    // Retain the source context for the legacy low-level API. Atomic extension
    // installation leaves this empty so the returned context owns the provider.
    _session_ctx: Option<Py<PyAny>>,
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
        let logical_codec = PyBallistaLogicalExtensionCodec::new(session_ctx.clone())?;
        let fallback_physical_codec = ballista_physical_ffi_codec(
            session_ctx.clone(),
            Arc::clone(&logical_codec.logical_codec),
        )?;
        Self::from_codecs(
            address,
            session_ctx,
            config_overrides,
            logical_codec.logical_codec,
            logical_codec.ffi_logical_codec,
            fallback_physical_codec,
        )
    }

    fn __datafusion_query_planner__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let planner: Arc<dyn QueryPlanner + Send + Sync> =
            Arc::new(LazyBallistaQueryPlanner {
                address: self.address.clone(),
                config: self.config.clone(),
                logical_codec: Arc::clone(&self.logical_codec),
                _session_ctx: self
                    ._session_ctx
                    .as_ref()
                    .map(|session_ctx| session_ctx.clone_ref(py)),
            });
        let ffi = FFI_QueryPlanner::new_with_ffi_codecs(
            planner,
            self.fallback_logical_codec.clone(),
            self.fallback_physical_codec.clone(),
        );

        // This exact name is the ABI consumed by datafusion-python.
        PyCapsule::new_with_value(py, ffi, cr"datafusion_query_planner")
    }
}

impl PyBallistaQueryPlanner {
    fn from_codecs(
        address: String,
        session_ctx: Bound<'_, PyAny>,
        config_overrides: Option<HashMap<String, String>>,
        logical_codec: Arc<dyn LogicalExtensionCodec>,
        fallback_logical_codec: FFI_LogicalExtensionCodec,
        fallback_physical_codec: FFI_PhysicalExtensionCodec,
    ) -> PyResult<Self> {
        let mut session_config = SessionConfig::new_with_ballista();
        if let Some(overrides) = config_overrides {
            for (key, value) in overrides {
                if !key.starts_with("ballista.") {
                    return Err(PyValueError::new_err(format!(
                        "unsupported Ballista configuration key: {key}"
                    )));
                }
                session_config
                    .options_mut()
                    .set(&key, &value)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
            }
        }

        Ok(Self {
            address,
            config: session_config.ballista_config(),
            logical_codec,
            fallback_logical_codec,
            fallback_physical_codec,
            _session_ctx: Some(session_ctx.unbind()),
        })
    }
}

struct LazyBallistaQueryPlanner {
    address: String,
    config: ballista_core::config::BallistaConfig,
    logical_codec: Arc<dyn LogicalExtensionCodec>,
    _session_ctx: Option<Py<PyAny>>,
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

        // Parse only when DataFusion asks for a physical plan so malformed
        // addresses fail lazily, not during context construction.
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

fn ballista_extension_components<'py>(
    py: Python<'py>,
    session_ctx: Bound<'py, PyAny>,
    address: String,
    config_overrides: Option<HashMap<String, String>>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut logical_codec = PyBallistaLogicalExtensionCodec::new(session_ctx.clone())?;
    let logical_native = Arc::clone(&logical_codec.logical_codec);
    let fallback_logical_codec = logical_codec.ffi_logical_codec.clone();
    logical_codec._session_ctx = None;
    let logical_codec = Py::new(py, logical_codec)?;

    let mut physical_codec = PyBallistaPhysicalExtensionCodec::from_logical_codec(
        session_ctx.clone(),
        Arc::clone(&logical_native),
        logical_codec.clone_ref(py).into_any(),
    )?;
    let fallback_physical_codec = physical_codec.ffi_physical_codec.clone();
    physical_codec._session_ctx = None;
    let physical_codec = Py::new(py, physical_codec)?;

    let mut planner = PyBallistaQueryPlanner::from_codecs(
        address,
        session_ctx,
        config_overrides,
        logical_native,
        fallback_logical_codec,
        fallback_physical_codec,
    )?;
    planner._session_ctx = None;
    let planner = Py::new(py, planner)?;

    let components = py
        .import("datafusion")?
        .getattr("SessionExtensionComponents")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("logical_extension_codecs", (logical_codec,))?;
    kwargs.set_item("physical_extension_codecs", (physical_codec,))?;
    kwargs.set_item("query_planner", planner)?;
    components.call((), Some(&kwargs))
}

#[pyfunction]
#[pyo3(signature = (session_ctx, address, config_overrides=None))]
pub(crate) fn with_ballista_query_planner(
    py: Python<'_>,
    session_ctx: Bound<'_, PyAny>,
    address: String,
    config_overrides: Option<HashMap<String, String>>,
) -> PyResult<Py<PyAny>> {
    let mut ballista_overrides = HashMap::new();
    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            if key.starts_with("datafusion.") {
                apply_datafusion_override(&session_ctx, &key, &value)?;
            } else if key.starts_with("ballista.") {
                ballista_overrides.insert(key, value);
            } else {
                return Err(PyValueError::new_err(format!(
                    "configuration key must start with 'datafusion.' or 'ballista.': {key}"
                )));
            }
        }
    }

    let extension = Py::new(
        py,
        PyBallistaExtension::new(address, Some(ballista_overrides)),
    )?;
    Ok(session_ctx
        .call_method1("with_extensions", (extension.bind(py),))?
        .unbind())
}

fn ballista_physical_ffi_codec(
    session_ctx: Bound<'_, PyAny>,
    logical_codec: Arc<dyn LogicalExtensionCodec>,
) -> PyResult<FFI_PhysicalExtensionCodec> {
    let physical_codec: Arc<dyn PhysicalExtensionCodec> =
        Arc::new(NativeBallistaPhysicalExtensionCodec::new(logical_codec));
    let task_ctx_provider = ffi_task_ctx_provider_from_python(session_ctx)?;
    Ok(FFI_PhysicalExtensionCodec::new(
        physical_codec,
        Some(tokio_runtime_handle()),
        task_ctx_provider,
    ))
}

fn apply_datafusion_override(
    session_ctx: &Bound<'_, PyAny>,
    key: &str,
    value: &str,
) -> PyResult<()> {
    if !key
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'))
    {
        return Err(PyValueError::new_err(format!(
            "invalid DataFusion configuration key: {key}"
        )));
    }

    // SET mutates the shared session state. The contexts derived below retain
    // that state, including registered tables and functions.
    let escaped_value = value.replace('\'', "''");
    let statement = format!("SET {key} = '{escaped_value}'");
    session_ctx.call_method1("sql", (statement,))?;
    Ok(())
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
