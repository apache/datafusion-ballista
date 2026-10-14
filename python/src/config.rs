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

use ballista_core::config::BallistaConfig;
use datafusion::config::{ConfigExtension, ExtensionOptions};
use datafusion_ffi::config::extension_options::FFI_ExtensionOptions;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::collections::HashMap;

/// Ballista configuration exposed as a DataFusion config extension, to be
/// registered with `SessionConfig.with_extension(...)`.
///
/// Once registered, `ballista.*` keys are listed in
/// `information_schema.df_settings` and can be changed with `SET`.
#[pyclass(
    name = "BallistaConfigExtension",
    module = "ballista._internal_ballista",
    skip_from_py_object
)]
pub(crate) struct PyBallistaConfigExtension {
    config: BallistaConfig,
}

#[pymethods]
impl PyBallistaConfigExtension {
    #[new]
    #[pyo3(signature = (settings=None))]
    fn new(settings: Option<HashMap<String, String>>) -> PyResult<Self> {
        let mut config = BallistaConfig::default();
        for (key, value) in settings.unwrap_or_default() {
            let Some(inner_key) = key
                .strip_prefix(BallistaConfig::PREFIX)
                .and_then(|k| k.strip_prefix('.'))
            else {
                return Err(PyValueError::new_err(format!(
                    "unsupported Ballista configuration key: {key}"
                )));
            };
            config
                .set(inner_key, &value)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
        Ok(Self { config })
    }

    fn __datafusion_extension_options__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        // `BallistaConfig` entry keys already carry the `ballista.` prefix,
        // so `FFI_ExtensionOptions::add_config` (which prepends it again)
        // is not used.
        let options = self
            .config
            .entries()
            .into_iter()
            .filter_map(|entry| Some((entry.key, entry.value?)))
            .collect::<HashMap<_, _>>();
        PyCapsule::new_with_value(
            py,
            FFI_ExtensionOptions::from(options),
            cr"datafusion_extension_options",
        )
    }
}
