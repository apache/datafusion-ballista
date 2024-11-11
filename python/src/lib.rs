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

use std::collections::HashMap;

use ballista::prelude::*;
use ballista_core::config::BallistaConfigBuilder;
use datafusion::prelude::*;
use datafusion_python::context::PySessionContext as DataFusionPythonSessionContext;
use datafusion_python::utils::wait_for_future;

use pyo3::prelude::*;
mod utils;

#[pymodule]
fn ballista_internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    // Ballista structs
    m.add_class::<PyBallista>()?;
    // DataFusion structs
    m.add_class::<datafusion_python::dataframe::PyDataFrame>()?;
    // Ballista Config
    m.add_class::<PyBallistaConfig>()?;
    m.add_class::<PyBallistaConfigBuilder>()?;
    Ok(())
}

/// Ballista configuration builder
#[pyclass(name = "BallistaConfigBuilder", module = "ballista", subclass)]
pub struct PyBallistaConfigBuilder {
    settings: HashMap<String, String>,
}

#[pymethods]
impl PyBallistaConfigBuilder {
    #[new]
    pub fn new() -> Self {
        Self {
            settings: HashMap::new()
        }
    }
    /// Create a new config with an additional setting
    pub fn set(&self, k: &str, v: &str) -> Self {
        let mut settings = self.settings.clone();
        settings.insert(k.to_owned(), v.to_owned());
        Self { settings }
    }
    
    #[staticmethod]
    pub fn build() -> PyBallistaConfig {
        PyBallistaConfig::new()
    }
}

#[pyclass(name = "BallistaConfig", module = "ballista", subclass)]
pub struct PyBallistaConfig {
    // Settings stored in map for easy serde
    pub config: BallistaConfig,
}

#[pymethods]
impl PyBallistaConfig {
    #[new]
    pub fn new() -> Self {   
        Self {
            config: BallistaConfig::with_settings(HashMap::new()).unwrap()
        }
    }
    
    #[staticmethod]
    pub fn with_settings(settings: HashMap<String, String>) -> Self {
        let settings = BallistaConfig::with_settings(settings).unwrap();
        
        Self {
            config: settings
        }
    }
    
    #[staticmethod]
    pub fn builder() -> PyBallistaConfigBuilder {
        PyBallistaConfigBuilder {
            settings: HashMap::new()
        }
    }
    
    pub fn settings(&self) -> HashMap<String, String> {
            let settings = &self.config.settings().clone();
            settings.to_owned()
    }
    
    pub fn default_shuffle_partitions(&self) -> usize {
        self.config.default_shuffle_partitions()
    }

    pub fn default_batch_size(&self) -> usize {
        self.config.default_batch_size()
    }

    pub fn hash_join_single_partition_threshold(&self) -> usize {
        self.config.hash_join_single_partition_threshold()
    }

    pub fn default_grpc_client_max_message_size(&self) -> usize {
        self.config.default_grpc_client_max_message_size()
    }

    pub fn repartition_joins(&self) -> bool {
        self.config.repartition_joins()
    }

    pub fn repartition_aggregations(&self) -> bool {
        self.config.repartition_aggregations()
    }

    pub fn repartition_windows(&self) -> bool {
        self.config.repartition_windows()
    }

    pub fn parquet_pruning(&self) -> bool {
        self.config.parquet_pruning()
    }

    pub fn collect_statistics(&self) -> bool {
        self.config.collect_statistics()
    }

    pub fn default_standalone_parallelism(&self) -> usize {
        self.config.default_standalone_parallelism()
    }

    pub fn default_with_information_schema(&self) -> bool {
        self.config.default_with_information_schema()
    }
}

#[pyclass(name = "Ballista", module = "ballista", subclass)]
pub struct PyBallista {
    pub config: PyBallistaConfig
}

#[pymethods]
impl PyBallista {
    #[new]
    pub fn new() -> Self {
        Self {
            config: PyBallistaConfig::new()
        }
    }
    
    pub fn configuration(&mut self, settings: HashMap<String, String>) {
        let settings = BallistaConfig::with_settings(settings).expect("Non-Valid entries");
        let ballista_config = PyBallistaConfig { config: settings };
        self.config = ballista_config
    }

    #[staticmethod]
    /// Construct the standalone instance from the SessionContext
    pub fn standalone(py: Python) -> PyResult<DataFusionPythonSessionContext> {
        // Define the SessionContext
        let session_context = SessionContext::standalone();
        // SessionContext is an async function
        let ctx = wait_for_future(py, session_context).unwrap();

        // Convert the SessionContext into a Python SessionContext
        Ok(ctx.into())
    }

    #[staticmethod]
    /// Construct the remote instance from the SessionContext
    pub fn remote(url: &str, py: Python) -> PyResult<DataFusionPythonSessionContext> {
        let session_context = SessionContext::remote(url);
        let ctx = wait_for_future(py, session_context)?;

        // Convert the SessionContext into a Python SessionContext
        Ok(ctx.into())
    }
    
    pub fn settings(&self) -> HashMap<String, String> {
        let settings = &self.config.settings();
        settings.to_owned()
    }
    
    pub fn default_shuffle_partitions(&self) -> usize {
        self.config.default_shuffle_partitions()
    }

    pub fn default_batch_size(&self) -> usize {
        self.config.default_batch_size()
    }

    pub fn hash_join_single_partition_threshold(&self) -> usize {
        self.config.hash_join_single_partition_threshold()
    }

    pub fn default_grpc_client_max_message_size(&self) -> usize {
        self.config.default_grpc_client_max_message_size()
    }

    pub fn repartition_joins(&self) -> bool {
        self.config.repartition_joins()
    }

    pub fn repartition_aggregations(&self) -> bool {
        self.config.repartition_aggregations()
    }

    pub fn repartition_windows(&self) -> bool {
        self.config.repartition_windows()
    }

    pub fn parquet_pruning(&self) -> bool {
        self.config.parquet_pruning()
    }

    pub fn collect_statistics(&self) -> bool {
        self.config.collect_statistics()
    }

    pub fn default_standalone_parallelism(&self) -> usize {
        self.config.default_standalone_parallelism()
    }

    pub fn default_with_information_schema(&self) -> bool {
        self.config.default_with_information_schema()
    }
}
