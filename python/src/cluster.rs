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

use std::future::IntoFuture;
use std::sync::Arc;

use crate::codec::{PyLogicalCodec, PyPhysicalCodec};
use crate::utils::to_pyerr;
use crate::utils::{spawn_feature, wait_for_future};
use ballista_executor::executor_process::{
    start_executor_process, ExecutorProcessConfig,
};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use pyo3::exceptions::PyException;
use pyo3::{pyclass, pymethods, PyResult, Python};
use tokio::task::JoinHandle;

#[pyclass(name = "BallistaScheduler", module = "ballista", subclass)]
pub struct PyScheduler {
    config: SchedulerConfig,
    handle: Option<JoinHandle<()>>,
}

#[pymethods]
impl PyScheduler {
    #[pyo3(signature = (bind_host=None, bind_port=None))]
    #[new]
    pub fn new(py: Python, bind_host: Option<String>, bind_port: Option<u16>) -> Self {
        let mut config = SchedulerConfig::default();

        if let Some(bind_port) = bind_port {
            config.bind_port = bind_port;
        }

        if let Some(host) = bind_host {
            config.bind_host = host;
        }

        config.override_logical_codec =
            Some(Arc::new(PyLogicalCodec::try_new(py).unwrap()));
        config.override_physical_codec =
            Some(Arc::new(PyPhysicalCodec::try_new(py).unwrap()));

        Self {
            config,
            handle: None,
        }
    }

    pub fn start(&mut self, py: Python) -> PyResult<()> {
        if self.handle.is_some() {
            return Err(PyException::new_err("Scheduler already started"));
        }
        let cluster = wait_for_future(py, BallistaCluster::new_from_config(&self.config))
            .map_err(|e| to_pyerr(e))?;

        let config = self.config.clone();
        let address = format!("{}:{}", config.bind_host, config.bind_port);
        let address = address.parse()?;
        let handle = spawn_feature(py, async move {
            start_server(cluster, address, Arc::new(config))
                .await
                .unwrap();
        });
        self.handle = Some(handle);

        Ok(())
    }

    pub fn wait_for_termination(&mut self, py: Python) -> PyResult<()> {
        if self.handle.is_none() {
            return Err(PyException::new_err("Scheduler not started"));
        }
        let mut handle = None;
        std::mem::swap(&mut self.handle, &mut handle);

        match handle {
            Some(handle) => wait_for_future(py, handle.into_future())
                .map_err(|e| PyException::new_err(e.to_string())),
            None => Ok(()),
        }
    }
    #[classattr]
    pub fn version() -> &'static str {
        ballista_core::BALLISTA_VERSION
    }

    pub fn __str__(&self) -> String {
        match self.handle {
            Some(_) => format!(
                "listening address={}:{}",
                self.config.bind_host, self.config.bind_port,
            ),
            None => format!(
                "configured address={}:{}",
                self.config.bind_host, self.config.bind_port,
            ),
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "BallistaScheduler(config={:?}, listening= {})",
            self.config,
            self.handle.is_some()
        )
    }
}

#[pyclass(name = "BallistaExecutor", module = "ballista", subclass)]
pub struct PyExecutor {
    config: Arc<ExecutorProcessConfig>,
    handle: Option<JoinHandle<()>>,
}

#[pymethods]
impl PyExecutor {
    #[pyo3(signature = (bind_port=None, bind_host =None, scheduler_host = None, scheduler_port = None))]
    #[new]
    pub fn new(
        py: Python,
        bind_port: Option<u16>,
        bind_host: Option<String>,
        scheduler_host: Option<String>,
        scheduler_port: Option<u16>,
    ) -> PyResult<Self> {
        let mut config = ExecutorProcessConfig::default();
        if let Some(port) = bind_port {
            config.port = port;
        }

        if let Some(host) = bind_host {
            config.bind_host = host;
        }

        if let Some(port) = scheduler_port {
            config.scheduler_port = port;
        }

        if let Some(host) = scheduler_host {
            config.scheduler_host = host;
        }

        config.override_logical_codec = Some(Arc::new(PyLogicalCodec::try_new(py)?));
        config.override_physical_codec = Some(Arc::new(PyPhysicalCodec::try_new(py)?));

        let config = Arc::new(config);
        Ok(Self {
            config,
            handle: None,
        })
    }

    pub fn start(&mut self, py: Python) -> PyResult<()> {
        if self.handle.is_some() {
            return Err(PyException::new_err("Executor already started"));
        }

        let config = self.config.clone();

        let handle =
            spawn_feature(
                py,
                async move { start_executor_process(config).await.unwrap() },
            );
        self.handle = Some(handle);

        Ok(())
    }

    pub fn wait_for_termination(&mut self, py: Python) -> PyResult<()> {
        if self.handle.is_none() {
            return Err(PyException::new_err("Executor not started"));
        }
        let mut handle = None;
        std::mem::swap(&mut self.handle, &mut handle);

        match handle {
            Some(handle) => wait_for_future(py, handle.into_future())
                .map_err(|e| PyException::new_err(e.to_string()))
                .map(|_| ()),
            None => Ok(()),
        }
    }

    #[classattr]
    pub fn version() -> &'static str {
        ballista_core::BALLISTA_VERSION
    }

    pub fn __str__(&self) -> String {
        match self.handle {
            Some(_) => format!(
                "listening address={}:{}, scheduler={}:{}",
                self.config.bind_host,
                self.config.port,
                self.config.scheduler_host,
                self.config.scheduler_port
            ),
            None => format!(
                "configured address={}:{}, scheduler={}:{}",
                self.config.bind_host,
                self.config.port,
                self.config.scheduler_host,
                self.config.scheduler_port
            ),
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "BallistaExecutor(address={}:{}, scheduler={}:{}, listening={})",
            self.config.bind_host,
            self.config.port,
            self.config.scheduler_host,
            self.config.scheduler_port,
            self.handle.is_some()
        )
    }
}
