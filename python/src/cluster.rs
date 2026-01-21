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

use crate::utils::to_pyerr;
use crate::utils::{spawn_feature, wait_for_future};
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
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
    pub fn new(_py: Python, bind_host: Option<String>, bind_port: Option<u16>) -> Self {
        let mut config = SchedulerConfig::default();

        if let Some(bind_port) = bind_port {
            config.bind_port = bind_port;
        }

        if let Some(host) = bind_host {
            config.bind_host = host;
        }

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
            .map_err(to_pyerr)?;

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

    pub fn close(&mut self) -> PyResult<()> {
        let mut handle = None;
        std::mem::swap(&mut self.handle, &mut handle);

        if let Some(handle) = handle {
            handle.abort()
        }

        Ok(())
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
            "BallistaScheduler(listening address={}:{}, listening= {})",
            self.config.bind_host,
            self.config.bind_port,
            self.handle.is_some()
        )
    }
}

#[pyclass(name = "BallistaExecutor", module = "ballista", subclass)]
pub struct PyExecutor {
    config: Arc<ExecutorProcessConfig>,
    handle: Option<JoinHandle<()>>,
}

// FIXME: there is outstanding issue of executor outliving python process
//        which forked it. This will be investigated further
#[pymethods]
impl PyExecutor {
    #[pyo3(signature = (bind_port=None, bind_host =None, scheduler_host = None, scheduler_port = None, concurrent_tasks = None))]
    #[new]
    pub fn new(
        _py: Python,
        bind_port: Option<u16>,
        bind_host: Option<String>,
        scheduler_host: Option<String>,
        scheduler_port: Option<u16>,
        concurrent_tasks: Option<u16>,
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

        if let Some(concurrent_tasks) = concurrent_tasks {
            config.concurrent_tasks = concurrent_tasks as usize
        }

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

    pub fn close(&mut self) -> PyResult<()> {
        let mut handle = None;
        std::mem::swap(&mut self.handle, &mut handle);

        if let Some(handle) = handle {
            handle.abort()
        }

        Ok(())
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
                self.config.scheduler_port,
            ),
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "BallistaExecutor(address={}:{}, scheduler={}:{}, concurrent_tasks={} listening={})",
            self.config.bind_host,
            self.config.port,
            self.config.scheduler_host,
            self.config.scheduler_port,
            self.config.concurrent_tasks,
            self.handle.is_some()
        )
    }
}

/// a method which setups standalone ballista cluster for
/// testing purposes. it returns address and port of
/// running cluster

#[pyo3::pyfunction]
pub fn setup_test_cluster(py: Python) -> PyResult<(String, u16)> {
    let tuple = wait_for_future(py, _setup_test_cluster());
    Ok(tuple)
}

async fn _setup_test_cluster() -> (String, u16) {
    let config = <datafusion::prelude::SessionConfig as ballista::prelude::SessionConfigExt>::new_with_ballista();
    let default_codec = ballista_core::serde::BallistaCodec::default();

    let addr = ballista_scheduler::standalone::new_standalone_scheduler()
        .await
        .expect("scheduler to be created");

    let host = "localhost".to_string();

    let scheduler =
        connect_to_scheduler(format!("http://{}:{}", host, addr.port())).await;

    ballista_executor::new_standalone_executor(
        scheduler,
        ballista::prelude::SessionConfigExt::ballista_standalone_parallelism(&config),
        default_codec,
    )
    .await
    .expect("executor to be created");

    log::info!("test scheduler created at: {}:{}", host, addr.port());

    (host, addr.port())
}

async fn connect_to_scheduler(
    scheduler_url: String,
) -> SchedulerGrpcClient<tonic::transport::Channel> {
    let mut retry = 50;
    loop {
        match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
            Err(_) if retry > 0 => {
                retry -= 1;
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                log::debug!("Re-attempting to connect to test scheduler...");
            }

            Err(_) => {
                log::error!("scheduler connection timed out");
                panic!("scheduler connection timed out")
            }
            Ok(scheduler) => break scheduler,
        }
    }
}
