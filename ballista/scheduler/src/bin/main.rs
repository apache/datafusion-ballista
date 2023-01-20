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

//! Ballista Rust scheduler binary.

use std::{env, io, sync::Arc};

use anyhow::{Context, Result};

use ballista_core::print_version;
use ballista_scheduler::scheduler_process::start_server;
#[cfg(feature = "etcd")]
use ballista_scheduler::state::backend::etcd::EtcdClient;
use ballista_scheduler::state::backend::memory::MemoryBackendClient;
#[cfg(feature = "sled")]
use ballista_scheduler::state::backend::sled::SledClient;
use ballista_scheduler::state::backend::{StateBackend, StateBackendClient};

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(
        env!("OUT_DIR"),
        "/scheduler_configure_me_config.rs"
    ));
}

use ballista_core::config::LogRotationPolicy;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::state::backend::cluster::DefaultClusterState;
use config::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // parse options
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/scheduler.toml"])
            .unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let config_backend = init_kv_backend(&opt.config_backend, &opt).await?;

    let cluster_state = if opt.cluster_backend == opt.config_backend {
        Arc::new(DefaultClusterState::new(config_backend.clone()))
    } else {
        let cluster_kv_store = init_kv_backend(&opt.cluster_backend, &opt).await?;

        Arc::new(DefaultClusterState::new(cluster_kv_store))
    };

    let special_mod_log_level = opt.log_level_setting;
    let namespace = opt.namespace;
    let external_host = opt.external_host;
    let bind_host = opt.bind_host;
    let port = opt.bind_port;
    let log_dir = opt.log_dir;
    let print_thread_info = opt.print_thread_info;
    let log_file_name_prefix =
        format!("scheduler_{}_{}_{}", namespace, external_host, port);
    let scheduler_name = format!("{}:{}", external_host, port);

    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or(special_mod_log_level));
    // File layer
    if let Some(log_dir) = log_dir {
        let log_file = match opt.log_rotation_policy {
            LogRotationPolicy::Minutely => {
                tracing_appender::rolling::minutely(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Hourly => {
                tracing_appender::rolling::hourly(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Daily => {
                tracing_appender::rolling::daily(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Never => {
                tracing_appender::rolling::never(log_dir, &log_file_name_prefix)
            }
        };
        tracing_subscriber::fmt()
            .with_ansi(true)
            .with_thread_names(print_thread_info)
            .with_thread_ids(print_thread_info)
            .with_writer(log_file)
            .with_env_filter(log_filter)
            .init();
    } else {
        // Console layer
        tracing_subscriber::fmt()
            .with_ansi(true)
            .with_thread_names(print_thread_info)
            .with_thread_ids(print_thread_info)
            .with_writer(io::stdout)
            .with_env_filter(log_filter)
            .init();
    }

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;

    let config = SchedulerConfig {
        scheduling_policy: opt.scheduler_policy,
        event_loop_buffer_size: opt.event_loop_buffer_size,
        executor_slots_policy: opt.executor_slots_policy,
        finished_job_data_clean_up_interval_seconds: opt
            .finished_job_data_clean_up_interval_seconds,
        finished_job_state_clean_up_interval_seconds: opt
            .finished_job_state_clean_up_interval_seconds,
        advertise_flight_sql_endpoint: opt.advertise_flight_sql_endpoint,
    };
    start_server(scheduler_name, config_backend, cluster_state, addr, config).await?;
    Ok(())
}

async fn init_kv_backend(
    backend: &StateBackend,
    opt: &Config,
) -> Result<Arc<dyn StateBackendClient>> {
    let cluster_backend: Arc<dyn StateBackendClient> = match backend {
        #[cfg(feature = "etcd")]
        StateBackend::Etcd => {
            let etcd = etcd_client::Client::connect(&[opt.etcd_urls.clone()], None)
                .await
                .context("Could not connect to etcd")?;
            Arc::new(EtcdClient::new(opt.namespace.clone(), etcd))
        }
        #[cfg(not(feature = "etcd"))]
        StateBackend::Etcd => {
            unimplemented!(
                "build the scheduler with the `etcd` feature to use the etcd config backend"
            )
        }
        #[cfg(feature = "sled")]
        StateBackend::Sled => {
            if opt.sled_dir.is_empty() {
                Arc::new(
                    SledClient::try_new_temporary()
                        .context("Could not create sled config backend")?,
                )
            } else {
                println!("{}", opt.sled_dir);
                Arc::new(
                    SledClient::try_new(opt.sled_dir.clone())
                        .context("Could not create sled config backend")?,
                )
            }
        }
        #[cfg(not(feature = "sled"))]
        StateBackend::Sled => {
            unimplemented!(
                "build the scheduler with the `sled` feature to use the sled config backend"
            )
        }
        StateBackend::Memory => Arc::new(MemoryBackendClient::new()),
    };

    Ok(cluster_backend)
}
