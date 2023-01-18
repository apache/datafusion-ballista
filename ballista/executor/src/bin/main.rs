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

//! Ballista Rust executor binary.

use anyhow::Result;

use ballista_core::config::{LogRotationPolicy, TaskSchedulingPolicy};
use ballista_core::print_version;
use ballista_executor::executor_process::{
    start_executor_process, ExecutorProcessConfig,
};
use config::prelude::*;

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(env!("OUT_DIR"), "/executor_configure_me_config.rs"));
}

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // parse command-line arguments
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/executor.toml"])
            .unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let log_file_name_prefix = format!(
        "executor_{}_{}",
        opt.external_host
            .clone()
            .unwrap_or_else(|| "localhost".to_string()),
        opt.bind_port
    );

    let config = ExecutorProcessConfig {
        special_mod_log_level: opt.log_level_setting,
        external_host: opt.external_host,
        bind_host: opt.bind_host,
        port: opt.bind_port,
        grpc_port: opt.bind_grpc_port,
        scheduler_host: "".to_string(),
        scheduler_port: 0,
        scheduler_connect_timeout_seconds: 0,
        concurrent_tasks: 0,
        task_scheduling_policy: TaskSchedulingPolicy::PullStaged,
        work_dir: None,
        log_dir: opt.log_dir,
        log_file_name_prefix,
        log_rotation_policy: LogRotationPolicy::Minutely,
        print_thread_info: opt.print_thread_info,
        job_data_ttl_seconds: 0,
        job_data_clean_up_interval_seconds: 0,
    };

    start_executor_process(config).await
}
