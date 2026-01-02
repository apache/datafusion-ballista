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

use ballista_core::config::LogRotationPolicy;
use ballista_core::object_store::{
    runtime_env_with_s3_support, session_config_with_s3_support,
};
use ballista_executor::config::Config;
use ballista_executor::executor_process::{
    ExecutorProcessConfig, start_executor_process,
};
use clap::Parser;
use std::env;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    // parse command-line arguments
    let opt = Config::parse();

    let mut config: ExecutorProcessConfig = opt.try_into()?;
    config.override_config_producer = Some(Arc::new(session_config_with_s3_support));
    config.override_runtime_producer = Some(Arc::new(runtime_env_with_s3_support));

    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter =
        EnvFilter::new(rust_log.unwrap_or(config.special_mod_log_level.clone()));

    let tracing = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_thread_names(config.print_thread_info)
        .with_thread_ids(config.print_thread_info)
        .with_env_filter(log_filter);

    // File layer
    if let Some(log_dir) = &config.log_dir {
        let log_file = match config.log_rotation_policy {
            LogRotationPolicy::Minutely => tracing_appender::rolling::minutely(
                log_dir,
                config.log_file_name_prefix(),
            ),
            LogRotationPolicy::Hourly => {
                tracing_appender::rolling::hourly(log_dir, config.log_file_name_prefix())
            }
            LogRotationPolicy::Daily => {
                tracing_appender::rolling::daily(log_dir, config.log_file_name_prefix())
            }
            LogRotationPolicy::Never => {
                tracing_appender::rolling::never(log_dir, config.log_file_name_prefix())
            }
        };

        tracing.with_writer(log_file).init();
    } else {
        tracing.init();
    }

    start_executor_process(Arc::new(config)).await
}
