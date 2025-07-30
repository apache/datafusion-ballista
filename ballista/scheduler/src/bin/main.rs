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

use ballista_core::config::LogRotationPolicy;
use ballista_core::error::BallistaError;
use ballista_core::object_store::{
    session_config_with_s3_support, session_state_with_s3_support,
};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{Config, SchedulerConfig};
use ballista_scheduler::scheduler_process::start_server;
use clap::Parser;
use std::sync::Arc;
use std::{env, io};
use tracing_subscriber::EnvFilter;

fn main() -> ballista_core::error::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .thread_stack_size(32 * 1024 * 1024) // 32MB
        .build()
        .unwrap();

    runtime.block_on(inner())
}
async fn inner() -> ballista_core::error::Result<()> {
    // parse options
    let opt = Config::parse();

    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or(opt.log_level_setting.clone()));

    let tracing = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_thread_names(opt.print_thread_info)
        .with_thread_ids(opt.print_thread_info)
        .with_writer(io::stdout)
        .with_env_filter(log_filter);

    // File layer
    if let Some(log_dir) = &opt.log_dir {
        let log_file_name_prefix = format!(
            "scheduler_{}_{}_{}",
            opt.namespace, opt.external_host, opt.bind_port
        );

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

        tracing.with_writer(log_file).init();
    } else {
        tracing.init();
    }
    let addr = format!("{}:{}", opt.bind_host, opt.bind_port);
    let addr = addr.parse().map_err(|e: std::net::AddrParseError| {
        BallistaError::Configuration(e.to_string())
    })?;

    let config: SchedulerConfig = opt.try_into()?;
    let config = config
        .with_override_config_producer(Arc::new(session_config_with_s3_support))
        .with_override_session_builder(Arc::new(session_state_with_s3_support));

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
