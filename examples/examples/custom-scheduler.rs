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

use anyhow::Result;
use ballista_core::print_version;
use ballista_examples::object_store::{
    custom_session_config_with_s3_options, custom_session_state_with_s3_support,
};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{Config, ResultExt, SchedulerConfig};
use ballista_scheduler::scheduler_process::start_server;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

///
/// # Custom Ballista Scheduler
///
/// This example demonstrates how to crate custom made ballista schedulers.
///
#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    // parse options
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/scheduler.toml"])
            .unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let addr = format!("{}:{}", opt.bind_host, opt.bind_port);
    let addr = addr.parse()?;
    let mut config: SchedulerConfig = opt.try_into()?;

    // overriding default runtime producer with custom producer
    // which knows how to create S3 connections
    config.override_config_producer =
        Some(Arc::new(custom_session_config_with_s3_options));
    // overriding default session builder, which has custom session configuration
    // runtime environment and session state.
    config.override_session_builder = Some(Arc::new(|session_config: SessionConfig| {
        custom_session_state_with_s3_support(session_config)
    }));
    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
