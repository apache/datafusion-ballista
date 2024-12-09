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

use ballista_examples::object_store::{
    custom_runtime_env_with_s3_support, custom_session_config_with_s3_options,
};
use ballista_executor::config::prelude::*;
use ballista_executor::executor_process::{
    start_executor_process, ExecutorProcessConfig,
};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
///
/// # Custom Ballista Executor
///
/// This example demonstrates how to crate custom ballista executors.
///
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/executor.toml"])
            .unwrap_or_exit();

    if opt.version {
        ballista_core::print_version();
        std::process::exit(0);
    }

    let mut config: ExecutorProcessConfig = opt.try_into().unwrap();

    // overriding default config producer with custom producer
    // which has required S3 configuration options
    config.override_config_producer =
        Some(Arc::new(custom_session_config_with_s3_options));

    // overriding default runtime producer with custom producer
    // which knows how to create S3 connections
    config.override_runtime_producer =
        Some(Arc::new(|session_config: &SessionConfig| {
            custom_runtime_env_with_s3_support(session_config)
        }));

    start_executor_process(Arc::new(config)).await
}
