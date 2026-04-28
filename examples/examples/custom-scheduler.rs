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

use ballista_core::error::BallistaError;
use ballista_core::object_store::{
    session_config_with_s3_support, session_state_with_s3_support,
};
use ballista_core::serde::scheduler::ExecutorMetadata;

use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, SchedulerEventListener};
use ballista_scheduler::scheduler_process::start_server;
use log::info;
use std::net::AddrParseError;
use std::sync::Arc;

/// Example event listener that logs executor lifecycle events.
struct LoggingEventListener;

impl SchedulerEventListener for LoggingEventListener {
    fn on_executor_registered(&self, metadata: &ExecutorMetadata) {
        info!(
            "Executor registered: {} at {}:{}",
            metadata.id, metadata.host, metadata.port
        );
    }

    fn on_executor_lost(&self, executor_id: &str) {
        info!("Executor lost: {executor_id}");
    }
}

///
/// # Custom Ballista Scheduler
///
/// This example demonstrates how to create custom ballista schedulers
/// with overridden config, session builder, and event listeners.
///
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let config: SchedulerConfig = SchedulerConfig {
        // overriding default runtime producer with custom producer
        // which knows how to create S3 connections
        override_config_producer: Some(Arc::new(session_config_with_s3_support)),
        // overriding default session builder, which has custom session configuration
        // runtime environment and session state.
        override_session_builder: Some(Arc::new(session_state_with_s3_support)),
        ..Default::default()
    }
    // add event listener for executor lifecycle events
    .with_event_listener(Arc::new(LoggingEventListener));

    let addr = format!("{}:{}", config.bind_host, config.bind_port);
    let addr = addr
        .parse()
        .map_err(|e: AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
