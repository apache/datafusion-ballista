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

#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

use std::sync::Arc;

use datafusion::{execution::runtime_env::RuntimeEnv, prelude::SessionConfig};
/// The current version of Ballista, derived from the Cargo package version.
pub const BALLISTA_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Prints the current Ballista version to stdout.
pub fn print_version() {
    println!("Ballista version: {BALLISTA_VERSION}")
}

/// Client utilities for connecting to Ballista schedulers.
pub mod client;
/// Configuration options and settings for Ballista components.
pub mod config;
/// Consistent hashing implementation for data distribution.
pub mod consistent_hash;
/// Utilities for generating execution plan diagrams.
pub mod diagram;
/// Error types and result definitions for Ballista operations.
pub mod error;
/// Event loop infrastructure for asynchronous message processing.
pub mod event_loop;
/// Physical execution plans for distributed query processing.
pub mod execution_plans;
/// Extension traits and utilities for DataFusion integration.
pub mod extension;
#[cfg(feature = "build-binary")]
/// Object store configuration and utilities for distributed file access.
pub mod object_store;
/// Query planning utilities for distributed execution.
pub mod planner;
/// Runtime registry for codec and function registration.
pub mod registry;
/// Serialization and deserialization for Ballista messages and plans.
pub mod serde;
/// General utility functions for Ballista operations.
pub mod utils;

///
/// [RuntimeProducer] is a factory which creates runtime [RuntimeEnv]
/// from [SessionConfig]. As [SessionConfig] will be propagated
/// from client to executors, this provides possibility to
/// create [RuntimeEnv] components and configure them according to
/// [SessionConfig] or some of its config extension
///
/// It is intended to be used with executor configuration
///
pub type RuntimeProducer = Arc<
    dyn Fn(&SessionConfig) -> datafusion::error::Result<Arc<RuntimeEnv>> + Send + Sync,
>;
///
/// [ConfigProducer] is a factory which can create [SessionConfig], with
/// additional extension or configuration codecs
///
/// It is intended to be used with executor configuration
///
pub type ConfigProducer = Arc<dyn Fn() -> SessionConfig + Send + Sync>;
