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

use std::sync::Arc;

use datafusion::{execution::runtime_env::RuntimeEnv, prelude::SessionConfig};
pub const BALLISTA_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn print_version() {
    println!("Ballista version: {BALLISTA_VERSION}")
}

pub mod client;
pub mod config;
pub mod consistent_hash;
pub mod diagram;
pub mod error;
pub mod event_loop;
pub mod execution_plans;
pub mod extension;
#[cfg(feature = "build-binary")]
pub mod object_store;
pub mod planner;
pub mod registry;
pub mod remote_catalog;
pub mod serde;
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
