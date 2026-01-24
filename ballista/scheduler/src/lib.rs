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

#![doc = include_str ! ("../README.md")]
#![warn(missing_docs)]
#[cfg(feature = "rest-api")]
/// REST API endpoints for scheduler operations.
pub mod api;
/// Cluster management and executor coordination.
pub mod cluster;
/// Scheduler configuration options.
pub mod config;
/// Display utilities for execution plans and state.
pub mod display;
/// Metrics collection and reporting.
pub mod metrics;
/// Physical query plan optimizers.
pub mod physical_optimizer;
/// Query planning utilities for distributed execution.
pub mod planner;
/// Scheduler process management and lifecycle.
pub mod scheduler_process;
/// Core scheduler server implementation.
pub mod scheduler_server;
/// Standalone scheduler mode utilities.
pub mod standalone;
/// Scheduler state management.
pub mod state;

mod flight_proxy_service;

/// Test utilities for scheduler testing.
#[cfg(test)]
pub mod test_utils;

pub use scheduler_server::SessionBuilder;
