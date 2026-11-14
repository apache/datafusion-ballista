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

//! Fixtures shared by the unit tests of the executor.

use std::sync::Arc;

use ballista_core::RuntimeProducer;
use ballista_core::serde::protobuf::ExecutorRegistration;
use ballista_core::utils::default_config_producer;
use datafusion::execution::runtime_env::RuntimeEnv;

use crate::executor::Executor;

/// A runtime producer that builds a new default `RuntimeEnv` on every call.
pub(crate) fn fresh_runtime_producer() -> RuntimeProducer {
    Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())))
}

/// An executor with id `executor` and the default config producer, running
/// up to `concurrent_tasks` tasks at once.
pub(crate) fn test_executor(
    work_dir: &str,
    runtime_producer: RuntimeProducer,
    concurrent_tasks: usize,
) -> Executor {
    Executor::new_basic(
        ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        },
        work_dir,
        runtime_producer,
        Arc::new(default_config_producer),
        concurrent_tasks,
    )
}
