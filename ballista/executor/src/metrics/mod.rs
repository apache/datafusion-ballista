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

use crate::execution_engine::QueryStageExecutor;
use log::info;
use std::{fmt::Display, sync::Arc};

/// `ExecutorMetricsCollector` records metrics for `ShuffleWriteExec`
/// after they are executed.
///
/// After each stage completes, `ShuffleWriteExec::record_stage` will be
/// called.
pub trait ExecutorMetricsCollector: Send + Sync {
    /// Record metrics for stage after it is executed
    fn record_stage(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        plan: Arc<dyn QueryStageExecutor>,
    );
}

/// Implementation of `ExecutorMetricsCollector` which logs the completed
/// plan to stdout.
#[derive(Default)]
pub struct LoggingMetricsCollector {}

impl ExecutorMetricsCollector for LoggingMetricsCollector {
    fn record_stage(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        plan: Arc<dyn QueryStageExecutor>,
    ) {
        info!(
            "\n=== [{job_id}/{stage_id}/{partition}] Physical plan with metrics ===\n{plan}\n"
        );
    }
}

/// Configures which executor's metrics should be collected
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum ExecutorMetricCollectionPolicy {
    /// Collect only system-wide metrics
    SystemOnly,
    /// Collect only current process metrics
    ProcessOnly,
    /// Collect both system-wide and process metrics
    SystemAndProcess,
    /// Default value - no metrics collected
    #[default]
    Off,
}

impl Display for ExecutorMetricCollectionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorMetricCollectionPolicy::SystemOnly => f.write_str("sys"),
            ExecutorMetricCollectionPolicy::ProcessOnly => f.write_str("proc"),
            ExecutorMetricCollectionPolicy::SystemAndProcess => f.write_str("all"),
            ExecutorMetricCollectionPolicy::Off => f.write_str("off"),
        }
    }
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for ExecutorMetricCollectionPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}
