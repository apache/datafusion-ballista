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

//! Runs DataFusion's sqllogictest files against standalone Ballista.
//!
//! The harness itself is the `sqllogictests` test (`tests/sqllogictests/`).
//! This library holds the parts that don't need a cluster: the outcome of a
//! run, the checked-in list of expected failures, and the report.

use std::collections::BTreeSet;
use std::time::Duration;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

pub mod expected;
pub mod report;

/// An engine the files run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, ValueEnum)]
pub enum Engine {
    /// Plain DataFusion, the baseline
    Datafusion,
    /// Standalone Ballista with the static planner
    BallistaStatic,
    /// Standalone Ballista with the adaptive planner
    BallistaAqe,
}

impl Engine {
    pub fn name(self) -> &'static str {
        match self {
            Engine::Datafusion => "datafusion",
            Engine::BallistaStatic => "ballista-static",
            Engine::BallistaAqe => "ballista-aqe",
        }
    }

    /// Whether Ballista's adaptive planner is on, or `None` for DataFusion.
    pub fn adaptive(self) -> Option<bool> {
        match self {
            Engine::Datafusion => None,
            Engine::BallistaStatic => Some(false),
            Engine::BallistaAqe => Some(true),
        }
    }
}

/// A record that failed, keyed by the line it starts on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Failure {
    pub line: u32,
    pub message: String,
}

/// What happened to one file on one engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Outcome {
    /// `TestContext` declined the file (a feature it needs isn't enabled).
    Skipped,
    /// The expected-failure list says not to run it, for this reason.
    NotRun(String),
    /// Every record ran; `failures` lists the ones that failed.
    Ran {
        failures: Vec<Failure>,
    },
    /// Building the context or running the file failed outright.
    Error(String),
    TimedOut,
}

impl Outcome {
    /// The kinds of outcome, in the order the report lists them.
    pub const KINDS: [&str; 6] = ["pass", "fail", "error", "timeout", "skip", "not run"];

    /// One of [`Self::KINDS`].
    pub fn kind(&self) -> &'static str {
        match self {
            Outcome::Ran { failures } if failures.is_empty() => "pass",
            Outcome::Ran { .. } => "fail",
            Outcome::Error(_) => "error",
            Outcome::TimedOut => "timeout",
            Outcome::Skipped => "skip",
            Outcome::NotRun(_) => "not run",
        }
    }

    /// The kind, with the number of failed records.
    pub fn short(&self) -> String {
        match self {
            Outcome::Ran { failures } if !failures.is_empty() => {
                format!("{} fail", failures.len())
            }
            _ => self.kind().to_string(),
        }
    }

    /// No record failed (or the file didn't run).
    pub fn clean(&self) -> bool {
        match self {
            Outcome::Error(_) | Outcome::TimedOut => false,
            _ => self.failures().is_empty(),
        }
    }

    pub fn failures(&self) -> &[Failure] {
        match self {
            Outcome::Ran { failures } => failures,
            _ => &[],
        }
    }

    /// The lines of the failed records, sorted and without duplicates (a
    /// record can fail once per `configMatrix` configuration).
    pub fn failed_lines(&self) -> Vec<u32> {
        let lines: BTreeSet<u32> = self.failures().iter().map(|f| f.line).collect();
        lines.into_iter().collect()
    }
}

/// One file's outcome on every engine it ran on.
#[derive(Debug, Clone)]
pub struct FileResult {
    /// The path under `test_files/`, with `/` separators.
    pub file: String,
    pub outcomes: Vec<(Engine, Outcome)>,
    pub elapsed: Duration,
}

impl FileResult {
    pub fn outcome(&self, engine: Engine) -> Option<&Outcome> {
        self.outcomes
            .iter()
            .find(|(e, _)| *e == engine)
            .map(|(_, o)| o)
    }
}
