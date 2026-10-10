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

//! How clients fetch query results from a test cluster.

use std::fmt;

/// Environment variable that overrides a cluster's fetch mode.
pub const RESULT_FETCH_ENV: &str = "CHAOS_RESULT_FETCH";

/// How clients fetch query results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultFetch {
    /// Straight from the executors.
    Direct,
    /// Through the scheduler's deprecated embedded proxy.
    SchedulerProxy,
    /// Through a result service.
    ResultService,
}

impl ResultFetch {
    /// Every mode.
    pub const ALL: [ResultFetch; 3] = [
        ResultFetch::Direct,
        ResultFetch::SchedulerProxy,
        ResultFetch::ResultService,
    ];

    /// The mode's value for [`RESULT_FETCH_ENV`].
    pub fn name(self) -> &'static str {
        match self {
            ResultFetch::Direct => "direct",
            ResultFetch::SchedulerProxy => "proxy",
            ResultFetch::ResultService => "result-service",
        }
    }

    /// The mode [`RESULT_FETCH_ENV`] names, or `default` when it is unset.
    /// Either must be one of `allowed`.
    pub fn forced_or(
        default: ResultFetch,
        allowed: &[ResultFetch],
    ) -> Result<ResultFetch, String> {
        let forced = std::env::var(RESULT_FETCH_ENV)
            .ok()
            .filter(|name| !name.is_empty());
        resolve(forced.as_deref(), default, allowed)
    }
}

impl fmt::Display for ResultFetch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

fn names(modes: &[ResultFetch]) -> String {
    modes
        .iter()
        .map(|mode| mode.name())
        .collect::<Vec<_>>()
        .join(", ")
}

fn resolve(
    forced: Option<&str>,
    default: ResultFetch,
    allowed: &[ResultFetch],
) -> Result<ResultFetch, String> {
    let mode = match forced {
        Some(name) => ResultFetch::ALL
            .into_iter()
            .find(|mode| mode.name() == name)
            .ok_or_else(|| {
                format!(
                    "{RESULT_FETCH_ENV}={name} is not one of: {}",
                    names(&ResultFetch::ALL)
                )
            })?,
        None => default,
    };
    if allowed.contains(&mode) {
        Ok(mode)
    } else {
        Err(format!(
            "this cluster can't fetch results via {mode}; it supports: {}",
            names(allowed)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const K8S: [ResultFetch; 2] =
        [ResultFetch::ResultService, ResultFetch::SchedulerProxy];

    #[test]
    fn the_default_applies_unless_a_mode_is_forced() {
        assert_eq!(
            resolve(None, ResultFetch::Direct, &ResultFetch::ALL),
            Ok(ResultFetch::Direct)
        );
        for mode in ResultFetch::ALL {
            assert_eq!(
                resolve(Some(mode.name()), ResultFetch::Direct, &ResultFetch::ALL),
                Ok(mode)
            );
        }
    }

    #[test]
    fn the_mode_must_be_known_and_allowed() {
        let default = ResultFetch::ResultService;
        assert!(resolve(Some("nonsense"), default, &K8S).is_err());
        assert!(resolve(Some("direct"), default, &K8S).is_err());
        assert!(resolve(None, ResultFetch::Direct, &K8S).is_err());
    }
}
