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

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;

/// Environment variable that forces a fetch mode instead of a random one.
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

    /// Picks one of `allowed`: the mode [`RESULT_FETCH_ENV`] names if it is
    /// set, otherwise a random one, so repeated runs cover every mode.
    pub fn choose(allowed: &[ResultFetch]) -> Result<ResultFetch, String> {
        choose_from(
            forced().as_deref(),
            allowed,
            RandomState::new().hash_one(()),
        )
    }

    /// The mode [`RESULT_FETCH_ENV`] names, which must be one of `allowed`,
    /// or `default` when it is unset.
    pub fn forced_or(
        default: ResultFetch,
        allowed: &[ResultFetch],
    ) -> Result<ResultFetch, String> {
        match forced() {
            Some(name) => choose_from(Some(&name), allowed, 0),
            None => Ok(default),
        }
    }
}

impl fmt::Display for ResultFetch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

fn forced() -> Option<String> {
    std::env::var(RESULT_FETCH_ENV)
        .ok()
        .filter(|name| !name.is_empty())
}

fn choose_from(
    forced: Option<&str>,
    allowed: &[ResultFetch],
    random: u64,
) -> Result<ResultFetch, String> {
    match forced {
        Some(name) => allowed
            .iter()
            .copied()
            .find(|mode| mode.name() == name)
            .ok_or_else(|| {
                let names: Vec<_> = allowed.iter().map(|mode| mode.name()).collect();
                format!(
                    "{RESULT_FETCH_ENV}={name} is not one of: {}",
                    names.join(", ")
                )
            }),
        None => Ok(allowed[(random % allowed.len() as u64) as usize]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forced_mode_wins_over_the_random_pick() {
        for mode in ResultFetch::ALL {
            assert_eq!(
                choose_from(Some(mode.name()), &ResultFetch::ALL, 0),
                Ok(mode)
            );
        }
    }

    #[test]
    fn a_forced_mode_must_be_allowed() {
        let k8s = [ResultFetch::ResultService, ResultFetch::SchedulerProxy];
        assert!(choose_from(Some("direct"), &k8s, 0).is_err());
        assert!(choose_from(Some("nonsense"), &ResultFetch::ALL, 0).is_err());
    }

    #[test]
    fn random_picks_cover_every_allowed_mode() {
        let picked: Vec<_> = (0..3)
            .map(|random| choose_from(None, &ResultFetch::ALL, random).unwrap())
            .collect();
        assert_eq!(picked, ResultFetch::ALL);
    }
}
