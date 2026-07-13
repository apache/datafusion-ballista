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

//! End-to-end high-availability scenarios against a real multi-process cluster.
//!
//! Every scenario runs under both AQE settings. The AQE-on axis is where bugs are
//! expected: a resubmitted stage under AQE is re-planned against runtime
//! statistics, so a re-run map stage can come back with a different plan than the
//! one whose output was lost.
//!
//! Every test in this file spawns a whole multi-process cluster, so this file
//! must always be run with `--test-threads=1` or ports and CPU will be
//! exhausted by concurrent clusters.

mod common;

use common::ChaosRun;
use rstest::rstest;

/// The cluster must agree with local DataFusion before any fault is injected.
/// Every recovery scenario asserts against this baseline, so if it is wrong,
/// every other assertion is meaningless.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn baseline_matches_local_datafusion(#[case] aqe: bool) {
    let run = ChaosRun::start(aqe, 2).await;

    let expected = run.local_baseline().await;
    let actual = run
        .sql(ballista_chaos_query_baseline())
        .await
        .expect("baseline query must succeed on the cluster");

    assert_eq!(
        actual, expected,
        "cluster result must match local DataFusion"
    );
}

fn ballista_chaos_query_baseline() -> &'static str {
    chaos_testing::fixture::Fixture::baseline_query()
}
