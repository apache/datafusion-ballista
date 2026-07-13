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

use chaos_testing::fixture::Fixture;

/// Scenario A: one retryable (IO) fault, budget 1.
///
/// A single task attempt anywhere in the cluster faults; the budget is then
/// exhausted, so the retry must succeed. The load-bearing assertion is that the
/// result still equals the baseline: a retried stage is exactly where duplicated
/// or dropped partitions would show up.
#[rstest]
#[case::aqe_off(false)]
// FINDING (real, reproduced every run, not a harness bug): under AQE, this join
// plans through a broadcast build side collected once and shared across output
// partitions (DataFusion's OnceFut/OnceAsync pattern). When that collection
// fails, the shared future's error is re-wrapped as `DataFusionError::Shared`.
// Ballista's failure classifier (ballista/core/src/error.rs:237-238) only
// recognizes a *direct* `DataFusionError::IoError`, so a `Shared(IoError(..))`
// falls through to the `other` arm and is misclassified `retryable: false`.
// The job fails on the first attempt instead of retrying. Reproduced with:
// `cargo test -p ballista-chaos --test ha retryable_fault -- --test-threads=1`.
// Fixing this requires unwrapping `DataFusionError::Shared` in ballista/core,
// which is a production crate this plan may not touch — see chaos-testing/README.md.
#[ignore = "known bug: AQE-on shared build-side IoError is misclassified non-retryable (ballista/core/src/error.rs); see chaos-testing/README.md"]
#[case::aqe_on(true)]
#[tokio::test]
async fn retryable_fault_is_retried_and_result_is_correct(#[case] aqe: bool) {
    let run = ChaosRun::start(aqe, 2).await;
    let expected = run.local_baseline().await;

    let budget = run.budget("scenario-a", 1);
    let sql = Fixture::chaos_query(&format!(
        "chaos_fail(f.key = 7, 'io', '{}')",
        budget.dir().display()
    ));

    let actual = run
        .sql(&sql)
        .await
        .expect("query must recover from one IO fault");

    assert_eq!(
        actual, expected,
        "result after retry must match the baseline"
    );
    assert_eq!(budget.remaining(), 0, "the fault must actually have fired");
}

/// Scenario B: an inexhaustible retryable fault.
///
/// The budget far exceeds task_max_failures (4), so every attempt faults. The job
/// must fail rather than retry forever, and the cluster must remain usable
/// afterwards — a scheduler that wedges after a failed job is an HA bug.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn exhausted_retries_fail_the_job_and_leave_the_cluster_healthy(#[case] aqe: bool) {
    let run = ChaosRun::start(aqe, 2).await;
    let expected = run.local_baseline().await;

    let budget = run.budget("scenario-b", 99);
    let sql = Fixture::chaos_query(&format!(
        "chaos_fail(f.key = 7, 'io', '{}')",
        budget.dir().display()
    ));

    let err = run
        .sql(&sql)
        .await
        .expect_err("the job must fail once retries are exhausted");
    assert!(!err.is_empty(), "the failure must carry an error message");

    // The cluster must still serve queries. A chaos-free query proves the
    // scheduler and both executors survived the failed job.
    let after = run
        .sql(Fixture::baseline_query())
        .await
        .expect("cluster must still be healthy after a failed job");
    assert_eq!(after, expected);
}

/// Scenario C: a panicking task.
///
/// The executor catches the panic (executor.rs:237) and turns it into a
/// non-retryable Internal error, so the job fails immediately with no retry. This
/// test encodes *today's* behaviour, not necessarily the desired behaviour: if we
/// later decide panics should be retryable, this is the test that changes.
///
/// The second assertion is the important one: the executor process must survive.
/// A panic in one task must not take down the whole executor and every other task
/// running on it.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn panicking_task_fails_the_job_but_the_executor_survives(#[case] aqe: bool) {
    let mut run = ChaosRun::start(aqe, 2).await;
    let expected = run.local_baseline().await;

    let budget = run.budget("scenario-c", 1);
    let sql = Fixture::chaos_query(&format!(
        "chaos_fail(f.key = 7, 'panic', '{}')",
        budget.dir().display()
    ));

    let err = run
        .sql(&sql)
        .await
        .expect_err("a panicking task must fail the job");
    assert!(!err.is_empty());
    assert_eq!(budget.remaining(), 0, "the panic must actually have fired");

    // Both executor processes must still be alive.
    assert!(
        run.cluster.executor_is_alive(0),
        "executor 0 died on a task panic"
    );
    assert!(
        run.cluster.executor_is_alive(1),
        "executor 1 died on a task panic"
    );

    // And the cluster must still serve queries.
    let after = run
        .sql(Fixture::baseline_query())
        .await
        .expect("cluster must still be healthy after a panicking task");
    assert_eq!(after, expected);
}
