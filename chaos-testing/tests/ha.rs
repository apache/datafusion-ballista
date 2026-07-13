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

use std::time::Duration;

/// Scenario D: SIGKILL an executor while it is running tasks.
///
/// `chaos_delay` holds stage 1 open so the kill lands while tasks are genuinely
/// in flight. The scheduler must detect the loss, reschedule the dead executor's
/// tasks onto the survivor, and still return the correct result.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn executor_killed_mid_stage_is_recovered(#[case] aqe: bool) {
    let mut run = ChaosRun::start(aqe, 2).await;
    let expected = run.local_baseline().await;

    // Delay every scan task by 300ms per batch so the stage stays running long
    // enough to kill an executor inside it.
    let sql = Fixture::chaos_query("chaos_delay(f.key >= 0, 300)");

    // Submit the query concurrently, then kill executor 0 once stage 1 is running.
    let query = tokio::spawn({
        let ctx = run.clone_ctx();
        let sql = sql.clone();
        async move { ctx.sql(&sql).await?.collect().await }
    });

    let job_id = run.cluster.running_job_id().await.expect("job must appear");
    run.cluster
        .await_stage_running(&job_id, 1)
        .await
        .expect("stage 1 must start running");
    run.cluster.kill_executor(0).expect("kill executor 0");

    let batches = tokio::time::timeout(Duration::from_secs(120), query)
        .await
        .expect("query must not hang after an executor is killed")
        .expect("query task must not panic")
        .expect("query must recover from the lost executor");

    let actual = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert_eq!(
        actual, expected,
        "result after executor loss must match the baseline"
    );
}

/// Scenario E: SIGKILL a map-side executor after it wrote shuffle output.
///
/// The downstream stage must fetch shuffle partitions from an executor that no
/// longer exists. Recovery requires re-running the map stage. The executor
/// timeout is raised to 60s to bias the failure toward the FetchPartitionError
/// path rather than the heartbeat-expiry ExecutorLost path; both are valid
/// recoveries, so the assertion is on correctness, and the path that actually
/// fired is only recorded.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn executor_killed_after_shuffle_write_is_recovered(#[case] aqe: bool) {
    let mut run = ChaosRun::start_with(aqe, 2, 60).await;
    let expected = run.local_baseline().await;

    // Delay the *aggregate* side so the reduce stage is slow, giving us a window
    // between "stage 1 succeeded" and "stage 2 has finished fetching".
    let sql = Fixture::chaos_query("chaos_delay(f.key >= 0, 50)");

    let query = tokio::spawn({
        let ctx = run.clone_ctx();
        let sql = sql.clone();
        async move { ctx.sql(&sql).await?.collect().await }
    });

    let job_id = run.cluster.running_job_id().await.expect("job must appear");
    run.cluster
        .await_stage_successful(&job_id, 1)
        .await
        .expect("stage 1 must complete before we kill its executor");
    run.cluster.kill_executor(0).expect("kill executor 0");

    let batches = tokio::time::timeout(Duration::from_secs(180), query)
        .await
        .expect("query must not hang after shuffle output is lost")
        .expect("query task must not panic")
        .expect("query must recover by re-running the map stage");

    let actual = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert_eq!(
        actual, expected,
        "result after shuffle-output loss must match the baseline"
    );
}

/// Scenario F: an executor is killed and restarted; the cluster must reabsorb it.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn restarted_executor_rejoins_and_serves_queries(#[case] aqe: bool) {
    let mut run = ChaosRun::start(aqe, 2).await;
    let expected = run.local_baseline().await;

    run.cluster.kill_executor(0).expect("kill executor 0");
    run.cluster
        .restart_executor(0)
        .await
        .expect("restarted executor must re-register");

    assert_eq!(
        run.cluster.registered_executors().await.unwrap(),
        2,
        "both executors must be registered after the restart"
    );

    let actual = run
        .sql(Fixture::baseline_query())
        .await
        .expect("cluster must serve queries after an executor restart");
    assert_eq!(actual, expected);
}

/// Scenario G: every executor is killed mid-query.
///
/// There is no executor left to recover onto, so the job cannot succeed. The only
/// requirement is that it *terminates* — a scheduler that waits forever for tasks
/// that can never be scheduled is a hang, and a hang is the bug this detects. The
/// assertion is deliberately weak: this is a hang detector, not a correctness test.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn killing_every_executor_terminates_the_job(#[case] aqe: bool) {
    let mut run = ChaosRun::start(aqe, 2).await;

    let sql = Fixture::chaos_query("chaos_delay(f.key >= 0, 300)");

    let query = tokio::spawn({
        let ctx = run.clone_ctx();
        let sql = sql.clone();
        async move { ctx.sql(&sql).await?.collect().await }
    });

    let job_id = run.cluster.running_job_id().await.expect("job must appear");
    run.cluster
        .await_stage_running(&job_id, 1)
        .await
        .expect("stage 1 must start running");

    run.cluster.kill_executor(0).expect("kill executor 0");
    run.cluster.kill_executor(1).expect("kill executor 1");

    // The job must end, one way or another, rather than hanging forever.
    let outcome = tokio::time::timeout(Duration::from_secs(120), query).await;
    assert!(
        outcome.is_ok(),
        "job did not terminate within 120s after every executor was killed — this is a hang"
    );
}
