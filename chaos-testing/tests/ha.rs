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
//! Query-recovery scenarios run under both AQE settings. The AQE-on axis is where
//! bugs are expected: a resubmitted stage under AQE is re-planned against runtime
//! statistics, so a re-run map stage can come back with a different plan than the
//! one whose output was lost.
//!
//! Every test in this file spawns a whole multi-process cluster. `TestCluster`
//! serializes those clusters, so `--test-threads=1` is useful for readable
//! local output but is not required for correctness.

mod common;

use ballista::prelude::SessionConfigExt;
use chaos_testing::fixture::{Fixture, LARGE_RESULT_ROWS};
use common::ChaosRun;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::logical_expr::JoinType;
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use rstest::rstest;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

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

/// Scenario A: one retryable (IO) fault, budget 1.
///
/// A single task attempt anywhere in the cluster faults; the budget is then
/// exhausted, so the retry must succeed. The load-bearing assertion is that the
/// result still equals the baseline: a retried stage is exactly where duplicated
/// or dropped partitions would show up.
///
#[tokio::test]
async fn retryable_fault_is_retried_and_result_is_correct_aqe_off() {
    retryable_fault_is_retried_and_result_is_correct(false).await;
}

#[tokio::test]
async fn retryable_fault_is_retried_and_result_is_correct_aqe_on() {
    retryable_fault_is_retried_and_result_is_correct(true).await;
}

async fn retryable_fault_is_retried_and_result_is_correct(aqe: bool) {
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

/// Scenario D: SIGKILL an executor while it is running tasks.
///
/// `chaos_delay` holds the delayed scan stage open so the kill lands while
/// tasks are genuinely in flight. The scheduler must detect the loss,
/// reschedule the dead executor's tasks onto the survivor, and still return the
/// correct result.
///
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

    // Submit the query concurrently, then kill executor 0 once the delayed
    // stage is running. AQE and non-AQE planning assign different stage ids
    // to that work.
    let query = tokio::spawn({
        let ctx = run.clone_ctx();
        let sql = sql.clone();
        async move { ctx.sql(&sql).await?.collect().await }
    });

    let delayed_stage_id = if aqe { 0 } else { 1 };
    let job_id = run.cluster.running_job_id().await.expect("job must appear");
    run.cluster
        .await_stage_running(&job_id, delayed_stage_id)
        .await
        .expect("delayed stage must start running");
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
///
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn executor_killed_after_shuffle_write_is_recovered(#[case] aqe: bool) {
    let mut run = ChaosRun::start_with_concurrent_tasks(aqe, 2, 60, 1).await;
    let expected = run.local_sql(Fixture::shuffle_loss_query()).await;

    run.sql("SET ballista.shuffle.force_remote_read = true")
        .await
        .expect("force remote shuffle reads");
    run.sql("SET ballista.shuffle.max_concurrent_read_requests = 1")
        .await
        .expect("serialize shuffle reads");

    let sql = Fixture::shuffle_loss_chaos_query("chaos_delay(d.name IS NOT NULL, 300)");

    let query = tokio::spawn({
        let ctx = run.clone_ctx();
        let sql = sql.clone();
        async move { ctx.sql(&sql).await?.collect().await }
    });

    let job_id = run.cluster.running_job_id().await.expect("job must appear");
    let (executor_index, shuffle_stage_id) = run
        .cluster
        .await_successful_shuffle_output(&job_id)
        .await
        .expect("a shuffle-writing stage must complete before we kill its executor");
    if run
        .cluster
        .job_status(&job_id)
        .await
        .expect("job status must be readable")
        == "Completed"
    {
        panic!(
            "job completed before executor {executor_index} could be killed after stage {shuffle_stage_id} wrote shuffle output\n{}",
            run.cluster.diagnostics(&job_id).await
        );
    }
    if query.is_finished() {
        panic!(
            "query completed before executor {executor_index} could be killed after stage {shuffle_stage_id} wrote shuffle output\n{}",
            run.cluster.diagnostics(&job_id).await
        );
    }
    run.cluster
        .kill_executor(executor_index)
        .unwrap_or_else(|e| {
            panic!(
                "kill executor {executor_index} with stage {shuffle_stage_id} shuffle output: {e}"
            )
        });

    let batches = match tokio::time::timeout(Duration::from_secs(180), query).await {
        Ok(result) => match result.expect("query task must not panic") {
            Ok(batches) => batches,
            Err(e) => {
                panic!(
                    "query must recover by re-running the map stage: {e}\n{}",
                    run.cluster.diagnostics(&job_id).await
                )
            }
        },
        Err(e) => {
            panic!(
                "query must not hang after shuffle output is lost: {e}\n{}",
                run.cluster.diagnostics(&job_id).await
            )
        }
    };

    let actual = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert_eq!(
        actual, expected,
        "result after shuffle-output loss must match the baseline"
    );
}

/// Scenario F: an executor is killed and restarted; the cluster must reabsorb it.
///
/// The kill and the restart are separated by a wait for the scheduler to
/// actually reap the dead executor (`registered_executors` dropping to 1),
/// rather than restarting immediately. SIGKILL does not deregister the
/// executor: the scheduler keeps listing it until its heartbeat expires
/// (`executor_timeout_seconds`), so a restart fired immediately after the
/// kill lands while the scheduler still counts *three* executors (the dead
/// one, the survivor, and the freshly restarted one) — a harness race, not a
/// Ballista bug, that used to make this scenario fail with `left: 3, right:
/// 2`. Waiting for the reap first means the final assertion is actually
/// testing what the scenario name promises: that a restarted executor
/// rejoins a cluster that has already noticed it was gone.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn restarted_executor_rejoins_and_serves_queries(#[case] aqe: bool) {
    let mut run = ChaosRun::start(aqe, 2).await;
    let expected = run.local_baseline().await;

    run.cluster.kill_executor(0).expect("kill executor 0");
    run.cluster
        .await_executor_count(1)
        .await
        .expect("scheduler must reap the killed executor before we restart it");

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
/// There is no executor left to recover onto, so the job cannot succeed. Once
/// the last executor is reaped, the scheduler waits a bounded grace period for an
/// executor to (re)register and then fails the job, rather than waiting forever
/// for tasks that can never be scheduled. Both cases assert the query terminates
/// with an error that names the executor loss, instead of hanging.
///
/// Regression test for #2029. The grace period is turned down via the cluster
/// builder so the job fails a second or so after the reap instead of waiting out
/// the 30s default.
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
        .await_any_stage_running(&job_id)
        .await
        .expect("the job must start running a task before we kill its executors");

    run.cluster.kill_executor(0).expect("kill executor 0");
    run.cluster.kill_executor(1).expect("kill executor 1");

    // Once the executors are reaped and the grace period elapses, the scheduler
    // must fail the job rather than hang forever (#2029).
    let result = tokio::time::timeout(Duration::from_secs(120), query)
        .await
        .expect("job must terminate, not hang, after every executor is killed")
        .expect("query task should not panic");
    let err = result.expect_err("query must fail once every executor is lost");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("executor"),
        "failure should name the executor loss, got: {err}"
    );
}

/// Scenario H: SIGTERM an executor while the client is fetching final results.
///
/// Receiving the first batch means task execution has finished and the client is
/// reading the final Flight response. Graceful shutdown must let that response
/// finish before the executor exits.
///
/// Regression test for #2357.
#[tokio::test]
async fn final_results_survive_sigterm() {
    let mut run = ChaosRun::start(false, 1).await;
    let ctx = run.clone_ctx();
    let config = ctx
        .copied_config()
        .with_target_partitions(1)
        .with_ballista_override_create_grpc_client_endpoint(Arc::new(|endpoint| {
            Ok(endpoint
                .initial_stream_window_size(16 * 1024)
                .http2_adaptive_window(false))
        }));
    *ctx.state_ref().write().config_mut() = config;

    timeout(Duration::from_secs(120), async {
        let df = ctx.sql(Fixture::large_result_query()).await.unwrap();
        let mut stream = df.execute_stream().await.unwrap();
        let mut rows = stream.try_next().await.unwrap().unwrap().num_rows();
        assert!(
            rows < LARGE_RESULT_ROWS,
            "result must span multiple batches"
        );

        run.cluster.terminate_executor(0).unwrap();
        run.cluster
            .await_executor_count(0)
            .await
            .expect("scheduler must deregister the terminating executor");

        while let Some(batch) = stream.try_next().await.unwrap_or_else(|e| {
            panic!(
                "result stream failed after {rows}/{LARGE_RESULT_ROWS} rows: {e}\n{}",
                run.cluster.log_tails()
            )
        }) {
            rows += batch.num_rows();
        }
        assert_eq!(rows, LARGE_RESULT_ROWS, "result was truncated");
        assert!(run.cluster.await_executor_exit(0).await.unwrap().success());
    })
    .await
    .expect("query and shutdown must not hang");
}

/// Scenario I: SIGKILL the executor holding a `DataFrame::cache()`, then query it.
///
/// As in Scenario E, the kill lands after the data is written and before the
/// stage reading it starts, with a 60s executor timeout so the scheduler still
/// lists the dead executor: the query hits the cache, and its read must fail.
/// The read job cannot re-run another job's output, so it must fail with a clear
/// error instead of hanging, under both planners; the cache is dropped, so the
/// next query rebuilds it and returns the baseline result.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn executor_holding_a_cache_killed_before_it_is_read_fails_cleanly(
    #[case] aqe: bool,
) {
    let mut run = ChaosRun::start_with(aqe, 2, 60).await;
    let expected = run.local_baseline().await;
    run.sql("SET ballista.cache.noop = false")
        .await
        .expect("enable DataFrame::cache()");

    let ctx = run.clone_ctx();
    let cached = ctx
        .sql("SELECT key, value FROM facts ORDER BY key, value")
        .await
        .unwrap()
        .cache()
        .await
        .unwrap();
    let before = run.cluster.job_ids().await.unwrap();
    cached.clone().count().await.expect("build the cache");

    // The first action runs a build job, then a job reading it.
    let first: Vec<String> = run
        .cluster
        .job_ids()
        .await
        .unwrap()
        .into_iter()
        .filter(|id| !before.contains(id))
        .collect();
    assert_eq!(first.len(), 2, "the first action must build, then read");
    let build_job = &first[0];
    let victim = run
        .cluster
        .executor_with_final_output(build_job)
        .unwrap_or_else(|| panic!("no executor holds the cache of job {build_job}"));
    run.cluster
        .kill_executor(victim)
        .unwrap_or_else(|e| panic!("kill executor {victim}: {e}"));

    // The baseline query, over the cache instead of `facts`.
    let over_cache = || {
        let cached = cached.clone();
        let ctx = ctx.clone();
        async move {
            cached
                .join(
                    ctx.table("dims").await?,
                    JoinType::Inner,
                    &["key"],
                    &["key"],
                    None,
                )?
                .aggregate(
                    vec![col("name")],
                    vec![count(lit(1)).alias("n"), sum(col("value")).alias("total")],
                )?
                .sort(vec![col("name").sort(true, false)])?
                .collect()
                .await
        }
    };

    let known = run.cluster.job_ids().await.unwrap();
    let result = match timeout(Duration::from_secs(120), over_cache()).await {
        Ok(result) => result,
        Err(_) => {
            let jobs = run.cluster.job_ids().await.unwrap();
            let read_job = jobs.iter().find(|id| !known.contains(id)).unwrap();
            panic!(
                "query must not hang after the cache is lost\n{}",
                run.cluster.diagnostics(read_job).await
            )
        }
    };
    let err = result.expect_err("losing the cache's data must fail the query");
    assert!(
        err.to_string().contains("lost DataFrame::cache() data"),
        "failure should name the lost cache data, got: {err}"
    );

    let batches = timeout(Duration::from_secs(120), over_cache())
        .await
        .expect("the retry must not hang")
        .expect("the retry must rebuild the cache");
    assert_eq!(
        pretty_format_batches(&batches).unwrap().to_string(),
        expected,
        "the retry must return the baseline result"
    );
}
