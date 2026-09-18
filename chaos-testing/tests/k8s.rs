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

//! Kubernetes (kind) backend scenarios.
//!
//! Gated behind the `k8s` feature and `CHAOS_BACKEND=kind`, so the default
//! `cargo test` never touches a cluster. Run with the kind runbook in the crate
//! README:
//!
//! ```sh
//! dev/build-chaos-docker.sh
//! kind create cluster --config chaos-testing/k8s/kind-config.yaml
//! kind load docker-image ballista-chaos:test
//! CHAOS_BACKEND=kind cargo test -p ballista-chaos --features k8s --test k8s -- --test-threads=1
//! ```
//!
//! These are the scenarios that genuinely need a cluster — real pod lifecycle,
//! rescheduling, and the port-forward/flight-proxy path — rather than the
//! fault-injection scenarios in `ha.rs`, which are backend-agnostic and stay on
//! the fast process harness. Which planner each scenario runs under mirrors
//! `ha.rs`: both AQE settings only where the planner changes the code path.
#![cfg(feature = "k8s")]

use std::time::{Duration, Instant};

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::config::BALLISTA_ADAPTIVE_PLANNER_ENABLED;
use chaos_testing::fixture::Fixture;
use chaos_testing::k8s::{K8sCluster, KillMode};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use rstest::rstest;

/// The k8s scenarios need a running kind cluster with the chaos image loaded;
/// they are opt-in via `CHAOS_BACKEND=kind` so a plain `cargo test` skips them.
fn kind_backend_selected() -> bool {
    if std::env::var("CHAOS_BACKEND").as_deref() == Ok("kind") {
        true
    } else {
        eprintln!(
            "skipping k8s scenario: set CHAOS_BACKEND=kind and provide a kind cluster \
             with the chaos image loaded (see the crate README runbook)"
        );
        false
    }
}

/// One kind cluster plus its fixture and a connected client, wired for a single
/// scenario. The k8s counterpart of `ha.rs`'s `ChaosRun`: it centralises the
/// fixture write, the client connect, and the UDF-after-upgrade registration so
/// each scenario reads as just its fault and its assertions.
struct K8sRun {
    cluster: K8sCluster,
    fixture: Fixture,
    ctx: SessionContext,
}

impl K8sRun {
    /// Deploy a cluster of `executors`, write the fixture into the shared mount,
    /// and connect a client with AQE set to `aqe`.
    async fn start(aqe: bool, executors: usize) -> Self {
        let cluster = K8sCluster::start(executors)
            .await
            .expect("kind cluster must start");

        // Written into the shared mount, so the scheduler and executor pods see it.
        let fixture = Fixture::write(cluster.shared_dir())
            .await
            .expect("fixture must be written to the shared mount");

        let config = SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, aqe);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let ctx = SessionContext::remote_with_state(&cluster.scheduler_url(), state)
            .await
            .expect("client must connect to the scheduler");

        // Registered *after* `remote_with_state`: `upgrade_for_ballista` rebuilds
        // the state with `with_scalar_functions(...)`, which replaces rather than
        // merges the scalar-function map and would drop a UDF registered before.
        // (Same subtlety documented in `ha.rs`'s `ChaosRun`.)
        ctx.register_udf(chaos_testing::udf::chaos_fail_udf().as_ref().clone());
        ctx.register_udf(chaos_testing::udf::chaos_delay_udf().as_ref().clone());

        for stmt in fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }

        Self {
            cluster,
            fixture,
            ctx,
        }
    }

    /// A clone of the session context, for running a query concurrently with a
    /// fault (the query is spawned while the main task kills executors).
    fn clone_ctx(&self) -> SessionContext {
        self.ctx.clone()
    }

    /// Run a query on the cluster, returning the formatted result.
    async fn sql(&self, sql: &str) -> Result<String, String> {
        let df = self.ctx.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;
        Ok(pretty_format_batches(&batches)
            .map_err(|e| e.to_string())?
            .to_string())
    }

    /// The expected answer, computed by plain local DataFusion over the same
    /// fixture — the reference the cluster must reproduce exactly.
    async fn local_baseline(&self) -> String {
        let ctx = SessionContext::new();
        for stmt in self.fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }
        let batches = ctx
            .sql(Fixture::baseline_query())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }
}

/// Smoke test: a real query on a real kind cluster returns the same result as
/// plain local DataFusion. Exercises the whole path — client → scheduler → pods
/// → shuffle → result — with the fixture shared through the `hostPath` mount.
///
/// Single planner: the wiring this smoke-tests (mount, port-forward, flight
/// proxy, shuffle) does not vary by planner, so there is nothing to gain from
/// running it under both.
#[tokio::test]
async fn baseline_matches_local_datafusion_on_k8s() {
    if !kind_backend_selected() {
        return;
    }

    let run = K8sRun::start(false, 2).await;
    let expected = run.local_baseline().await;
    let actual = run
        .sql(Fixture::baseline_query())
        .await
        .expect("cluster must serve the baseline query");

    assert_eq!(
        actual, expected,
        "cluster result must match plain local DataFusion"
    );
}

/// Scenario G (#2029) on k8s: every executor is lost mid-query.
///
/// The kill force-deletes every executor pod while holding the Deployment at
/// zero replicas (`kill_all_executors_hard`). Scaling to zero alone is not
/// enough: it is a graceful SIGTERM, so the executors drain the in-flight query
/// to completion and it *succeeds*. Force-deleting the pods SIGKILLs them so the
/// work is genuinely lost, and holding the Deployment at zero means the
/// controller will not reschedule replacements. With no executor left, the job
/// cannot succeed: once the last one is reaped the scheduler waits the bounded
/// no-executors grace period and then fails the job rather than hanging forever.
///
/// Both AQE settings: #2029 had an AQE-on-only second hang path (a task-launch
/// failure that removed the last executor without arming the grace timer), so
/// the planner genuinely changes the code path here.
#[rstest]
#[case::aqe_off(false)]
#[case::aqe_on(true)]
#[tokio::test]
async fn killing_every_executor_terminates_the_job_on_k8s(#[case] aqe: bool) {
    if !kind_backend_selected() {
        return;
    }

    let run = K8sRun::start(aqe, 2).await;
    // A long per-batch delay keeps the query in flight while the hard kill runs
    // its two kubectl round-trips (scale to zero, then force-delete), so the
    // pods die mid-query rather than after it would have finished.
    let sql = Fixture::chaos_query("chaos_delay(f.key >= 0, 2000)");

    let query = tokio::spawn({
        let ctx = run.clone_ctx();
        async move { ctx.sql(&sql).await?.collect().await }
    });

    let job_id = run.cluster.running_job_id().await.expect("job must appear");
    run.cluster
        .await_any_stage_running(&job_id)
        .await
        .expect("the job must start running a task before we remove its executors");

    // Total loss that stays lost and is not drained: force-kill every pod and
    // hold the Deployment at zero so no replacement is scheduled.
    run.cluster
        .kill_all_executors_hard()
        .await
        .expect("force-kill every executor");

    let result = tokio::time::timeout(Duration::from_secs(120), query)
        .await
        .expect("job must terminate, not hang, after every executor is lost")
        .expect("query task should not panic");
    let err = result.expect_err("query must fail once every executor is lost");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("executor"),
        "failure should name the executor loss, got: {err}"
    );
}

/// Scenario F on k8s: an executor pod is killed and the cluster reabsorbs its
/// replacement.
///
/// Unlike the process harness — where the test spawns a fresh executor itself —
/// deleting a pod lets the Deployment reschedule a replacement automatically,
/// with a brand-new executor id. That is the k8s-unique behaviour this asserts:
/// after a forced pod delete, the scheduler settles back to two executors, one
/// of which is genuinely new (an id not present before), and the cluster still
/// serves queries.
///
/// Single planner (aqe off): rescheduling and re-registration are
/// planner-independent, and the post-restart query correctness is already
/// covered by the baseline scenario.
#[tokio::test]
async fn restarted_executor_rejoins_and_serves_queries_on_k8s() {
    if !kind_backend_selected() {
        return;
    }

    let run = K8sRun::start(false, 2).await;
    let expected = run.local_baseline().await;

    let before = run
        .cluster
        .executor_ids()
        .await
        .expect("must list executors before the kill");
    assert_eq!(before.len(), 2, "expected two executors to start");

    run.cluster
        .kill_one_executor(KillMode::Forced)
        .await
        .expect("force-delete one executor pod");

    // Wait for steady state: exactly two executors again, one of which is new
    // (a fresh id). A plain count==2 wait is not enough — the killed executor's
    // heartbeat lingers, so the count passes transiently through 2 (ghost +
    // survivor) and 3 (ghost + survivor + replacement) before settling.
    let after = await_replacement(&run.cluster, &before).await;
    assert_eq!(after.len(), 2, "cluster must settle back to two executors");
    assert!(
        after.iter().any(|id| !before.contains(id)),
        "a rescheduled executor with a new id must have joined; before={before:?} after={after:?}"
    );

    let actual = run
        .sql(Fixture::baseline_query())
        .await
        .expect("cluster must serve queries after an executor is rescheduled");
    assert_eq!(actual, expected);
}

/// Poll until the scheduler lists exactly two executors and at least one is not
/// in `before` — i.e. the killed executor has been reaped and its rescheduled
/// replacement (new id) has registered.
async fn await_replacement(cluster: &K8sCluster, before: &[String]) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(ids) = cluster.executor_ids().await
            && ids.len() == 2
            && ids.iter().any(|id| !before.contains(id))
        {
            return ids;
        }
        if Instant::now() > deadline {
            cluster.dump_diagnostics().await;
            panic!(
                "timed out waiting for a rescheduled executor to replace the killed one"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
