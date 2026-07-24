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

//! End-to-end integration tests for client-triggered job data cleanup.
//!
//! When the Ballista client finishes consuming a job's results,
//! `DistributedQueryExec` fires the scheduler's `CleanJobData` RPC
//! (fire-and-forget, from the dropped result stream), which deletes the
//! job's on-disk data on the executor immediately instead of waiting for the
//! scheduler's timed cleanup (default ~300s). This is gated by the
//! `ballista.job.client_side_cleanup` config flag (default enabled).
//!
//! The standalone executor used by [`ballista::prelude::SessionContextExt`]
//! does not expose its work dir through any public API: it always creates a
//! fresh `tempfile::TempDir` internally (see
//! `ballista-executor::standalone::new_standalone_executor_from_builder`).
//! To locate it from the outside, these tests snapshot the process-wide temp
//! directory before and after starting the standalone cluster and diff the
//! two snapshots to find the directory the executor just created.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod client_side_cleanup_tests {
    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_core::config::BALLISTA_JOB_CLIENT_SIDE_CLEANUP;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    /// Starting a standalone cluster spawns exactly one new `tempfile::TempDir`
    /// (the executor's work dir) under the process temp directory. Serialize
    /// cluster startup across the tests in this file so that the "snapshot the
    /// temp dir before/after starting the cluster" trick below can't race
    /// against another test in this binary doing the same thing at the same
    /// time.
    static CLUSTER_STARTUP_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    /// Returns the set of directories directly under the process temp
    /// directory (`std::env::temp_dir()`).
    fn temp_dir_entries() -> HashSet<PathBuf> {
        std::fs::read_dir(std::env::temp_dir())
            .map(|read_dir| {
                read_dir
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| path.is_dir())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Number of entries directly under `dir` (0 if `dir` doesn't exist).
    fn dir_entry_count(dir: &PathBuf) -> usize {
        std::fs::read_dir(dir).map(|rd| rd.count()).unwrap_or(0)
    }

    /// Starts a standalone cluster (optionally with a custom session config)
    /// and returns it together with the set of directories that appeared
    /// under the process temp dir while it was starting up. In the common
    /// case that set contains exactly one entry: the executor's work dir.
    async fn start_standalone_and_diff_temp_dir(
        config: Option<SessionConfig>,
    ) -> (SessionContext, Vec<PathBuf>) {
        let _lock = CLUSTER_STARTUP_LOCK.lock().await;

        let before = temp_dir_entries();

        let ctx = match config {
            Some(config) => {
                let state = datafusion::execution::SessionStateBuilder::new()
                    .with_config(config)
                    .with_default_features()
                    .build();
                SessionContext::standalone_with_state(state)
                    .await
                    .expect("standalone cluster should start")
            }
            None => SessionContext::standalone()
                .await
                .expect("standalone cluster should start"),
        };

        let after = temp_dir_entries();
        let candidates: Vec<PathBuf> = after.difference(&before).cloned().collect();
        assert!(
            !candidates.is_empty(),
            "expected the standalone executor to create a new directory under {:?}, found none",
            std::env::temp_dir()
        );

        (ctx, candidates)
    }

    /// Registers the shared test parquet fixture used by the sibling
    /// standalone tests in this crate.
    async fn register_test_data(ctx: &SessionContext) {
        ctx.register_parquet(
            "test",
            "testdata/alltypes_plain.parquet",
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    }

    /// Runs a query with a `GROUP BY`, forcing a shuffle stage so that the
    /// executor writes job data (shuffle files) to its work dir.
    async fn run_shuffling_query(ctx: &SessionContext) {
        let df = ctx
            .sql("SELECT bool_col, COUNT(*) as cnt FROM test GROUP BY bool_col")
            .await
            .unwrap();
        // Dropping the collected batches drops the underlying client result
        // stream, which is what fires the (fire-and-forget) cleanup guard.
        let _ = df.collect().await.unwrap();
    }

    /// Out of the candidate directories produced by starting the cluster,
    /// picks the one that currently contains job data. This is also used
    /// right after `collect()` returns (before any further `.await`) to
    /// disambiguate which candidate is really the executor's work dir, since
    /// on the single-threaded `#[tokio::test]` runtime the fire-and-forget
    /// cleanup task spawned by the dropped result stream cannot have run yet
    /// at that point: the executor's work dir must still hold the job's
    /// shuffle output.
    fn find_populated_dir(candidates: &[PathBuf]) -> PathBuf {
        let populated: Vec<&PathBuf> = candidates
            .iter()
            .filter(|dir| dir_entry_count(dir) > 0)
            .collect();
        assert_eq!(
            populated.len(),
            1,
            "expected exactly one candidate temp dir to contain job output \
             right after collect(), found {populated:?} (all candidates: {candidates:?})"
        );
        populated[0].clone()
    }

    /// With client-side cleanup enabled (the default), a job's on-disk data
    /// on the executor should be removed shortly after the client finishes
    /// consuming results -- long before the scheduler's timed cleanup
    /// (default 300s).
    #[tokio::test]
    async fn job_data_removed_after_client_consumes_results() {
        let (ctx, candidates) = start_standalone_and_diff_temp_dir(None).await;
        register_test_data(&ctx).await;

        run_shuffling_query(&ctx).await;

        // No `.await` has happened since `collect()` returned above, so the
        // cleanup task the dropped stream spawned cannot have executed yet:
        // the job dir must still be present, which lets us reliably identify
        // the executor's work dir among the candidates.
        let work_dir = find_populated_dir(&candidates);

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if dir_entry_count(&work_dir) == 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "job dir under {work_dir:?} was not cleaned up within timeout"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// With `ballista.job.client_side_cleanup` disabled, the client must not
    /// send the `CleanJobData` RPC, so the job's on-disk data should still be
    /// present a short time after the client finishes consuming results.
    #[tokio::test]
    async fn job_data_kept_when_client_side_cleanup_disabled() {
        let config = SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_JOB_CLIENT_SIDE_CLEANUP, false);
        let (ctx, candidates) = start_standalone_and_diff_temp_dir(Some(config)).await;
        register_test_data(&ctx).await;

        run_shuffling_query(&ctx).await;

        let work_dir = find_populated_dir(&candidates);

        // Keep this well short of the scheduler's ~300s timed cleanup: we're
        // only proving the client didn't clean up on its own.
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(
            dir_entry_count(&work_dir) > 0,
            "job dir under {work_dir:?} was removed even though \
             client_side_cleanup was disabled"
        );
    }
}
