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
//! does not expose its work dir through any public API, and the work dir is
//! not even created at cluster startup: `new_standalone_executor_from_builder`
//! (`ballista-executor::standalone`) does
//! `let work_dir = TempDir::new()?.path().to_str().unwrap().to_string();` --
//! the `TempDir` guard is never bound to a variable, so it drops (and
//! deletes the directory it just created) at the end of that statement. Only
//! the *path string* survives. The directory tree is lazily recreated later
//! by `create_dir_all` in `ShuffleWriterExec`
//! (`ballista/core/src/execution_plans/shuffle_writer.rs`) the first time a
//! query actually writes shuffle output there.
//!
//! To locate that path from the outside without racing the shared system
//! temp directory (which every other standalone-cluster test in this crate
//! also uses), these tests pin `TMPDIR` to a private, per-test root before
//! starting the cluster. `std::env::temp_dir()` (used by both
//! `tempfile::TempDir::new()` and DataFusion's own disk manager) honors
//! `TMPDIR` on macOS/Linux, so the executor's work dir path ends up
//! somewhere under that root, and it's the only thing that will ever be
//! written there.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod client_side_cleanup_tests {
    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_core::config::BALLISTA_JOB_CLIENT_SIDE_CLEANUP;
    use datafusion::common::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    /// Mutating `TMPDIR` is process-global, so the two tests in this file
    /// serialize on this lock for as long as `TMPDIR` matters: from just
    /// before it's set to just after it's restored in
    /// [`start_standalone_under`].
    static CLUSTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Number of entries directly under `dir` (0 if `dir` doesn't exist).
    fn dir_entry_count(dir: &Path) -> usize {
        std::fs::read_dir(dir).map(|rd| rd.count()).unwrap_or(0)
    }

    /// True if `path` is a directory that itself contains at least one
    /// sub-directory. Used to pick the executor's work dir
    /// (`{work_dir}/{job_id}/...`) out from under `root` once a job has
    /// written to it.
    fn is_populated_dir(path: &Path) -> bool {
        path.is_dir()
            && std::fs::read_dir(path)
                .map(|mut entries| {
                    entries.any(|e| e.map(|e| e.path().is_dir()).unwrap_or(false))
                })
                .unwrap_or(false)
    }

    /// Finds the single top-level directory under `root` that currently
    /// holds a job subdirectory. Only meaningful *after* a query has run --
    /// see the module docs for why the work dir doesn't exist right after
    /// cluster startup.
    fn find_populated_work_dir(root: &Path) -> PathBuf {
        let populated: Vec<PathBuf> = std::fs::read_dir(root)
            .expect("temp root should be readable")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| is_populated_dir(path))
            .collect();
        assert_eq!(
            populated.len(),
            1,
            "expected exactly one populated work dir under {root:?}, found {populated:?}"
        );
        populated[0].clone()
    }

    /// Starts a standalone cluster with its temp-file root pinned to `root`
    /// for the duration of startup, so the executor's work dir path (chosen
    /// once, at startup) ends up under `root` where we can find it later.
    /// `TMPDIR` is restored immediately after startup completes -- the
    /// executor keeps using its already-resolved absolute work dir path
    /// regardless of `TMPDIR`'s value from then on, so this doesn't need to
    /// stay set for the life of the test.
    async fn start_standalone_under(
        root: &Path,
        config: Option<SessionConfig>,
    ) -> SessionContext {
        let _lock = CLUSTER_LOCK.lock().await;

        let previous_tmpdir = std::env::var("TMPDIR").ok();
        // SAFETY: `env::set_var`/`remove_var` are unsafe because mutating
        // the environment races with other threads reading it. `CLUSTER_LOCK`
        // is held for this entire function, and this is the only place in
        // this test binary that touches `TMPDIR`, so no such race exists.
        unsafe {
            std::env::set_var("TMPDIR", root);
        }

        let result = match config {
            Some(config) => {
                let state = SessionStateBuilder::new()
                    .with_config(config)
                    .with_default_features()
                    .build();
                SessionContext::standalone_with_state(state).await
            }
            None => SessionContext::standalone().await,
        };

        // SAFETY: see above.
        unsafe {
            match &previous_tmpdir {
                Some(value) => std::env::set_var("TMPDIR", value),
                None => std::env::remove_var("TMPDIR"),
            }
        }

        result.expect("standalone cluster should start")
    }

    /// Registers the shared test parquet fixture used by the sibling
    /// standalone tests in this crate.
    async fn register_test_data(ctx: &SessionContext) -> Result<()> {
        ctx.register_parquet(
            "test",
            "testdata/alltypes_plain.parquet",
            ParquetReadOptions::default(),
        )
        .await?;
        Ok(())
    }

    /// Runs a query with a `GROUP BY`, forcing a shuffle stage so that the
    /// executor writes job data (shuffle files) to its work dir, and
    /// consumes all results.
    async fn run_shuffling_query(ctx: &SessionContext) -> Result<()> {
        let df = ctx
            .sql("SELECT bool_col, COUNT(*) as cnt FROM test GROUP BY bool_col")
            .await?;
        // Dropping the collected batches drops the underlying client result
        // stream, which is what fires the (fire-and-forget) cleanup guard.
        let _ = df.collect().await?;
        Ok(())
    }

    /// With client-side cleanup enabled (the default), a job's on-disk data
    /// on the executor should be removed shortly after the client finishes
    /// consuming results -- long before the scheduler's timed cleanup
    /// (default 300s).
    #[tokio::test]
    async fn job_data_removed_after_client_consumes_results() -> Result<()> {
        let root = TempDir::new().expect("temp root should be created");
        let ctx = start_standalone_under(root.path(), None).await;
        register_test_data(&ctx).await?;

        run_shuffling_query(&ctx).await?;

        // IMPORTANT / internal ordering detail: no `.await` has happened
        // since `collect()` returned above. The cleanup RPC is fired from
        // `JobCleanupGuard::drop`
        // (`ballista/core/src/execution_plans/distributed_query.rs`), which
        // synchronously calls `Handle::spawn` -- it enqueues a task to send
        // the RPC, it does not send it inline. `#[tokio::test]` defaults to
        // the single-threaded runtime, and the standalone executor's own
        // background tasks (poll loop, flight service) run on that same
        // runtime, so that enqueued task cannot have been polled even once
        // yet at this point: the job's shuffle output is guaranteed to
        // still be on disk here. This is correct today but is an internal
        // scheduling detail, not a public guarantee -- it's only used to
        // reliably locate the work dir below, not as the assertion that the
        // feature works (that's the poll loop further down).
        let work_dir = find_populated_work_dir(root.path());

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
        Ok(())
    }

    /// With `ballista.job.client_side_cleanup` disabled, the client must not
    /// send the `CleanJobData` RPC, so the job's on-disk data should still be
    /// present a short time after the client finishes consuming results.
    #[tokio::test]
    async fn job_data_kept_when_client_side_cleanup_disabled() -> Result<()> {
        let root = TempDir::new().expect("temp root should be created");
        let config = SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_JOB_CLIENT_SIDE_CLEANUP, false);
        let ctx = start_standalone_under(root.path(), Some(config)).await;
        register_test_data(&ctx).await?;

        run_shuffling_query(&ctx).await?;

        let work_dir = find_populated_work_dir(root.path());

        // Keep this well short of the scheduler's ~300s timed cleanup: we're
        // only proving the client didn't clean up on its own.
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(
            dir_entry_count(&work_dir) > 0,
            "job dir under {work_dir:?} was removed even though \
             client_side_cleanup was disabled"
        );
        Ok(())
    }
}
