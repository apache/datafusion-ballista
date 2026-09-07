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

//! When [`super::spawn_service_tasks`]'s background loops should act.

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use tokio::sync::{Mutex, mpsc};

/// Decides when [`super::spawn_service_tasks`]'s scan loop should make a full
/// directory pass.
///
/// [`OnceTrigger`] is the only implementation today: it fires a single time,
/// so the loop makes one pass over whatever is already on disk and then
/// parks. Updates after that are carried incrementally by [`EventLogTrigger`].
/// Keeping the "when" decision behind a trait leaves room for e.g. a periodic
/// trigger later without touching the loop itself.
#[async_trait::async_trait]
pub(crate) trait ScanTrigger: Send + Sync {
    /// Wait until a full directory pass should happen.
    async fn scan_tick(&self);
}

#[derive(Default)]
pub(crate) struct OnceTrigger {
    fired: AtomicBool,
}

#[async_trait::async_trait]
impl ScanTrigger for OnceTrigger {
    async fn scan_tick(&self) {
        if self.fired.swap(true, std::sync::atomic::Ordering::Relaxed) {
            // Already fired once; park forever so the scan loop makes exactly
            // one pass. Safe because the scan loop is the only consumer.
            std::future::pending::<()>().await
        }
    }
}

#[derive(Default)]
pub(crate) struct NoopTrigger {}

#[async_trait::async_trait]
impl ScanTrigger for NoopTrigger {
    async fn scan_tick(&self) {
        std::future::pending::<()>().await
    }
}

#[async_trait::async_trait]
impl EventLogTrigger for NoopTrigger {
    async fn next_event(&self) -> EventLogEvent {
        std::future::pending::<_>().await
    }
}

/// What the watch trigger observed about a single `*.eventlog` file.
pub(crate) enum EventLogEvent {
    /// The file appeared: created in place, or renamed into the directory
    /// (how a finished `<job_id>.eventlog.running` -> `<job_id>.eventlog`
    /// arrives).
    Created(PathBuf),
    /// The file was removed: deleted, or renamed out of the directory.
    Removed(PathBuf),
}

/// Reports single `*.eventlog` files as they appear in or disappear from the
/// directory, so each can be folded into (or dropped from) the index without
/// waiting for the next full directory pass.
#[async_trait::async_trait]
pub(crate) trait EventLogTrigger: Send + Sync {
    /// Wait until a `*.eventlog` file appears or disappears in the watched
    /// directory; return which, with its absolute path.
    async fn next_event(&self) -> EventLogEvent;
}

/// Watches an event-log directory with the `notify` crate and reports each
/// `*.eventlog` file as it appears or disappears, so [`super::spawn_service_tasks`]'s
/// watch loop can index or drop it without waiting for the next full directory
/// pass.
///
/// A completed log lands by rename (`<job_id>.eventlog.running` ->
/// `<job_id>.eventlog`), which is a rename-to rather than a create, so both are
/// treated as "a new file appeared". A log that is deleted or renamed away is
/// reported as [`EventLogEvent::Removed`]. The still-running `.eventlog.running`
/// file is filtered out by extension, exactly as `LocalDirSource::scan_jobs`
/// does.
pub(crate) struct NotifyTrigger {
    /// Kept alive for its `Drop`: dropping the watcher stops the OS watch.
    _watcher: notify::RecommendedWatcher,
    /// `*.eventlog` appear/disappear events, oldest first. Behind a `Mutex`
    /// because `next_event` takes `&self`; the watch loop is the only consumer.
    rx: Mutex<mpsc::UnboundedReceiver<EventLogEvent>>,
}

impl NotifyTrigger {
    /// Start a non-recursive watch on `dir`, which must already exist.
    pub(crate) fn new(dir: &Path) -> notify::Result<Self> {
        use notify::Watcher;

        // Resolve to an absolute base so the paths handed to `next_event`'s
        // caller are absolute even for a relative --event-log-dir. If the cwd
        // cannot be read, fall back to the path as given (emitted paths then
        // match how it was passed).
        let dir = std::path::absolute(dir).unwrap_or_else(|_| dir.to_path_buf());
        let (tx, rx) = mpsc::unbounded_channel();
        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                let Ok(event) = res else { return };
                use notify::EventKind::*;
                use notify::event::{ModifyKind, RenameMode};
                // A file created in place, or renamed into the directory (how a
                // finished log arrives). Linux reports the rename as
                // Modify(Name(To|Both)); coarser backends collapse it to Any.
                //
                // A removal is a delete or a rename out of the directory:
                // Modify(Name(From)) on Linux, Remove otherwise. On coarse
                // backends (macOS FSEvents) a rename can surface as Name(Any)
                // in either direction and so be reported as Created; the watch
                // loop's `index_one` tolerates a path it can no longer read, so
                // a missed removal just waits for the next full scan.
                let make = match event.kind {
                    Create(_)
                    | Modify(ModifyKind::Name(
                        RenameMode::To | RenameMode::Both | RenameMode::Any,
                    )) => EventLogEvent::Created,
                    Remove(_) | Modify(ModifyKind::Name(RenameMode::From)) => {
                        EventLogEvent::Removed
                    }
                    _ => return,
                };
                for path in event.paths {
                    if path.extension().and_then(|e| e.to_str()) == Some("eventlog") {
                        // Fails only once the receiver is gone, i.e. shutdown.
                        let _ = tx.send(make(path));
                    }
                }
            })?;
        watcher.watch(&dir, notify::RecursiveMode::NonRecursive)?;
        Ok(Self {
            _watcher: watcher,
            rx: Mutex::new(rx),
        })
    }
}

#[async_trait::async_trait]
impl EventLogTrigger for NotifyTrigger {
    async fn next_event(&self) -> EventLogEvent {
        match self.rx.lock().await.recv().await {
            Some(event) => event,
            // The sender lives as long as the watcher, so this only happens at
            // shutdown; never resolve rather than report a bogus event.
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Filesystem events are delivered asynchronously by the OS, so a report
    /// can lag the write by a noticeable amount on a loaded machine.
    const REPORT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Long enough that a watch which was going to fire would have.
    const SILENCE_WINDOW: Duration = Duration::from_millis(750);

    /// The reported path must be absolute and, once symlinks are resolved (macOS
    /// puts the temp dir behind `/var` -> `/private/var`), name the same file.
    fn assert_points_at(got: &Path, expected: &Path) {
        assert!(got.is_absolute(), "expected an absolute path, got {got:?}");
        assert_eq!(
            std::fs::canonicalize(got).unwrap(),
            std::fs::canonicalize(expected).unwrap(),
        );
    }

    /// Like [`assert_points_at`], but for a path whose file may no longer exist
    /// (a removal): resolve symlinks on the parent directory only, then compare
    /// that plus the file name.
    fn assert_names_file(got: &Path, expected: &Path) {
        assert!(got.is_absolute(), "expected an absolute path, got {got:?}");
        let resolve = |p: &Path| {
            std::fs::canonicalize(p.parent().unwrap())
                .unwrap()
                .join(p.file_name().unwrap())
        };
        assert_eq!(resolve(got), resolve(expected));
    }

    /// Wait for the next event and assert it is a `Created`, returning its path.
    async fn expect_created(trigger: &NotifyTrigger) -> PathBuf {
        match tokio::time::timeout(REPORT_TIMEOUT, trigger.next_event())
            .await
            .expect("expected an event to be reported")
        {
            EventLogEvent::Created(path) => path,
            EventLogEvent::Removed(path) => {
                panic!("expected a Created event, got Removed({})", path.display())
            }
        }
    }

    #[tokio::test]
    async fn reports_a_newly_created_eventlog_by_absolute_path() {
        let dir = tempdir().unwrap();
        let trigger = NotifyTrigger::new(dir.path()).unwrap();

        let expected = dir.path().join("j1.eventlog");
        std::fs::write(&expected, b"{}\n").unwrap();

        assert_points_at(&expect_created(&trigger).await, &expected);
    }

    /// The real path a completed job takes: the writer renames
    /// `<job_id>.eventlog.running` to `<job_id>.eventlog` in place. That is a
    /// rename-to, not a create, and must still fire.
    #[tokio::test]
    async fn reports_an_eventlog_that_arrives_by_rename() {
        let dir = tempdir().unwrap();
        let trigger = NotifyTrigger::new(dir.path()).unwrap();

        let running = dir.path().join("j2.eventlog.running");
        let final_path = dir.path().join("j2.eventlog");
        std::fs::write(&running, b"{}\n").unwrap();
        std::fs::rename(&running, &final_path).unwrap();

        assert_points_at(&expect_created(&trigger).await, &final_path);
    }

    /// A `*.eventlog` that is deleted after it was reported must come back as a
    /// `Removed` for the same path, so the watch loop can drop it from the index.
    #[tokio::test]
    async fn reports_a_removed_eventlog() {
        let dir = tempdir().unwrap();
        let trigger = NotifyTrigger::new(dir.path()).unwrap();

        let path = dir.path().join("j4.eventlog");
        std::fs::write(&path, b"{}\n").unwrap();
        assert_points_at(&expect_created(&trigger).await, &path);

        // Let the OS flush the create before deleting: a coalesced
        // create-then-delete of one path is reported as a bare duplicate create
        // by some backends (macOS FSEvents), never as a removal. A log that is
        // deleted always sat on disk for a while first, so this matches real
        // use rather than papering over a bug.
        tokio::time::sleep(SILENCE_WINDOW).await;
        std::fs::remove_file(&path).unwrap();

        // A late duplicate `Created` for the same path can still be queued from
        // the write above; the removal is what must eventually arrive.
        let deadline = tokio::time::Instant::now() + REPORT_TIMEOUT;
        loop {
            match tokio::time::timeout_at(deadline, trigger.next_event())
                .await
                .expect("expected the removed .eventlog to be reported")
            {
                EventLogEvent::Removed(got) => {
                    assert_names_file(&got, &path);
                    break;
                }
                EventLogEvent::Created(got) => assert_names_file(&got, &path),
            }
        }
    }

    /// A still-running log and unrelated files share the directory but are not
    /// `*.eventlog`, so neither their creation nor their removal is reported.
    #[tokio::test]
    async fn ignores_running_logs_and_unrelated_files() {
        let dir = tempdir().unwrap();
        let trigger = NotifyTrigger::new(dir.path()).unwrap();

        let running = dir.path().join("j3.eventlog.running");
        let notes = dir.path().join("notes.txt");
        std::fs::write(&running, b"{}\n").unwrap();
        std::fs::write(&notes, b"hello").unwrap();
        std::fs::remove_file(&running).unwrap();
        std::fs::remove_file(&notes).unwrap();

        assert!(
            tokio::time::timeout(SILENCE_WINDOW, trigger.next_event())
                .await
                .is_err(),
            "no .eventlog appeared or vanished, so next_event must stay pending"
        );
    }
}
