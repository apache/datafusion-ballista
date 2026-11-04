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

//! Executor-owned work-directory activity: queued stages and blocking IPC writes
//! must release their leases before a default temporary directory can be deleted.

use futures::task::AtomicWaker;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::task::Waker;

struct State {
    active: usize,
    sealed: bool,
}

/// Tracks tasks queued before registration and synchronous IPC workers that
/// may continue after their awaiting async stage has been cancelled.
pub struct WorkDirActivity {
    state: Mutex<State>,
    drained_waker: AtomicWaker,
}

/// Explicit denial for new work against an executor-private directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkDirLeaseError {
    /// The directory has been sealed for safe shutdown.
    Sealed,
    /// The active lease counter cannot safely accept more work.
    CountOverflow,
}

/// A lease retained by the actual job, not by its abortable async waiter.
pub struct WorkDirLease(Arc<WorkDirActivity>);

impl std::fmt::Debug for WorkDirLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkDirLease(..)")
    }
}

type Registry = Mutex<Vec<(PathBuf, Weak<WorkDirActivity>)>>;
static REGISTRY: OnceLock<Registry> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

impl WorkDirActivity {
    /// Register a newly created, executor-owned private work directory.
    pub fn register(root: PathBuf) -> Arc<Self> {
        let activity = Arc::new(Self {
            state: Mutex::new(State {
                active: 0,
                sealed: false,
            }),
            drained_waker: AtomicWaker::new(),
        });
        let mut entries = registry().lock();
        entries.retain(|(_, weak)| weak.strong_count() > 0);
        entries.push((root, Arc::downgrade(&activity)));
        activity
    }

    /// Acquire ownership before queueing work or before scheduling a blocking
    /// writer. Once sealed, a late worker must not create files in a deleted path.
    pub fn lease(self: &Arc<Self>) -> Result<WorkDirLease, WorkDirLeaseError> {
        let mut state = self.state.lock();
        if state.sealed {
            return Err(WorkDirLeaseError::Sealed);
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or(WorkDirLeaseError::CountOverflow)?;
        Ok(WorkDirLease(self.clone()))
    }

    /// Register before checking whether the activity counter is empty.
    pub fn register_waker(&self, waker: &Waker) {
        self.drained_waker.register(waker);
    }

    /// Quiescence snapshot, including writers detached from cancelled stages.
    pub fn is_idle(&self) -> bool {
        self.state.lock().active == 0
    }

    /// Count leases held by queued tasks, coordinator futures and disk I/O.
    pub fn active_count(&self) -> usize {
        self.state.lock().active
    }

    /// Atomically reject all new leases if and only if no lease remains.
    /// Call only after all executor service handlers have stopped accepting work.
    pub fn seal_if_idle(&self) -> bool {
        let mut state = self.state.lock();
        if state.active != 0 {
            return false;
        }
        state.sealed = true;
        true
    }
}

impl Drop for WorkDirLease {
    fn drop(&mut self) {
        let mut state = self.0.state.lock();
        debug_assert!(state.active > 0);
        state.active -= 1;
        if state.active == 0 {
            self.0.drained_waker.wake();
        }
    }
}

/// Match the nearest executor-owned directory (never configured/caller-owned
/// paths). Acquire before spawning the blocking worker: queued workers count.
pub fn lease_for_writer(path: &Path) -> Result<Option<WorkDirLease>, WorkDirLeaseError> {
    let owner = {
        let mut entries = registry().lock();
        entries.retain(|(_, weak)| weak.strong_count() > 0);
        entries
            .iter()
            .filter(|(root, _)| path.starts_with(root))
            .max_by_key(|(root, _)| root.components().count())
            .and_then(|(_, weak)| weak.upgrade())
    };
    owner
        .map(|activity| activity.lease().map(Some))
        .unwrap_or(Ok(None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_and_blocking_leases_prevent_sealing_until_both_finish() {
        let tmp = tempfile::TempDir::new().unwrap();
        let active = WorkDirActivity::register(tmp.path().to_path_buf());
        let queued = active.lease().unwrap();
        let blocking = lease_for_writer(&tmp.path().join("job/stage/data.arrow"))
            .unwrap()
            .unwrap();
        assert!(!active.seal_if_idle());
        drop(queued);
        assert!(!active.seal_if_idle());
        drop(blocking);
        assert!(active.seal_if_idle());
        assert!(active.lease().is_err());
        assert!(lease_for_writer(&tmp.path().join("late.arrow")).is_err());
    }
}
