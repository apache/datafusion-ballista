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

//! Where [`super::HistoryStore`] finds and reads completed event logs.

use ballista_history::event::JobIndex;
use ballista_history::reader::{ReadError, ReplayedJob};
use futures::stream::{self, BoxStream, StreamExt};
use std::path::{Path, PathBuf};

/// Where [`super::HistoryStore`] gets its directory listing and file
/// contents from.
///
/// The only implementation today, [`LocalDirSource`], is exactly what
/// `HistoryStore` did inline before this was pulled out: a flat local
/// directory read with `std::fs`. Keeping it behind a trait lets the indexing
/// logic in [`super::spawn_service_tasks`] stay independent of where the logs
/// live and how they are listed and read.
#[async_trait::async_trait]
pub(crate) trait EventLogSource: Send + Sync {
    /// Stream every completed log currently present. Streamed rather than
    /// returned as a `Vec` so an implementation backed by a paginated remote
    /// listing need not materialize the whole directory first, and a caller
    /// can stop polling to abandon the rest; [`LocalDirSource`] is eager. A
    /// failure to list the directory is yielded as an `Err` item rather than
    /// ending the stream; an entry that cannot be read is skipped.
    fn scan_jobs(&self) -> BoxStream<'_, std::io::Result<PathBuf>>;

    /// Read and parse one log's index summary.
    async fn read_job_index(&self, path: &Path) -> Result<Option<JobIndex>, ReadError>;

    /// Read one job's full stored payload back.
    async fn read_completed_job(
        &self,
        path: &Path,
    ) -> Result<Option<ReplayedJob>, ReadError>;
}

/// A flat local directory of `.eventlog` files, read with `std::fs`.
pub(crate) struct LocalDirSource {
    dir: PathBuf,
}

impl LocalDirSource {
    pub(crate) fn new(dir: PathBuf) -> Self {
        LocalDirSource { dir }
    }
}

#[async_trait::async_trait]
impl EventLogSource for LocalDirSource {
    /// Stat every `.eventlog` in the directory. A directory that does not
    /// exist yet is an empty one: the history server is routinely started
    /// before the scheduler has written anything.
    fn scan_jobs(&self) -> BoxStream<'_, std::io::Result<PathBuf>> {
        if !self.dir.exists() {
            return stream::empty().boxed();
        }
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) => return stream::once(async move { Err(e) }).boxed(),
        };
        stream::iter(entries.filter_map(|entry| {
            // A file that disappears mid-scan is simply not there this pass.
            let entry = entry.ok()?;
            let path = entry.path();
            // A job still running (or abandoned by a crashed scheduler) is
            // named `<job_id>.eventlog.running`, whose extension is
            // "running", not "eventlog" — this check relies on that to skip
            // it without ever opening it.
            if path.extension().and_then(|e| e.to_str()) != Some("eventlog") {
                return None;
            }

            Some(Ok(path))
        }))
        .boxed()
    }

    async fn read_job_index(&self, path: &Path) -> Result<Option<JobIndex>, ReadError> {
        ballista_history::reader::read_job_index(path)
    }

    async fn read_completed_job(
        &self,
        path: &Path,
    ) -> Result<Option<ReplayedJob>, ReadError> {
        ballista_history::reader::read_completed_job(path)
    }
}
