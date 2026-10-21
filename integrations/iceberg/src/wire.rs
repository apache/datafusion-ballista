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

//! What the codecs send in place of a live [`Table`], and how they frame it.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex};

use datafusion::common::{DataFusionError, exec_datafusion_err, exec_err};
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataRef};
use iceberg::table::Table;
use iceberg::{Runtime, TableIdent};
use iceberg_datafusion::to_datafusion_error;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::runtime::{Handle, RuntimeFlavor};

/// Runs the codecs' I/O and owns the tables they rebuild: Iceberg plans scans on
/// a table's runtime, which must outlive the table, and a codec's caller may not.
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("iceberg-ballista")
        .enable_all()
        .build()
        .expect("failed to build the iceberg-ballista runtime")
});

/// Runs `fut` on [`RUNTIME`] and waits for it, from the synchronous codec API.
pub(crate) fn block_on<T: Send + 'static>(
    fut: impl Future<Output = T> + Send + 'static,
) -> Result<T, DataFusionError> {
    let wait = || futures::executor::block_on(RUNTIME.spawn(fut));
    let joined = match Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(wait),
        _ => wait(),
    };
    joined.map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Bounds memory rather than tuning hits: a query touches a few table versions,
/// but each can hold megabytes of metadata.
const TABLE_CACHE_CAPACITY: usize = 16;

/// Tables rebuilt by [`TableWire::load`], least recently used first, so each
/// process rebuilds a version once. Metadata files never change, so entries
/// can't go stale.
static TABLES: LazyLock<Mutex<VecDeque<(TableWire, Table)>>> =
    LazyLock::new(|| Mutex::new(VecDeque::new()));

/// A [`Table`] as its metadata file and `FileIO`, from which any node can
/// rebuild exactly that version without a catalog.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableWire {
    identifier: TableIdent,
    metadata_location: String,
    file_io: serde_json::Value,
    readonly: bool,
    /// The content of `metadata_location`, sent from the client so the
    /// scheduler needn't read it; scans sent to executors leave it out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<TableMetadataRef>,
}

impl fmt::Debug for TableWire {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableWire")
            .field("identifier", &self.identifier)
            .field("metadata_location", &self.metadata_location)
            .field("file_io", &"<redacted>")
            .field("readonly", &self.readonly)
            .finish_non_exhaustive()
    }
}

impl TableWire {
    pub(crate) fn new(table: &Table) -> Result<Self, DataFusionError> {
        let file_io = table
            .file_io()
            .serialize_all()
            .map_err(to_datafusion_error)?;
        Ok(Self {
            identifier: table.identifier().clone(),
            metadata_location: table
                .metadata_location_result()
                .map_err(to_datafusion_error)?
                .to_string(),
            file_io: serde_json::from_slice(&file_io).map_err(json_err)?,
            readonly: table.readonly(),
            metadata: None,
        })
    }

    /// Like [`TableWire::new`], also carrying the table's metadata.
    pub(crate) fn with_metadata(table: &Table) -> Result<Self, DataFusionError> {
        Ok(Self {
            metadata: Some(table.metadata_ref()),
            ..Self::new(table)?
        })
    }

    /// Rebuilds the table, or returns it from [`TABLES`].
    pub(crate) fn load(mut self) -> Result<Table, DataFusionError> {
        // Keyed without the metadata, which its location determines.
        let metadata = self.metadata.take();
        if let Some(table) = self.touch(&mut TABLES.lock().unwrap()) {
            return Ok(table);
        }
        let table =
            block_on(self.clone().read(metadata))?.map_err(to_datafusion_error)?;
        let mut tables = TABLES.lock().unwrap();
        // Another thread may have rebuilt it meanwhile.
        if let Some(table) = self.touch(&mut tables) {
            return Ok(table);
        }
        if tables.len() == TABLE_CACHE_CAPACITY {
            tables.pop_front();
        }
        tables.push_back((self.clone(), table.clone()));
        Ok(table)
    }

    /// Moves this table's entry, if any, to the back of `tables` and returns it.
    fn touch(&self, tables: &mut VecDeque<(TableWire, Table)>) -> Option<Table> {
        let entry = tables.remove(tables.iter().position(|(wire, _)| wire == self)?)?;
        let table = entry.1.clone();
        tables.push_back(entry);
        Some(table)
    }

    async fn read(self, metadata: Option<TableMetadataRef>) -> iceberg::Result<Table> {
        let file_io = FileIO::deserialize_all(&serde_json::to_vec(&self.file_io)?)?;
        let metadata = match metadata {
            Some(metadata) => metadata,
            None => Arc::new(
                TableMetadata::read_from(&file_io, &self.metadata_location).await?,
            ),
        };
        Table::builder()
            .metadata(metadata)
            .metadata_location(self.metadata_location)
            .identifier(self.identifier)
            .file_io(file_io)
            .readonly(self.readonly)
            .runtime(Runtime::new(&RUNTIME))
            .build()
    }
}

/// Leads a blob that belongs to the inner codec.
pub(crate) const TAG_DELEGATED: u8 = 0;
/// Leads a blob that belongs to this crate, JSON-encoded. Also the version of
/// that JSON: a change that older decoders can't read takes the next tag.
pub(crate) const TAG_ICEBERG: u8 = 1;

/// Appends `payload` to `buf` as an Iceberg blob.
pub(crate) fn encode<T: Serialize>(
    buf: &mut Vec<u8>,
    payload: &T,
) -> Result<(), DataFusionError> {
    buf.push(TAG_ICEBERG);
    serde_json::to_writer(buf, payload).map_err(json_err)
}

/// A blob's payload: this crate's, decoded, or the inner codec's bytes.
pub(crate) enum Frame<'a, T> {
    Iceberg(T),
    Delegated(&'a [u8]),
}

/// Splits `buf` by its leading tag, decoding an Iceberg payload as `T`.
pub(crate) fn decode<T: DeserializeOwned>(
    buf: &[u8],
) -> Result<Frame<'_, T>, DataFusionError> {
    match buf.split_first() {
        Some((&TAG_ICEBERG, rest)) => serde_json::from_slice(rest)
            .map(Frame::Iceberg)
            .map_err(|e| exec_datafusion_err!("invalid iceberg-ballista payload: {e}")),
        Some((&TAG_DELEGATED, rest)) => Ok(Frame::Delegated(rest)),
        Some((tag, _)) => exec_err!(
            "unknown iceberg-ballista codec tag {tag}: was the plan encoded by \
             another version of iceberg-ballista?"
        ),
        None => exec_err!("empty iceberg-ballista codec buffer"),
    }
}

fn json_err(e: serde_json::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
