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

//! Bridge between Ballista's synchronous, serialization-oriented codec API and
//! Iceberg's asynchronous, live-handle world, shared by the logical and
//! physical extension codecs.
//!
//! Ballista serializes physical and logical plans to ship them to remote nodes,
//! but the Iceberg plan nodes hold live state: a [`Table`] with an open
//! `FileIO`, and for commits an `Arc<dyn Catalog>`. Each is sent as the smallest
//! description it can be rebuilt from:
//!
//! - A [`Table`] travels as a [`TableWire`]: its metadata file, which never
//!   changes and so pins the exact version planned against, and its serialized
//!   `FileIO`, which carries the storage access the planner had. The receiving
//!   node reads the metadata file and needs no catalog.
//! - A catalog travels as its [`IcebergCatalogConfig`], inside a
//!   [`TableRefWire`]. Only the nodes that talk to the catalog need one: a
//!   commit, and the catalog-backed table provider that reloads its table on
//!   every scan.
//!
//! Rebuilding is asynchronous (metadata reads and catalog calls do I/O) but the
//! codec entry points are synchronous, so [`block_on`] bridges the two by
//! running every such future on [`CATALOG_RT`], a dedicated process-lived
//! runtime. The tables this crate rebuilds are bound to a second one,
//! [`TABLE_RT`], where Iceberg runs their scan planning.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};

use datafusion::common::DataFusionError;
use datafusion_iceberg::{
    IcebergCatalogConfig, IcebergMetadataTableProvider, IcebergStaticTableProvider,
    to_datafusion_error,
};
use iceberg::inspect::MetadataTableType;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, Runtime, TableIdent};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde::{Deserialize, Serialize};

/// Converts a serde error into a [`DataFusionError`]. Deliberately concrete:
/// Iceberg errors must go through [`to_datafusion_error`] instead, so they keep
/// their error kind rather than collapsing into `External`.
pub(crate) fn json_err(e: serde_json::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

fn missing_config(node: &str, remedy: &str) -> DataFusionError {
    DataFusionError::Internal(format!(
        "{node} has no IcebergCatalogConfig and cannot be distributed; {remedy}."
    ))
}

/// Error for a table-level node/provider that carries no
/// [`IcebergCatalogConfig`] and therefore cannot be rebuilt on a remote node.
pub(crate) fn missing_table_config_err(node: &str) -> DataFusionError {
    missing_config(
        node,
        "register the table with IcebergTableProvider::with_catalog_config (see \
         iceberg_ballista::register_iceberg_table)",
    )
}

/// Dedicated process-lived runtime that runs every future [`block_on`] is
/// given: catalog calls and the metadata-file reads of plan decoding.
///
/// A catalog's HTTP/connection pool is bound to the runtime that drives it, so
/// running every catalog future here — instead of on whatever runtime the codec
/// caller happens to be on — means a cached catalog can never reference an
/// already-dropped runtime, no matter which thread or test asks for it later.
/// This work happens only while plans are encoded and decoded, so one worker is
/// plenty; scan planning, which is heavier, runs on [`TABLE_RT`] instead.
static CATALOG_RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("iceberg-catalog")
        .enable_all()
        .build()
        .expect("failed to build iceberg catalog runtime")
});

/// Dedicated process-lived runtime that the tables this crate rebuilds are
/// bound to (see [`table_runtime`]).
///
/// Iceberg runs a table's scan planning, reading and parsing its manifests and
/// delete files, on tasks it spawns onto the table's runtime. So this runtime
/// has a worker per core, and is separate from [`CATALOG_RT`] so that catalog
/// calls made while decoding plans don't wait behind that work.
///
/// It must outlive every table bound to it, which is why it lives as long as
/// the process rather than being the runtime of the codec's caller. Rebuilt
/// tables are cached and handed to later decodes ([`TABLES`]), possibly on
/// other runtimes. Were a table bound to a runtime that has shut down, such as
/// a finished test's or a dropped standalone context's, Iceberg would spawn its
/// scan planning onto that runtime, where it never runs, and the scan would
/// return no rows instead of failing.
static TABLE_RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get());
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("iceberg-table")
        .enable_all()
        .build()
        .expect("failed to build iceberg table runtime")
});

/// The Iceberg [`Runtime`] every table this crate builds is bound to: [`TABLE_RT`].
fn table_runtime() -> Runtime {
    Runtime::new(&TABLE_RT)
}

/// Runs an async future to completion on [`CATALOG_RT`] from a synchronous
/// context, whatever runtime (if any) the caller happens to be on.
///
/// The future runs on a scoped helper thread (entering another runtime's
/// `block_on` is forbidden from inside a runtime context, and the helper thread
/// also lets `fut` borrow from the caller). If the caller is itself on a
/// multi-thread runtime worker, [`tokio::task::block_in_place`] tells that
/// scheduler the worker is parked so the rest of its runtime keeps making
/// progress.
pub(crate) fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    use tokio::runtime::{Handle, RuntimeFlavor};

    let wait = move || {
        std::thread::scope(|scope| {
            scope
                .spawn(|| CATALOG_RT.block_on(fut))
                .join()
                .expect("iceberg catalog access thread panicked")
        })
    };

    match Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(wait),
        _ => wait(),
    }
}

/// Process-wide cache of reconstructed catalogs, keyed by config.
///
/// Building a catalog client (and its underlying HTTP/connection pool) is
/// relatively expensive, and the codec may decode many plan nodes that share
/// one catalog, so we cache by config. Every cached catalog lives on
/// [`CATALOG_RT`], which never shuts down, so entries stay valid for the life
/// of the process and can be served to any caller.
static CATALOGS: LazyLock<Mutex<HashMap<CatalogKey, Arc<dyn Catalog>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A [`CATALOGS`] key: a config's type, name and properties, the properties
/// sorted so the key can be hashed.
type CatalogKey = (String, String, BTreeMap<String, String>);

fn catalog_key(config: &IcebergCatalogConfig) -> CatalogKey {
    let props = config
        .props
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    (config.catalog_type.clone(), config.name.clone(), props)
}

/// Builds a catalog from its config.
///
/// The catalog type is resolved through [`iceberg_catalog_loader`], so any
/// catalog it supports (`rest`, `sql`, `glue`, `hms`, `s3tables`) works here.
/// Storage is provided by [`OpenDalResolvingStorageFactory`], which picks the
/// object-store backend (S3, GCS, Azure, local fs, …) from each file's path
/// scheme, configured from the same `props`. So a single code path covers every
/// catalog/storage combination the iceberg crates support.
pub(crate) async fn build_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    iceberg_catalog_loader::load(&config.catalog_type)
        .map_err(to_datafusion_error)?
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load(config.name.clone(), config.props.clone())
        .await
        .map_err(to_datafusion_error)
}

/// Returns a catalog built from `config`, cached process-wide.
pub(crate) fn get_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    let key = catalog_key(config);
    if let Some(catalog) = CATALOGS.lock().unwrap().get(&key) {
        return Ok(catalog.clone());
    }
    let catalog = block_on(build_catalog(config))?;
    CATALOGS.lock().unwrap().insert(key, catalog.clone());
    Ok(catalog)
}

/// Drops any cached catalog for `config`, so the next [`get_catalog`] rebuilds
/// it — reopening connections and re-resolving credentials.
fn evict_catalog(config: &IcebergCatalogConfig) {
    CATALOGS.lock().unwrap().remove(&catalog_key(config));
}

/// Whether a catalog error is worth one rebuild-and-retry.
///
/// Iceberg has no dedicated auth/transport [`ErrorKind`] — the REST client maps
/// every HTTP failure, including the 401/403 of an expired token, to
/// [`ErrorKind::Unexpected`]. So only that catch-all can hide a stale-client
/// failure a rebuild would fix; the semantic kinds are deterministic and would
/// fail identically.
fn is_retryable(err: &Error) -> bool {
    matches!(err.kind(), ErrorKind::Unexpected)
}

/// Loads a fresh [`Table`] from the catalog described by `config`, returning
/// the catalog that served the successful load alongside it.
///
/// A retryable failure (see [`is_retryable`]) evicts the cached catalog and
/// retries once against a rebuilt one, so an executor whose credentials went
/// stale recovers instead of failing every decode until restart. A second
/// failure propagates.
///
/// Returning the catalog matters for that retry: a caller that needs both (the
/// commit node holds a catalog handle) must get the *rebuilt* catalog, not the
/// stale one a separate [`get_catalog`] call before the eviction would have
/// returned.
pub(crate) fn load_table_with_catalog(
    config: &IcebergCatalogConfig,
    ident: &TableIdent,
) -> Result<(Arc<dyn Catalog>, Table), DataFusionError> {
    let catalog = get_catalog(config)?;
    match block_on(catalog.load_table(ident)) {
        Ok(table) => Ok((catalog, table)),
        Err(e) if is_retryable(&e) => {
            evict_catalog(config);
            let fresh = get_catalog(config)?;
            let table = block_on(fresh.load_table(ident)).map_err(to_datafusion_error)?;
            Ok((fresh, table))
        }
        Err(e) => Err(to_datafusion_error(e)),
    }
}

/// Loads `ident` as described by the metadata file at `metadata_location`,
/// returning the catalog alongside it like [`load_table_with_catalog`].
///
/// Serves the commit, the one node that needs a catalog. A write is planned
/// against one version of the table, and its writer tasks rebuild exactly that
/// version from a [`TableWire`]; the commit must see the same version, not
/// whatever the catalog serves when it decodes.
///
/// Loading through the catalog first also rebuilds a stale catalog client (see
/// [`load_table_with_catalog`]) before the commit relies on it, and yields a
/// `FileIO` with fresh credentials the catalog may vend. When
/// the catalog has moved on, the planned metadata file is read through that
/// `FileIO`. Metadata files are immutable, so this is exactly the planned
/// version. An encrypted table cannot be rebuilt this way (there is no KMS
/// client to pass on) and fails instead.
///
/// The table is returned as the catalog loaded it when it is still at the
/// planned version, keeping whatever the catalog set up (such as a KMS client),
/// and is bound to the catalog's runtime. Otherwise it is bound to
/// [`TABLE_RT`], like every other table this crate builds.
pub(crate) fn load_table_at(
    config: &IcebergCatalogConfig,
    ident: &TableIdent,
    metadata_location: &str,
) -> Result<(Arc<dyn Catalog>, Table), DataFusionError> {
    let (catalog, current) = load_table_with_catalog(config, ident)?;
    if current.metadata_location() == Some(metadata_location) {
        return Ok((catalog, current));
    }
    let planned = block_on(async {
        let metadata =
            TableMetadata::read_from(current.file_io(), metadata_location).await?;
        Table::builder()
            .metadata(metadata)
            .metadata_location(metadata_location)
            .identifier(ident.clone())
            .file_io(current.file_io().clone())
            .readonly(current.readonly())
            .runtime(table_runtime())
            .build()
    })
    .map_err(to_datafusion_error)?;
    Ok((catalog, planned))
}

/// Builds a read-only [`IcebergStaticTableProvider`] over `table`, pinned to
/// `snapshot_id` (or to the table's current snapshot when `None`).
pub(crate) async fn static_provider(
    table: Table,
    snapshot_id: Option<i64>,
) -> Result<IcebergStaticTableProvider, DataFusionError> {
    match snapshot_id {
        Some(id) => {
            IcebergStaticTableProvider::try_new_from_table_snapshot(table, id).await
        }
        None => IcebergStaticTableProvider::try_new_from_table(table).await,
    }
}

/// Builds an [`IcebergMetadataTableProvider`] (e.g. `tbl$snapshots`) over
/// `table`, from the metadata table kind's lowercase name. Shared by the logical
/// and physical codecs, whose wire formats both carry exactly these fields.
pub(crate) fn metadata_provider(
    table: Table,
    metadata_type: &str,
) -> Result<IcebergMetadataTableProvider, DataFusionError> {
    let kind =
        MetadataTableType::try_from(metadata_type).map_err(DataFusionError::Internal)?;
    Ok(IcebergMetadataTableProvider::new(table, kind))
}

/// How many rebuilt tables [`TableWire::load`] keeps.
const TABLE_CACHE_CAPACITY: usize = 32;

/// Tables rebuilt by [`TableWire::load`], oldest first.
///
/// Every task of a stage decodes the same node, so without this each task would
/// read the same metadata file. A [`TableWire`] fully determines the table it
/// rebuilds, because metadata files never change, so an entry can never go
/// stale; it can only age out. New credentials make a new wire value, and so a
/// new entry, rather than reusing a table that holds the old ones.
///
/// Sharing the table also shares its manifest cache, so later scans of it skip
/// reading and parsing its manifests. That is only safe because every cached
/// table is bound to the process-lived [`TABLE_RT`].
static TABLES: LazyLock<Mutex<VecDeque<(TableWire, Table)>>> =
    LazyLock::new(|| Mutex::new(VecDeque::new()));

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// Leading tag byte that frames every blob this crate's codecs produce.
///
/// Both the logical and physical codecs handle some nodes themselves and
/// delegate the rest to an inner Ballista codec. We write a tag for *both* branches. Decode
/// then dispatches on a value we always control, and the inner payload is nested
/// after the tag — never content-inspected. An unknown or missing tag is a hard
/// error instead of a silent misparse.
pub(crate) const TAG_DELEGATED: u8 = 0;
/// Tag for a payload owned by this crate's Iceberg codecs (JSON follows).
pub(crate) const TAG_ICEBERG: u8 = 1;

/// Frames `payload` as an Iceberg-owned blob: [`TAG_ICEBERG`] then JSON.
pub(crate) fn encode_blob<T: Serialize>(
    buf: &mut Vec<u8>,
    payload: &T,
) -> Result<(), DataFusionError> {
    buf.push(TAG_ICEBERG);
    buf.extend_from_slice(&serde_json::to_vec(payload).map_err(json_err)?);
    Ok(())
}

/// A codec blob split into its framing tag and payload.
pub(crate) enum Frame<'a> {
    /// Payload owned by the inner (delegate) codec.
    Delegated(&'a [u8]),
    /// Payload owned by this crate's Iceberg codecs (JSON).
    Iceberg(&'a [u8]),
}

/// Splits a codec blob into its [`Frame`], so which tags are legal is decided
/// here — next to the tags — rather than at every decode site. `context` names
/// the blob kind in errors.
pub(crate) fn split_frame<'a>(
    buf: &'a [u8],
    context: &str,
) -> Result<Frame<'a>, DataFusionError> {
    match buf.split_first() {
        Some((&TAG_DELEGATED, rest)) => Ok(Frame::Delegated(rest)),
        Some((&TAG_ICEBERG, rest)) => Ok(Frame::Iceberg(rest)),
        Some((&tag, _)) => Err(DataFusionError::Internal(format!(
            "unknown {context} tag {tag}"
        ))),
        None => Err(DataFusionError::Internal(format!("empty {context} buffer"))),
    }
}

/// The `(catalog, table)` header every Iceberg wire payload begins with.
///
/// `#[serde(flatten)]`ed into each wire variant, so the JSON is identical to
/// spelling the two fields inline — this is a code-dedup device, not a wire
/// change.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TableRefWire {
    pub catalog: IcebergCatalogConfig,
    pub table: TableIdent,
}

impl TableRefWire {
    pub(crate) fn new(config: &IcebergCatalogConfig, table: &TableIdent) -> Self {
        Self {
            catalog: config.clone(),
            table: table.clone(),
        }
    }
    pub(crate) fn into_parts(self) -> (IcebergCatalogConfig, TableIdent) {
        (self.catalog, self.table)
    }
}

/// A [`Table`] as the planner loaded it, rebuildable on any node without a
/// catalog.
///
/// The metadata file at `metadata_location` never changes once written, so it
/// fixes the exact version planned against: schema, partition spec and
/// snapshots. `file_io` is the table's `FileIO`, serialized with
/// [`FileIO::serialize_all`]. It carries the storage access the planner had,
/// including any credentials the catalog vended for this table, in plain text,
/// like [`IcebergCatalogConfig`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TableWire {
    pub table: TableIdent,
    pub metadata_location: String,
    pub file_io: FileIoWire,
    pub readonly: bool,
}

impl TableWire {
    pub(crate) fn new(table: &Table) -> Result<Self, DataFusionError> {
        let file_io = table
            .file_io()
            .serialize_all()
            .map_err(to_datafusion_error)?;
        Ok(Self {
            table: table.identifier().clone(),
            metadata_location: table
                .metadata_location_result()
                .map_err(to_datafusion_error)?
                .to_string(),
            file_io: FileIoWire(serde_json::from_slice(&file_io).map_err(json_err)?),
            readonly: table.readonly(),
        })
    }

    /// Rebuilds the table, served from [`TABLES`] when this exact wire value
    /// was rebuilt before.
    pub(crate) fn load(&self) -> Result<Table, DataFusionError> {
        if let Some((_, table)) = TABLES.lock().unwrap().iter().find(|(w, _)| w == self) {
            return Ok(table.clone());
        }
        let table = block_on(self.read()).map_err(to_datafusion_error)?;
        let mut tables = TABLES.lock().unwrap();
        if tables.len() == TABLE_CACHE_CAPACITY {
            tables.pop_front();
        }
        tables.push_back((self.clone(), table.clone()));
        Ok(table)
    }

    async fn read(&self) -> iceberg::Result<Table> {
        let file_io = FileIO::deserialize_all(&serde_json::to_vec(&self.file_io.0)?)?;
        let metadata =
            TableMetadata::read_from(&file_io, &self.metadata_location).await?;
        Table::builder()
            .metadata(metadata)
            .metadata_location(&self.metadata_location)
            .identifier(self.table.clone())
            .file_io(file_io)
            .readonly(self.readonly)
            .runtime(table_runtime())
            .build()
    }
}

/// A serialized `FileIO`, kept as JSON so the wire payload stays readable.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct FileIoWire(serde_json::Value);

/// Hides everything: the storage properties often hold credentials.
impl fmt::Debug for FileIoWire {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FileIO(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_props() -> [(String, String); 3] {
        [
            ("uri".to_string(), "http://localhost:8181".to_string()),
            ("warehouse".to_string(), "s3://bucket/wh".to_string()),
            ("s3.region".to_string(), "us-east-1".to_string()),
        ]
    }

    #[test]
    fn only_the_catch_all_error_kind_is_retryable() {
        assert!(is_retryable(&Error::new(
            ErrorKind::Unexpected,
            "http 401 unauthorized"
        )));

        for kind in [
            ErrorKind::PreconditionFailed,
            ErrorKind::DataInvalid,
            ErrorKind::NamespaceAlreadyExists,
            ErrorKind::TableAlreadyExists,
            ErrorKind::NamespaceNotFound,
            ErrorKind::TableNotFound,
            ErrorKind::FeatureUnsupported,
            ErrorKind::CatalogCommitConflicts,
        ] {
            assert!(
                !is_retryable(&Error::new(kind, "deterministic")),
                "{kind:?} must not trigger a rebuild"
            );
        }
    }

    #[test]
    fn evicting_an_uncached_config_is_a_noop() {
        // The retry path evicts unconditionally, so an uncached config must not
        // panic and poison the cache for every later decode.
        let config = IcebergCatalogConfig::new(
            "rest",
            "never-cached",
            sample_props().into_iter().collect(),
        );
        evict_catalog(&config);
        evict_catalog(&config);
    }

    #[test]
    fn table_wire_debug_hides_storage_properties() {
        use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};

        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
            .with_prop("s3.secret-access-key", "hunter2")
            .build();
        let wire =
            TableWire::new(&crate::test_util::table_with_file_io(&[], file_io)).unwrap();

        // The secret does travel, so it must not leak through Debug.
        assert!(serde_json::to_string(&wire).unwrap().contains("hunter2"));
        let debug = format!("{wire:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(debug.contains("FileIO(<redacted>)"), "{debug}");
    }

    #[test]
    fn catalog_key_ignores_property_order() {
        // Equal configs must share a cached catalog, however their
        // properties happen to be ordered.
        let forward = sample_props().into_iter().collect();
        let reversed = sample_props().into_iter().rev().collect();
        assert_eq!(
            catalog_key(&IcebergCatalogConfig::new("rest", "rest", forward)),
            catalog_key(&IcebergCatalogConfig::new("rest", "rest", reversed))
        );
    }
}
