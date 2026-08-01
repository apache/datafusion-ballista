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
//! The central problem this module solves is that Ballista serializes physical
//! and logical plans to ship them to remote nodes, but the Iceberg plan nodes
//! hold live, non-serializable state (an `Arc<dyn Catalog>` and a `Table` with
//! an open `FileIO`). We side-step this by serializing only the minimal,
//! self-contained [`IcebergCatalogConfig`] plus the [`TableIdent`], and then
//! *reconstructing* the catalog and table on the receiving node by building the
//! catalog from the config and loading the table from it.
//!
//! Reconstruction is asynchronous (catalog clients and table loads do I/O) but
//! the codec entry points are synchronous, so [`block_on`] bridges the two by
//! running every catalog future on [`CATALOG_RT`], a dedicated process-lived
//! runtime.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};

use datafusion::common::DataFusionError;
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, TableIdent};
use iceberg_datafusion::{IcebergCatalogConfig, to_datafusion_error};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde::{Deserialize, Serialize};

/// Converts a non-Iceberg error (serde, etc.) into a [`DataFusionError`].
/// Iceberg errors use [`to_datafusion_error`], the workspace's conversion.
pub(crate) fn to_df_err<E: std::error::Error + Send + Sync + 'static>(
    e: E,
) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Dedicated process-lived runtime that drives all Iceberg catalog I/O.
///
/// A catalog's HTTP/connection pool is bound to the runtime that drives it, so
/// running every catalog future here — instead of on whatever runtime the codec
/// caller happens to be on — means a cached catalog can never reference an
/// already-dropped runtime, no matter which thread or test asks for it later.
/// Catalog operations only happen at plan encode/decode time, so one worker is
/// plenty.
static CATALOG_RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("iceberg-catalog")
        .enable_all()
        .build()
        .expect("failed to build iceberg catalog runtime")
});

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
static CATALOGS: LazyLock<Mutex<HashMap<CatalogConfigWire, Arc<dyn Catalog>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Builds a catalog from its config.
///
/// The catalog type is resolved through [`iceberg_catalog_loader`], so any
/// catalog it supports (`rest`, `sql`, `glue`, `hms`, `s3tables`) works here.
/// Storage is provided by [`OpenDalResolvingStorageFactory`], which picks the
/// object-store backend (S3, GCS, Azure, local fs, …) from each file's path
/// scheme, configured from the same `props`. So a single code path covers every
/// catalog/storage combination the workspace supports.
pub(crate) async fn build_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    iceberg_catalog_loader::load(&config.r#type)
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
    let key = CatalogConfigWire::from(config);
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
    CATALOGS
        .lock()
        .unwrap()
        .remove(&CatalogConfigWire::from(config));
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
/// returned. Callers that only want the table ignore it: `let (_, table) = …`.
pub(crate) fn load_table(
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
    buf.extend_from_slice(&serde_json::to_vec(payload).map_err(to_df_err)?);
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

/// Serializable mirror of [`IcebergCatalogConfig`] (which is intentionally not
/// serde-aware in the iceberg crate to avoid a serde dependency there).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct CatalogConfigWire {
    pub r#type: String,
    pub name: String,
    // BTreeMap (not HashMap) so the struct can derive Hash for the catalog
    // cache, and so encode→decode→encode round-trips to identical bytes.
    pub props: BTreeMap<String, String>,
}

impl From<&IcebergCatalogConfig> for CatalogConfigWire {
    fn from(c: &IcebergCatalogConfig) -> Self {
        Self {
            r#type: c.r#type.clone(),
            name: c.name.clone(),
            props: c
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

impl From<CatalogConfigWire> for IcebergCatalogConfig {
    fn from(p: CatalogConfigWire) -> Self {
        IcebergCatalogConfig::new(p.r#type, p.name, p.props.into_iter().collect())
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
    fn catalog_config_wire_roundtrips_to_identical_config() {
        let config = IcebergCatalogConfig::new(
            "rest",
            "rest",
            sample_props().into_iter().collect(),
        );
        let restored: IcebergCatalogConfig = CatalogConfigWire::from(&config).into();
        assert_eq!(restored, config);
    }

    #[test]
    fn catalog_config_wire_serialization_is_deterministic() {
        // BTreeMap props serialize in key order, so the same entries yield the same
        // bytes regardless of HashMap ordering — what the cache key relies on.
        let forward: HashMap<_, _> = sample_props().into_iter().collect();
        let reversed: HashMap<_, _> = sample_props().into_iter().rev().collect();

        let a =
            CatalogConfigWire::from(&IcebergCatalogConfig::new("rest", "rest", forward));
        let b =
            CatalogConfigWire::from(&IcebergCatalogConfig::new("rest", "rest", reversed));
        let a_bytes = serde_json::to_vec(&a).unwrap();
        assert_eq!(a_bytes, serde_json::to_vec(&b).unwrap());

        // encode→decode→encode is a fixed point.
        let decoded: CatalogConfigWire = serde_json::from_slice(&a_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), a_bytes);
    }
}
