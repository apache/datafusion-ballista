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

//! The catalogs this crate builds, and the config each is built from.
//!
//! A catalog-backed table provider, and the commits it plans, hold a live
//! catalog. To rebuild them on another node, the codecs send the config that
//! catalog was built from. So that the config always describes the catalog
//! actually in use, this crate builds every catalog itself, from a config, and
//! remembers which config built which catalog ([`catalog_config_of`]). A plan
//! over a catalog built some other way cannot be distributed, and fails to
//! encode with an error saying so.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex};

use datafusion::common::DataFusionError;
use datafusion_iceberg::to_datafusion_error;
use iceberg::Catalog;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde::{Deserialize, Serialize};

use crate::bridge::{CATALOG_RT, block_on};

/// Describes an Iceberg catalog: the inputs a catalog loader takes, and no live
/// connections.
///
/// `catalog_type` selects the loader (`rest`, `sql`, `glue`, `hms`,
/// `s3tables`); `name` and `props` are what
/// [`CatalogBuilder::load`](iceberg::CatalogBuilder::load) receives. Remote
/// nodes rebuild the catalog from it.
///
/// Serializing it includes the property values, credentials among them, unlike
/// its [`Debug`] output, which hides them:
///
/// ```
/// use std::collections::HashMap;
///
/// use iceberg_ballista::IcebergCatalogConfig;
///
/// let config = IcebergCatalogConfig::new(
///     "rest",
///     "prod",
///     HashMap::from([("token".to_string(), "secret".to_string())]),
/// );
/// assert!(!format!("{config:?}").contains("secret"));
///
/// let json = serde_json::to_string(&config)?;
/// assert!(json.contains("secret"));
/// assert_eq!(serde_json::from_str::<IcebergCatalogConfig>(&json)?, config);
/// # Ok::<(), serde_json::Error>(())
/// ```
///
/// Non-exhaustive so fields can be added; build one with [`Self::new`]. A field
/// added later must deserialize with a default, so that serialized configs
/// without it still deserialize.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct IcebergCatalogConfig {
    /// Catalog type, e.g. `"rest"`, `"sql"`, `"glue"`.
    pub catalog_type: String,
    /// Catalog name, as it would be passed to a catalog loader.
    pub name: String,
    /// Catalog connection properties and storage/`FileIO` properties, which in
    /// practice live together in a single map.
    pub props: HashMap<String, String>,
}

impl IcebergCatalogConfig {
    /// Creates a config from the inputs a catalog loader takes.
    pub fn new(
        catalog_type: impl Into<String>,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> Self {
        Self {
            catalog_type: catalog_type.into(),
            name: name.into(),
            props,
        }
    }
}

/// Shows the property keys but not their values, which often hold credentials
/// such as storage keys or catalog tokens.
impl fmt::Debug for IcebergCatalogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut keys: Vec<&str> = self.props.keys().map(String::as_str).collect();
        keys.sort_unstable();
        f.debug_struct("IcebergCatalogConfig")
            .field("catalog_type", &self.catalog_type)
            .field("name", &self.name)
            .field("props", &RedactedProps(&keys))
            .finish()
    }
}

struct RedactedProps<'a>(&'a [&'a str]);

impl fmt::Debug for RedactedProps<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.iter().map(|key| (key, format_args!("<redacted>"))))
            .finish()
    }
}

/// The catalogs this crate built.
///
/// Every catalog lives on [`CATALOG_RT`], which never shuts down, so a catalog
/// stays usable for the life of the process and can be handed to any caller.
struct Catalogs {
    /// The catalog to hand out for each config. Building a catalog client (and
    /// its HTTP/connection pool) is relatively expensive, and many plan nodes
    /// share one catalog.
    current: HashMap<CatalogKey, Arc<dyn Catalog>>,
    /// Every catalog built, with the config it was built from, for
    /// [`catalog_config_of`]. A catalog replaced by [`evict_catalog`] stays
    /// here, because plans made before the replacement still hold it. Holding
    /// each catalog also keeps its address from being reused by another.
    built: Vec<(Arc<dyn Catalog>, IcebergCatalogConfig)>,
}

static CATALOGS: LazyLock<Mutex<Catalogs>> = LazyLock::new(|| {
    Mutex::new(Catalogs {
        current: HashMap::new(),
        built: Vec::new(),
    })
});

/// A [`Catalogs::current`] key: a config's type, name and properties, the
/// properties sorted so the key can be hashed.
type CatalogKey = (String, String, BTreeMap<String, String>);

fn catalog_key(config: &IcebergCatalogConfig) -> CatalogKey {
    let props = config
        .props
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    (config.catalog_type.clone(), config.name.clone(), props)
}

/// Returns the catalog iceberg-ballista uses for `config`, building it the
/// first time.
///
/// A table provider built on a catalog from here (or from the `register_*`
/// helpers, which use this) can be distributed: the codecs know which config
/// built its catalog, so remote nodes rebuild the same catalog. A provider built
/// on any other catalog fails to encode.
///
/// # Errors
///
/// Returns an error if the catalog type is unknown or the catalog fails to
/// load, for example because it cannot be reached.
pub async fn load_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    if let Some(catalog) = cached(config) {
        return Ok(catalog);
    }
    let owned = config.clone();
    let catalog = CATALOG_RT
        .spawn(async move { build_catalog(&owned).await })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;
    Ok(remember(config, catalog))
}

/// [`load_catalog`] for the synchronous codec entry points.
pub(crate) fn get_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    if let Some(catalog) = cached(config) {
        return Ok(catalog);
    }
    let catalog = block_on(build_catalog(config))?;
    Ok(remember(config, catalog))
}

/// Drops the catalog handed out for `config`, so the next [`get_catalog`]
/// builds a new one: reopening connections and re-resolving credentials.
pub(crate) fn evict_catalog(config: &IcebergCatalogConfig) {
    CATALOGS
        .lock()
        .unwrap()
        .current
        .remove(&catalog_key(config));
}

/// The config `catalog` was built from, if this crate built it.
pub(crate) fn catalog_config_of(
    catalog: &Arc<dyn Catalog>,
) -> Option<IcebergCatalogConfig> {
    CATALOGS
        .lock()
        .unwrap()
        .built
        .iter()
        .find(|(built, _)| Arc::ptr_eq(built, catalog))
        .map(|(_, config)| config.clone())
}

/// Error for a plan node or provider whose catalog this crate did not build,
/// so no config describes it.
pub(crate) fn unknown_catalog_err(node: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "{node} uses a catalog iceberg-ballista did not build, so it cannot be \
         rebuilt on another node; register the table with \
         iceberg_ballista::register_iceberg_table or register_iceberg_catalog, or \
         build its catalog with iceberg_ballista::load_catalog"
    ))
}

fn cached(config: &IcebergCatalogConfig) -> Option<Arc<dyn Catalog>> {
    CATALOGS
        .lock()
        .unwrap()
        .current
        .get(&catalog_key(config))
        .cloned()
}

/// Records `catalog` as built from `config` and returns the catalog to use for
/// `config`: `catalog`, unless another caller built and recorded one first, in
/// which case that one, and `catalog` is dropped unrecorded.
fn remember(
    config: &IcebergCatalogConfig,
    catalog: Arc<dyn Catalog>,
) -> Arc<dyn Catalog> {
    let mut catalogs = CATALOGS.lock().unwrap();
    if let Some(current) = catalogs.current.get(&catalog_key(config)) {
        return current.clone();
    }
    catalogs
        .current
        .insert(catalog_key(config), catalog.clone());
    catalogs.built.push((catalog.clone(), config.clone()));
    catalog
}

/// Builds a catalog from its config.
///
/// The catalog type is resolved through [`iceberg_catalog_loader`], so any
/// catalog it supports (`rest`, `sql`, `glue`, `hms`, `s3tables`) works here.
/// Storage is provided by [`OpenDalResolvingStorageFactory`], which picks the
/// object-store backend (S3, GCS, Azure, local fs, …) from each file's path
/// scheme, configured from the same `props`. So a single code path covers every
/// catalog/storage combination the iceberg crates support.
async fn build_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    iceberg_catalog_loader::load(&config.catalog_type)
        .map_err(to_datafusion_error)?
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load(config.name.clone(), config.props.clone())
        .await
        .map_err(to_datafusion_error)
}

/// Records `catalog` as built from `config`, for tests that need a registered
/// catalog without a catalog server. Returns the catalog to use for `config`.
#[cfg(test)]
pub(crate) fn register_for_test(
    config: &IcebergCatalogConfig,
    catalog: Arc<dyn Catalog>,
) -> Arc<dyn Catalog> {
    remember(config, catalog)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util;

    fn sample_props() -> [(String, String); 3] {
        [
            ("uri".to_string(), "http://localhost:8181".to_string()),
            ("warehouse".to_string(), "s3://bucket/wh".to_string()),
            ("s3.region".to_string(), "us-east-1".to_string()),
        ]
    }

    #[test]
    fn debug_hides_property_values() {
        let config = IcebergCatalogConfig::new(
            "rest",
            "prod",
            HashMap::from([
                ("uri".to_string(), "http://catalog:8181".to_string()),
                ("s3.secret-access-key".to_string(), "hunter2".to_string()),
            ]),
        );
        assert_eq!(
            format!("{config:?}"),
            r#"IcebergCatalogConfig { catalog_type: "rest", name: "prod", props: {"s3.secret-access-key": <redacted>, "uri": <redacted>} }"#
        );
    }

    #[test]
    fn catalog_key_ignores_property_order() {
        // Equal configs must share a cached catalog, however their properties
        // happen to be ordered.
        let forward = sample_props().into_iter().collect();
        let reversed = sample_props().into_iter().rev().collect();
        assert_eq!(
            catalog_key(&IcebergCatalogConfig::new("rest", "rest", forward)),
            catalog_key(&IcebergCatalogConfig::new("rest", "rest", reversed))
        );
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

    #[tokio::test]
    async fn only_catalogs_built_here_have_a_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_util::unique_catalog_config();
        let built =
            register_for_test(&config, test_util::memory_catalog(dir.path()).await);
        let other = test_util::memory_catalog(dir.path()).await;

        assert_eq!(catalog_config_of(&built), Some(config));
        assert_eq!(catalog_config_of(&other), None);
    }

    #[tokio::test]
    async fn config_hands_out_one_catalog_until_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_util::unique_catalog_config();
        let first =
            register_for_test(&config, test_util::memory_catalog(dir.path()).await);

        // A second catalog built for the same config, for example by a caller
        // that raced the first, is dropped in favour of the recorded one.
        let raced = test_util::memory_catalog(dir.path()).await;
        let handed_out = register_for_test(&config, raced.clone());
        assert!(Arc::ptr_eq(&handed_out, &first));
        assert_eq!(catalog_config_of(&raced), None);

        // After an eviction the next catalog replaces it, and the first still
        // resolves, for the plans that hold it.
        evict_catalog(&config);
        let second =
            register_for_test(&config, test_util::memory_catalog(dir.path()).await);
        assert!(!Arc::ptr_eq(&second, &first));
        assert!(Arc::ptr_eq(&get_catalog(&config).unwrap(), &second));
        assert_eq!(catalog_config_of(&first), Some(config));
    }
}
