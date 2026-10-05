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

//! # Extending Ballista
//!
//! This example demonstrates extending standard ballista behavior,
//! integrating external `ObjectStoreRegistry`.
//!
//! `ObjectStore` is provided by `ObjectStoreRegistry`, and configured
//! using `ExtensionOptions`, which can be configured using SQL `SET` command.

use datafusion::common::{config_err, exec_err};
use datafusion::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreRegistry;

use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionConfig;
use lru::LruCache;
use object_store::aws::AmazonS3Builder;
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::{ClientOptions, ObjectStore};
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};
use url::Url;

use crate::extension::SessionConfigExt;

/// Custom [SessionConfig] constructor method
///
/// This method registers config extension [S3Options]
/// which is used to configure [ObjectStore] with ACCESS and
/// SECRET key
pub fn session_config_with_s3_support() -> SessionConfig {
    SessionConfig::new_with_ballista()
        .with_information_schema(true)
        .with_option_extension(S3Options::default())
}

/// Custom [RuntimeEnv] constructor method
///
/// It will register [CustomObjectStoreRegistry] which will
/// use configuration extension [S3Options] to configure
/// and created [ObjectStore]s
pub fn runtime_env_with_s3_support(
    session_config: &SessionConfig,
) -> Result<Arc<RuntimeEnv>> {
    let s3options = session_config
        .options()
        .extensions
        .get::<S3Options>()
        .ok_or(DataFusionError::Configuration(
            "S3 Options not set".to_string(),
        ))?;

    let runtime_env = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(CustomObjectStoreRegistry::new(
            s3options.clone(),
        )))
        .build()?;

    Ok(Arc::new(runtime_env))
}

/// Custom [SessionState] with S3 support enabled
///
/// It will configure [SessionState] with provided [SessionConfig],
/// and [RuntimeEnv].
pub fn session_state_with_s3_support(
    session_config: SessionConfig,
) -> datafusion::common::Result<SessionState> {
    use crate::extension::{
        ballista_aggregate_functions, ballista_scalar_functions,
        ballista_window_functions,
    };

    let runtime_env = runtime_env_with_s3_support(&session_config)?;

    Ok(SessionStateBuilder::new()
        .with_runtime_env(runtime_env)
        .with_config(session_config)
        .with_default_features()
        .with_scalar_functions(ballista_scalar_functions())
        .with_aggregate_functions(ballista_aggregate_functions())
        .with_window_functions(ballista_window_functions())
        .build())
}

/// Custom [SessionState] with S3 support.
/// It is alias to [session_state_with_s3_support] with [session_config_with_s3_support] as a
/// parameter
///
/// It will configure [SessionState] S3 enabled [SessionConfig],
/// and [RuntimeEnv].
pub fn state_with_s3_support() -> datafusion::common::Result<SessionState> {
    session_state_with_s3_support(session_config_with_s3_support())
}

/// Custom [ObjectStoreRegistry] which will create
/// and configure [ObjectStore] using provided [S3Options]
///
/// Built stores are cached for the whole process and shared by every
/// registry. An `s3://` store is reused for the same bucket and the same
/// [S3Options] values, so changing an option builds a new store rather than
/// reusing one built from the old values.
#[derive(Debug)]
pub struct CustomObjectStoreRegistry {
    local: Arc<LocalFileSystem>,
    s3options: S3Options,
}

impl CustomObjectStoreRegistry {
    /// Creates a new custom object store registry with the given S3 options.
    pub fn new(s3options: S3Options) -> Self {
        Self {
            s3options,
            local: Arc::new(LocalFileSystem::new()),
        }
    }
}

impl ObjectStoreRegistry for CustomObjectStoreRegistry {
    fn register_store(
        &self,
        _url: &Url,
        _store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        unimplemented!("register_store not supported")
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let scheme = url.scheme();
        log::trace!("get_store: {:?}", self.s3options.config.read());
        match scheme {
            "" | "file" => Ok(self.local.clone()),
            "http" | "https" => {
                STORE_CACHE.get_or_build(StoreKey::new(url, None), || {
                    let http_store = HttpBuilder::new()
                        .with_client_options(ClientOptions::new().with_allow_http(true))
                        .with_url(url.origin().ascii_serialization())
                        .build()?;

                    Ok(Arc::new(http_store))
                })
            }
            "s3" => {
                // Build from the same snapshot the key holds, so a concurrent
                // `SET` can't cache a store under options it wasn't built from.
                let config = self.s3options.config.read().clone();
                let key = StoreKey::new(url, Some(config.clone()));

                STORE_CACHE.get_or_build(key, || {
                    let s3store = Self::s3_object_store_builder(url, &config)?.build()?;

                    Ok(Arc::new(s3store))
                })
            }

            _ => exec_err!("get_store - store not supported, url {}", url),
        }
    }
}

impl CustomObjectStoreRegistry {
    fn s3_object_store_builder(
        url: &Url,
        aws_options: &S3RegistryConfiguration,
    ) -> Result<AmazonS3Builder> {
        let S3RegistryConfiguration {
            access_key_id,
            secret_access_key,
            session_token,
            region,
            endpoint,
            allow_http,
        } = aws_options;

        let bucket_name = Self::get_bucket_name(url)?;
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);

        if let (Some(access_key_id), Some(secret_access_key)) =
            (access_key_id, secret_access_key)
        {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);

            if let Some(session_token) = session_token {
                builder = builder.with_token(session_token);
            }
        }

        if let Some(region) = region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = endpoint {
            if let Ok(endpoint_url) = Url::try_from(endpoint.as_str())
                && !matches!(allow_http, Some(true))
                && endpoint_url.scheme() == "http"
            {
                return config_err!(
                    "Invalid endpoint: {endpoint}. HTTP is not allowed for S3 endpoints. To allow HTTP, set 's3.allow_http' to true"
                );
            }

            builder = builder.with_endpoint(endpoint);
        }

        if let Some(allow_http) = allow_http {
            builder = builder.with_allow_http(*allow_http);
        }

        Ok(builder)
    }

    fn get_bucket_name(url: &Url) -> Result<&str> {
        url.host_str().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Not able to parse bucket name from url: {}",
                url.as_str()
            ))
        })
    }
}

/// Maximum number of stores kept in [STORE_CACHE].
const STORE_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// Stores built by every [CustomObjectStoreRegistry] in the process.
///
/// Each store owns an HTTP connection pool and a credential provider that
/// caches temporary credentials (web identity, container credentials,
/// instance metadata). Reusing stores lets scan partitions and sessions share
/// connections and credentials, rather than each opening its own connections
/// and making its own credential request.
static STORE_CACHE: LazyLock<StoreCache> =
    LazyLock::new(|| StoreCache::new(STORE_CACHE_CAPACITY));

/// Identifies a store in a [StoreCache].
#[derive(PartialEq, Eq, Hash)]
struct StoreKey {
    /// `scheme://host[:port]`, the part of the URL a store is built for
    url: String,
    /// Options an `s3://` store is built from, `None` for other stores
    s3_config: Option<S3RegistryConfiguration>,
}

impl StoreKey {
    fn new(url: &Url, s3_config: Option<S3RegistryConfiguration>) -> Self {
        Self {
            url: format!(
                "{}://{}",
                url.scheme(),
                &url[url::Position::BeforeHost..url::Position::AfterPort]
            ),
            s3_config,
        }
    }
}

/// Least recently used cache of built [ObjectStore]s.
///
/// It is bounded because a long-running executor serves many sessions, and
/// each distinct set of options (a refreshed session token, say) adds a key.
/// An evicted store keeps working for anyone still holding it.
struct StoreCache {
    stores: Mutex<LruCache<StoreKey, Arc<dyn ObjectStore>>>,
}

impl StoreCache {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            stores: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Returns the store cached under `key`, or caches and returns the one
    /// `build` creates.
    fn get_or_build(
        &self,
        key: StoreKey,
        build: impl FnOnce() -> Result<Arc<dyn ObjectStore>>,
    ) -> Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.stores.lock().get(&key) {
            return Ok(Arc::clone(store));
        }

        // Build without holding the lock, so a miss doesn't stall lookups of
        // other stores. Concurrent misses for one key may each build a store,
        // but only the first one inserted is kept, and all of them return it.
        let store = build()?;
        Ok(Arc::clone(self.stores.lock().get_or_insert(key, || store)))
    }
}

/// Custom [SessionConfig] extension which allows
/// users to configure [ObjectStore] access using SQL
/// interface
#[derive(Debug, Clone, Default)]
pub struct S3Options {
    config: Arc<RwLock<S3RegistryConfiguration>>,
}

impl ExtensionOptions for S3Options {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        log::debug!("set config, key:{key},  value:{value}");
        match key {
            "access_key_id" => {
                let mut c = self.config.write();
                c.access_key_id.set(key, value)?;
            }
            "secret_access_key" => {
                let mut c = self.config.write();
                c.secret_access_key.set(key, value)?;
            }
            "session_token" => {
                let mut c = self.config.write();
                c.session_token.set(key, value)?;
            }
            "region" => {
                let mut c = self.config.write();
                c.region.set(key, value)?;
            }
            "endpoint" => {
                let mut c = self.config.write();
                c.endpoint.set(key, value)?;
            }
            "allow_http" => {
                let mut c = self.config.write();
                c.allow_http.set(key, value)?;
            }
            _ => {
                log::warn!("Config value {key} cant be set to {value}");
                return config_err!("Config value \"{}\" not found in S3Options", key);
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: format!("{}.{}", S3Options::PREFIX, key),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: format!("{}.{}", S3Options::PREFIX, key),
                    value: None,
                    description,
                })
            }
        }
        let c = self.config.read();

        let mut v = Visitor(vec![]);
        c.access_key_id
            .visit(&mut v, "access_key_id", "S3 Access Key");
        c.secret_access_key
            .visit(&mut v, "secret_access_key", "S3 Secret Key");
        c.session_token
            .visit(&mut v, "session_token", "S3 Session token");
        c.region.visit(&mut v, "region", "S3 region");
        c.endpoint.visit(&mut v, "endpoint", "S3 Endpoint");
        c.allow_http.visit(&mut v, "allow_http", "S3 Allow Http");

        v.0
    }
}

impl ConfigExtension for S3Options {
    const PREFIX: &'static str = "s3";
}
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
struct S3RegistryConfiguration {
    /// Access Key ID
    pub access_key_id: Option<String>,
    /// Secret Access Key
    pub secret_access_key: Option<String>,
    /// Session token
    pub session_token: Option<String>,
    /// AWS Region
    pub region: Option<String>,
    /// OSS or COS Endpoint
    pub endpoint: Option<String>,
    /// Allow HTTP (otherwise will always use https)
    pub allow_http: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use object_store::memory::InMemory;
    use std::sync::Barrier;
    use std::thread;

    /// Builds options the same way `SET s3.<key> = '<value>'` does.
    fn s3_options(entries: &[(&str, &str)]) -> S3Options {
        let mut options = S3Options::default();
        for (key, value) in entries {
            options.set(key, value).unwrap();
        }
        options
    }

    fn get_store(
        registry: &CustomObjectStoreRegistry,
        url: &str,
    ) -> Arc<dyn ObjectStore> {
        registry.get_store(&Url::parse(url).unwrap()).unwrap()
    }

    #[test]
    fn reuses_s3_store_per_bucket() {
        let registry = CustomObjectStoreRegistry::new(S3Options::default());

        let first = get_store(&registry, "s3://reuse-bucket-one/a/data.parquet");
        let same_bucket = get_store(&registry, "s3://reuse-bucket-one/b/");
        let other_bucket = get_store(&registry, "s3://reuse-bucket-two/a/data.parquet");

        assert!(Arc::ptr_eq(&first, &same_bucket));
        assert!(!Arc::ptr_eq(&first, &other_bucket));
    }

    #[test]
    fn shares_s3_store_across_registries_with_equal_options() {
        // Two sessions with the same S3 settings each get their own registry.
        let entries = [
            ("access_key_id", "KEY"),
            ("secret_access_key", "SECRET"),
            ("region", "us-east-1"),
        ];
        let first = CustomObjectStoreRegistry::new(s3_options(&entries));
        let second = CustomObjectStoreRegistry::new(s3_options(&entries));

        assert!(Arc::ptr_eq(
            &get_store(&first, "s3://shared-bucket"),
            &get_store(&second, "s3://shared-bucket"),
        ));
    }

    #[test]
    fn isolates_s3_stores_by_credentials() {
        let first = CustomObjectStoreRegistry::new(s3_options(&[
            ("access_key_id", "KEY_ONE"),
            ("secret_access_key", "SECRET"),
        ]));
        let second = CustomObjectStoreRegistry::new(s3_options(&[
            ("access_key_id", "KEY_TWO"),
            ("secret_access_key", "SECRET"),
        ]));

        let first_store = get_store(&first, "s3://tenant-bucket");
        let second_store = get_store(&second, "s3://tenant-bucket");

        assert!(!Arc::ptr_eq(&first_store, &second_store));
        assert!(format!("{second_store:?}").contains("KEY_TWO"));
    }

    #[test]
    fn builds_new_s3_store_after_options_change() {
        let mut config = session_config_with_s3_support();
        config.options_mut().set("s3.region", "us-east-1").unwrap();
        let runtime = runtime_env_with_s3_support(&config).unwrap();
        let url = ObjectStoreUrl::parse("s3://changed-options-bucket").unwrap();
        let before = runtime.object_store(&url).unwrap();

        config.options_mut().set("s3.region", "eu-west-1").unwrap();
        let after = runtime.object_store(&url).unwrap();

        assert!(!Arc::ptr_eq(&before, &after));
        assert!(format!("{after:?}").contains("eu-west-1"));
    }

    #[test]
    fn reuses_http_store_per_origin() {
        let registry = CustomObjectStoreRegistry::new(S3Options::default());

        let first = get_store(&registry, "http://localhost:8080/a/data.parquet");
        let same_origin = get_store(&registry, "http://localhost:8080/b/");
        let other_port = get_store(&registry, "http://localhost:9090/a/data.parquet");

        assert!(Arc::ptr_eq(&first, &same_origin));
        assert!(!Arc::ptr_eq(&first, &other_port));
    }

    #[test]
    fn concurrent_first_calls_share_one_store() {
        let registry = CustomObjectStoreRegistry::new(S3Options::default());
        let barrier = Barrier::new(8);

        let stores: Vec<_> = thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        get_store(&registry, "s3://concurrent-bucket")
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        assert!(stores.iter().all(|store| Arc::ptr_eq(store, &stores[0])));
    }

    #[test]
    fn evicts_least_recently_used_store() {
        let cache = StoreCache::new(NonZeroUsize::new(2).unwrap());
        let get = |url: &str| {
            let key = StoreKey::new(&Url::parse(url).unwrap(), None);
            cache
                .get_or_build(key, || Ok(Arc::new(InMemory::new())))
                .unwrap()
        };

        let one = get("memory://one");
        let two = get("memory://two");
        // Using `one` again leaves `two` as the least recently used.
        get("memory://one");
        get("memory://three");

        assert!(Arc::ptr_eq(&one, &get("memory://one")));
        assert!(!Arc::ptr_eq(&two, &get("memory://two")));
    }
}
