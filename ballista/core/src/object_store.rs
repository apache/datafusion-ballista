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
use object_store::aws::AmazonS3Builder;
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::{ClientOptions, ObjectStore};
use parking_lot::RwLock;
use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;
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
    let runtime_env = runtime_env_with_s3_support(&session_config)?;

    Ok(SessionStateBuilder::new()
        .with_runtime_env(runtime_env)
        .with_config(session_config)
        .with_default_features()
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
        log::trace!("get_store: {:?}", &self.s3options.config.read());
        match scheme {
            "" | "file" => Ok(self.local.clone()),
            "http" | "https" => Ok(Arc::new(
                HttpBuilder::new()
                    .with_client_options(ClientOptions::new().with_allow_http(true))
                    .with_url(url.origin().ascii_serialization())
                    .build()?,
            )),
            "s3" => {
                let s3store =
                    Self::s3_object_store_builder(url, &self.s3options.config.read())?
                        .build()?;

                Ok(Arc::new(s3store))
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
        } else {
            return config_err!(
                "'s3.access_key_id' & 's3.secret_access_key' must be configured"
            );
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
#[derive(Default, Debug, Clone)]
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
