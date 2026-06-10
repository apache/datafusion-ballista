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

use super::{ThemeName, ThemeOverride};
use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;

/// Theme configuration: built-in preset name plus optional colour overrides.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ThemeSettings {
    #[serde(default)]
    pub name: ThemeName,
    #[serde(default)]
    pub custom: ThemeOverride,
}

#[derive(Debug, Deserialize)]
pub struct SchedulerSettings {
    pub url: String,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Deserialize)]
pub struct HttpSettings {
    /// Connection timeout. In millis
    pub timeout: u64,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub scheduler: SchedulerSettings,
    #[cfg(not(target_arch = "wasm32"))]
    pub http: HttpSettings,
    /// How often to poll the scheduler for state. In millis.
    pub data_reload_interval_ms: u64,
    /// How often to refresh the UI. In millis.
    pub repaint_interval_ms: u64,
    #[serde(default)]
    pub theme: ThemeSettings,
}

const DEFAULT_CONFIG: &str = r#"
{
    "data_reload_interval_ms": 2000,
    "repaint_interval_ms": 50,

    "scheduler": {
        "url": "http://localhost:50050"
    },

    "http": {
        "timeout": 2000
    }
}
"#;

impl Settings {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let builder = Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Json));

        #[cfg(all(feature = "web", not(feature = "tui")))]
        let builder = builder.add_source(web::QueryString::parse());

        #[cfg(all(feature = "tui", not(feature = "web")))]
        let builder = {
            use config::Environment;

            let config_dir = dirs::config_local_dir()
                .or_else(|| dirs::home_dir().map(|h| h.join(".config")))
                .unwrap_or_else(|| std::path::PathBuf::from(".config"))
                .join("ballista");

            builder // Add in user's config file
                .add_source(
                    File::with_name(&format!("{}/tui", config_dir.display()))
                        .format(FileFormat::Json)
                        .required(false),
                )
                // Add in settings from the environment (with a prefix of BALLISTA_)
                // E.g. `BALLISTA_SCHEDULER_URL=http://localhost:50051 ballista_cli`
                // would set the scheduler url key
                .add_source(Environment::with_prefix("BALLISTA").separator("_"))
        };

        let config = builder.build()?;

        config.try_deserialize()
    }
}

#[cfg(feature = "web")]
mod web {
    use config::{File, FileFormat, FileSourceString};

    #[derive(Debug, Clone)]
    pub(super) struct QueryString;

    impl QueryString {
        pub(super) fn parse() -> File<FileSourceString, FileFormat> {
            let mut data_reload_interval_ms = 2000;
            let mut repaint_interval_ms = 50;
            let mut scheduler_url = "http://localhost:50050";
            let mut http_timeout_ms = 2000;

            let query_string = Self::decode_request();
            query_string.split('&').for_each(|setting| {
                let mut pair = setting.split('=');

                if let (Some(key), Some(value)) = (pair.next(), pair.next()) {
                    match key {
                        "ballista_tick_interval" => {
                            data_reload_interval_ms = value.parse::<u64>().unwrap_or(2000)
                        }
                        "ballista_repaint_interval" => {
                            repaint_interval_ms = value.parse::<u64>().unwrap_or(50)
                        }
                        "ballista_scheduler_url" => scheduler_url = value,
                        "ballista_http_timeout" => {
                            http_timeout_ms = value.parse::<u64>().unwrap_or(2000)
                        }
                        _ => {}
                    }
                }
            });
            let config = format!(
                r#"
data_reload_interval_ms: {data_reload_interval_ms}
repaint_interval_ms: {repaint_interval_ms}

scheduler:
  url: {scheduler_url}

http:
  timeout: {http_timeout_ms}

"#
            );
            tracing::info!("Using query string: {}", config);
            File::from_str(&config, FileFormat::Json)
        }

        fn decode_request() -> std::string::String {
            let window = web_sys::window().expect("no global `window` exists");
            let document = window.document().expect("no global document exist");
            let location = document.location().expect("no location exists");
            let raw_search = location.search().expect("no search exists");
            raw_search.trim_start_matches("?").to_string()
        }
    }
}
