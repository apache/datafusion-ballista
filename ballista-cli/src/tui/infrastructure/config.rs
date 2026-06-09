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

use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;

/// Selects a built-in colour preset.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ThemeName {
    #[default]
    Dark,
    Light,
}

/// A colour specification usable in the user's config file.
/// Supports named colours (`"Red"`, `"LightBlue"`, …), 256-colour indices
/// (`108`), and 24-bit RGB (`{r: 180, g: 100, b: 0}`).
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum ColorSpec {
    Named(String),
    Indexed(u8),
    Rgb { r: u8, g: u8, b: u8 },
}

/// Optional per-role fg-colour overrides applied on top of the chosen preset.
/// Each absent field keeps the preset's value.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ThemeOverride {
    pub status_running: Option<ColorSpec>,
    pub status_queued: Option<ColorSpec>,
    pub status_failed: Option<ColorSpec>,
    pub status_completed: Option<ColorSpec>,
    pub status_unknown: Option<ColorSpec>,
    pub table_header: Option<ColorSpec>,
    pub row_even: Option<ColorSpec>,
    pub row_odd: Option<ColorSpec>,
    pub row_selected: Option<ColorSpec>,
    pub popup_border: Option<ColorSpec>,
    pub popup_border_alt: Option<ColorSpec>,
    pub popup_border_jobs_stages: Option<ColorSpec>,
    pub nav_active: Option<ColorSpec>,
    pub nav_inactive: Option<ColorSpec>,
    pub search_active: Option<ColorSpec>,
    pub search_inactive: Option<ColorSpec>,
    pub search_cursor: Option<ColorSpec>,
    pub help_header: Option<ColorSpec>,
    pub help_section: Option<ColorSpec>,
    pub help_item: Option<ColorSpec>,
    pub help_item_dim: Option<ColorSpec>,
    pub detail_label: Option<ColorSpec>,
    pub graph_border: Option<ColorSpec>,
    pub graph_label: Option<ColorSpec>,
    pub graph_stage: Option<ColorSpec>,
    pub graph_arrow: Option<ColorSpec>,
    pub tile_running: Option<ColorSpec>,
    pub tile_queued: Option<ColorSpec>,
    pub tile_completed: Option<ColorSpec>,
    pub tile_failed: Option<ColorSpec>,
    pub scheduler_down: Option<ColorSpec>,
    pub cancel_success: Option<ColorSpec>,
    pub cancel_not_done: Option<ColorSpec>,
    pub cancel_failure: Option<ColorSpec>,
    pub banner: Option<ColorSpec>,
    pub feature_enabled: Option<ColorSpec>,
    pub feature_disabled: Option<ColorSpec>,
    pub text_error: Option<ColorSpec>,
}

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
data_reload_interval_ms: 2000
repaint_interval_ms: 50

scheduler:
  url: http://localhost:50050

http:
  timeout: 2000

theme:
  name: dark
  #custom:
    #table_header: "red"
    #row_selected: 108
    #status_running: { r: 0, g: 128, b: 255 }
"#;

impl Settings {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let builder = Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml));

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
                        .format(FileFormat::Yaml)
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
            File::from_str(&config, FileFormat::Yaml)
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
