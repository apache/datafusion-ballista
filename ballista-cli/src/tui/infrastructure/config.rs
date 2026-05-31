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
pub struct JobSettings {
    /// The job stages' settings
    pub stage: JobStageSettings,
}

#[derive(Debug, Deserialize)]
pub struct JobStageSettings {
    /// The job plan's settings
    pub plan: JobPlanSettings,
}

#[derive(Debug, Deserialize)]
pub struct JobPlanSettings {
    /// Whether to render the plan as a tree.
    pub tree: bool,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub scheduler: SchedulerSettings,
    #[cfg(not(target_arch = "wasm32"))]
    pub http: HttpSettings,
    /// How often to refresh the UI. In millis.
    pub tick_interval_ms: u64,

    pub job: JobSettings,
}

const DEFAULT_CONFIG: &str = r#"
tick_interval_ms: 2000

scheduler:
  url: http://localhost:50050

http:
  timeout: 2000

job:
  stage:
    plan:
      tree: false
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
            let mut tick_interval_ms = 2000;
            let mut scheduler_url = "http://localhost:50050";
            let mut http_timeout_ms = 2000;
            let mut format_tree = false;

            let query_string = Self::decode_request();
            query_string.split('&').for_each(|setting| {
                let mut pair = setting.split('=');

                if let (Some(key), Some(value)) = (pair.next(), pair.next()) {
                    match key {
                        "ballista_tick_interval" => {
                            tick_interval_ms = value.parse::<u64>().unwrap_or(2000)
                        }
                        "ballista_scheduler_url" => scheduler_url = value,
                        "ballista_http_timeout" => {
                            http_timeout_ms = value.parse::<u64>().unwrap_or(2000)
                        }
                        "ballista_job_stage_plan_tree" => {
                            format_tree = value.parse::<bool>().unwrap_or(false)
                        }
                        _ => {}
                    }
                }
            });
            let config = format!(
                r#"
tick_interval_ms: {tick_interval_ms}

scheduler:
  url: {scheduler_url}

http:
  timeout: {http_timeout_ms}

job:
  stage:
    plan:
      tree: {format_tree}
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
