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

use config::{Config, ConfigError, Environment, File, FileFormat};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SchedulerSettings {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct HttpSettings {
    /// Connection timeout. In millis
    pub timeout: u64,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub scheduler: SchedulerSettings,
    pub http: HttpSettings,
}

const DEFAULT_CONFIG: &str = r#"
scheduler:
  url: http://localhost:50050

http:
  timeout: 2000
"#;

impl Settings {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let config_dir = dirs::config_dir().unwrap().join("ballista-tui");

        let s = Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml))
            // Add in user's config file
            .add_source(
                File::with_name(&format!("{}/config", config_dir.display()))
                    .required(false),
            )
            // Add in settings from the environment (with a prefix of BALLISTA_TUI_)
            // E.g. `BALLISTA_TUI_SCHEDULER_URL=http://localhost:50051 ./target/app`
            // would set the scheduler url key
            .add_source(Environment::with_prefix("ballista-tui"))
            .build()?;

        s.try_deserialize()
    }
}
