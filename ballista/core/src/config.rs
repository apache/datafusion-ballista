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
//

//! Ballista configuration

use std::collections::HashMap;
use std::result;

use crate::error::{BallistaError, Result};

use datafusion::{
    arrow::datatypes::DataType, common::config_err, config::ConfigExtension,
};

pub const BALLISTA_JOB_NAME: &str = "ballista.job.name";
pub const BALLISTA_STANDALONE_PARALLELISM: &str = "ballista.standalone.parallelism";
/// max message size for gRPC clients
pub const BALLISTA_GRPC_CLIENT_MAX_MESSAGE_SIZE: &str =
    "ballista.grpc_client_max_message_size";
/// enable or disable ballista dml planner extension.
/// when enabled planner will use custom logical planner DML
/// extension which will serialize table provider used in DML
///
/// this configuration should be disabled if using remote schema
/// registries.
pub const BALLISTA_PLANNER_DML_EXTENSION: &str = "ballista.planner.dml_extension";

pub type ParseResult<T> = result::Result<T, String>;
use std::sync::LazyLock;

static CONFIG_ENTRIES: LazyLock<HashMap<String, ConfigEntry>> = LazyLock::new(|| {
    let entries = vec![
        ConfigEntry::new(BALLISTA_JOB_NAME.to_string(),
                         "Sets the job name that will appear in the web user interface for any submitted jobs".to_string(),
                         DataType::Utf8, None),
        ConfigEntry::new(BALLISTA_STANDALONE_PARALLELISM.to_string(),
                        "Standalone processing parallelism ".to_string(),
                        DataType::UInt16, Some(std::thread::available_parallelism().map(|v| v.get()).unwrap_or(1).to_string())),
        ConfigEntry::new(BALLISTA_GRPC_CLIENT_MAX_MESSAGE_SIZE.to_string(),
                         "Configuration for max message size in gRPC clients".to_string(),
                         DataType::UInt64,
                         Some((16 * 1024 * 1024).to_string())),
        ConfigEntry::new(BALLISTA_PLANNER_DML_EXTENSION.to_string(),
                         "Enable ballista planner DML extension".to_string(),
                         DataType::Boolean,
                         Some((true).to_string())),
    ];
    entries
        .into_iter()
        .map(|e| (e.name.clone(), e))
        .collect::<HashMap<_, _>>()
});

/// Configuration option meta-data
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    name: String,
    description: String,
    data_type: DataType,
    default_value: Option<String>,
}

impl ConfigEntry {
    fn new(
        name: String,
        description: String,
        data_type: DataType,
        default_value: Option<String>,
    ) -> Self {
        Self {
            name,
            description,
            data_type,
            default_value,
        }
    }
}

/// Ballista configuration
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BallistaConfig {
    /// Settings stored in map for easy serde
    settings: HashMap<String, String>,
}

impl Default for BallistaConfig {
    fn default() -> Self {
        Self::with_settings(HashMap::new()).unwrap()
    }
}

impl BallistaConfig {
    /// Create a new configuration based on key-value pairs
    fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        let supported_entries = BallistaConfig::valid_entries();
        for (name, entry) in supported_entries {
            if let Some(v) = settings.get(name) {
                // validate that we can parse the user-supplied value
                Self::parse_value(v.as_str(), entry.data_type.clone()).map_err(|e| BallistaError::General(format!("Failed to parse user-supplied value '{name}' for configuration setting '{v}': {e}")))?;
            } else if let Some(v) = entry.default_value.clone() {
                Self::parse_value(v.as_str(), entry.data_type.clone()).map_err(|e| BallistaError::General(format!("Failed to parse default value '{name}' for configuration setting '{v}': {e}")))?;
            } else if entry.default_value.is_none() {
                // optional config
            } else {
                return Err(BallistaError::General(format!(
                    "No value specified for mandatory configuration setting '{name}'"
                )));
            }
        }

        Ok(Self { settings })
    }

    pub fn parse_value(val: &str, data_type: DataType) -> ParseResult<()> {
        match data_type {
            DataType::UInt16 => {
                val.to_string()
                    .parse::<usize>()
                    .map_err(|e| format!("{e:?}"))?;
            }
            DataType::UInt32 => {
                val.to_string()
                    .parse::<usize>()
                    .map_err(|e| format!("{e:?}"))?;
            }
            DataType::UInt64 => {
                val.to_string()
                    .parse::<usize>()
                    .map_err(|e| format!("{e:?}"))?;
            }
            DataType::Boolean => {
                val.to_string()
                    .parse::<bool>()
                    .map_err(|e| format!("{e:?}"))?;
            }
            DataType::Utf8 => {
                val.to_string();
            }
            _ => {
                return Err(format!("not support data type: {data_type}"));
            }
        }

        Ok(())
    }

    // All available configuration options
    pub fn valid_entries() -> &'static HashMap<String, ConfigEntry> {
        &CONFIG_ENTRIES
    }

    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    pub fn default_grpc_client_max_message_size(&self) -> usize {
        self.get_usize_setting(BALLISTA_GRPC_CLIENT_MAX_MESSAGE_SIZE)
    }

    pub fn default_standalone_parallelism(&self) -> usize {
        self.get_usize_setting(BALLISTA_STANDALONE_PARALLELISM)
    }

    pub fn planner_dml_extension(&self) -> bool {
        self.get_bool_setting(BALLISTA_PLANNER_DML_EXTENSION)
    }

    fn get_usize_setting(&self, key: &str) -> usize {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.parse().unwrap()
        } else {
            let entries = Self::valid_entries();
            // infallible because we validate all configs in the constructor
            let v = entries.get(key).unwrap().default_value.as_ref().unwrap();
            v.parse().unwrap()
        }
    }

    #[allow(dead_code)]
    fn get_bool_setting(&self, key: &str) -> bool {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.parse::<bool>().unwrap()
        } else {
            let entries = Self::valid_entries();
            // infallible because we validate all configs in the constructor
            let v = entries.get(key).unwrap().default_value.as_ref().unwrap();
            v.parse::<bool>().unwrap()
        }
    }
    #[allow(dead_code)]
    fn get_string_setting(&self, key: &str) -> String {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.to_string()
        } else {
            let entries = Self::valid_entries();
            // infallible because we validate all configs in the constructor
            let v = entries.get(key).unwrap().default_value.as_ref().unwrap();
            v.to_string()
        }
    }
}

impl datafusion::config::ExtensionOptions for BallistaConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn datafusion::config::ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        let entries = Self::valid_entries();
        let k = format!("{}.{key}", BallistaConfig::PREFIX);

        if entries.contains_key(&k) {
            self.settings.insert(k, value.to_string());
            Ok(())
        } else {
            config_err!("configuration key `{}` does not exist", key)
        }
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        Self::valid_entries()
            .iter()
            .map(|(key, value)| datafusion::config::ConfigEntry {
                key: key.clone(),
                value: self
                    .settings
                    .get(key)
                    .cloned()
                    .or(value.default_value.clone()),
                description: &value.description,
            })
            .collect()
    }
}

impl datafusion::config::ConfigExtension for BallistaConfig {
    const PREFIX: &'static str = "ballista";
}

// an enum used to configure the scheduler policy
// needs to be visible to code generated by configure_me
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum TaskSchedulingPolicy {
    #[default]
    PullStaged,
    PushStaged,
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for TaskSchedulingPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}
#[cfg(feature = "build-binary")]
impl configure_me::parse_arg::ParseArgFromStr for TaskSchedulingPolicy {
    fn describe_type<W: core::fmt::Write>(mut writer: W) -> core::fmt::Result {
        write!(writer, "The scheduler policy for the scheduler")
    }
}

// an enum used to configure the log rolling policy
// needs to be visible to code generated by configure_me
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum LogRotationPolicy {
    Minutely,
    Hourly,
    Daily,
    #[default]
    Never,
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for LogRotationPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

#[cfg(feature = "build-binary")]
impl configure_me::parse_arg::ParseArgFromStr for LogRotationPolicy {
    fn describe_type<W: core::fmt::Write>(mut writer: W) -> core::fmt::Result {
        write!(writer, "The log rotation policy")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() -> Result<()> {
        let config = BallistaConfig::default();
        assert_eq!(16777216, config.default_grpc_client_max_message_size());
        Ok(())
    }
}
