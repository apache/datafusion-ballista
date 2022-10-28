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

use clap::ArgEnum;
use core::fmt;
use std::collections::HashMap;
use std::result;

use crate::error::{BallistaError, Result};

use datafusion::arrow::datatypes::DataType;

pub const BALLISTA_JOB_NAME: &str = "ballista.job.name";
pub const BALLISTA_DEFAULT_SHUFFLE_PARTITIONS: &str = "ballista.shuffle.partitions";
pub const BALLISTA_DEFAULT_BATCH_SIZE: &str = "ballista.batch.size";
pub const BALLISTA_REPARTITION_JOINS: &str = "ballista.repartition.joins";
pub const BALLISTA_REPARTITION_AGGREGATIONS: &str = "ballista.repartition.aggregations";
pub const BALLISTA_REPARTITION_WINDOWS: &str = "ballista.repartition.windows";
pub const BALLISTA_PARQUET_PRUNING: &str = "ballista.parquet.pruning";
pub const BALLISTA_WITH_INFORMATION_SCHEMA: &str = "ballista.with_information_schema";
/// give a plugin files dir, and then the dynamic library files in this dir will be load when scheduler state init.
pub const BALLISTA_PLUGIN_DIR: &str = "ballista.plugin_dir";

pub type ParseResult<T> = result::Result<T, String>;

/// Configuration option meta-data
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    name: String,
    _description: String,
    data_type: DataType,
    default_value: Option<String>,
}

impl PartialEq<Self> for ConfigEntry {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
            && self.data_type.eq(&other.data_type)
            && self.default_value.eq(&other.default_value)
    }
}

impl Eq for ConfigEntry {}

impl ConfigEntry {
    fn new(
        name: String,
        _description: String,
        _data_type: DataType,
        default_value: Option<String>,
    ) -> Self {
        Self {
            name,
            _description,
            data_type: _data_type,
            default_value,
        }
    }
}

/// Configuration with values in a valid String format
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidConfiguration {
    settings: HashMap<String, String>,
    valid_entries: HashMap<String, ConfigEntry>,
}

impl ValidConfiguration {
    // When constructing a ValidConfiguration, necessary validation check will be done
    fn new(
        settings: HashMap<String, String>,
        valid_entries: Vec<ConfigEntry>,
    ) -> Result<Self> {
        let valid_entries = valid_entries
            .into_iter()
            .map(|e| (e.name.clone(), e))
            .collect::<HashMap<_, _>>();

        // Firstly, check whether the entries in settings are valid or not
        for (name, _) in settings.iter() {
            if valid_entries.get(name).is_none() {
                return Err(BallistaError::General(format!(
                    "The configuration setting '{}' is not valid",
                    name
                )));
            }
        }

        // Secondly, check each entry in the valid_entries:
        // if its value is specified in settings, then check whether it's valid to be parsed to the related data type
        // else check its default value:
        //    if its default value exists, then check whether it's valid to be parsed to the related data type
        //    else it's a mandatory entry which should be set in settings and should return Error;
        for (name, entry) in valid_entries.iter() {
            if let Some(v) = settings.get(&entry.name) {
                // validate that we can parse the user-supplied value
                Self::parse_value(v.as_str(), entry.data_type.clone()).map_err(|e| BallistaError::General(format!("Failed to parse user-supplied value '{}' for configuration setting '{}': {}", name, v, e)))?;
            } else if let Some(v) = entry.default_value.clone() {
                Self::parse_value(v.as_str(), entry.data_type.clone()).map_err(|e| BallistaError::General(format!("Failed to parse default value '{}' for configuration setting '{}': {}", name, v, e)))?;
            } else {
                return Err(BallistaError::General(format!(
                    "No value specified for mandatory configuration setting '{}'",
                    name
                )));
            }
        }

        Ok(Self {
            settings,
            valid_entries,
        })
    }

    pub fn get_usize_setting(&self, key: &str) -> usize {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.parse().unwrap()
        } else {
            // infallible because we validate all configs in the constructor
            let v = self
                .valid_entries
                .get(key)
                .unwrap()
                .default_value
                .as_ref()
                .unwrap();
            v.parse().unwrap()
        }
    }

    pub fn get_bool_setting(&self, key: &str) -> bool {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.parse::<bool>().unwrap()
        } else {
            // infallible because we validate all configs in the constructor
            let v = self
                .valid_entries
                .get(key)
                .unwrap()
                .default_value
                .as_ref()
                .unwrap();
            v.parse::<bool>().unwrap()
        }
    }

    pub fn get_string_setting(&self, key: &str) -> String {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.to_string()
        } else {
            // infallible because we validate all configs in the constructor
            let v = self
                .valid_entries
                .get(key)
                .unwrap()
                .default_value
                .as_ref()
                .unwrap();
            v.to_string()
        }
    }

    /// Error when the value is not able to parsed to the data type
    fn parse_value(val: &str, data_type: DataType) -> ParseResult<()> {
        match data_type {
            DataType::UInt16 => {
                val.to_string()
                    .parse::<usize>()
                    .map_err(|e| format!("{:?}", e))?;
            }
            DataType::Boolean => {
                val.to_string()
                    .parse::<bool>()
                    .map_err(|e| format!("{:?}", e))?;
            }
            DataType::Utf8 => {
                val.to_string();
            }
            _ => {
                return Err(format!("not support data type: {}", data_type));
            }
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct ValidConfigurationBuilder {
    settings: HashMap<String, String>,
}

impl ValidConfigurationBuilder {
    /// Create a new config with an additional setting
    pub fn set(&self, k: &str, v: &str) -> Self {
        let mut settings = self.settings.clone();
        settings.insert(k.to_owned(), v.to_owned());
        Self { settings }
    }

    pub fn build(&self, valid_entries: Vec<ConfigEntry>) -> Result<ValidConfiguration> {
        ValidConfiguration::new(self.settings.clone(), valid_entries)
    }
}

/// Ballista configuration builder
#[derive(Default)]
pub struct BallistaConfigBuilder {
    valid_config_builder: ValidConfigurationBuilder,
}

impl BallistaConfigBuilder {
    /// Create a new configuration based on key-value pairs
    pub fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            valid_config_builder: ValidConfigurationBuilder { settings },
        })
    }

    /// Create a new config with an additional setting
    pub fn set(&self, k: &str, v: &str) -> Self {
        Self {
            valid_config_builder: self.valid_config_builder.set(k, v),
        }
    }

    pub fn build(&self) -> Result<BallistaConfig> {
        self.valid_config_builder
            .build(Self::valid_entries())
            .map(|valid_config| BallistaConfig { valid_config })
    }

    /// All available configuration options
    pub fn valid_entries() -> Vec<ConfigEntry> {
        vec![
            ConfigEntry::new(BALLISTA_JOB_NAME.to_string(),
                             "Sets the job name that will appear in the web user interface for any submitted jobs".to_string(),
                             DataType::Utf8, Some("BallistaJob".to_string())),
            ConfigEntry::new(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS.to_string(),
                             "Sets the default number of partitions to create when repartitioning query stages".to_string(),
                             DataType::UInt16, Some("16".to_string())),
            ConfigEntry::new(BALLISTA_DEFAULT_BATCH_SIZE.to_string(),
                             "Sets the default batch size".to_string(),
                             DataType::UInt16, Some("8192".to_string())),
            ConfigEntry::new(BALLISTA_REPARTITION_JOINS.to_string(),
                             "Configuration for repartition joins".to_string(),
                             DataType::Boolean, Some("true".to_string())),
            ConfigEntry::new(BALLISTA_REPARTITION_AGGREGATIONS.to_string(),
                             "Configuration for repartition aggregations".to_string(),
                             DataType::Boolean, Some("true".to_string())),
            ConfigEntry::new(BALLISTA_REPARTITION_WINDOWS.to_string(),
                             "Configuration for repartition windows".to_string(),
                             DataType::Boolean, Some("true".to_string())),
            ConfigEntry::new(BALLISTA_PARQUET_PRUNING.to_string(),
                             "Configuration for parquet prune".to_string(),
                             DataType::Boolean, Some("true".to_string())),
            ConfigEntry::new(BALLISTA_WITH_INFORMATION_SCHEMA.to_string(),
                             "Sets whether enable information_schema".to_string(),
                             DataType::Boolean, Some("false".to_string())),
            ConfigEntry::new(BALLISTA_PLUGIN_DIR.to_string(),
                             "Sets the plugin dir".to_string(),
                             DataType::Utf8, Some("".to_string())),
        ]
    }
}

/// Ballista configuration
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BallistaConfig {
    /// Settings stored in map for easy serde
    valid_config: ValidConfiguration,
}

impl BallistaConfig {
    /// Create a configuration builder
    pub fn builder() -> BallistaConfigBuilder {
        BallistaConfigBuilder::default()
    }

    /// Create a default configuration
    pub fn new() -> Result<Self> {
        Self::with_settings(HashMap::new())
    }

    /// Create a new configuration based on key-value pairs
    pub fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        BallistaConfigBuilder::with_settings(settings).and_then(|builder| builder.build())
    }

    pub fn settings(&self) -> &HashMap<String, String> {
        &self.valid_config.settings
    }

    pub fn default_shuffle_partitions(&self) -> usize {
        self.valid_config
            .get_usize_setting(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS)
    }

    pub fn default_plugin_dir(&self) -> String {
        self.valid_config.get_string_setting(BALLISTA_PLUGIN_DIR)
    }

    pub fn default_batch_size(&self) -> usize {
        self.valid_config
            .get_usize_setting(BALLISTA_DEFAULT_BATCH_SIZE)
    }

    pub fn repartition_joins(&self) -> bool {
        self.valid_config
            .get_bool_setting(BALLISTA_REPARTITION_JOINS)
    }

    pub fn repartition_aggregations(&self) -> bool {
        self.valid_config
            .get_bool_setting(BALLISTA_REPARTITION_AGGREGATIONS)
    }

    pub fn repartition_windows(&self) -> bool {
        self.valid_config
            .get_bool_setting(BALLISTA_REPARTITION_WINDOWS)
    }

    pub fn parquet_pruning(&self) -> bool {
        self.valid_config.get_bool_setting(BALLISTA_PARQUET_PRUNING)
    }

    pub fn default_with_information_schema(&self) -> bool {
        self.valid_config
            .get_bool_setting(BALLISTA_WITH_INFORMATION_SCHEMA)
    }
}

// an enum used to configure the scheduler policy
// needs to be visible to code generated by configure_me
#[derive(Clone, ArgEnum, Copy, Debug, serde::Deserialize)]
pub enum TaskSchedulingPolicy {
    PullStaged,
    PushStaged,
}

impl std::str::FromStr for TaskSchedulingPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ArgEnum::from_str(s, true)
    }
}

impl parse_arg::ParseArgFromStr for TaskSchedulingPolicy {
    fn describe_type<W: fmt::Write>(mut writer: W) -> fmt::Result {
        write!(writer, "The scheduler policy for the scheduler")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() -> Result<()> {
        let config = BallistaConfig::new()?;
        assert_eq!(16, config.default_shuffle_partitions());
        assert!(!config.default_with_information_schema());
        assert_eq!("", config.default_plugin_dir().as_str());
        Ok(())
    }

    #[test]
    fn custom_config() -> Result<()> {
        let config = BallistaConfig::builder()
            .set(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS, "123")
            .set(BALLISTA_WITH_INFORMATION_SCHEMA, "true")
            .build()?;
        assert_eq!(123, config.default_shuffle_partitions());
        assert!(config.default_with_information_schema());
        Ok(())
    }

    #[test]
    fn custom_config_invalid() -> Result<()> {
        let config = BallistaConfig::builder()
            .set(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS, "true")
            .set(BALLISTA_PLUGIN_DIR, "test_dir")
            .build();
        assert!(config.is_err());
        assert_eq!("General(\"Failed to parse user-supplied value 'ballista.shuffle.partitions' for configuration setting 'true': ParseIntError { kind: InvalidDigit }\")", format!("{:?}", config.unwrap_err()));

        let config = BallistaConfig::builder()
            .set(BALLISTA_WITH_INFORMATION_SCHEMA, "123")
            .build();
        assert!(config.is_err());
        assert_eq!("General(\"Failed to parse user-supplied value 'ballista.with_information_schema' for configuration setting '123': ParseBoolError\")", format!("{:?}", config.unwrap_err()));
        Ok(())
    }
}
