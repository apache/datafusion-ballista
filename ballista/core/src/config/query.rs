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

//! Ballista query configuration

use crate::config::{ConfigEntry, ValidConfiguration, ValidConfigurationBuilder};
use crate::error::Result;
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;

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

/// Ballista configuration, mainly for the query
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

/// Ballista configuration builder
#[derive(Default)]
pub struct BallistaConfigBuilder {
    valid_config_builder: ValidConfigurationBuilder,
}

impl BallistaConfigBuilder {
    /// Create a new configuration based on key-value pairs
    pub fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            valid_config_builder: ValidConfigurationBuilder::with_settings(settings),
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
