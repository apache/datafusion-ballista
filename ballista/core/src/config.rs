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

use std::result;
use std::{collections::HashMap, fmt::Display};

use crate::error::{BallistaError, Result};

use datafusion::{
    arrow::datatypes::DataType, common::config_err, config::ConfigExtension,
};

/// Configuration key for setting the job name displayed in the web UI.
pub const BALLISTA_JOB_NAME: &str = "ballista.job.name";
/// Configuration key for standalone processing parallelism.
pub const BALLISTA_STANDALONE_PARALLELISM: &str = "ballista.standalone.parallelism";
/// max message size for gRPC clients
pub const BALLISTA_GRPC_CLIENT_MAX_MESSAGE_SIZE: &str =
    "ballista.grpc_client_max_message_size";
/// Configuration key for maximum concurrent shuffle read requests.
pub const BALLISTA_SHUFFLE_READER_MAX_REQUESTS: &str =
    "ballista.shuffle.max_concurrent_read_requests";
/// Configuration key to force remote reads even for local partitions.
pub const BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ: &str =
    "ballista.shuffle.force_remote_read";
/// Configuration key to prefer Flight protocol for remote shuffle reads.
pub const BALLISTA_SHUFFLE_READER_REMOTE_PREFER_FLIGHT: &str =
    "ballista.shuffle.remote_read_prefer_flight";

/// Configuration key for gRPC client connection timeout in seconds.
pub const BALLISTA_GRPC_CLIENT_CONNECT_TIMEOUT_SECONDS: &str =
    "ballista.grpc.client.connect_timeout_seconds";
/// Configuration key for gRPC client request timeout in seconds.
pub const BALLISTA_GRPC_CLIENT_TIMEOUT_SECONDS: &str =
    "ballista.grpc.client.timeout_seconds";
/// Configuration key for TCP keep-alive interval for gRPC clients in seconds.
pub const BALLISTA_GRPC_CLIENT_TCP_KEEPALIVE_SECONDS: &str =
    "ballista.grpc.client.tcp_keepalive_seconds";
/// Configuration key for HTTP/2 keep-alive interval for gRPC clients in seconds.
pub const BALLISTA_GRPC_CLIENT_HTTP2_KEEPALIVE_INTERVAL_SECONDS: &str =
    "ballista.grpc.client.http2_keepalive_interval_seconds";

/// Configuration key for enabling sort-based shuffle.
pub const BALLISTA_SHUFFLE_SORT_BASED_ENABLED: &str =
    "ballista.shuffle.sort_based.enabled";
/// Configuration key for sort shuffle per-partition buffer size in bytes.
pub const BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE: &str =
    "ballista.shuffle.sort_based.buffer_size";
/// Configuration key for sort shuffle total memory limit in bytes.
pub const BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT: &str =
    "ballista.shuffle.sort_based.memory_limit";
/// Configuration key for sort shuffle spill threshold (0.0-1.0).
pub const BALLISTA_SHUFFLE_SORT_BASED_SPILL_THRESHOLD: &str =
    "ballista.shuffle.sort_based.spill_threshold";

/// Result type for configuration parsing operations.
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
        ConfigEntry::new(BALLISTA_SHUFFLE_READER_MAX_REQUESTS.to_string(),
                         "Maximum concurrent requests shuffle reader can process".to_string(),
                         DataType::UInt64,
                         Some((64).to_string())),
        ConfigEntry::new(BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ.to_string(),
                         "Forces the shuffle reader to always read partitions via the Arrow Flight client, even when partitions are local to the node.".to_string(),
                         DataType::Boolean,
                         Some((false).to_string())),
        ConfigEntry::new(BALLISTA_SHUFFLE_READER_REMOTE_PREFER_FLIGHT.to_string(),
                         "Forces the shuffle reader to use flight reader instead of block reader for remote read. Block reader usually has better performance and resource utilization".to_string(),
                         DataType::Boolean,
                         Some((false).to_string())),
        ConfigEntry::new(BALLISTA_GRPC_CLIENT_CONNECT_TIMEOUT_SECONDS.to_string(),
                         "Connection timeout for gRPC client in seconds".to_string(),
                         DataType::UInt64,
                         Some((20).to_string())),
        ConfigEntry::new(BALLISTA_GRPC_CLIENT_TIMEOUT_SECONDS.to_string(),
                         "Request timeout for gRPC client in seconds".to_string(),
                         DataType::UInt64,
                         Some((20).to_string())),
        ConfigEntry::new(BALLISTA_GRPC_CLIENT_TCP_KEEPALIVE_SECONDS.to_string(),
                         "TCP keep-alive interval for gRPC client in seconds".to_string(),
                         DataType::UInt64,
                         Some((3600).to_string())),
        ConfigEntry::new(BALLISTA_GRPC_CLIENT_HTTP2_KEEPALIVE_INTERVAL_SECONDS.to_string(),
                         "HTTP/2 keep-alive interval for gRPC client in seconds".to_string(),
                         DataType::UInt64,
                         Some((300).to_string())),
        ConfigEntry::new(BALLISTA_SHUFFLE_SORT_BASED_ENABLED.to_string(),
                         "Enable sort-based shuffle which writes consolidated files with index".to_string(),
                         DataType::Boolean,
                         Some((false).to_string())),
        ConfigEntry::new(BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE.to_string(),
                         "Per-partition buffer size in bytes for sort shuffle".to_string(),
                         DataType::UInt64,
                         Some((1024 * 1024).to_string())),
        ConfigEntry::new(BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT.to_string(),
                         "Total memory limit in bytes for sort shuffle buffers".to_string(),
                         DataType::UInt64,
                         Some((256 * 1024 * 1024).to_string())),
        ConfigEntry::new(BALLISTA_SHUFFLE_SORT_BASED_SPILL_THRESHOLD.to_string(),
                         "Spill threshold as decimal fraction (0.0-1.0) of memory limit".to_string(),
                         DataType::Utf8,
                         Some("0.8".to_string()))
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

    /// Validates that a string value can be parsed as the specified data type.
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

    /// Returns all available configuration entries with their metadata.
    pub fn valid_entries() -> &'static HashMap<String, ConfigEntry> {
        &CONFIG_ENTRIES
    }

    /// Returns the current configuration settings as a map.
    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    /// Returns the maximum message size for gRPC clients in bytes.
    pub fn default_grpc_client_max_message_size(&self) -> usize {
        self.get_usize_setting(BALLISTA_GRPC_CLIENT_MAX_MESSAGE_SIZE)
    }

    /// Returns the standalone processing parallelism level.
    pub fn default_standalone_parallelism(&self) -> usize {
        self.get_usize_setting(BALLISTA_STANDALONE_PARALLELISM)
    }

    /// Returns the maximum number of concurrent shuffle reader requests.
    pub fn shuffle_reader_maximum_concurrent_requests(&self) -> usize {
        self.get_usize_setting(BALLISTA_SHUFFLE_READER_MAX_REQUESTS)
    }

    /// Returns the gRPC client connection timeout in seconds.
    pub fn default_grpc_client_connect_timeout_seconds(&self) -> usize {
        self.get_usize_setting(BALLISTA_GRPC_CLIENT_CONNECT_TIMEOUT_SECONDS)
    }

    /// Returns the gRPC client request timeout in seconds.
    pub fn default_grpc_client_timeout_seconds(&self) -> usize {
        self.get_usize_setting(BALLISTA_GRPC_CLIENT_TIMEOUT_SECONDS)
    }

    /// Returns the TCP keep-alive interval for gRPC clients in seconds.
    pub fn default_grpc_client_tcp_keepalive_seconds(&self) -> usize {
        self.get_usize_setting(BALLISTA_GRPC_CLIENT_TCP_KEEPALIVE_SECONDS)
    }

    /// Returns the HTTP/2 keep-alive interval for gRPC clients in seconds.
    pub fn default_grpc_client_http2_keepalive_interval_seconds(&self) -> usize {
        self.get_usize_setting(BALLISTA_GRPC_CLIENT_HTTP2_KEEPALIVE_INTERVAL_SECONDS)
    }

    /// Forces the shuffle reader to always read partitions via the Arrow Flight client,
    /// even when partitions are local to the node.
    ///
    /// Enabling forced remote read may significantly reduce performance,
    /// as all partition reads will go through the Arrow Flight client even for local data.
    /// Use only when necessary, like in tests
    pub fn shuffle_reader_force_remote_read(&self) -> bool {
        self.get_bool_setting(BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ)
    }
    /// Forces the shuffle reader to prefer flight protocol over block protocol
    /// to read remote shuffle partition.
    ///
    /// Block protocol is usually more performant than flight protocol
    pub fn shuffle_reader_remote_prefer_flight(&self) -> bool {
        self.get_bool_setting(BALLISTA_SHUFFLE_READER_REMOTE_PREFER_FLIGHT)
    }

    /// Returns whether sort-based shuffle is enabled.
    ///
    /// When enabled, shuffle writes produce a single consolidated file per input
    /// partition with an index file, rather than one file per output partition.
    pub fn shuffle_sort_based_enabled(&self) -> bool {
        self.get_bool_setting(BALLISTA_SHUFFLE_SORT_BASED_ENABLED)
    }

    /// Returns the per-partition buffer size for sort-based shuffle in bytes.
    pub fn shuffle_sort_based_buffer_size(&self) -> usize {
        self.get_usize_setting(BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE)
    }

    /// Returns the total memory limit for sort-based shuffle buffers in bytes.
    pub fn shuffle_sort_based_memory_limit(&self) -> usize {
        self.get_usize_setting(BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT)
    }

    /// Returns the spill threshold for sort-based shuffle (0.0-1.0).
    pub fn shuffle_sort_based_spill_threshold(&self) -> f64 {
        self.get_f64_setting(BALLISTA_SHUFFLE_SORT_BASED_SPILL_THRESHOLD)
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

    fn get_f64_setting(&self, key: &str) -> f64 {
        if let Some(v) = self.settings.get(key) {
            v.parse::<f64>().unwrap()
        } else {
            let entries = Self::valid_entries();
            let v = entries.get(key).unwrap().default_value.as_ref().unwrap();
            v.parse::<f64>().unwrap()
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

/// Ballista supports both push-based and pull-based task scheduling.
/// It is recommended that you try both to determine which is the best for your use case.
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum TaskSchedulingPolicy {
    /// Pull-based scheduling works in a similar way to Apache Spark
    #[default]
    PullStaged,
    /// push-based scheduling can result in lower latency.
    PushStaged,
}
impl Display for TaskSchedulingPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskSchedulingPolicy::PullStaged => f.write_str("pull-staged"),
            TaskSchedulingPolicy::PushStaged => f.write_str("push-staged"),
        }
    }
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for TaskSchedulingPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

/// Configures the log file rotation policy.
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum LogRotationPolicy {
    /// Rotate log files every minute.
    Minutely,
    /// Rotate log files every hour.
    Hourly,
    /// Rotate log files daily.
    Daily,
    /// Never rotate log files.
    #[default]
    Never,
}

impl Display for LogRotationPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogRotationPolicy::Minutely => f.write_str("minutely"),
            LogRotationPolicy::Hourly => f.write_str("hourly"),
            LogRotationPolicy::Daily => f.write_str("daily"),
            LogRotationPolicy::Never => f.write_str("never"),
        }
    }
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for LogRotationPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
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
