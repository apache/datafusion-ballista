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

pub mod query;

use crate::error::{BallistaError, Result};
use clap::ArgEnum;
use core::fmt;
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;
use std::result;

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
    pub fn new(
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
            DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
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
    pub fn with_settings(settings: HashMap<String, String>) -> Self {
        Self { settings }
    }

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
