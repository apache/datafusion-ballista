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

use color_eyre::eyre::Result;
use tracing_subscriber::EnvFilter;

/// Initializes a tracing subscriber that logs to a file.
///
/// # Arguments
///
/// * `log_file_prefix` - Prefix for the log file name (e.g., "ballista-tui")
/// * `default_level` - Default log level (e.g., "info", "debug", "trace")
///
/// # Returns
///
/// Returns `Ok(())` if initialization succeeds, or an error if it fails.
pub fn init_file_logger(log_file_prefix: &str, default_level: &str) -> Result<()> {
    let dir_name = "logs";
    std::fs::create_dir_all(dir_name)?;

    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_level))?;

    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(format!("{dir_name}/{log_file_prefix}.log"))?;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(file)
        .with_ansi(true)
        .try_init();

    Ok(())
}
