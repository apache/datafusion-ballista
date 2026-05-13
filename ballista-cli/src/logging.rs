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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tracing_subscriber::EnvFilter;

#[cfg(feature = "tui")]
mod tui {
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tracing_subscriber::fmt::MakeWriter;

    /// A logger that writes to stdout or a file depending on the current TUI mode.
    #[derive(Clone, Debug)]
    pub(super) struct DynamicLogger {
        file: Arc<File>,
        tui_mode: Arc<AtomicBool>,
    }

    impl DynamicLogger {
        pub fn try_new(
            file_name: String,
            tui_mode: Arc<AtomicBool>,
        ) -> io::Result<DynamicLogger> {
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(&file_name)
                .map(|file| Self {
                    file: Arc::new(file),
                    tui_mode,
                })
        }
    }

    impl<'a> MakeWriter<'a> for DynamicLogger {
        type Writer = Self;
        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }
    impl io::Write for DynamicLogger {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.tui_mode.load(Ordering::Acquire) {
                self.file.write_all(buf)?
            } else {
                io::stdout().write_all(buf)?
            }

            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.tui_mode.load(Ordering::Acquire) {
                self.file.flush()
            } else {
                io::stdout().flush()
            }
        }
    }
}

/// Initializes a tracing subscriber that logs to stdout or a file depending
/// on whether the application is used as a CLI or TUI.
///
/// # Arguments
///
/// * `log_file_prefix` - Prefix for the log file name (e.g., "ballista-tui")
/// * `tui_mode` - A flag indicating whether the application is used as a CLI or TUI
///
/// # Returns
///
/// Returns `Ok(())` if file setup succeeds. Subscriber init failures are logged
/// to stderr but do not cause an error return (a global subscriber may already exist).
pub fn init_logging(
    #[allow(unused_variables)] tui_mode: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let env_filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?;

    #[cfg(feature = "tui")]
    let writer = {
        let dir_name = "logs";
        std::fs::create_dir_all(dir_name)?;

        let file_name = format!("{dir_name}/ballista-tui.log");
        tui::DynamicLogger::try_new(file_name, tui_mode)?
    };
    #[cfg(not(feature = "tui"))]
    let writer = std::io::stdout;

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(writer)
        .with_ansi(true)
        .init();

    Ok(())
}
