use std::env;

use config::{Config, ConfigError, Environment, File};
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

impl Settings {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let config_dir = dirs::config_dir().unwrap().join("ballista-tui");

        let s = Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::with_name("etc/config/default"))
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
