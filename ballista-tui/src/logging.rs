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

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(file)
        .with_ansi(true)
        .init();

    Ok(())
}
