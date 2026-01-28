use color_eyre::eyre::Result;
use std::path::Path;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initializes a tracing subscriber that logs to a file.
///
/// # Arguments
///
/// * `log_dir` - Directory where the log file will be created
/// * `log_file_prefix` - Prefix for the log file name (e.g., "ballista-tui")
/// * `default_level` - Default log level (e.g., "info", "debug", "trace")
///
/// # Returns
///
/// Returns `Ok(())` if initialization succeeds, or an error if it fails.
///
/// # Example
///
/// ```no_run
/// use ballista_tui::log::init_file_logger;
///
/// init_file_logger("./logs", "ballista-tui", "info").unwrap();
/// ```
pub fn init_file_logger(
    log_dir: impl AsRef<Path>,
    log_file_prefix: &str,
    default_level: &str,
) -> Result<()> {
    let file_appender = tracing_appender::rolling::daily(log_dir, log_file_prefix);
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_level))?;

    // let subscriber = tracing_subscriber::fmt().with_writer(non_blocking);

    // tracing::subscriber::with_default(subscriber.finish(), || {
    //     tracing::event!(tracing::Level::INFO, "Hello");
    // });
    tracing_subscriber::registry()
        // .with(env_filter)
        .with(fmt::layer().with_writer(non_blocking).with_ansi(false))
        .init();

    Ok(())
}
