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

//! Standalone Ballista history server binary: loads completed event logs from
//! a directory and serves the same `/api/*` responses the live scheduler does,
//! so the existing TUI can connect to it unchanged.

use ballista_core::error::{BallistaError, Result};
use ballista_scheduler::history::{HistoryStore, history_router};
use clap::Parser;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Debug, clap::Parser)]
#[command(
    name = "ballista-history-server",
    version,
    about = "Ballista history server"
)]
struct Args {
    /// Directory containing per-job event logs.
    #[arg(long)]
    event_log_dir: PathBuf,
    /// Host to bind the HTTP server to.
    #[arg(long, default_value = "0.0.0.0")]
    bind_host: String,
    /// Port to bind the HTTP server to.
    #[arg(long, default_value_t = 50060)]
    bind_port: u16,
}

fn main() -> Result<()> {
    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or_else(|_| "info".to_string()));
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(std::io::stdout)
        .with_env_filter(log_filter)
        .init();

    let args = Args::parse();

    // `HistoryStore::load` walks the log directory with blocking file I/O, and
    // how long it takes scales with the number of stored jobs. Run it here,
    // before the runtime exists, rather than parking a runtime worker on it.
    let store = Arc::new(HistoryStore::load(&args.event_log_dir)?);
    tracing::info!(
        "Indexed {} completed job(s) from {}",
        store.len(),
        args.event_log_dir.display()
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(BallistaError::IoError)?;

    runtime.block_on(serve(args, store))
}

async fn serve(args: Args, store: Arc<HistoryStore>) -> Result<()> {
    let app = history_router(store);

    let addr: SocketAddr = format!("{}:{}", args.bind_host, args.bind_port)
        .parse()
        .map_err(|e: std::net::AddrParseError| {
            BallistaError::Configuration(e.to_string())
        })?;

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(BallistaError::IoError)?;
    tracing::info!("History server listening on http://{addr}");

    axum::serve(listener, app.into_make_service())
        .await
        .map_err(BallistaError::IoError)?;

    Ok(())
}
