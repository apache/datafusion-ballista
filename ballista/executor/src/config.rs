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

//! Command-line configuration parsing for the Ballista executor binary.
//!
//! This module provides the `Config` struct which captures all command-line
//! arguments for configuring an executor instance.

//
// configure me does not follow clippy spec
// we need to replace it with some other library
// as mentioned in https://github.com/apache/datafusion-ballista/issues/1271
//
#![allow(clippy::uninlined_format_args, clippy::unused_unit)]
use ballista_core::error::BallistaError;

use crate::executor_process::ExecutorProcessConfig;
use crate::metrics::ExecutorMetricCollectionPolicy;

/// Parse a human-readable size string into a byte count.
///
/// Accepts decimal SI suffixes (`KB`, `MB`, `GB`) where 1KB = 1000 bytes,
/// binary IEC suffixes (`KiB`, `MiB`, `GiB`) where 1KiB = 1024 bytes, and
/// plain integers (interpreted as bytes).
fn parse_memory_pool_size(s: &str) -> Result<u64, String> {
    s.parse::<bytesize::ByteSize>()
        .map(|b| b.as_u64())
        .map_err(|e| format!("invalid byte size '{s}': {e}"))
}

/// Parse and validate the `--memory-pool-fraction` value, which must be in
/// the range `(0.0, 1.0]`.
fn parse_memory_pool_fraction(s: &str) -> Result<f64, String> {
    let f = s
        .parse::<f64>()
        .map_err(|e| format!("invalid fraction '{s}': {e}"))?;
    if f > 0.0 && f <= 1.0 {
        Ok(f)
    } else {
        Err(format!(
            "memory pool fraction must be in (0.0, 1.0], got {f}"
        ))
    }
}

/// Command-line arguments for configuring a Ballista executor.
///
/// This struct is parsed from command-line arguments using clap and contains
/// all the configuration options needed to start an executor process.
#[cfg(feature = "build-binary")]
#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// Hostname or IP address of the scheduler to connect to.
    #[arg(long, default_value_t = String::from("localhost"), help = "Scheduler host.")]
    pub scheduler_host: String,
    /// Port number of the scheduler's gRPC service.
    #[arg(long, default_value_t = 50050, help = "Scheduler port.")]
    pub scheduler_port: u16,
    /// Local IP address for the executor to bind its services to.
    #[arg(long, default_value_t = String::from("0.0.0.0"), help = "Local IP address to bind to.")]
    pub bind_host: String,
    /// External hostname or IP address advertised to other executors for connectivity.
    #[arg(
        long,
        help = "Host name or IP address to register with scheduler so that other executors can connect to this executor. If none is provided, the scheduler will use the connecting IP address to communicate with the executor."
    )]
    pub external_host: Option<String>,
    /// Port for the Arrow Flight service (used for shuffle data transfer).
    #[arg(short = 'p', long, default_value_t = 50051, help = "bind port")]
    pub bind_port: u16,
    /// Port for the executor's gRPC service (used for task management).
    #[arg(long, default_value_t = 50052, help = "Grpc service bind port.")]
    pub bind_grpc_port: u16,
    /// Port for the executor's HTTP health server (/healthz and /readyz).
    #[arg(
        long,
        default_value_t = 50053,
        help = "HTTP port for the executor's Kubernetes health probes (/healthz and /readyz)."
    )]
    pub bind_health_port: u16,
    /// Timeout in seconds for establishing connection to scheduler (0 = fail immediately).
    #[arg(
        long,
        default_value_t = 0,
        help = "How long to try connecting to scheduler before failing. Set to zero to fail after first attempt."
    )]
    pub scheduler_connect_timeout_seconds: u16,
    /// Directory for storing temporary shuffle data and IPC files.
    #[arg(long, help = "Directory for temporary IPC files")]
    pub work_dir: Option<String>,
    /// Number of virtual cores (vcores) this executor advertises to the scheduler
    /// (0 = use all physical CPU cores). One task per executor drives this many
    /// DataFusion partitions in parallel through one plan-Arc. Analogous to YARN
    /// vcores (`yarn.nodemanager.resource.cpu-vcores`) or Spark's
    /// `spark.executor.cores`.
    #[arg(
        short = 'c',
        long,
        default_value_t = 0,
        help = "Virtual cores advertised to the scheduler (defaults to physical CPU count if zero)."
    )]
    pub vcores: usize,
    /// Deprecated alias for `--vcores`. Retained for one release to keep
    /// in-place cluster upgrades working; will be removed in the next
    /// major release.
    #[arg(
        long = "concurrent-tasks",
        hide = true,
        conflicts_with = "vcores",
        help = "[deprecated] renamed to --vcores"
    )]
    pub concurrent_tasks: Option<usize>,
    /// Task scheduling policy: pull-staged (executor polls) or push-staged (scheduler pushes).
    #[arg(short = 's', long, default_value_t = ballista_core::config::TaskSchedulingPolicy::default(), help = "The task scheduling policy used by scheduler. Configuration must match with scheduler configured policy.")]
    pub task_scheduling_policy: ballista_core::config::TaskSchedulingPolicy,
    /// Interval in seconds between job data cleanup runs (0 = disabled).
    #[arg(
        long,
        default_value_t = 0,
        help = "Controls the interval in seconds, which the worker cleans up old job dirs on the local machine. 0 means the clean up is disabled."
    )]
    pub job_data_clean_up_interval_seconds: u64,
    /// Time-to-live in seconds for job data before cleanup (default: 7 days).
    #[arg(
        long,
        default_value_t = 604800,
        help = "The number of seconds to retain job directories on each worker 604800 (7 days, 7 * 24 * 3600), In other words, after job done, how long the resulting data is retained."
    )]
    pub job_data_ttl_seconds: u64,
    /// Directory path for storing executor log files.
    #[arg(
        long,
        help = "Log dir: a path to save log. This will create a new storage directory at the specified path if it does not already exist."
    )]
    pub log_dir: Option<String>,
    /// Whether to include thread IDs and names in log output.
    #[arg(
        long,
        default_value_t = true,
        help = "Enable print thread ids and names in log file."
    )]
    pub print_thread_info: bool,
    /// Log level configuration string (e.g., "INFO,datafusion=DEBUG").
    #[arg(
        long,
        default_value_t = String::from("INFO,datafusion=INFO"),
        help = "special log level for sub mod. link: https://docs.rs/env_logger/latest/env_logger/#enabling-logging. For example we want whole level is INFO but datafusion mode is DEBUG."
    )]
    pub log_level_setting: String,
    /// Log file rotation policy: minutely, hourly, daily, or never.
    #[arg(
        long,
        default_value_t = ballista_core::config::LogRotationPolicy::Daily,
        help = "Tracing log rotation policy."
    )]
    pub log_rotation_policy: ballista_core::config::LogRotationPolicy,
    /// Maximum size of incoming gRPC messages in bytes (default: 128MB).
    #[arg(
        long,
        default_value_t = 134217728,
        help = "The maximum size of a decoded message at the grpc server side."
    )]
    pub grpc_server_max_decoding_message_size: u32,
    /// Maximum size of outgoing gRPC messages in bytes (default: 128MB).
    #[arg(
        long,
        default_value_t = 134217728,
        help = "The maximum size of an encoded message at the grpc server side."
    )]
    pub grpc_server_max_encoding_message_size: u32,
    /// Interval in seconds between heartbeat messages sent to the scheduler.
    #[arg(
        long,
        default_value_t = 60,
        help = "The heartbeat interval in seconds to the scheduler for push-based task scheduling."
    )]
    pub executor_heartbeat_interval_seconds: u64,
    /// Specifying which metrics should be collected and sent to scheduler
    #[arg(
        short = 'm',
        long = "metrics",
        default_value_t = ExecutorMetricCollectionPolicy::default(),
        help = "Metric collection policy of this executor instance"
    )]
    pub metric_collection_policy: ExecutorMetricCollectionPolicy,
    /// Total memory budget for the executor. When unset, defaults to
    /// `--memory-pool-fraction` of the detected host/cgroup memory limit. `0`
    /// disables the pool (unbounded, no spilling). Accepts human-readable values
    /// like "8GB", "512MiB", or a plain byte count. Split evenly across vcores.
    #[arg(
        long,
        value_parser = parse_memory_pool_size,
        help = "Total executor memory budget (e.g. \"8GB\"). Unset = fraction of detected memory; 0 = unbounded. Split across vcores."
    )]
    pub memory_pool_size: Option<u64>,
    /// Fraction (0.0, 1.0] of the detected host/cgroup memory limit used for the
    /// auto memory pool when `--memory-pool-size` is unset. Ignored when
    /// `--memory-pool-size` is given.
    #[arg(
        long,
        default_value_t = crate::executor_process::DEFAULT_MEMORY_POOL_FRACTION,
        value_parser = parse_memory_pool_fraction,
        help = "Fraction (0-1] of detected memory for the auto pool. Default 0.70. Ignored if --memory-pool-size is set."
    )]
    pub memory_pool_fraction: f64,
    /// Maximum number of sessions whose shared runtime state (object-store
    /// clients, Parquet footer cache) is retained on the executor (LRU). `0`
    /// disables caching.
    #[arg(
        long,
        default_value_t = 16,
        help = "Max number of sessions whose shared runtime state (object-store clients, Parquet footer cache) is retained on the executor (LRU). 0 disables caching."
    )]
    pub session_runtime_cache_capacity: usize,
    /// Number of seconds an established client connection is cached while idle
    /// (0 disables the cache, so every shuffle fetch opens a new connection)
    #[arg(
        long,
        default_value_t = 30,
        help = "Number of seconds established client connection should be cached if not used (0 means no cache, connection will be disposed; a shuffle-heavy query can then exhaust the host's ephemeral ports)."
    )]
    pub client_ttl: u64,
}

impl TryFrom<Config> for ExecutorProcessConfig {
    type Error = BallistaError;

    fn try_from(opt: Config) -> Result<Self, Self::Error> {
        let vcores = match opt.concurrent_tasks {
            Some(value) => {
                eprintln!(
                    "warning: --concurrent-tasks is deprecated; use --vcores instead"
                );
                value
            }
            None => opt.vcores,
        };
        Ok(ExecutorProcessConfig {
            special_mod_log_level: opt.log_level_setting,
            external_host: opt.external_host,
            bind_host: opt.bind_host,
            port: opt.bind_port,
            grpc_port: opt.bind_grpc_port,
            scheduler_host: opt.scheduler_host,
            scheduler_port: opt.scheduler_port,
            scheduler_connect_timeout_seconds: opt.scheduler_connect_timeout_seconds,
            vcores,
            task_scheduling_policy: opt.task_scheduling_policy,
            work_dir: opt.work_dir,
            log_dir: opt.log_dir,
            log_rotation_policy: opt.log_rotation_policy,
            print_thread_info: opt.print_thread_info,
            job_data_ttl_seconds: opt.job_data_ttl_seconds,
            job_data_clean_up_interval_seconds: opt.job_data_clean_up_interval_seconds,
            grpc_max_decoding_message_size: opt.grpc_server_max_decoding_message_size,
            grpc_max_encoding_message_size: opt.grpc_server_max_encoding_message_size,
            grpc_server_config: ballista_core::utils::GrpcServerConfig::default(),
            executor_heartbeat_interval_seconds: opt.executor_heartbeat_interval_seconds,
            metric_collection_policy: opt.metric_collection_policy,
            memory_pool_size: opt.memory_pool_size,
            memory_pool_fraction: opt.memory_pool_fraction,
            session_runtime_cache_capacity: opt.session_runtime_cache_capacity,
            override_execution_engine: None,
            override_function_registry: None,
            override_config_producer: None,
            override_runtime_producer: None,
            override_logical_codec: None,
            override_physical_codec: None,
            override_arrow_flight_service: None,
            override_create_grpc_client_endpoint: None,
            client_ttl: opt.client_ttl,
            health: crate::health::ExecutorHealth::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::parse_memory_pool_fraction;
    use super::parse_memory_pool_size;

    #[test]
    fn parse_fraction_accepts_valid_and_rejects_out_of_range() {
        assert_eq!(parse_memory_pool_fraction("0.7").unwrap(), 0.7);
        assert_eq!(parse_memory_pool_fraction("1.0").unwrap(), 1.0);
        assert!(parse_memory_pool_fraction("0").is_err()); // must be > 0
        assert!(parse_memory_pool_fraction("1.5").is_err()); // must be <= 1
        assert!(parse_memory_pool_fraction("banana").is_err());
    }

    #[test]
    fn parse_decimal_suffix() {
        assert_eq!(parse_memory_pool_size("8GB").unwrap(), 8_000_000_000);
        assert_eq!(parse_memory_pool_size("1KB").unwrap(), 1_000);
    }

    #[test]
    fn parse_binary_suffix() {
        assert_eq!(parse_memory_pool_size("512MiB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_pool_size("1KiB").unwrap(), 1024);
    }

    #[test]
    fn parse_plain_integer_is_bytes() {
        assert_eq!(parse_memory_pool_size("1024").unwrap(), 1024);
    }

    #[test]
    fn parse_rejects_invalid() {
        assert!(parse_memory_pool_size("banana").is_err());
        assert!(parse_memory_pool_size("").is_err());
    }

    /// The client pool is what keeps shuffle fetches from opening a fresh TCP
    /// connection each time; with it off, one shuffle-heavy query can exhaust
    /// the host's ephemeral ports and take the executor down with it. Pin the
    /// default on so it cannot regress to the disabled state unnoticed.
    #[cfg(feature = "build-binary")]
    #[test]
    fn client_pool_is_enabled_by_default() {
        use clap::Parser;
        let opt = super::Config::parse_from(["ballista-executor"]);
        assert!(
            opt.client_ttl > 0,
            "client_ttl must default to a pooling value, got {}",
            opt.client_ttl
        );
    }
}
