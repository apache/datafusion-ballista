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

/// Command-line arguments for configuring a Ballista executor.
///
/// This struct is parsed from command-line arguments using clap and contains
/// all the configuration options needed to start an executor process.
#[cfg(feature = "build-binary")]
#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// Hostname or IP address of the scheduler to connect to.
    #[arg(long, default_value_t = String::from("localhost"), help = "Scheduler host")]
    pub scheduler_host: String,
    /// Port number of the scheduler's gRPC service.
    #[arg(long, default_value_t = 50050, help = "scheduler port")]
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
    #[arg(long, default_value_t = 50052, help = "bind grpc service port")]
    pub bind_grpc_port: u16,
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
    /// Maximum number of concurrent tasks this executor can run (0 = use all CPU cores).
    #[arg(
        short = 'c',
        long,
        default_value_t = 0,
        help = "Max concurrent tasks. (defaults to all available cores if left as zero)"
    )]
    pub concurrent_tasks: usize,
    /// Task scheduling policy: pull-staged (executor polls) or push-staged (scheduler pushes).
    #[arg(short = 's', long, default_value_t = ballista_core::config::TaskSchedulingPolicy::PullStaged, help = "The task scheduling policy for the scheduler, possible values: pull-staged, push-staged. Default: pull-staged")]
    pub task_scheduling_policy: ballista_core::config::TaskSchedulingPolicy,
    /// Interval in seconds between job data cleanup runs (0 = disabled).
    #[arg(
        long,
        default_value_t = 0,
        help = "Controls the interval in seconds, which the worker cleans up old job dirs on the local machine. 0 means the clean up is disabled"
    )]
    pub job_data_clean_up_interval_seconds: u64,
    /// Time-to-live in seconds for job data before cleanup (default: 7 days).
    #[arg(
        long,
        default_value_t = 604800,
        help = "The number of seconds to retain job directories on each worker 604800 (7 days, 7 * 24 * 3600), In other words, after job done, how long the resulting data is retained"
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
        help = "special log level for sub mod. link: https://docs.rs/env_logger/latest/env_logger/#enabling-logging. For example we want whole level is INFO but datafusion mode is DEBUG"
    )]
    pub log_level_setting: String,
    /// Log file rotation policy: minutely, hourly, daily, or never.
    #[arg(
        long,
        default_value_t = ballista_core::config::LogRotationPolicy::Daily,
        help = "Tracing log rotation policy, possible values: minutely, hourly, daily, never. Default: daily"
    )]
    pub log_rotation_policy: ballista_core::config::LogRotationPolicy,
    /// Maximum size of incoming gRPC messages in bytes (default: 16MB).
    #[arg(
        long,
        default_value_t = 16777216,
        help = "The maximum size of a decoded message at the grpc server side. Default: 16MB"
    )]
    pub grpc_server_max_decoding_message_size: u32,
    /// Maximum size of outgoing gRPC messages in bytes (default: 16MB).
    #[arg(
        long,
        default_value_t = 16777216,
        help = "The maximum size of an encoded message at the grpc server side. Default: 16MB"
    )]
    pub grpc_server_max_encoding_message_size: u32,
    /// Interval in seconds between heartbeat messages sent to the scheduler.
    #[arg(
        long,
        default_value_t = 60,
        help = "The heartbeat interval in seconds to the scheduler for push-based task scheduling"
    )]
    pub executor_heartbeat_interval_seconds: u64,
}

impl TryFrom<Config> for ExecutorProcessConfig {
    type Error = BallistaError;

    fn try_from(opt: Config) -> Result<Self, Self::Error> {
        Ok(ExecutorProcessConfig {
            special_mod_log_level: opt.log_level_setting,
            external_host: opt.external_host,
            bind_host: opt.bind_host,
            port: opt.bind_port,
            grpc_port: opt.bind_grpc_port,
            scheduler_host: opt.scheduler_host,
            scheduler_port: opt.scheduler_port,
            scheduler_connect_timeout_seconds: opt.scheduler_connect_timeout_seconds,
            concurrent_tasks: opt.concurrent_tasks,
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
            override_execution_engine: None,
            override_function_registry: None,
            override_config_producer: None,
            override_runtime_producer: None,
            override_logical_codec: None,
            override_physical_codec: None,
            override_arrow_flight_service: None,
            override_create_grpc_client_endpoint: None,
        })
    }
}
