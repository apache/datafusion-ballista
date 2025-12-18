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

//! Ballista scheduler specific configuration

//
// configure me does not follow clippy spec
// we need to replace it with some other library
// as mentioned in https://github.com/apache/datafusion-ballista/issues/1271
//
#![allow(clippy::uninlined_format_args)]

use crate::cluster::DistributionPolicy;
use crate::SessionBuilder;
use ballista_core::{config::TaskSchedulingPolicy, ConfigProducer};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::fmt::Display;
use std::sync::Arc;

/// Configuration of the application
#[cfg(feature = "build-binary")]
#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Config {
    #[arg(
        long,
        help = "Route for proxying flight results via scheduler. Should be of the form 'IP:PORT'"
    )]
    pub advertise_flight_sql_endpoint: Option<String>,
    #[arg(short = 'n', long, default_value_t = String::from("ballista"), help = "Namespace for the ballista cluster that this executor will join. Default: ballista")]
    pub namespace: String,
    #[arg(long, default_value_t = String::from("0.0.0.0"), help = "Local host name or IP address to bind to. Default: 0.0.0.0")]
    pub bind_host: String,
    #[arg(long, default_value_t = String::from("localhost"), help = "Host name or IP address so that executors can connect to this scheduler. Default: localhost")]
    pub external_host: String,
    #[arg(
        short = 'p',
        long,
        default_value_t = 50050,
        help = "bind port. Default: 50050"
    )]
    pub bind_port: u16,
    #[arg(
        short = 's',
        long,
        default_value_t = ballista_core::config::TaskSchedulingPolicy::PullStaged,
        help = "The scheduling policy for the scheduler, possible values: pull-staged, push-staged. Default: pull-staged"
    )]
    pub scheduler_policy: ballista_core::config::TaskSchedulingPolicy,
    #[arg(
        long,
        default_value_t = 1000,
        help = "Event loop buffer size. Default: 10000"
    )]
    pub event_loop_buffer_size: u32,
    #[arg(
        long,
        default_value_t = 300,
        help = "Delayed interval for cleaning up finished job data. Default: 300"
    )]
    pub finished_job_data_clean_up_interval_seconds: u64,
    #[arg(
        long,
        default_value_t = 3600,
        help = "Delayed interval for cleaning up finished job state. Default: 3600"
    )]
    pub finished_job_state_clean_up_interval_seconds: u64,
    #[arg(
        long,
        default_value_t = crate::config::TaskDistribution::Bias,
        help = "The policy of distributing tasks to available executor slots, possible values: bias, round-robin, consistent-hash. Default: bias"
    )]
    pub task_distribution: crate::config::TaskDistribution,
    #[arg(
        long,
        default_value_t = 31,
        help = "Replica number of each node for the consistent hashing. Default: 31"
    )]
    pub consistent_hash_num_replicas: u32,
    #[arg(
        long,
        default_value_t = 0,
        help = "Tolerance of the consistent hashing policy for task scheduling. Default: 0"
    )]
    pub consistent_hash_tolerance: u32,
    #[arg(
        long,
        help = "Log dir: a path to save log. This will create a new storage directory at the specified path if it does not already exist."
    )]
    pub log_dir: Option<String>,
    #[arg(
        long,
        default_value_t = true,
        help = "Enable print thread ids and names in log file."
    )]
    pub print_thread_info: bool,
    #[arg(
        long,
        default_value_t = String::from("INFO,datafusion=INFO"),
        help = "special log level for sub mod. link: https://docs.rs/env_logger/latest/env_logger/#enabling-logging. For example we want whole level is INFO but datafusion mode is DEBUG"
    )]
    pub log_level_setting: String,
    #[arg(
        long,
        default_value_t = ballista_core::config::LogRotationPolicy::Daily,
        help = "Tracing log rotation policy, possible values: minutely, hourly, daily, never. Default: daily"
    )]
    pub log_rotation_policy: ballista_core::config::LogRotationPolicy,
    #[arg(
        long,
        default_value_t = 0,
        help = "If job is not able to be scheduled on submission, wait for this interval and resubmit. Default value of 0 indicates that job should not be resubmitted"
    )]
    pub job_resubmit_interval_ms: u64,
    #[arg(
        long,
        default_value_t = 30,
        help = "Time in seconds an executor should be considered lost after it enters terminating status"
    )]
    pub executor_termination_grace_period: u64,
    #[arg(
        long,
        default_value_t = 0,
        help = "The maximum expected processing time of a scheduler event (microseconds). Zero means disable."
    )]
    pub scheduler_event_expected_processing_duration: u64,
    #[arg(
        long,
        default_value_t = 16777216,
        help = "The maximum size of a decoded message at the grpc server side. Default: 16MB"
    )]
    pub grpc_server_max_decoding_message_size: u32,
    #[arg(
        long,
        default_value_t = 16777216,
        help = "The maximum size of an encoded message at the grpc server side. Default: 16MB"
    )]
    pub grpc_server_max_encoding_message_size: u32,
    #[arg(
        long,
        default_value_t = 180,
        help = "The executor timeout in seconds. It should be longer than executor's heartbeat intervals. Only after missing two or tree consecutive heartbeats from a executor, the executor is mark to be dead"
    )]
    pub executor_timeout_seconds: u64,
    #[arg(
        long,
        default_value_t = 15,
        help = "The interval to check expired or dead executors"
    )]
    pub expire_dead_executor_interval_seconds: u64,
}

/// Configurations for the ballista scheduler of scheduling jobs and tasks
#[derive(Clone)]
pub struct SchedulerConfig {
    /// Namespace of this scheduler. Schedulers using the same cluster storage and namespace
    /// will share global cluster state.
    pub namespace: String,
    /// The external hostname of the scheduler
    pub external_host: String,
    /// The bind host for the scheduler's gRPC service
    pub bind_host: String,
    /// The bind port for the scheduler's gRPC service
    pub bind_port: u16,
    /// The task scheduling policy for the scheduler
    pub scheduling_policy: TaskSchedulingPolicy,
    /// The event loop buffer size. for a system of high throughput, a larger value like 1000000 is recommended
    pub event_loop_buffer_size: u32,
    /// Policy of distributing tasks to available executor slots. For a cluster with single scheduler, round-robin is recommended
    pub task_distribution: TaskDistributionPolicy,
    /// The delayed interval for cleaning up finished job data, mainly the shuffle data, 0 means the cleaning up is disabled
    pub finished_job_data_clean_up_interval_seconds: u64,
    /// The delayed interval for cleaning up finished job state stored in the backend, 0 means the cleaning up is disabled.
    pub finished_job_state_clean_up_interval_seconds: u64,
    /// The route endpoint for proxying flight sql results via scheduler
    pub advertise_flight_sql_endpoint: Option<String>,
    /// If provided, submitted jobs which do not have tasks scheduled will be resubmitted after `job_resubmit_interval_ms`
    /// milliseconds
    pub job_resubmit_interval_ms: Option<u64>,
    /// Time in seconds to allow executor for graceful shutdown. Once an executor signals it has entered Terminating status
    /// the scheduler should only consider the executor dead after this time interval has elapsed
    pub executor_termination_grace_period: u64,
    /// The maximum expected processing time of a scheduler event (microseconds). Zero means disable.
    pub scheduler_event_expected_processing_duration: u64,
    /// The maximum size of a decoded message at the grpc server side.
    pub grpc_server_max_decoding_message_size: u32,
    /// The maximum size of an encoded message at the grpc server side.
    pub grpc_server_max_encoding_message_size: u32,
    /// The executor timeout in seconds. It should be longer than executor's heartbeat intervals.
    pub executor_timeout_seconds: u64,
    /// The interval to check expired or dead executors
    pub expire_dead_executor_interval_seconds: u64,

    /// [ConfigProducer] override option
    pub override_config_producer: Option<ConfigProducer>,
    /// [SessionBuilder] override option
    pub override_session_builder: Option<SessionBuilder>,
    /// [PhysicalExtensionCodec] override option
    pub override_logical_codec: Option<Arc<dyn LogicalExtensionCodec>>,
    /// [PhysicalExtensionCodec] override option
    pub override_physical_codec: Option<Arc<dyn PhysicalExtensionCodec>>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            namespace: String::default(),
            external_host: "localhost".into(),
            bind_port: 50050,
            bind_host: "127.0.0.1".into(),
            scheduling_policy: Default::default(),
            event_loop_buffer_size: 10000,
            task_distribution: Default::default(),
            finished_job_data_clean_up_interval_seconds: 300,
            finished_job_state_clean_up_interval_seconds: 3600,
            advertise_flight_sql_endpoint: None,
            job_resubmit_interval_ms: None,
            executor_termination_grace_period: 0,
            scheduler_event_expected_processing_duration: 0,
            grpc_server_max_decoding_message_size: 16777216,
            grpc_server_max_encoding_message_size: 16777216,
            executor_timeout_seconds: 180,
            expire_dead_executor_interval_seconds: 15,
            override_config_producer: None,
            override_session_builder: None,
            override_logical_codec: None,
            override_physical_codec: None,
        }
    }
}

impl SchedulerConfig {
    pub fn scheduler_name(&self) -> String {
        format!("{}:{}", self.external_host, self.bind_port)
    }

    pub fn is_push_staged_scheduling(&self) -> bool {
        matches!(self.scheduling_policy, TaskSchedulingPolicy::PushStaged)
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.external_host = hostname.into();
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.bind_port = port;
        self
    }

    pub fn with_scheduler_policy(mut self, policy: TaskSchedulingPolicy) -> Self {
        self.scheduling_policy = policy;
        self
    }

    pub fn with_event_loop_buffer_size(mut self, buffer_size: u32) -> Self {
        self.event_loop_buffer_size = buffer_size;
        self
    }

    pub fn with_finished_job_data_clean_up_interval_seconds(
        mut self,
        interval_seconds: u64,
    ) -> Self {
        self.finished_job_data_clean_up_interval_seconds = interval_seconds;
        self
    }

    pub fn with_finished_job_state_clean_up_interval_seconds(
        mut self,
        interval_seconds: u64,
    ) -> Self {
        self.finished_job_state_clean_up_interval_seconds = interval_seconds;
        self
    }

    pub fn with_advertise_flight_sql_endpoint(
        mut self,
        endpoint: Option<String>,
    ) -> Self {
        self.advertise_flight_sql_endpoint = endpoint;
        self
    }

    pub fn with_task_distribution(mut self, policy: TaskDistributionPolicy) -> Self {
        self.task_distribution = policy;
        self
    }

    pub fn with_job_resubmit_interval_ms(mut self, interval_ms: u64) -> Self {
        self.job_resubmit_interval_ms = Some(interval_ms);
        self
    }

    pub fn with_remove_executor_wait_secs(mut self, value: u64) -> Self {
        self.executor_termination_grace_period = value;
        self
    }

    pub fn with_grpc_server_max_decoding_message_size(mut self, value: u32) -> Self {
        self.grpc_server_max_decoding_message_size = value;
        self
    }

    pub fn with_grpc_server_max_encoding_message_size(mut self, value: u32) -> Self {
        self.grpc_server_max_encoding_message_size = value;
        self
    }

    pub fn with_override_config_producer(
        mut self,
        override_config_producer: ConfigProducer,
    ) -> Self {
        self.override_config_producer = Some(override_config_producer);
        self
    }

    pub fn with_override_session_builder(
        mut self,
        override_session_builder: SessionBuilder,
    ) -> Self {
        self.override_session_builder = Some(override_session_builder);
        self
    }
}

/// Policy of distributing tasks to available executor slots
///
#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum TaskDistribution {
    /// Eagerly assign tasks to executor slots. This will assign as many task slots per executor
    /// as are currently available
    Bias,
    /// Distribute tasks evenly across executors. This will try and iterate through available executors
    /// and assign one task to each executor until all tasks are assigned.
    RoundRobin,
    /// 1. Firstly, try to bind tasks without scanning source files by `RoundRobin` policy.
    /// 2. Then for a task for scanning source files, firstly calculate a hash value based on input files.
    ///    And then bind it with an execute according to consistent hashing policy.
    /// 3. If needed, work stealing can be enabled based on the tolerance of the consistent hashing.
    ConsistentHash,
}

impl Display for TaskDistribution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskDistribution::Bias => f.write_str("bias"),
            TaskDistribution::RoundRobin => f.write_str("round-robin"),
            TaskDistribution::ConsistentHash => f.write_str("consistent-hash"),
        }
    }
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for TaskDistribution {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

#[derive(Debug, Clone, Default)]
pub enum TaskDistributionPolicy {
    /// Eagerly assign tasks to executor slots. This will assign as many task slots per executor
    /// as are currently available
    #[default]
    Bias,
    /// Distribute tasks evenly across executors. This will try and iterate through available executors
    /// and assign one task to each executor until all tasks are assigned.
    RoundRobin,
    /// 1. Firstly, try to bind tasks without scanning source files by `RoundRobin` policy.
    /// 2. Then for a task for scanning source files, firstly calculate a hash value based on input files.
    ///    And then bind it with an execute according to consistent hashing policy.
    /// 3. If needed, work stealing can be enabled based on the tolerance of the consistent hashing.
    ConsistentHash {
        num_replicas: usize,
        tolerance: usize,
    },
    /// User provided task distribution policy
    Custom(Arc<dyn DistributionPolicy>),
}

#[cfg(feature = "build-binary")]
impl TryFrom<Config> for SchedulerConfig {
    type Error = ballista_core::error::BallistaError;

    fn try_from(opt: Config) -> Result<Self, Self::Error> {
        let task_distribution = match opt.task_distribution {
            TaskDistribution::Bias => TaskDistributionPolicy::Bias,
            TaskDistribution::RoundRobin => TaskDistributionPolicy::RoundRobin,
            TaskDistribution::ConsistentHash => {
                let num_replicas = opt.consistent_hash_num_replicas as usize;
                let tolerance = opt.consistent_hash_tolerance as usize;
                TaskDistributionPolicy::ConsistentHash {
                    num_replicas,
                    tolerance,
                }
            }
        };

        let config = SchedulerConfig {
            namespace: opt.namespace,
            external_host: opt.external_host,
            bind_port: opt.bind_port,
            bind_host: opt.bind_host,
            scheduling_policy: opt.scheduler_policy,
            event_loop_buffer_size: opt.event_loop_buffer_size,
            task_distribution,
            finished_job_data_clean_up_interval_seconds: opt
                .finished_job_data_clean_up_interval_seconds,
            finished_job_state_clean_up_interval_seconds: opt
                .finished_job_state_clean_up_interval_seconds,
            advertise_flight_sql_endpoint: opt.advertise_flight_sql_endpoint,
            job_resubmit_interval_ms: (opt.job_resubmit_interval_ms > 0)
                .then_some(opt.job_resubmit_interval_ms),
            executor_termination_grace_period: opt.executor_termination_grace_period,
            scheduler_event_expected_processing_duration: opt
                .scheduler_event_expected_processing_duration,
            grpc_server_max_decoding_message_size: opt
                .grpc_server_max_decoding_message_size,
            grpc_server_max_encoding_message_size: opt
                .grpc_server_max_encoding_message_size,
            executor_timeout_seconds: opt.executor_timeout_seconds,
            expire_dead_executor_interval_seconds: opt
                .expire_dead_executor_interval_seconds,
            override_config_producer: None,
            override_logical_codec: None,
            override_physical_codec: None,
            override_session_builder: None,
        };

        Ok(config)
    }
}
