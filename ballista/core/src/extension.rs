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

use crate::config::{
    BallistaConfig, BALLISTA_GRPC_CLIENT_MAX_MESSAGE_SIZE, BALLISTA_JOB_NAME,
    BALLISTA_SHUFFLE_READER_MAX_REQUESTS, BALLISTA_STANDALONE_PARALLELISM,
};
use crate::planner::BallistaQueryPlanner;
use crate::serde::protobuf::KeyValuePair;
use crate::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionState};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::LogicalPlanNode;
use std::sync::Arc;

/// Provides methods which adapt [SessionState]
/// for Ballista usage
pub trait SessionStateExt {
    /// Setups new [SessionState] for ballista usage
    ///
    /// State will be created with appropriate [SessionConfig] configured
    fn new_ballista_state(
        scheduler_url: String,
    ) -> datafusion::error::Result<SessionState>;
    /// Upgrades [SessionState] for ballista usage
    ///
    /// State will be upgraded to appropriate [SessionConfig]
    fn upgrade_for_ballista(
        self,
        scheduler_url: String,
    ) -> datafusion::error::Result<SessionState>;
}

/// [SessionConfig] extension with methods needed
/// for Ballista configuration
pub trait SessionConfigExt {
    /// Creates session config which has
    /// ballista configuration initialized
    fn new_with_ballista() -> SessionConfig;

    /// update [SessionConfig] with Ballista specific settings
    fn upgrade_for_ballista(self) -> SessionConfig;

    /// return ballista specific configuration or
    /// creates one if does not exist
    fn ballista_config(&self) -> BallistaConfig;

    /// Overrides ballista's [LogicalExtensionCodec]
    fn with_ballista_logical_extension_codec(
        self,
        codec: Arc<dyn LogicalExtensionCodec>,
    ) -> SessionConfig;

    /// Overrides ballista's [PhysicalExtensionCodec]
    fn with_ballista_physical_extension_codec(
        self,
        codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> SessionConfig;

    /// returns [LogicalExtensionCodec] if set
    /// or default ballista codec if not
    fn ballista_logical_extension_codec(&self) -> Arc<dyn LogicalExtensionCodec>;

    /// returns [PhysicalExtensionCodec] if set
    /// or default ballista codec if not
    fn ballista_physical_extension_codec(&self) -> Arc<dyn PhysicalExtensionCodec>;

    /// Overrides ballista's [QueryPlanner]
    fn with_ballista_query_planner(
        self,
        planner: Arc<dyn QueryPlanner + Send + Sync + 'static>,
    ) -> SessionConfig;

    /// Returns ballista's [QueryPlanner] if overridden
    fn ballista_query_planner(
        &self,
    ) -> Option<Arc<dyn QueryPlanner + Send + Sync + 'static>>;

    /// Returns parallelism of standalone cluster
    fn ballista_standalone_parallelism(&self) -> usize;
    /// Sets parallelism of standalone cluster
    ///
    /// This option to be used to configure standalone session context
    fn with_ballista_standalone_parallelism(self, parallelism: usize) -> Self;

    /// retrieves grpc client max message size
    fn ballista_grpc_client_max_message_size(&self) -> usize;

    /// sets grpc client max message size
    fn with_ballista_grpc_client_max_message_size(self, max_size: usize) -> Self;

    /// Sets ballista job name
    fn with_ballista_job_name(self, job_name: &str) -> Self;

    /// get maximum in flight requests for shuffle reader
    fn ballista_shuffle_reader_maximum_concurrent_requests(&self) -> usize;

    /// Sets maximum in flight requests for shuffle reader
    fn with_ballista_shuffle_reader_maximum_concurrent_requests(
        self,
        max_requests: usize,
    ) -> Self;
}

/// [SessionConfigHelperExt] is set of [SessionConfig] extension methods
/// which are used internally (not exposed in client)
pub trait SessionConfigHelperExt {
    /// converts [SessionConfig] to proto
    fn to_key_value_pairs(&self) -> Vec<KeyValuePair>;
    /// updates [SessionConfig] from proto
    fn update_from_key_value_pair(self, key_value_pairs: &[KeyValuePair]) -> Self;
    /// updates mut [SessionConfig] from proto
    fn update_from_key_value_pair_mut(&mut self, key_value_pairs: &[KeyValuePair]);
    /// changes some of default datafusion configuration
    /// in order to make it suitable for ballista
    fn ballista_restricted_configuration(self) -> Self;
}

impl SessionStateExt for SessionState {
    fn new_ballista_state(
        scheduler_url: String,
    ) -> datafusion::error::Result<SessionState> {
        let session_config = SessionConfig::new_with_ballista();
        let planner = BallistaQueryPlanner::<LogicalPlanNode>::new(
            scheduler_url,
            BallistaConfig::default(),
        );

        let runtime_env = RuntimeEnvBuilder::new().build()?;
        let session_state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_query_planner(Arc::new(planner))
            .build();

        Ok(session_state)
    }

    fn upgrade_for_ballista(
        self,
        scheduler_url: String,
    ) -> datafusion::error::Result<SessionState> {
        let codec_logical = self.config().ballista_logical_extension_codec();
        let planner_override = self.config().ballista_query_planner();

        let session_config = self.config().clone().upgrade_for_ballista();

        let ballista_config = session_config.ballista_config();

        let builder =
            SessionStateBuilder::new_from_existing(self).with_config(session_config);

        let builder = match planner_override {
            Some(planner) => builder.with_query_planner(planner),
            None => {
                let planner = BallistaQueryPlanner::<LogicalPlanNode>::with_extension(
                    scheduler_url,
                    ballista_config,
                    codec_logical,
                );
                builder.with_query_planner(Arc::new(planner))
            }
        };

        Ok(builder.build())
    }
}

impl SessionConfigExt for SessionConfig {
    fn new_with_ballista() -> SessionConfig {
        SessionConfig::new()
            .with_option_extension(BallistaConfig::default())
            .with_information_schema(true)
            .with_target_partitions(16)
            .ballista_restricted_configuration()
    }

    fn upgrade_for_ballista(self) -> SessionConfig {
        // if ballista config is not provided
        // one is created and session state is updated
        let ballista_config = self.ballista_config();

        // session config has ballista config extension and
        // default datafusion configuration is altered
        // to fit ballista execution
        self.with_option_extension(ballista_config)
            .ballista_restricted_configuration()
    }

    fn ballista_config(&self) -> BallistaConfig {
        self.options()
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_else(BallistaConfig::default)
    }
    fn with_ballista_logical_extension_codec(
        self,
        codec: Arc<dyn LogicalExtensionCodec>,
    ) -> SessionConfig {
        let extension = BallistaConfigExtensionLogicalCodec::new(codec);
        self.with_extension(Arc::new(extension))
    }
    fn with_ballista_physical_extension_codec(
        self,
        codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> SessionConfig {
        let extension = BallistaConfigExtensionPhysicalCodec::new(codec);
        self.with_extension(Arc::new(extension))
    }

    fn ballista_logical_extension_codec(&self) -> Arc<dyn LogicalExtensionCodec> {
        self.get_extension::<BallistaConfigExtensionLogicalCodec>()
            .map(|c| c.codec())
            .unwrap_or_else(|| Arc::new(BallistaLogicalExtensionCodec::default()))
    }
    fn ballista_physical_extension_codec(&self) -> Arc<dyn PhysicalExtensionCodec> {
        self.get_extension::<BallistaConfigExtensionPhysicalCodec>()
            .map(|c| c.codec())
            .unwrap_or_else(|| Arc::new(BallistaPhysicalExtensionCodec::default()))
    }

    fn with_ballista_query_planner(
        self,
        planner: Arc<dyn QueryPlanner + Send + Sync + 'static>,
    ) -> SessionConfig {
        let extension = BallistaQueryPlannerExtension::new(planner);
        self.with_extension(Arc::new(extension))
    }

    fn ballista_query_planner(
        &self,
    ) -> Option<Arc<dyn QueryPlanner + Send + Sync + 'static>> {
        self.get_extension::<BallistaQueryPlannerExtension>()
            .map(|c| c.planner())
    }

    fn ballista_standalone_parallelism(&self) -> usize {
        self.options()
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.default_standalone_parallelism())
            .unwrap_or_else(|| BallistaConfig::default().default_standalone_parallelism())
    }

    fn ballista_grpc_client_max_message_size(&self) -> usize {
        self.options()
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.default_grpc_client_max_message_size())
            .unwrap_or_else(|| {
                BallistaConfig::default().default_grpc_client_max_message_size()
            })
    }

    fn with_ballista_job_name(self, job_name: &str) -> Self {
        if self.options().extensions.get::<BallistaConfig>().is_some() {
            self.set_str(BALLISTA_JOB_NAME, job_name)
        } else {
            self.with_option_extension(BallistaConfig::default())
                .set_str(BALLISTA_JOB_NAME, job_name)
        }
    }

    fn with_ballista_grpc_client_max_message_size(self, max_size: usize) -> Self {
        if self.options().extensions.get::<BallistaConfig>().is_some() {
            self.set_usize(BALLISTA_GRPC_CLIENT_MAX_MESSAGE_SIZE, max_size)
        } else {
            self.with_option_extension(BallistaConfig::default())
                .set_usize(BALLISTA_GRPC_CLIENT_MAX_MESSAGE_SIZE, max_size)
        }
    }

    fn with_ballista_standalone_parallelism(self, parallelism: usize) -> Self {
        if self.options().extensions.get::<BallistaConfig>().is_some() {
            self.set_usize(BALLISTA_STANDALONE_PARALLELISM, parallelism)
        } else {
            self.with_option_extension(BallistaConfig::default())
                .set_usize(BALLISTA_STANDALONE_PARALLELISM, parallelism)
        }
    }

    fn ballista_shuffle_reader_maximum_concurrent_requests(&self) -> usize {
        self.options()
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.shuffle_reader_maximum_concurrent_requests())
            .unwrap_or_else(|| {
                BallistaConfig::default().shuffle_reader_maximum_concurrent_requests()
            })
    }

    fn with_ballista_shuffle_reader_maximum_concurrent_requests(
        self,
        max_requests: usize,
    ) -> Self {
        if self.options().extensions.get::<BallistaConfig>().is_some() {
            self.set_usize(BALLISTA_SHUFFLE_READER_MAX_REQUESTS, max_requests)
        } else {
            self.with_option_extension(BallistaConfig::default())
                .set_usize(BALLISTA_SHUFFLE_READER_MAX_REQUESTS, max_requests)
        }
    }
}

impl SessionConfigHelperExt for SessionConfig {
    fn to_key_value_pairs(&self) -> Vec<KeyValuePair> {
        self.options()
            .entries()
            .iter()
            // TODO: revisit this log once we this option is removed
            //
            // filtering this key as it's creating a lot of warning logs
            // at the executor side.
            .filter(|c| {
                c.key != "datafusion.sql_parser.enable_options_value_normalization"
            })
            .map(|datafusion::config::ConfigEntry { key, value, .. }| {
                log::trace!("sending configuration key: `{key}`, value`{value:?}`");
                KeyValuePair {
                    key: key.to_owned(),
                    value: value.clone(),
                }
            })
            .collect()
    }

    fn update_from_key_value_pair(self, key_value_pairs: &[KeyValuePair]) -> Self {
        let mut s = self;
        s.update_from_key_value_pair_mut(key_value_pairs);
        s
    }

    fn update_from_key_value_pair_mut(&mut self, key_value_pairs: &[KeyValuePair]) {
        for KeyValuePair { key, value } in key_value_pairs {
            match value {
                Some(value) => {
                    log::trace!(
                        "setting up configuration key: `{key}`, value: `{value:?}`"
                    );
                    if let Err(e) = self.options_mut().set(key, value) {
                        // there is not much we can do about this error at the moment.
                        // it used to be warning but it gets very verbose
                        // as even datafusion properties can't be parsed
                        log::debug!(
                            "could not set configuration key: `{key}`, value: `{value:?}`, reason: {e}"
                        )
                    }
                }
                None => {
                    log::trace!(
                        "can't set up configuration key: `{key}`, as value is None",
                    )
                }
            }
        }
    }

    fn ballista_restricted_configuration(self) -> Self {
        self
            // round robbin repartition does not work well with ballista.
            // this setting it will also be enforced by the scheduler
            // thus user will not be able to override it.
            .with_round_robin_repartition(false)
            // There is issue with Utv8View(s) where Arrow IPC will generate
            // frames which would be too big to send using Arrow Flight.
            //
            // This configuration option will be disabled temporary.
            //
            // This configuration is not enforced by the scheduler, thus
            // user could override this setting using `SET` operation.
            //
            // TODO: enable this option once we get to root of the problem
            //       between `IpcWriter` and `ViewTypes`
            .set_bool(
                "datafusion.execution.parquet.schema_force_view_types",
                false,
            )
            // same like previous comment
            .set_bool("datafusion.sql_parser.map_string_types_to_utf8view", false)
    }
}

/// Wrapper for [SessionConfig] extension
/// holding [LogicalExtensionCodec] if overridden
struct BallistaConfigExtensionLogicalCodec {
    codec: Arc<dyn LogicalExtensionCodec>,
}

impl BallistaConfigExtensionLogicalCodec {
    fn new(codec: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self { codec }
    }
    fn codec(&self) -> Arc<dyn LogicalExtensionCodec> {
        self.codec.clone()
    }
}

/// Wrapper for [SessionConfig] extension
/// holding [PhysicalExtensionCodec] if overridden
struct BallistaConfigExtensionPhysicalCodec {
    codec: Arc<dyn PhysicalExtensionCodec>,
}

impl BallistaConfigExtensionPhysicalCodec {
    fn new(codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { codec }
    }
    fn codec(&self) -> Arc<dyn PhysicalExtensionCodec> {
        self.codec.clone()
    }
}

/// Wrapper for [SessionConfig] extension
/// holding overridden [QueryPlanner]
struct BallistaQueryPlannerExtension {
    planner: Arc<dyn QueryPlanner + Send + Sync + 'static>,
}

impl BallistaQueryPlannerExtension {
    fn new(planner: Arc<dyn QueryPlanner + Send + Sync + 'static>) -> Self {
        Self { planner }
    }
    fn planner(&self) -> Arc<dyn QueryPlanner + Send + Sync + 'static> {
        self.planner.clone()
    }
}

#[cfg(test)]
mod test {
    use datafusion::{
        execution::{SessionState, SessionStateBuilder},
        prelude::SessionConfig,
    };

    use crate::{
        config::BALLISTA_JOB_NAME,
        extension::{SessionConfigExt, SessionConfigHelperExt, SessionStateExt},
    };

    // Ballista disables round robin repatriations
    #[tokio::test]
    async fn should_disable_round_robin_repartition() {
        let state =
            SessionState::new_ballista_state("scheduler_url".to_string()).unwrap();

        assert!(!state.config().round_robin_repartition());

        let state = SessionStateBuilder::new().build();

        assert!(state.config().round_robin_repartition());
        let state = state
            .upgrade_for_ballista("scheduler_url".to_string())
            .unwrap();

        assert!(!state.config().round_robin_repartition());
    }
    #[test]
    fn should_convert_to_key_value_pairs() {
        // key value pairs should contain datafusion and ballista values

        let config =
            SessionConfig::new_with_ballista().with_ballista_job_name("job_name");
        let pairs = config.to_key_value_pairs();

        assert!(pairs.iter().any(|p| p.key == BALLISTA_JOB_NAME));
        assert!(pairs
            .iter()
            .any(|p| p.key == "datafusion.catalog.information_schema"))
    }
}
