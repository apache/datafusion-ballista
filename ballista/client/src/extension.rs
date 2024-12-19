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

use ballista_core::extension::SessionConfigHelperExt;
pub use ballista_core::extension::{SessionConfigExt, SessionStateExt};
use ballista_core::{
    serde::protobuf::{scheduler_grpc_client::SchedulerGrpcClient, CreateSessionParams},
    utils::create_grpc_client_connection,
};
use datafusion::{
    error::DataFusionError,
    execution::SessionState,
    prelude::{SessionConfig, SessionContext},
};
use url::Url;

const DEFAULT_SCHEDULER_PORT: u16 = 50050;

/// Module provides [SessionContextExt] which adds `standalone*` and `remote*`
/// methods to [SessionContext].
///
/// Provided methods set up [SessionContext] with [BallistaQueryPlanner](ballista_core::utils), which
/// handles running plans on Ballista clusters.
///
///```no_run
/// use ballista::prelude::SessionContextExt;
/// use datafusion::prelude::SessionContext;
///
/// # #[tokio::main]
/// # async fn main() -> datafusion::error::Result<()> {
/// let ctx: SessionContext = SessionContext::remote("df://localhost:50050").await?;
/// # Ok(())
/// # }
///```
///
/// [SessionContextExt::standalone()] provides an easy way to start up
/// local cluster. It is an optional feature which should be enabled
/// with `standalone`
///
///```no_run
/// use ballista::prelude::SessionContextExt;
/// use datafusion::prelude::SessionContext;
///
/// # #[tokio::main]
/// # async fn main() -> datafusion::error::Result<()> {
/// let ctx: SessionContext = SessionContext::standalone().await?;
/// # Ok(())
/// # }
///```
///
/// There are still few limitations on query distribution, thus not all
/// [SessionContext] functionalities are supported.
///

#[async_trait::async_trait]
pub trait SessionContextExt {
    /// Creates a context for executing queries against a standalone Ballista scheduler instance
    ///
    /// It wills start local ballista cluster with scheduler and executor.
    #[cfg(feature = "standalone")]
    async fn standalone() -> datafusion::error::Result<SessionContext>;

    /// Creates a context for executing queries against a standalone Ballista scheduler instance
    /// with custom session state.
    ///
    /// It wills start local ballista cluster with scheduler and executor.
    #[cfg(feature = "standalone")]
    async fn standalone_with_state(
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext>;

    /// Creates a context for executing queries against a remote Ballista scheduler instance
    async fn remote(url: &str) -> datafusion::error::Result<SessionContext>;

    /// Creates a context for executing queries against a remote Ballista scheduler instance
    /// with custom session state
    async fn remote_with_state(
        url: &str,
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext>;
}

#[async_trait::async_trait]
impl SessionContextExt for SessionContext {
    async fn remote_with_state(
        url: &str,
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext> {
        let config = state.config();

        let scheduler_url = Extension::parse_url(url)?;
        log::info!(
            "Connecting to Ballista scheduler at {}",
            scheduler_url.clone()
        );
        let remote_session_id =
            Extension::setup_remote(config, scheduler_url.clone()).await?;
        log::info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );
        let session_state =
            state.upgrade_for_ballista(scheduler_url, remote_session_id)?;

        Ok(SessionContext::new_with_state(session_state))
    }

    async fn remote(url: &str) -> datafusion::error::Result<SessionContext> {
        let config = SessionConfig::new_with_ballista();
        let scheduler_url = Extension::parse_url(url)?;
        log::info!(
            "Connecting to Ballista scheduler at: {}",
            scheduler_url.clone()
        );
        let remote_session_id =
            Extension::setup_remote(&config, scheduler_url.clone()).await?;
        log::info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );
        let session_state =
            SessionState::new_ballista_state(scheduler_url, remote_session_id)?;

        Ok(SessionContext::new_with_state(session_state))
    }

    #[cfg(feature = "standalone")]
    async fn standalone_with_state(
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext> {
        let (remote_session_id, scheduler_url) =
            Extension::setup_standalone(Some(&state)).await?;

        let session_state =
            state.upgrade_for_ballista(scheduler_url, remote_session_id.clone())?;

        log::info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );

        Ok(SessionContext::new_with_state(session_state))
    }

    #[cfg(feature = "standalone")]
    async fn standalone() -> datafusion::error::Result<Self> {
        log::info!("Running in local mode. Scheduler will be run in-proc");

        let (remote_session_id, scheduler_url) =
            Extension::setup_standalone(None).await?;

        let session_state =
            SessionState::new_ballista_state(scheduler_url, remote_session_id.clone())?;

        log::info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );

        Ok(SessionContext::new_with_state(session_state))
    }
}

struct Extension {}

impl Extension {
    fn parse_url(url: &str) -> datafusion::error::Result<String> {
        let url =
            Url::parse(url).map_err(|e| DataFusionError::Configuration(e.to_string()))?;
        let host = url.host().ok_or(DataFusionError::Configuration(
            "hostname should be provided".to_string(),
        ))?;
        let port = url.port().unwrap_or(DEFAULT_SCHEDULER_PORT);
        let scheduler_url = format!("http://{}:{}", &host, port);

        Ok(scheduler_url)
    }

    #[cfg(feature = "standalone")]
    async fn setup_standalone(
        session_state: Option<&SessionState>,
    ) -> datafusion::error::Result<(String, String)> {
        use ballista_core::{serde::BallistaCodec, utils::default_config_producer};

        let addr = match session_state {
            None => ballista_scheduler::standalone::new_standalone_scheduler()
                .await
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?,
            Some(session_state) => {
                ballista_scheduler::standalone::new_standalone_scheduler_from_state(
                    session_state,
                )
                .await
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?
            }
        };
        let config = session_state
            .map(|s| s.config().clone())
            .unwrap_or_else(default_config_producer);

        let scheduler_url = format!("http://localhost:{}", addr.port());

        let mut scheduler = loop {
            match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    log::info!("Attempting to connect to in-proc scheduler...");
                }
                Ok(scheduler) => break scheduler,
            }
        };

        let remote_session_id = scheduler
            .create_session(CreateSessionParams {
                settings: config.to_key_value_pairs(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner()
            .session_id;

        let concurrent_tasks = config.ballista_standalone_parallelism();

        match session_state {
            None => {
                ballista_executor::new_standalone_executor(
                    scheduler,
                    concurrent_tasks,
                    BallistaCodec::default(),
                )
                .await
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
            }
            Some(session_state) => {
                ballista_executor::new_standalone_executor_from_state(
                    scheduler,
                    concurrent_tasks,
                    session_state,
                )
                .await
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
            }
        }

        Ok((remote_session_id, scheduler_url))
    }

    async fn setup_remote(
        config: &SessionConfig,
        scheduler_url: String,
    ) -> datafusion::error::Result<String> {
        let connection = create_grpc_client_connection(scheduler_url.clone())
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

        let limit = config.ballista_grpc_client_max_message_size();
        let mut scheduler = SchedulerGrpcClient::new(connection)
            .max_encoding_message_size(limit)
            .max_decoding_message_size(limit);

        let remote_session_id = scheduler
            .create_session(CreateSessionParams {
                settings: config.to_key_value_pairs(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner()
            .session_id;

        Ok(remote_session_id)
    }
}
