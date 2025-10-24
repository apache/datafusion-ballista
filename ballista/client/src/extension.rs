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

pub use ballista_core::extension::{SessionConfigExt, SessionStateExt};
use ballista_core::remote_catalog::remote_table_provider::RemoteTableProvider;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::GetCatalogParams;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::datasource::MemTable;
use datafusion::{
    error::DataFusionError, execution::SessionState, prelude::SessionContext,
};
use std::sync::Arc;
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

    /// Populates the local catalog with metadata from the remote scheduler.
    /// This allows catalog queries like SHOW TABLES to work on the client.
    async fn populate_catalog_from_scheduler(
        &self,
        scheduler_url: &str,
    ) -> datafusion::error::Result<()>;
}

#[async_trait::async_trait]
impl SessionContextExt for SessionContext {
    async fn remote_with_state(
        url: &str,
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext> {
        let scheduler_url = Extension::parse_url(url)?;
        log::info!(
            "Connecting to Ballista scheduler at {}",
            scheduler_url.clone()
        );

        let session_state = state.upgrade_for_ballista(scheduler_url.clone())?;

        log::info!(
            "Server side SessionContext created with session id: {}",
            session_state.session_id()
        );

        let ctx = SessionContext::new_with_state(session_state);

        // Populate local catalog from scheduler
        ctx.populate_catalog_from_scheduler(&scheduler_url).await?;

        Ok(ctx)
    }

    async fn remote(url: &str) -> datafusion::error::Result<SessionContext> {
        let scheduler_url = Extension::parse_url(url)?;
        log::info!(
            "Connecting to Ballista scheduler at: {}",
            scheduler_url.clone()
        );

        let session_state = SessionState::new_ballista_state(scheduler_url.clone())?;
        log::info!(
            "Server side SessionContext created with session id: {}",
            session_state.session_id()
        );

        let ctx = SessionContext::new_with_state(session_state);

        // Populate local catalog from scheduler
        ctx.populate_catalog_from_scheduler(&scheduler_url).await?;

        Ok(ctx)
    }

    #[cfg(feature = "standalone")]
    async fn standalone_with_state(
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext> {
        let scheduler_url = Extension::setup_standalone(Some(&state)).await?;

        let session_state = state.upgrade_for_ballista(scheduler_url)?;

        log::info!(
            "Server side SessionContext created with session id: {}",
            session_state.session_id()
        );

        Ok(SessionContext::new_with_state(session_state))
    }

    #[cfg(feature = "standalone")]
    async fn standalone() -> datafusion::error::Result<Self> {
        log::info!("Running in local mode. Scheduler will be run in-proc");

        let scheduler_url = Extension::setup_standalone(None).await?;

        let session_state = SessionState::new_ballista_state(scheduler_url)?;

        log::info!(
            "Server side SessionContext created with session id: {}",
            session_state.session_id()
        );

        Ok(SessionContext::new_with_state(session_state))
    }

    async fn populate_catalog_from_scheduler(
        &self,
        scheduler_url: &str,
    ) -> datafusion::error::Result<()> {
        let mut client = SchedulerGrpcClient::connect(scheduler_url.to_string())
            .await
            .map_err(|e| {
                DataFusionError::External(
                    format!("Failed to connect to scheduler: {}", e).into(),
                )
            })?;

        let request = tonic::Request::new(GetCatalogParams {
            session_id: self.state().session_id().to_string(),
        });

        let response = client.get_catalog(request).await.map_err(|e| {
            DataFusionError::External(format!("Failed to fetch catalog: {}", e).into())
        })?;

        let catalog_result = response.into_inner();

        log::info!(
            "Received {} catalogs from scheduler",
            catalog_result.catalogs.len()
        );

        for catalog_info in catalog_result.catalogs {
            let catalog_name = catalog_info.catalog_name;

            let catalog: Arc<dyn CatalogProvider> =
                if let Some(_existing_catalog) = self.catalog(&catalog_name) {
                    continue;
                } else {
                    let new_catalog: Arc<dyn CatalogProvider> =
                        Arc::new(MemoryCatalogProvider::new());
                    self.register_catalog(&catalog_name, Arc::clone(&new_catalog));
                    new_catalog
                };

            for schema_info in catalog_info.schemas {
                let schema_name = schema_info.schema_name;
                let schema = if let Some(_existing_schema) = catalog.schema(&schema_name)
                {
                    continue;
                } else {
                    let new_schema: Arc<dyn SchemaProvider> =
                        Arc::new(MemorySchemaProvider::new());
                    catalog.register_schema(&schema_name, Arc::clone(&new_schema))?;
                    new_schema
                };

                // Bind `RemoteTableProvider` tables
                for table_info in schema_info.tables {
                    if let Some(proto_schema) = table_info.schema {
                        let arrow_schema: datafusion::arrow::datatypes::Schema =
                            (&proto_schema)
                                .try_into()
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        let stub_table = RemoteTableProvider::new(
                            &catalog_name,
                            &schema_name,
                            &table_info.table_name,
                            Arc::new(arrow_schema),
                        );

                        schema.register_table(
                            table_info.table_name.clone(),
                            Arc::new(stub_table),
                        )?;
                    }
                }
            }
        }

        Ok(())
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
    ) -> datafusion::error::Result<String> {
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

        let scheduler = loop {
            match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    log::info!("Attempting to connect to in-proc scheduler...");
                }
                Ok(scheduler) => break scheduler,
            }
        };

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

        Ok(scheduler_url)
    }
}
