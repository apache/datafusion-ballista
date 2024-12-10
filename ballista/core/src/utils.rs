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

use crate::config::BallistaConfig;
use crate::error::{BallistaError, Result};
use crate::execution_plans::DistributedQueryExec;

use crate::extension::SessionConfigExt;
use crate::serde::scheduler::PartitionStats;
use crate::serde::BallistaLogicalExtensionCodec;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeVisitor};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{DdlStatement, LogicalPlan, TableScan};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{metrics, ExecutionPlan, RecordBatchStream};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use futures::StreamExt;
use log::error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs::File, pin::Pin};
use tonic::codegen::StdError;
use tonic::transport::{Channel, Error, Server};

/// Default session builder using the provided configuration
pub fn default_session_builder(
    config: SessionConfig,
) -> datafusion::common::Result<SessionState> {
    Ok(SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(Arc::new(RuntimeEnv::new(RuntimeConfig::default())?))
        .build())
}

pub fn default_config_producer() -> SessionConfig {
    SessionConfig::new_with_ballista()
}

/// Stream data to disk in Arrow IPC format
pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &str,
    disk_write_metric: &metrics::Time,
) -> Result<PartitionStats> {
    let file = File::create(path).map_err(|e| {
        error!("Failed to create partition file at {}: {:?}", path, e);
        BallistaError::IoError(e)
    })?;

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;

    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))?;

    let mut writer =
        StreamWriter::try_new_with_options(file, stream.schema().as_ref(), options)?;

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch.get_array_memory_size();
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;

        let timer = disk_write_metric.timer();
        writer.write(&batch)?;
        timer.done();
    }
    let timer = disk_write_metric.timer();
    writer.finish()?;
    timer.done();
    Ok(PartitionStats::new(
        Some(num_rows as u64),
        Some(num_batches),
        Some(num_bytes as u64),
    ))
}

pub async fn collect_stream(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

pub struct BallistaQueryPlanner<T: AsLogicalPlan> {
    scheduler_url: String,
    config: BallistaConfig,
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    local_planner: DefaultPhysicalPlanner,
    plan_repr: PhantomData<T>,
}

impl<T: 'static + AsLogicalPlan> BallistaQueryPlanner<T> {
    pub fn new(scheduler_url: String, config: BallistaConfig) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec: Arc::new(BallistaLogicalExtensionCodec::default()),
            local_planner: DefaultPhysicalPlanner::default(),
            plan_repr: PhantomData,
        }
    }

    pub fn with_extension(
        scheduler_url: String,
        config: BallistaConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            local_planner: DefaultPhysicalPlanner::default(),
            plan_repr: PhantomData,
        }
    }

    pub fn with_repr(
        scheduler_url: String,
        config: BallistaConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        plan_repr: PhantomData<T>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            plan_repr,
            local_planner: DefaultPhysicalPlanner::default(),
        }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan> QueryPlanner for BallistaQueryPlanner<T> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        log::debug!("create_physical_plan - plan: {:?}", logical_plan);
        // we inspect if plan scans local tables only,
        // like tables located in information_schema,
        // if that is the case, we run that plan
        // on this same context, not on cluster
        let mut local_run = LocalRun::default();
        let _ = logical_plan.visit(&mut local_run);

        if local_run.can_be_local {
            log::debug!("create_physical_plan - local run");

            self.local_planner
                .create_physical_plan(logical_plan, session_state)
                .await
        } else {
            match logical_plan {
                LogicalPlan::Ddl(DdlStatement::CreateExternalTable(_t)) => {
                    log::debug!("create_physical_plan - handling ddl statement");
                    Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
                }
                LogicalPlan::EmptyRelation(_) => {
                    log::debug!("create_physical_plan - handling empty exec");
                    Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
                }
                _ => {
                    log::debug!("create_physical_plan - handling general statement");

                    Ok(Arc::new(DistributedQueryExec::with_repr(
                        self.scheduler_url.clone(),
                        self.config.clone(),
                        logical_plan.clone(),
                        self.extension_codec.clone(),
                        self.plan_repr,
                        session_state.session_id().to_string(),
                    )))
                }
            }
        }
    }
}

pub async fn create_grpc_client_connection<D>(
    dst: D,
) -> std::result::Result<Channel, Error>
where
    D: std::convert::TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    let endpoint = tonic::transport::Endpoint::new(dst)?
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);
    endpoint.connect().await
}

pub fn create_grpc_server() -> Server {
    Server::builder()
        .timeout(Duration::from_secs(20))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keepalive_interval(Option::Some(Duration::from_secs(300)))
        .http2_keepalive_timeout(Option::Some(Duration::from_secs(20)))
}

pub fn collect_plan_metrics(plan: &dyn ExecutionPlan) -> Vec<MetricsSet> {
    let mut metrics_array = Vec::<MetricsSet>::new();
    if let Some(metrics) = plan.metrics() {
        metrics_array.push(metrics);
    }
    plan.children().iter().for_each(|c| {
        collect_plan_metrics(c.as_ref())
            .into_iter()
            .for_each(|e| metrics_array.push(e))
    });
    metrics_array
}

/// Given an interval in seconds, get the time in seconds before now
pub fn get_time_before(interval_seconds: u64) -> u64 {
    let now_epoch_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    now_epoch_ts
        .checked_sub(Duration::from_secs(interval_seconds))
        .unwrap_or_else(|| Duration::from_secs(0))
        .as_secs()
}

/// A Visitor which detect if query is using local tables,
/// such as tables located in `information_schema` and returns true
/// only if all scans are in from local tables
#[derive(Debug, Default)]
struct LocalRun {
    can_be_local: bool,
}

impl<'n> TreeNodeVisitor<'n> for LocalRun {
    type Node = LogicalPlan;

    fn f_down(
        &mut self,
        node: &'n Self::Node,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        match node {
            LogicalPlan::TableScan(TableScan { table_name, .. }) => match table_name {
                datafusion::sql::TableReference::Partial { schema, .. }
                | datafusion::sql::TableReference::Full { schema, .. }
                    if schema.as_ref() == "information_schema" =>
                {
                    self.can_be_local = true;
                    Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
                }
                _ => {
                    self.can_be_local = false;
                    Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop)
                }
            },
            _ => Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue),
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion::{
        common::tree_node::TreeNode,
        error::Result,
        execution::{
            runtime_env::{RuntimeConfig, RuntimeEnv},
            SessionStateBuilder,
        },
        prelude::{SessionConfig, SessionContext},
    };

    use crate::utils::LocalRun;

    fn context() -> SessionContext {
        let runtime_environment = RuntimeEnv::new(RuntimeConfig::new()).unwrap();

        let session_config = SessionConfig::new().with_information_schema(true);

        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime_environment.into())
            .with_default_features()
            .build();

        SessionContext::new_with_state(state)
    }

    #[tokio::test]
    async fn should_detect_show_table_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SHOW TABLES").await?;
        let lp = df.logical_plan();
        let mut local_run = LocalRun::default();

        lp.visit(&mut local_run).unwrap();

        assert!(local_run.can_be_local);

        Ok(())
    }

    #[tokio::test]
    async fn should_detect_select_from_information_schema_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SELECT * FROM information_schema.df_settings WHERE NAME LIKE 'ballista%'").await?;
        let lp = df.logical_plan();
        let mut local_run = LocalRun::default();

        lp.visit(&mut local_run).unwrap();

        assert!(local_run.can_be_local);

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_local_table() -> Result<()> {
        let ctx = context();
        ctx.sql("CREATE TABLE tt (c0 INT, c1 INT)")
            .await?
            .show()
            .await?;
        let df = ctx.sql("SELECT * FROM tt").await?;
        let lp = df.logical_plan();
        let mut local_run = LocalRun::default();

        lp.visit(&mut local_run).unwrap();

        assert!(!local_run.can_be_local);

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_external_table() -> Result<()> {
        let ctx = context();
        ctx.register_csv("tt", "tests/customer.csv", Default::default())
            .await?;
        let df = ctx.sql("SELECT * FROM tt").await?;
        let lp = df.logical_plan();
        let mut local_run = LocalRun::default();

        lp.visit(&mut local_run).unwrap();

        assert!(!local_run.can_be_local);

        Ok(())
    }
}
