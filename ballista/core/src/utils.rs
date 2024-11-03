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
    BALLISTA_STANDALONE_PARALLELISM,
};
use crate::error::{BallistaError, Result};
use crate::execution_plans::{
    DistributedQueryExec, ShuffleWriterExec, UnresolvedShuffleExec,
};
use crate::object_store_registry::with_object_store_registry;
use crate::serde::protobuf::KeyValuePair;
use crate::serde::scheduler::PartitionStats;
use crate::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeVisitor};
use datafusion::datasource::physical_plan::{CsvExec, ParquetExec};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{
    QueryPlanner, SessionConfig, SessionContext, SessionState,
};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{DdlStatement, LogicalPlan, TableScan};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{metrics, ExecutionPlan, RecordBatchStream};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::LogicalPlanNode;
use futures::StreamExt;
use log::error;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs::File, pin::Pin};
use tonic::codegen::StdError;
use tonic::transport::{Channel, Error, Server};

/// Default session builder using the provided configuration
pub fn default_session_builder(config: SessionConfig) -> SessionState {
    SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(Arc::new(
            RuntimeEnv::new(with_object_store_registry(RuntimeConfig::default()))
                .unwrap(),
        ))
        .build()
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

pub fn produce_diagram(filename: &str, stages: &[Arc<ShuffleWriterExec>]) -> Result<()> {
    let write_file = File::create(filename)?;
    let mut w = BufWriter::new(&write_file);
    writeln!(w, "digraph G {{")?;

    // draw stages and entities
    for stage in stages {
        writeln!(w, "\tsubgraph cluster{} {{", stage.stage_id())?;
        writeln!(w, "\t\tlabel = \"Stage {}\";", stage.stage_id())?;
        let mut id = AtomicUsize::new(0);
        build_exec_plan_diagram(
            &mut w,
            stage.children()[0].as_ref(),
            stage.stage_id(),
            &mut id,
            true,
        )?;
        writeln!(w, "\t}}")?;
    }

    // draw relationships
    for stage in stages {
        let mut id = AtomicUsize::new(0);
        build_exec_plan_diagram(
            &mut w,
            stage.children()[0].as_ref(),
            stage.stage_id(),
            &mut id,
            false,
        )?;
    }

    write!(w, "}}")?;
    Ok(())
}

fn build_exec_plan_diagram(
    w: &mut BufWriter<&File>,
    plan: &dyn ExecutionPlan,
    stage_id: usize,
    id: &mut AtomicUsize,
    draw_entity: bool,
) -> Result<usize> {
    let operator_str = if plan.as_any().downcast_ref::<AggregateExec>().is_some() {
        "AggregateExec"
    } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
        "SortExec"
    } else if plan.as_any().downcast_ref::<ProjectionExec>().is_some() {
        "ProjectionExec"
    } else if plan.as_any().downcast_ref::<HashJoinExec>().is_some() {
        "HashJoinExec"
    } else if plan.as_any().downcast_ref::<ParquetExec>().is_some() {
        "ParquetExec"
    } else if plan.as_any().downcast_ref::<CsvExec>().is_some() {
        "CsvExec"
    } else if plan.as_any().downcast_ref::<FilterExec>().is_some() {
        "FilterExec"
    } else if plan.as_any().downcast_ref::<ShuffleWriterExec>().is_some() {
        "ShuffleWriterExec"
    } else if plan
        .as_any()
        .downcast_ref::<UnresolvedShuffleExec>()
        .is_some()
    {
        "UnresolvedShuffleExec"
    } else if plan
        .as_any()
        .downcast_ref::<CoalesceBatchesExec>()
        .is_some()
    {
        "CoalesceBatchesExec"
    } else if plan
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .is_some()
    {
        "CoalescePartitionsExec"
    } else {
        println!("Unknown: {plan:?}");
        "Unknown"
    };

    let node_id = id.load(Ordering::SeqCst);
    id.store(node_id + 1, Ordering::SeqCst);

    if draw_entity {
        writeln!(
            w,
            "\t\tstage_{stage_id}_exec_{node_id} [shape=box, label=\"{operator_str}\"];"
        )?;
    }
    for child in plan.children() {
        if let Some(shuffle) = child.as_any().downcast_ref::<UnresolvedShuffleExec>() {
            if !draw_entity {
                writeln!(
                    w,
                    "\tstage_{}_exec_1 -> stage_{}_exec_{};",
                    shuffle.stage_id, stage_id, node_id
                )?;
            }
        } else {
            // relationships within same entity
            let child_id =
                build_exec_plan_diagram(w, child.as_ref(), stage_id, id, draw_entity)?;
            if draw_entity {
                writeln!(
                    w,
                    "\t\tstage_{stage_id}_exec_{child_id} -> stage_{stage_id}_exec_{node_id};"
                )?;
            }
        }
    }
    Ok(node_id)
}

/// Create a client DataFusion context that uses the BallistaQueryPlanner to send logical plans
/// to a Ballista scheduler
pub fn create_df_ctx_with_ballista_query_planner<T: 'static + AsLogicalPlan>(
    scheduler_url: String,
    session_id: String,
    config: &BallistaConfig,
) -> SessionContext {
    // TODO: put ballista configuration as part of sessions state
    //       planner can get it from there.
    //       This would make it changeable during run time
    //       using SQL SET statement
    let planner: Arc<BallistaQueryPlanner<T>> =
        Arc::new(BallistaQueryPlanner::new(scheduler_url, config.clone()));

    let session_config = SessionConfig::new_with_ballista()
        .with_information_schema(true)
        .with_option_extension(config.clone());

    let session_state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config)
        .with_runtime_env(Arc::new(
            RuntimeEnv::new(with_object_store_registry(RuntimeConfig::default()))
                .unwrap(),
        ))
        .with_query_planner(planner)
        .with_session_id(session_id)
        .build();
    // the SessionContext created here is the client side context, but the session_id is from server side.
    SessionContext::new_with_state(session_state)
}

pub trait SessionStateExt {
    fn new_ballista_state(
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<SessionState>;
    fn upgrade_for_ballista(
        self,
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<SessionState>;
    #[deprecated]
    fn ballista_config(&self) -> BallistaConfig;
}

impl SessionStateExt for SessionState {
    fn ballista_config(&self) -> BallistaConfig {
        self.config()
            .options()
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_else(BallistaConfig::default)
    }

    fn new_ballista_state(
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<SessionState> {
        let config = BallistaConfig::default();

        let planner =
            BallistaQueryPlanner::<LogicalPlanNode>::new(scheduler_url, config.clone());

        let session_config = SessionConfig::new()
            .with_information_schema(true)
            .with_option_extension(config.clone())
            // Ballista disables this option
            .with_round_robin_repartition(false);

        let runtime_config = RuntimeConfig::default();
        let runtime_env = RuntimeEnv::new(runtime_config)?;
        let session_state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_query_planner(Arc::new(planner))
            .with_session_id(session_id)
            .build();

        Ok(session_state)
    }

    fn upgrade_for_ballista(
        self,
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<SessionState> {
        let codec_logical = self.config().ballista_logical_extension_codec();
        let planner_override = self.config().ballista_query_planner();

        let new_config = self
            .config()
            .options()
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_else(BallistaConfig::default);

        let session_config = self
            .config()
            .clone()
            .with_option_extension(new_config.clone())
            // Ballista disables this option
            .with_round_robin_repartition(false);

        let builder = SessionStateBuilder::new_from_existing(self)
            .with_config(session_config)
            .with_session_id(session_id);

        let builder = match planner_override {
            Some(planner) => builder.with_query_planner(planner),
            None => {
                let planner = BallistaQueryPlanner::<LogicalPlanNode>::with_extension(
                    scheduler_url,
                    new_config,
                    codec_logical,
                );
                builder.with_query_planner(Arc::new(planner))
            }
        };

        Ok(builder.build())
    }
}

pub trait SessionConfigExt {
    /// Creates session config which has
    /// ballista configuration initialized
    fn new_with_ballista() -> SessionConfig;

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

    fn ballista_standalone_parallelism(&self) -> usize;

    fn ballista_grpc_client_max_message_size(&self) -> usize;

    fn to_key_value_pairs(&self) -> Vec<KeyValuePair>;

    fn update_from_key_value_pair(self, key_value_pairs: &[KeyValuePair]) -> Self;

    fn with_ballista_job_name(self, job_name: &str) -> Self;

    fn with_ballista_grpc_client_max_message_size(self, max_size: usize) -> Self;

    fn with_ballista_standalone_parallelism(self, parallelism: usize) -> Self;

    fn update_from_key_value_pair_mut(&mut self, key_value_pairs: &[KeyValuePair]);
}

impl SessionConfigExt for SessionConfig {
    fn new_with_ballista() -> SessionConfig {
        SessionConfig::new()
            .with_option_extension(BallistaConfig::default())
            .with_target_partitions(16)
            .with_round_robin_repartition(false)
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

    fn to_key_value_pairs(&self) -> Vec<KeyValuePair> {
        self.options()
            .entries()
            .iter()
            .filter(|v| v.value.is_some())
            .map(
                // TODO MM make value optional
                |datafusion::config::ConfigEntry { key, value, .. }| {
                    log::trace!(
                        "sending configuration key: `{}`, value`{:?}`",
                        key,
                        value
                    );
                    KeyValuePair {
                        key: key.to_owned(),
                        value: value.clone().unwrap(),
                    }
                },
            )
            .collect()
    }

    fn update_from_key_value_pair(self, key_value_pairs: &[KeyValuePair]) -> Self {
        let mut s = self;
        for KeyValuePair { key, value } in key_value_pairs {
            log::trace!(
                "setting up configuration key: `{}`, value: `{}`",
                key,
                value
            );
            if let Err(e) = s.options_mut().set(key, value) {
                log::warn!(
                    "could not set configuration key: `{}`, value: `{}`, reason: {}",
                    key,
                    value,
                    e.to_string()
                )
            }
        }
        s
    }

    fn update_from_key_value_pair_mut(&mut self, key_value_pairs: &[KeyValuePair]) {
        for KeyValuePair { key, value } in key_value_pairs {
            log::trace!(
                "setting up configuration key : `{}`, value: `{}`",
                key,
                value
            );
            if let Err(e) = self.options_mut().set(key, value) {
                log::warn!(
                    "could not set configuration key: `{}`, value: `{}`, reason: {}",
                    key,
                    value,
                    e.to_string()
                )
            }
        }
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
            SessionState, SessionStateBuilder,
        },
        prelude::{SessionConfig, SessionContext},
    };

    use crate::{
        config::BALLISTA_JOB_NAME,
        utils::{LocalRun, SessionStateExt},
    };

    use super::SessionConfigExt;

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

    // Ballista disables round robin repatriations
    #[tokio::test]
    async fn should_disable_round_robin_repartition() {
        let state = SessionState::new_ballista_state(
            "scheduler_url".to_string(),
            "session_id".to_string(),
        )
        .unwrap();

        assert!(!state.config().round_robin_repartition());

        let state = SessionStateBuilder::new().build();

        assert!(state.config().round_robin_repartition());
        let state = state
            .upgrade_for_ballista("scheduler_url".to_string(), "session_id".to_string())
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
