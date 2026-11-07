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

//! Execution plans which delegate all communication with a Ballista cluster
//! to a Python client object, see `ballista.client.BallistaClient`.
//!
//! The plan is still encoded here, as it needs Ballista's logical extension
//! codec, while job submission, status tracking and result fetching are done
//! in Python. Results come back through the Arrow C stream interface.
//!
//! Unlike [DistributedQueryExec](ballista_core::execution_plans::DistributedQueryExec)
//! these plans can not leave the client process, as they hold a Python object.

use ballista_core::execution_plans::{
    execute_query_params, format_metrics_as_record_batch,
};
use ballista_core::planner::scans_only_information_schema;
use ballista_core::serde::protobuf::GetJobMetricsResult;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::context::{QueryPlanner, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::protobuf::LogicalPlanNode;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule};
use std::sync::{Arc, Mutex};

/// Python object implementing the client protocol of
/// `ballista.client.BallistaClient`.
#[derive(Debug)]
pub(crate) struct PythonClient(Py<PyAny>);

impl PythonClient {
    pub(crate) fn new(client: Py<PyAny>) -> Self {
        Self(client)
    }

    /// Submits the query, returning the Python job and the reader of its
    /// results. Blocks until the scheduler has accepted the job.
    fn execute(
        &self,
        query: Vec<u8>,
        schema: SchemaRef,
    ) -> Result<(String, Py<PyAny>, ArrowArrayStreamReader)> {
        let schema_ipc = schema_to_ipc(&schema)?;
        Python::attach(|py| {
            let params = py
                .import("ballista.proto.ballista_pb2")?
                .getattr("ExecuteQueryParams")?
                .call_method1("FromString", (PyBytes::new(py, &query),))?;
            let schema = py
                .import("pyarrow.ipc")?
                .call_method1("open_stream", (PyBytes::new(py, &schema_ipc),))?
                .getattr("schema")?;
            let job = self.0.bind(py).call_method1("execute", (params, schema))?;
            let job_id: String = job.getattr("job_id")?.extract()?;

            let capsule = job.call_method0("__arrow_c_stream__")?;
            let capsule = capsule.cast::<PyCapsule>()?;
            let stream = capsule
                .pointer_checked(Some(c"arrow_array_stream"))?
                .cast::<FFI_ArrowArrayStream>();
            // Moves the stream out, leaving a released one in the capsule.
            let stream = unsafe { FFI_ArrowArrayStream::from_raw(stream.as_ptr()) };
            let reader = ArrowArrayStreamReader::try_new(stream)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok::<_, PyErr>((job_id, job.unbind(), reader))
        })
        .map_err(py_error)
    }

    fn job_metrics(&self, job_id: &str) -> Result<GetJobMetricsResult> {
        let bytes: Vec<u8> = Python::attach(|py| {
            self.0
                .bind(py)
                .call_method1("job_metrics", (job_id,))?
                .call_method0("SerializeToString")?
                .extract()
        })
        .map_err(py_error)?;
        GetJobMetricsResult::decode(bytes.as_slice()).map_err(|e| {
            DataFusionError::Execution(format!("failed to decode job metrics: {e}"))
        })
    }
}

fn py_error(error: PyErr) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

fn schema_to_ipc(schema: &Schema) -> Result<Vec<u8>> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema)?;
    writer.finish()?;
    Ok(writer.into_inner()?)
}

/// Closes the Python job when the result stream is dropped, cancelling it if
/// it has not finished.
struct JobGuard(Option<Py<PyAny>>);

impl Drop for JobGuard {
    fn drop(&mut self) {
        let Some(job) = self.0.take() else {
            return;
        };
        let close = move || {
            Python::attach(|py| {
                if let Err(e) = job.bind(py).call_method0("close") {
                    log::warn!("failed to close Ballista job: {e}");
                }
            })
        };
        // `close` may call the scheduler, keep it off the async workers
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn_blocking(close);
            }
            Err(_) => close(),
        }
    }
}

fn compute_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Sends a logical plan to a Ballista scheduler through a Python client and
/// streams back the results the client fetches.
#[derive(Debug, Clone)]
pub(crate) struct PythonDistributedQueryExec {
    plan: LogicalPlan,
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    session_id: String,
    client: Arc<PythonClient>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// The scheduler job id after the query has been accepted.
    job_id: Arc<Mutex<Option<String>>>,
}

impl PythonDistributedQueryExec {
    pub(crate) fn new(
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        session_id: String,
        client: Arc<PythonClient>,
    ) -> Self {
        let properties =
            Arc::new(compute_properties(plan.schema().as_arrow().clone().into()));
        Self {
            plan,
            extension_codec,
            session_id,
            client,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            job_id: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn job_id(&self) -> Option<String> {
        self.job_id.lock().expect("job id lock").clone()
    }
}

impl DisplayAs for PythonDistributedQueryExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PythonDistributedQueryExec")
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for PythonDistributedQueryExec {
    fn name(&self) -> &str {
        "PythonDistributedQueryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);

        let query = execute_query_params::<LogicalPlanNode>(
            &self.plan,
            self.extension_codec.as_ref(),
            context.session_config(),
            self.session_id.clone(),
        )?
        .encode_to_vec();

        let metric_row_count = MetricBuilder::new(&self.metrics).output_rows(partition);
        let metric_total_bytes =
            MetricBuilder::new(&self.metrics).counter("transferred_bytes", partition);

        let client = Arc::clone(&self.client);
        let job_id = Arc::clone(&self.job_id);
        let schema = self.schema();
        let stream = futures::stream::once(async move {
            let (id, job, reader) =
                tokio::task::spawn_blocking(move || client.execute(query, schema))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))??;
            *job_id.lock().expect("job id lock") = Some(id);
            Ok::<_, DataFusionError>(record_batches(reader, JobGuard(Some(job))))
        })
        .try_flatten()
        .inspect(move |batch| {
            if let Ok(batch) = batch {
                metric_total_bytes.add(batch.get_array_memory_size());
                metric_row_count.add(batch.num_rows());
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Reads `reader` batch by batch on the blocking pool, as every batch is
/// produced by Python, and closes the job once the stream is dropped.
fn record_batches(
    reader: ArrowArrayStreamReader,
    guard: JobGuard,
) -> futures::stream::BoxStream<'static, Result<RecordBatch>> {
    futures::stream::try_unfold(Some((reader, guard)), |state| async move {
        let Some((mut reader, guard)) = state else {
            return Ok(None);
        };
        let (reader, next) = tokio::task::spawn_blocking(move || {
            let next = reader.next();
            (reader, next)
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        match next {
            Some(batch) => Ok(Some((batch?, Some((reader, guard))))),
            None => Ok(None),
        }
    })
    .boxed()
}

/// Runs a [PythonDistributedQueryExec] and returns the metrics of the job it
/// ran, fetched through the same Python client.
#[derive(Debug, Clone)]
pub(crate) struct PythonDistributedExplainAnalyzeExec {
    query_exec: Arc<dyn ExecutionPlan>,
    client: Arc<PythonClient>,
    schema: SchemaRef,
    verbose: bool,
    properties: Arc<PlanProperties>,
}

impl PythonDistributedExplainAnalyzeExec {
    pub(crate) fn new(
        query_exec: Arc<PythonDistributedQueryExec>,
        client: Arc<PythonClient>,
        schema: SchemaRef,
        verbose: bool,
    ) -> Self {
        let properties = Arc::new(compute_properties(Arc::clone(&schema)));
        Self {
            query_exec,
            client,
            schema,
            verbose,
            properties,
        }
    }

    pub(crate) fn verbose(&self) -> bool {
        self.verbose
    }
}

impl DisplayAs for PythonDistributedExplainAnalyzeExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PythonDistributedExplainAnalyzeExec")
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for PythonDistributedExplainAnalyzeExec {
    fn name(&self) -> &str {
        "PythonDistributedExplainAnalyzeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.query_exec]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(query_exec), true) = (children.pop(), children.is_empty()) else {
            return internal_err!(
                "PythonDistributedExplainAnalyzeExec expected one child"
            );
        };
        if query_exec
            .downcast_ref::<PythonDistributedQueryExec>()
            .is_none()
        {
            return internal_err!(
                "PythonDistributedExplainAnalyzeExec requires a PythonDistributedQueryExec child"
            );
        }
        Ok(Arc::new(Self {
            query_exec,
            ..self.as_ref().clone()
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);

        let query_exec = Arc::clone(&self.query_exec);
        let client = Arc::clone(&self.client);
        let schema = Arc::clone(&self.schema);

        let stream = futures::stream::once(async move {
            let mut child_stream = query_exec.execute(partition, ctx)?;
            while let Some(batch) = child_stream.next().await {
                batch?;
            }

            let job_id = query_exec
                .downcast_ref::<PythonDistributedQueryExec>()
                .and_then(|exec| exec.job_id())
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Distributed query completed without exposing a job_id".into(),
                    )
                })?;
            let job_metrics =
                tokio::task::spawn_blocking(move || client.job_metrics(&job_id))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))??;
            format_metrics_as_record_batch(&job_metrics, schema)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }
}

/// Query planner which plans every query for the cluster behind a Python
/// client, the counterpart of
/// [BallistaQueryPlanner](ballista_core::planner::BallistaQueryPlanner).
pub(crate) struct PythonClientQueryPlanner {
    client: Arc<PythonClient>,
    logical_codec: Arc<dyn LogicalExtensionCodec>,
    local_planner: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for PythonClientQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("PythonClientQueryPlanner")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

impl PythonClientQueryPlanner {
    pub(crate) fn new(
        client: Arc<PythonClient>,
        logical_codec: Arc<dyn LogicalExtensionCodec>,
    ) -> Self {
        Self {
            client,
            logical_codec,
            local_planner: DefaultPhysicalPlanner::default(),
        }
    }

    fn query_exec(
        &self,
        plan: LogicalPlan,
        session: &dyn Session,
    ) -> PythonDistributedQueryExec {
        PythonDistributedQueryExec::new(
            plan,
            Arc::clone(&self.logical_codec),
            session.session_id().to_string(),
            Arc::clone(&self.client),
        )
    }
}

#[async_trait::async_trait]
impl QueryPlanner for PythonClientQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Same split as `BallistaQueryPlanner`: plans which only describe
        // this context's catalog run locally.
        if scans_only_information_schema(logical_plan)? {
            return self
                .local_planner
                .create_physical_plan(logical_plan, session)
                .await;
        }
        match logical_plan {
            LogicalPlan::EmptyRelation(_) => {
                Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
            }
            LogicalPlan::DescribeTable(_) => {
                self.local_planner
                    .create_physical_plan(logical_plan, session)
                    .await
            }
            LogicalPlan::Analyze(analyze) => {
                let query_exec =
                    Arc::new(self.query_exec(analyze.input.as_ref().clone(), session));
                Ok(Arc::new(PythonDistributedExplainAnalyzeExec::new(
                    query_exec,
                    Arc::clone(&self.client),
                    Arc::clone(analyze.schema.inner()),
                    analyze.verbose,
                )))
            }
            _ => Ok(Arc::new(self.query_exec(logical_plan.clone(), session))),
        }
    }
}
