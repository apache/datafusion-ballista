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

//! Value-index tap. Sits above a partition-locally-ordered shuffle
//! producer (ORRE today; any operator whose `output_ordering()` is
//! `Some` tomorrow), samples the ORDER BY expression at batch
//! boundaries, and writes a `.value.idx` Arrow IPC file next to the
//! producer's data file so a reader with any assigned cut range can
//! seek in without full-file read amplification.
//!
//! Refuses to construct on unordered input (e.g. URRE, which advertises
//! no ordering): a value-range index over unordered data would
//! degenerate to "read everything". Enforced at
//! [`ValueIndexExec::try_new`], not at execute time.
//!
//! File layout (matches the file-naming convention `ShuffleWriterExec`
//! uses so the sidecar sits next to the data file it describes):
//!
//! ```text
//! {work_dir}/{job_id}/{stage_id}/{partition_id}/data-{task_id}.value.idx
//! ```
//!
//! Format is a single Arrow IPC file with one RecordBatch of length L
//! (leaves), schema `[sampled_row: UInt64 NOT NULL, expr_0: <type>, ...]`,
//! and schema metadata: `version="1"`, `total_row_count="<u64>"`,
//! `row_index_scheme="full_address"`.

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::common::{
    DataFusionError, Result, ScalarValue, Statistics, internal_err,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt, ready};

use crate::JobId;
use crate::execution_plans::create_shuffle_path;

const VERSION_KEY: &str = "version";
const VERSION_VALUE: &str = "1";
const TOTAL_ROW_COUNT_KEY: &str = "total_row_count";
const ROW_INDEX_SCHEME_KEY: &str = "row_index_scheme";
const ROW_INDEX_SCHEME_FULL_ADDRESS: &str = "full_address";

/// Passthrough tap that samples the input's ORDER BY expression at
/// batch boundaries and writes a `.value.idx` file next to the
/// producer's data file on EOS.
pub struct ValueIndexExec {
    job_id: JobId,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    work_dir: String,
    task_id: usize,
    global_output_partition_ids: Vec<usize>,
    order_by: Vec<PhysicalSortExpr>,
    /// Schema of the `.value.idx` file (without schema metadata — that
    /// gets stamped at finalize time when `total_row_count` is known).
    value_index_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl ValueIndexExec {
    /// Wrap `plan`, resolving the ORDER BY expression from the plan's
    /// claimed output ordering. Fails if the plan has no ordering — a
    /// value-range index over an unordered stream would degenerate to
    /// "read everything", so this operator refuses to construct there.
    ///
    /// Constructor signature mirrors [`super::ShuffleWriterExec::try_new`] so
    /// the scheduler/executor can stamp the same `job_id`, `stage_id`,
    /// `work_dir` values that name the corresponding data file — the
    /// sidecar path is derived deterministically from them.
    pub fn try_new(
        job_id: JobId,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
    ) -> Result<Self> {
        let Some(ordering) = plan.output_ordering() else {
            return internal_err!(
                "ValueIndexExec requires ordered input — child plan claims no ordering"
            );
        };
        let order_by: Vec<PhysicalSortExpr> = ordering.iter().cloned().collect();

        let input_schema = plan.schema();
        let mut fields = vec![Field::new("sampled_row", DataType::UInt64, false)];
        for (expr_idx, sort_expr) in order_by.iter().enumerate() {
            let dtype = sort_expr.expr.data_type(&input_schema)?;
            let nullable = sort_expr.expr.nullable(&input_schema)?;
            fields.push(Field::new(format!("expr_{expr_idx}"), dtype, nullable));
        }
        let value_index_schema = Arc::new(Schema::new(fields));

        let child_partition_count =
            plan.properties().output_partitioning().partition_count();
        let default_partition_slice: Vec<usize> = (0..child_partition_count).collect();

        let properties = Arc::new(PlanProperties::new(
            plan.equivalence_properties().clone(),
            plan.output_partitioning().clone(),
            plan.pipeline_behavior(),
            plan.boundedness(),
        ));

        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
            task_id: 0,
            global_output_partition_ids: default_partition_slice,
            order_by,
            value_index_schema,
            properties,
        })
    }

    /// Bind this tap to a specific `task_id`. Called by the executor
    /// after decoding the plan so tap instances from different tasks in
    /// the same stage don't collide on file paths.
    pub fn with_task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    /// Task id (append-order slot within the stage) this tap instance
    /// is bound to.
    pub fn task_id(&self) -> usize {
        self.task_id
    }

    /// Bind this tap to the task's assigned global partition slice.
    /// Position `i` of the local plan corresponds to `slice[i]` globally.
    pub fn with_global_output_partition_ids(
        mut self,
        global_output_partition_ids: Vec<usize>,
    ) -> Self {
        self.global_output_partition_ids = global_output_partition_ids;
        self
    }

    /// Global partition ids this task's restricted plan covers.
    pub fn global_output_partition_ids(&self) -> &[usize] {
        &self.global_output_partition_ids
    }

    /// The job id this tap is bound to.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// The stage id this tap is bound to.
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// The ORDER BY expressions the tap will sample, resolved from the
    /// input's output ordering at construction time.
    pub fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    /// Derive the sidecar path for a given local output partition. The
    /// data file's path is what `create_shuffle_path` produces; the
    /// sidecar simply swaps `.arrow` for `.value.idx`.
    fn resolve_output_path(&self, partition: usize) -> Result<PathBuf> {
        let global_partition = self.global_output_partition_ids[partition];
        let data_path = create_shuffle_path(
            &self.work_dir,
            &self.job_id,
            self.stage_id,
            global_partition,
            Some(self.task_id as u64),
            false,
        )?;
        Ok(data_path.with_extension("value.idx"))
    }
}

impl Debug for ValueIndexExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueIndexExec")
            .field("job_id", &self.job_id)
            .field("stage_id", &self.stage_id)
            .field("task_id", &self.task_id)
            .finish()
    }
}

impl DisplayAs for ValueIndexExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(f, "ValueIndexExec"),
        }
    }
}

impl ExecutionPlan for ValueIndexExec {
    fn name(&self) -> &str {
        "ValueIndexExec"
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [plan] = children.as_slice() else {
            return internal_err!(
                "ValueIndexExec expects exactly one child, got {}",
                children.len()
            );
        };
        let mut new = ValueIndexExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            plan.clone(),
            self.work_dir.clone(),
        )?;
        new.task_id = self.task_id;
        new.global_output_partition_ids = self.global_output_partition_ids.clone();
        Ok(Arc::new(new))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input = self.plan.execute(partition, ctx)?;
        let output_path = self.resolve_output_path(partition)?;
        let stream = ValueIndexStream {
            schema: schema.clone(),
            input,
            order_by: self.order_by.clone(),
            value_index_schema: self.value_index_schema.clone(),
            output_path,
            sampled_rows: Vec::new(),
            sampled_values: vec![Vec::new(); self.order_by.len()],
            rows_seen: 0,
            finalized: false,
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

struct ValueIndexStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    order_by: Vec<PhysicalSortExpr>,
    value_index_schema: SchemaRef,
    output_path: PathBuf,
    /// Row indices (from stream start) at which samples were taken. One
    /// entry per leaf. Today the sampler emits at every non-empty batch
    /// boundary; that policy is a stream-level concern, not a naming
    /// concern.
    sampled_rows: Vec<u64>,
    /// ORDER BY values at each sample point, laid out column-major so the
    /// finalize path can hand each column straight to an Arrow builder.
    /// Outer index = ORDER BY expression, inner = sample.
    sampled_values: Vec<Vec<ScalarValue>>,
    rows_seen: u64,
    finalized: bool,
}

impl Stream for ValueIndexStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finalized {
            return Poll::Ready(None);
        }
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) if batch.num_rows() > 0 => {
                let sampled_row = self.rows_seen;
                let values = match sample_row(&batch, &self.order_by, 0) {
                    Ok(values) => values,
                    Err(err) => return Poll::Ready(Some(Err(err))),
                };
                self.sampled_rows.push(sampled_row);
                for (expr_idx, sample) in values.into_iter().enumerate() {
                    self.sampled_values[expr_idx].push(sample);
                }
                self.rows_seen += batch.num_rows() as u64;
                Poll::Ready(Some(Ok(batch)))
            }
            Some(other) => Poll::Ready(Some(other)),
            None => {
                self.finalized = true;
                if let Err(err) = self.write_index_file() {
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None)
            }
        }
    }
}

impl ValueIndexStream {
    fn write_index_file(&mut self) -> Result<()> {
        if self.sampled_rows.is_empty() {
            // No leaves — no file. Reader detects absence and falls back to
            // the sketch-overlap read path.
            return Ok(());
        }
        if let Some(parent) = self.output_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                DataFusionError::Execution(format!(
                    "ValueIndexExec: create_dir_all({}) failed: {e}",
                    parent.display()
                ))
            })?;
        }

        let metadata: HashMap<String, String> = HashMap::from([
            (VERSION_KEY.to_string(), VERSION_VALUE.to_string()),
            (TOTAL_ROW_COUNT_KEY.to_string(), self.rows_seen.to_string()),
            (
                ROW_INDEX_SCHEME_KEY.to_string(),
                ROW_INDEX_SCHEME_FULL_ADDRESS.to_string(),
            ),
        ]);
        let schema_with_metadata = Arc::new(Schema::new_with_metadata(
            self.value_index_schema.fields().clone(),
            metadata,
        ));

        let sampled_row_arr: ArrayRef =
            Arc::new(UInt64Array::from(std::mem::take(&mut self.sampled_rows)));
        let mut arrays = Vec::with_capacity(self.sampled_values.len() + 1);
        arrays.push(sampled_row_arr);
        for col_values in std::mem::take(&mut self.sampled_values) {
            arrays.push(ScalarValue::iter_to_array(col_values)?);
        }
        let batch = RecordBatch::try_new(schema_with_metadata.clone(), arrays)?;

        let file = std::fs::File::create(&self.output_path).map_err(|e| {
            DataFusionError::Execution(format!(
                "ValueIndexExec: create({}) failed: {e}",
                self.output_path.display()
            ))
        })?;
        let mut writer = FileWriter::try_new(file, schema_with_metadata.as_ref())?;
        writer.write(&batch)?;
        writer.finish()?;
        Ok(())
    }
}

impl RecordBatchStream for ValueIndexStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Evaluate each ORDER BY expression on `batch` and return the value at
/// `row`, in order. `row` is a batch-local index; callers translate from
/// their own sampling policy (batch-boundary today, stride tomorrow).
fn sample_row(
    batch: &RecordBatch,
    order_by: &[PhysicalSortExpr],
    row: usize,
) -> Result<Vec<ScalarValue>> {
    let mut out = Vec::with_capacity(order_by.len());
    for sort_expr in order_by {
        let arr = sort_expr
            .expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        out.push(ScalarValue::try_from_array(&arr, row)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::{
        OrderedRangeRepartitionExec, RuntimeStatsExec, ShuffleWriterExec,
    };
    use datafusion::arrow::array::{Float64Array, Int64Array, StructArray};
    use datafusion::arrow::ipc::reader::FileReader;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use datafusion::{arrow::compute::SortOptions, physical_expr::LexOrdering};
    use tempfile::TempDir;

    fn schema_v2_id() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("v2", DataType::Float64, false),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn batch(schema: &Arc<Schema>, keys: Vec<f64>, ids: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(keys)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
        .unwrap()
    }

    fn asc(schema: &Schema, name: &str) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col(name, schema).unwrap(),
            options: SortOptions::default(),
        }
    }

    #[test]
    fn try_new_rejects_unordered_input() {
        let schema = schema_v2_id();
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema, None).unwrap();
        let err = ValueIndexExec::try_new(JobId::new("j"), 0, source, String::new())
            .expect_err("unordered input must be rejected");
        assert!(
            err.to_string().contains("child plan claims no ordering"),
            "got: {err}"
        );
    }

    #[test]
    fn try_new_saves_order_by_from_child() {
        let schema = schema_v2_id();
        let source = MemorySourceConfig::try_new_exec(
            &[vec![batch(&schema, vec![1.0], vec![1])]],
            schema.clone(),
            None,
        )
        .unwrap();
        let sort_expr = asc(&schema, "v2");
        let sort_lex = LexOrdering::new(vec![sort_expr.clone()]).unwrap();
        let sorted: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(sort_lex, source).with_preserve_partitioning(true));
        let tap =
            ValueIndexExec::try_new(JobId::new("j"), 0, sorted, String::new()).unwrap();
        assert_eq!(tap.order_by().len(), 1);
        assert_eq!(tap.order_by()[0].expr.as_ref(), sort_expr.expr.as_ref());
    }

    /// Datasource → Sort/preserve → RuntimeStats → ORRE → ValueIndexExec →
    /// ShuffleWriter(passthrough). Drives all K output partitions
    /// concurrently, asserts row conservation, and reads back each
    /// per-partition `.value.idx` file to verify its shape and contents.
    #[tokio::test]
    async fn pipeline_writes_value_idx_per_partition() -> Result<()> {
        let schema = schema_v2_id();

        // Two pre-sorted input partitions.
        let input_batches: Vec<Vec<RecordBatch>> = vec![
            vec![batch(&schema, vec![1.0, 2.0, 3.0, 4.0], vec![1, 2, 3, 4])],
            vec![batch(&schema, vec![1.5, 2.5, 3.5, 4.5], vec![5, 6, 7, 8])],
        ];
        let expected_rows: u64 = input_batches
            .iter()
            .flat_map(|p| p.iter().map(|b| b.num_rows() as u64))
            .sum();

        let source =
            MemorySourceConfig::try_new_exec(&input_batches, schema.clone(), None)?;

        let sort_expr = asc(&schema, "v2");
        let sort_lex = LexOrdering::new(vec![sort_expr.clone()]).unwrap();
        let sorted: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(sort_lex, source).with_preserve_partitioning(true));

        let stats: Arc<dyn ExecutionPlan> = Arc::new(RuntimeStatsExec::try_new(
            sorted,
            Some(vec![sort_expr.clone()]),
        )?);

        let k_output = 3;
        let orre: Arc<dyn ExecutionPlan> = Arc::new(
            OrderedRangeRepartitionExec::try_new(stats, vec![sort_expr], k_output)?,
        );

        let work_dir = TempDir::new()?;
        let job_id = JobId::new("value-index-test");
        let stage_id = 0;
        let work_dir_str = work_dir.path().to_str().unwrap().to_owned();

        // Tap and writer share the same (job_id, stage_id, work_dir) so
        // the .value.idx files land next to the .arrow data files.
        let tap: Arc<dyn ExecutionPlan> = Arc::new(ValueIndexExec::try_new(
            job_id.clone(),
            stage_id,
            orre,
            work_dir_str.clone(),
        )?);
        let writer = Arc::new(ShuffleWriterExec::try_new(
            job_id.clone(),
            stage_id,
            tap,
            work_dir_str.clone(),
        )?);

        let ctx = Arc::new(SessionContext::new()).task_ctx();
        let mut handles = Vec::with_capacity(k_output);
        for p in 0..k_output {
            let writer = writer.clone();
            let ctx = ctx.clone();
            handles.push(tokio::spawn(async move {
                let mut stream = writer.execute(p, ctx)?;
                let mut batches = Vec::new();
                while let Some(b) =
                    futures::StreamExt::next(&mut stream).await.transpose()?
                {
                    batches.push(b);
                }
                Ok::<_, DataFusionError>(batches)
            }));
        }
        let mut metadata_batches = Vec::new();
        for h in handles {
            metadata_batches.extend(h.await.unwrap()?);
        }

        // Row conservation across the pipeline.
        let total_written: u64 = metadata_batches
            .iter()
            .flat_map(|b| {
                let stats = b.column(3).as_any().downcast_ref::<StructArray>().unwrap();
                let num_rows = stats
                    .column_by_name("num_rows")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .clone();
                (0..b.num_rows()).map(move |i| num_rows.value(i))
            })
            .sum();
        assert_eq!(expected_rows, total_written, "row conservation violated");

        // Every partition should have produced a .value.idx file next to
        // its .arrow data file. Read each back and check invariants.
        let mut total_indexed_rows: u64 = 0;
        for p in 0..k_output {
            let idx_path = work_dir
                .path()
                .join(job_id.as_str())
                .join(stage_id.to_string())
                .join(p.to_string())
                .join("data-0.value.idx");
            assert!(
                idx_path.exists(),
                "expected value-index file at {}",
                idx_path.display()
            );

            let file = std::fs::File::open(&idx_path)?;
            let reader = FileReader::try_new(file, None)?;
            let file_schema = reader.schema();

            // Schema metadata carries version, total_row_count, scheme.
            let md = file_schema.metadata();
            assert_eq!(md.get(VERSION_KEY).unwrap(), VERSION_VALUE);
            assert_eq!(
                md.get(ROW_INDEX_SCHEME_KEY).unwrap(),
                ROW_INDEX_SCHEME_FULL_ADDRESS
            );
            let per_partition_rows: u64 =
                md.get(TOTAL_ROW_COUNT_KEY).unwrap().parse().unwrap();
            total_indexed_rows += per_partition_rows;

            // Columns: sampled_row (UInt64) + expr_0 (Float64 for our v2).
            assert_eq!(file_schema.fields().len(), 2);
            assert_eq!(file_schema.field(0).name(), "sampled_row");
            assert_eq!(file_schema.field(0).data_type(), &DataType::UInt64);
            assert_eq!(file_schema.field(1).name(), "expr_0");
            assert_eq!(file_schema.field(1).data_type(), &DataType::Float64);

            let leaves: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
            assert_eq!(leaves.len(), 1, "expected a single leaf batch");
            let leaf = &leaves[0];
            assert!(leaf.num_rows() >= 1, "partition {p} produced zero leaves");

            let sampled_rows = leaf
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let values = leaf
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            // sampled_row starts at 0 and is strictly increasing.
            assert_eq!(sampled_rows.value(0), 0);
            for i in 1..leaf.num_rows() {
                assert!(
                    sampled_rows.value(i) > sampled_rows.value(i - 1),
                    "sampled_row must be strictly increasing"
                );
            }
            // Value column is monotonically ascending (ORDER BY asc).
            for i in 1..leaf.num_rows() {
                assert!(
                    values.value(i) >= values.value(i - 1),
                    "value column must be monotonically ascending"
                );
            }
        }
        assert_eq!(
            total_indexed_rows, expected_rows,
            "sum of per-partition total_row_count must equal total input rows"
        );
        Ok(())
    }
}
