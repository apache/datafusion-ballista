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
//! uses so the index sits next to the data file it describes):
//!
//! ```text
//! {work_dir}/{job_id}/{stage_id}/{partition_id}/data-{task_id}.value.idx
//! ```
//!
//! Format is a single Arrow IPC file with one RecordBatch of length L
//! (leaves), schema `[sampled_row: UInt64 NOT NULL, expr_0: <type>, ...]`,
//! and schema metadata: `version="1"`, `total_row_count="<u64>"`,
//! `row_index_scheme="full_address"`.
//!
//! # End-to-end read path
//!
//! Consumer wants everyone with `(last_name, first_name)` in
//! `["Baker", "Chen")`. Given `ORDER BY last_name, first_name`:
//!
//! ```text
//!   data-0.value.idx  (KB-scale, opened first)
//!   ┌─────────────┬────────────┬────────────┐
//!   │ sampled_row │ last_name  │ first_name │
//!   ├─────────────┼────────────┼────────────┤
//!   │       0     │ "Adams"    │ "Aaron"    │
//!   │    1,024    │ "Baker"    │ "Beth"     │◀── binary_search_by_value
//!   │    2,048    │ "Chen"     │ "Carla"    │      (("Baker","*")) → leaf 1
//!   │    3,072    │ "Diaz"     │ "Diana"    │      (("Chen","*"))  → leaf 2
//!   └─────────────┴────────────┴────────────┘
//!            │
//!            │  today's sampling policy: leaf_idx == batch_idx,
//!            │  so leaves [1, 2) → batches [1, 2)
//!            ▼
//!   data-0.arrow  (Arrow IPC file footer, one range-GET on S3)
//!   ┌───────┬──────────┬─────────┐
//!   │ batch │  offset  │  length │
//!   ├───────┼──────────┼─────────┤
//!   │   0   │      42  │  8,192  │        rows [0,     1024)
//!   │   1   │   8,234  │  9,001  │  ◀──   rows [1024,  2048)  set_index(1)
//!   │   2   │  17,235  │  8,500  │        rows [2048,  3072)
//!   │   3   │  25,735  │  7,000  │        rows [3072,  4000)
//!   └───────┴──────────┴─────────┘
//!            │
//!            │  range GET([8234, 8234+9001))
//!            ▼
//!   data-0.arrow body
//!   ┌────────────────────────────────────────────────┐
//!   │ ...  batch 1's IPC message (fetched)  ...       │
//!   └────────────────────────────────────────────────┘
//! ```
//!
//! Under sketch imprecision it's common for a producer file's advertised
//! `[min, max]` to overlap the consumer's cut range but for no actual
//! rows to fall inside — `binary_search_by_value(cut_lo) ==
//! binary_search_by_value(cut_hi) + 1` reports "no leaf covers this
//! range" and the consumer skips fetching *any* batch bodies from that
//! file. That's the read-amp win the design memo is chasing.
//!
//! Straddling rows in the first/last fetched batch (values that fall
//! before `cut_lo` or at/after `cut_hi`) are trimmed downstream by
//! `PerPartitionFilterExec`. The value index narrows the fetch to
//! batches with at least one row in range; the filter is the row-level
//! safety net.

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::compute::{SortOptions, concat_batches};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::row::{RowConverter, SortField};
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
const SAMPLED_ROW_COLUMN: &str = "sampled_row";

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
    /// index path is derived deterministically from them.
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
        let mut fields = vec![Field::new(SAMPLED_ROW_COLUMN, DataType::UInt64, false)];
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

    /// Derive the index-file path for a given local output partition.
    /// The data file's path is what `create_shuffle_path` produces; the
    /// index file swaps `.arrow` for `.value.idx`.
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

/// Reader for a `.value.idx` file produced by [`ValueIndexExec`].
///
/// Opens the file eagerly, validates the schema envelope (version,
/// row-index scheme, `sampled_row` column shape), and holds the leaves
/// in memory. Files are KB-scale by design.
pub struct ValueIndexReader {
    /// One row per leaf. Column 0 is the `sampled_row` UInt64 column;
    /// columns 1.. are the ORDER BY value columns, in declaration order.
    leaves: RecordBatch,
    /// Total rows in the sorted stream this index describes. Bounds the
    /// last leaf's implicit row range.
    total_row_count: u64,
}

impl ValueIndexReader {
    /// Open and validate a `.value.idx` file. Fails on missing / unknown
    /// version metadata, unknown row-index scheme, or a first column
    /// that doesn't match `sampled_row: UInt64`.
    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(|e| {
            DataFusionError::Execution(format!(
                "ValueIndexReader: open({}) failed: {e}",
                path.display()
            ))
        })?;
        let reader = FileReader::try_new(file, None)?;
        let schema = reader.schema();
        let md = schema.metadata();

        match md.get(VERSION_KEY).map(String::as_str) {
            Some(VERSION_VALUE) => {}
            Some(other) => {
                return internal_err!(
                    "ValueIndexReader: unsupported version `{other}`, expected `{VERSION_VALUE}`"
                );
            }
            None => {
                return internal_err!(
                    "ValueIndexReader: missing `{VERSION_KEY}` metadata"
                );
            }
        }
        match md.get(ROW_INDEX_SCHEME_KEY).map(String::as_str) {
            Some(ROW_INDEX_SCHEME_FULL_ADDRESS) => {}
            Some(other) => {
                return internal_err!(
                    "ValueIndexReader: unsupported `{ROW_INDEX_SCHEME_KEY}` `{other}`"
                );
            }
            None => {
                return internal_err!(
                    "ValueIndexReader: missing `{ROW_INDEX_SCHEME_KEY}` metadata"
                );
            }
        }
        let total_row_count: u64 = md
            .get(TOTAL_ROW_COUNT_KEY)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "ValueIndexReader: missing `{TOTAL_ROW_COUNT_KEY}` metadata"
                ))
            })?
            .parse()
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "ValueIndexReader: invalid `{TOTAL_ROW_COUNT_KEY}`: {e}"
                ))
            })?;

        let field0 = schema.field(0);
        if field0.name() != SAMPLED_ROW_COLUMN || field0.data_type() != &DataType::UInt64
        {
            return internal_err!(
                "ValueIndexReader: expected column 0 `{SAMPLED_ROW_COLUMN}: UInt64`, got `{}: {:?}`",
                field0.name(),
                field0.data_type()
            );
        }

        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
        let leaves = concat_batches(&schema, &batches)?;

        Ok(Self {
            leaves,
            total_row_count,
        })
    }

    /// Total rows in the sorted stream this index describes.
    pub fn total_row_count(&self) -> u64 {
        self.total_row_count
    }

    /// Number of leaf entries in the index.
    pub fn num_leaves(&self) -> usize {
        self.leaves.num_rows()
    }

    /// The raw leaf batch: column 0 is `sampled_row: UInt64`, columns
    /// 1.. are the ORDER BY value columns.
    pub fn leaf_batch(&self) -> &RecordBatch {
        &self.leaves
    }

    /// Typed accessor for the `sampled_row` column.
    pub fn sampled_rows(&self) -> &UInt64Array {
        self.leaves
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("column 0 validated as UInt64 by open()")
    }

    /// Row range `[start, end)` covered by leaf `idx`. `end` is either
    /// the next leaf's `sampled_row` or `total_row_count` for the last
    /// leaf. Panics if `idx >= num_leaves()`.
    pub fn leaf_row_range(&self, idx: usize) -> (u64, u64) {
        let sampled = self.sampled_rows();
        let start = sampled.value(idx);
        let end = if idx + 1 < sampled.len() {
            sampled.value(idx + 1)
        } else {
            self.total_row_count
        };
        (start, end)
    }

    /// Return the leaf index whose value range contains `target`, i.e.
    /// `values[i] <= target < values[i+1]` under the caller-supplied
    /// `sort_options` (the last leaf's implicit upper bound is +∞).
    ///
    /// `target` is one length-1 `ArrayRef` per ORDER BY column, in the
    /// same order the writer declared. `sort_options` mirrors the
    /// ORDER BY declaration — per-column asc/desc and null placement.
    /// Both slices must match the number of value columns
    /// (`num_columns() - 1`).
    ///
    /// Returns `None` when `target` sorts strictly below the first
    /// leaf's value (no leaf covers it).
    pub fn binary_search_by_value(
        &self,
        target: &[ArrayRef],
        sort_options: &[SortOptions],
    ) -> Result<Option<usize>> {
        let num_value_cols = self.leaves.num_columns().saturating_sub(1);
        if target.len() != num_value_cols || sort_options.len() != num_value_cols {
            return internal_err!(
                "binary_search_by_value: expected {num_value_cols} target columns and options, got target={} options={}",
                target.len(),
                sort_options.len()
            );
        }
        if self.num_leaves() == 0 {
            return Ok(None);
        }
        let leaf_schema = self.leaves.schema();
        let sort_fields: Vec<SortField> = (0..num_value_cols)
            .map(|col_idx| {
                SortField::new_with_options(
                    leaf_schema.field(col_idx + 1).data_type().clone(),
                    sort_options[col_idx],
                )
            })
            .collect();
        let converter = RowConverter::new(sort_fields)?;

        let leaf_value_cols: Vec<ArrayRef> = (1..self.leaves.num_columns())
            .map(|col_idx| self.leaves.column(col_idx).clone())
            .collect();
        let leaf_rows = converter.convert_columns(&leaf_value_cols)?;
        let target_rows = converter.convert_columns(target)?;
        let target_row = target_rows.row(0);

        // First index where leaf_rows[i] > target — one past the leaf
        // we want. `Row: Ord` respects the SortOptions we baked into
        // the converter.
        let num_leaves = self.num_leaves();
        let mut lo = 0usize;
        let mut hi = num_leaves;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if leaf_rows.row(mid) <= target_row {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 { Ok(None) } else { Ok(Some(lo - 1)) }
    }

    /// Return the leaf index whose row range contains `target`, i.e.
    /// `sampled_rows[i] <= target < sampled_rows[i+1]` (with
    /// `total_row_count` bounding the last leaf).
    ///
    /// Returns `None` when `target` falls outside the indexed range:
    /// before the first leaf's `sampled_row` (sampling gap at the
    /// start), or at/past `total_row_count`.
    pub fn binary_search_by_sampled_row(&self, target: u64) -> Option<usize> {
        if target >= self.total_row_count {
            return None;
        }
        let values: &[u64] = self.sampled_rows().values();
        let point = values.partition_point(|&v| v <= target);
        if point == 0 { None } else { Some(point - 1) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::{
        OrderedRangeRepartitionExec, RuntimeStatsExec, ShuffleWriterExec,
    };
    use datafusion::arrow::array::{Float64Array, Int64Array, StructArray};
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

    /// Write a minimal `.value.idx` file with an arbitrary sampled_rows
    /// column and arbitrary value columns. Lets search tests exercise
    /// the reader without running the whole operator pipeline.
    fn write_value_idx(
        path: &Path,
        sampled: Vec<u64>,
        value_columns: Vec<(&str, ArrayRef)>,
        total_row_count: u64,
    ) -> Result<()> {
        let mut fields = vec![Field::new(SAMPLED_ROW_COLUMN, DataType::UInt64, false)];
        for (name, arr) in &value_columns {
            fields.push(Field::new(
                *name,
                arr.data_type().clone(),
                arr.is_nullable(),
            ));
        }
        let md = HashMap::from([
            (VERSION_KEY.to_string(), VERSION_VALUE.to_string()),
            (TOTAL_ROW_COUNT_KEY.to_string(), total_row_count.to_string()),
            (
                ROW_INDEX_SCHEME_KEY.to_string(),
                ROW_INDEX_SCHEME_FULL_ADDRESS.to_string(),
            ),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(fields, md));
        let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from(sampled))];
        columns.extend(value_columns.into_iter().map(|(_, arr)| arr));
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let file = std::fs::File::create(path).map_err(DataFusionError::from)?;
        let mut writer = FileWriter::try_new(file, schema.as_ref())?;
        writer.write(&batch)?;
        writer.finish()?;
        Ok(())
    }

    #[test]
    fn binary_search_by_sampled_row_hits_and_misses() -> Result<()> {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("t.value.idx");
        // Leaves at rows 0, 100, 250; stream ends at row 400.
        write_value_idx(
            &path,
            vec![0, 100, 250],
            vec![(
                "v",
                Arc::new(Int64Array::from(vec![10i64, 20, 30])) as ArrayRef,
            )],
            400,
        )?;
        let idx = ValueIndexReader::open(&path)?;

        assert_eq!(idx.binary_search_by_sampled_row(0), Some(0));
        assert_eq!(idx.binary_search_by_sampled_row(50), Some(0));
        assert_eq!(idx.binary_search_by_sampled_row(99), Some(0));
        assert_eq!(idx.binary_search_by_sampled_row(100), Some(1));
        assert_eq!(idx.binary_search_by_sampled_row(200), Some(1));
        assert_eq!(idx.binary_search_by_sampled_row(250), Some(2));
        assert_eq!(idx.binary_search_by_sampled_row(399), Some(2));
        assert_eq!(idx.binary_search_by_sampled_row(400), None);
        assert_eq!(idx.binary_search_by_sampled_row(500), None);
        Ok(())
    }

    #[test]
    fn binary_search_by_sampled_row_gap_before_first_leaf() -> Result<()> {
        // First sampled_row is 100 — rows [0, 100) exist in the stream
        // but no leaf covers them; expect None.
        let tmp = TempDir::new()?;
        let path = tmp.path().join("t.value.idx");
        write_value_idx(
            &path,
            vec![100, 250],
            vec![("v", Arc::new(Int64Array::from(vec![10i64, 20])) as ArrayRef)],
            400,
        )?;
        let idx = ValueIndexReader::open(&path)?;

        assert_eq!(idx.binary_search_by_sampled_row(0), None);
        assert_eq!(idx.binary_search_by_sampled_row(99), None);
        assert_eq!(idx.binary_search_by_sampled_row(100), Some(0));
        Ok(())
    }

    #[test]
    fn binary_search_by_value_int64_asc() -> Result<()> {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("t.value.idx");
        write_value_idx(
            &path,
            vec![0, 100, 250],
            vec![(
                "v",
                Arc::new(Int64Array::from(vec![10i64, 20, 30])) as ArrayRef,
            )],
            400,
        )?;
        let idx = ValueIndexReader::open(&path)?;
        let opts = vec![SortOptions::default()];

        let search = |v: i64| -> Result<Option<usize>> {
            idx.binary_search_by_value(
                &[Arc::new(Int64Array::from(vec![v])) as ArrayRef],
                &opts,
            )
        };
        assert_eq!(search(5)?, None); // before first
        assert_eq!(search(10)?, Some(0)); // exact hit on first
        assert_eq!(search(15)?, Some(0)); // between 0 and 1
        assert_eq!(search(20)?, Some(1)); // exact hit on second
        assert_eq!(search(25)?, Some(1));
        assert_eq!(search(30)?, Some(2)); // exact hit on last
        assert_eq!(search(100)?, Some(2)); // past last → last leaf
        Ok(())
    }

    #[test]
    fn binary_search_by_value_composite() -> Result<()> {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("t.value.idx");
        // Leaves sorted by (a asc, b asc): (1,"a"), (1,"m"), (2,"a")
        write_value_idx(
            &path,
            vec![0, 100, 250],
            vec![
                (
                    "a",
                    Arc::new(Int64Array::from(vec![1i64, 1, 2])) as ArrayRef,
                ),
                (
                    "b",
                    Arc::new(datafusion::arrow::array::StringArray::from(vec![
                        "a", "m", "a",
                    ])) as ArrayRef,
                ),
            ],
            400,
        )?;
        let idx = ValueIndexReader::open(&path)?;
        let opts = vec![SortOptions::default(), SortOptions::default()];

        let search = |a: i64, b: &str| -> Result<Option<usize>> {
            idx.binary_search_by_value(
                &[
                    Arc::new(Int64Array::from(vec![a])) as ArrayRef,
                    Arc::new(datafusion::arrow::array::StringArray::from(vec![b]))
                        as ArrayRef,
                ],
                &opts,
            )
        };
        assert_eq!(search(0, "z")?, None); // strictly below first
        assert_eq!(search(1, "a")?, Some(0));
        assert_eq!(search(1, "b")?, Some(0));
        assert_eq!(search(1, "m")?, Some(1));
        assert_eq!(search(1, "z")?, Some(1));
        assert_eq!(search(2, "a")?, Some(2));
        assert_eq!(search(9, "z")?, Some(2));
        Ok(())
    }

    #[test]
    fn binary_search_by_value_int64_desc() -> Result<()> {
        // Values stored monotonically descending.
        let tmp = TempDir::new()?;
        let path = tmp.path().join("t.value.idx");
        write_value_idx(
            &path,
            vec![0, 100, 250],
            vec![(
                "v",
                Arc::new(Int64Array::from(vec![30i64, 20, 10])) as ArrayRef,
            )],
            400,
        )?;
        let idx = ValueIndexReader::open(&path)?;
        let opts = vec![SortOptions {
            descending: true,
            nulls_first: false,
        }];

        let search = |v: i64| -> Result<Option<usize>> {
            idx.binary_search_by_value(
                &[Arc::new(Int64Array::from(vec![v])) as ArrayRef],
                &opts,
            )
        };
        // Under desc ordering, "leaf whose range contains target" means
        // the target sorts <= this leaf's value and > the next leaf's
        // value.
        assert_eq!(search(100)?, None); // sorts above the max (30) under desc
        assert_eq!(search(30)?, Some(0));
        assert_eq!(search(25)?, Some(0));
        assert_eq!(search(20)?, Some(1));
        assert_eq!(search(15)?, Some(1));
        assert_eq!(search(10)?, Some(2));
        assert_eq!(search(0)?, Some(2)); // past the min → last leaf
        Ok(())
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

            let idx = ValueIndexReader::open(&idx_path)?;
            total_indexed_rows += idx.total_row_count();
            assert!(idx.num_leaves() >= 1, "partition {p} produced zero leaves");

            // Value column shape: reader validated column 0 already; check
            // the ORDER BY value column landed as declared.
            let leaf_schema = idx.leaf_batch().schema();
            assert_eq!(leaf_schema.fields().len(), 2);
            assert_eq!(leaf_schema.field(1).name(), "expr_0");
            assert_eq!(leaf_schema.field(1).data_type(), &DataType::Float64);

            let sampled_rows = idx.sampled_rows();
            let values = idx
                .leaf_batch()
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            // sampled_row starts at 0 and is strictly increasing.
            assert_eq!(sampled_rows.value(0), 0);
            for i in 1..idx.num_leaves() {
                assert!(
                    sampled_rows.value(i) > sampled_rows.value(i - 1),
                    "sampled_row must be strictly increasing"
                );
            }
            // Value column is monotonically ascending (ORDER BY asc).
            for i in 1..idx.num_leaves() {
                assert!(
                    values.value(i) >= values.value(i - 1),
                    "value column must be monotonically ascending"
                );
            }
            // Last leaf's range ends at total_row_count (sentinel).
            let (_, last_end) = idx.leaf_row_range(idx.num_leaves() - 1);
            assert_eq!(last_end, idx.total_row_count());
        }
        assert_eq!(
            total_indexed_rows, expected_rows,
            "sum of per-partition total_row_count must equal total input rows"
        );
        Ok(())
    }

    /// End-to-end: given a target value, use `ValueIndexReader` to pick a
    /// leaf, then use `FileReader::set_index` (footer-driven random
    /// access) to pull just that batch from the shuffle data file.
    /// Verifies the target value is present in the extracted batch.
    ///
    /// Under today's sampling policy (one sample per non-empty batch),
    /// `leaf_idx == batch_idx` is an invariant; stride sampling will
    /// need chunk-metadata translation instead.
    #[tokio::test]
    async fn range_download_via_value_index_and_footer() -> Result<()> {
        let schema = schema_v2_id();
        let input_batches: Vec<Vec<RecordBatch>> = vec![
            vec![batch(&schema, vec![1.0, 2.0, 3.0, 4.0], vec![1, 2, 3, 4])],
            vec![batch(&schema, vec![1.5, 2.5, 3.5, 4.5], vec![5, 6, 7, 8])],
        ];
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
        let job_id = JobId::new("range-download-test");
        let stage_id = 0;
        let work_dir_str = work_dir.path().to_str().unwrap().to_owned();

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
            work_dir_str,
        )?);

        // Drive all partitions to completion so both data files and
        // index files land on disk.
        let ctx = Arc::new(SessionContext::new()).task_ctx();
        let mut handles = Vec::with_capacity(k_output);
        for p in 0..k_output {
            let writer = writer.clone();
            let ctx = ctx.clone();
            handles.push(tokio::spawn(async move {
                let mut stream = writer.execute(p, ctx)?;
                while (futures::StreamExt::next(&mut stream).await)
                    .transpose()?
                    .is_some()
                {}
                Ok::<_, DataFusionError>(())
            }));
        }
        for h in handles {
            h.await.unwrap()?;
        }

        // For each output partition, pick a target value we know is in
        // the data, look up its leaf, and pull just that batch via the
        // FileReader footer.
        let targets_per_partition = [1.0_f64, 2.0, 3.5];
        let opts = vec![SortOptions::default()];
        for (p, &target_val) in targets_per_partition.iter().enumerate() {
            let partition_dir = work_dir
                .path()
                .join(job_id.as_str())
                .join(stage_id.to_string())
                .join(p.to_string());
            let data_path = partition_dir.join("data-0.arrow");
            let idx_path = partition_dir.join("data-0.value.idx");

            let idx = ValueIndexReader::open(&idx_path)?;
            let leaf_idx = idx
                .binary_search_by_value(
                    &[Arc::new(Float64Array::from(vec![target_val])) as ArrayRef],
                    &opts,
                )?
                .expect("target should land in a leaf");

            let file = std::fs::File::open(&data_path)?;
            let mut reader =
                datafusion::arrow::ipc::reader::FileReader::try_new(file, None)?;

            // Today's sampling policy: one sample per non-empty batch,
            // so leaf_idx == batch_idx and num_leaves == num_batches.
            // Locks the invariant so a future stride-sampling change
            // trips this test rather than silently producing wrong
            // reads.
            assert_eq!(idx.num_leaves(), reader.num_batches());

            // Footer-driven random access — seek straight to the batch,
            // no linear walk.
            reader.set_index(leaf_idx)?;
            let batch = reader.next().expect("set_index positioned at a batch")?;
            let v2 = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let found = (0..batch.num_rows()).any(|i| v2.value(i) == target_val);
            assert!(
                found,
                "target {target_val} missing from batch {leaf_idx} of partition {p}"
            );
        }
        Ok(())
    }
}
