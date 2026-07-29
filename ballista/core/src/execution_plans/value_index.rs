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
//! boundaries, and — once wired — writes a `.value.idx` file next to
//! the producer's data file so a reader with any assigned cut range
//! can seek in without full-file read amplification.
//!
//! Refuses to construct on unordered input (e.g. URRE, which advertises
//! no ordering): a value-range index over unordered data would
//! degenerate to "read everything". This is enforced at
//! [`ValueIndexExec::try_new`], not at execute time.
//!
//! Passthrough at the batch level. The write path is not yet wired —
//! samples are logged for now.

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, ScalarValue, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt, ready};
use log::debug;

/// Passthrough tap that samples the input's ORDER BY expression at
/// batch boundaries so a `.value.idx` file can be written next to the
/// producer's data file (write path still to come — currently logs).
pub struct ValueIndexExec {
    input: Arc<dyn ExecutionPlan>,
    order_by: Vec<PhysicalSortExpr>,
    properties: Arc<PlanProperties>,
}

impl ValueIndexExec {
    /// Wrap `input`, resolving the ORDER BY expression from the input's
    /// claimed output ordering. Fails if the input has no ordering — a
    /// value-range index over an unordered stream would degenerate to
    /// "read everything", so this operator refuses to construct there.
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let Some(ordering) = input.output_ordering() else {
            return internal_err!(
                "ValueIndexExec requires ordered input — child plan claims no ordering"
            );
        };
        let order_by: Vec<PhysicalSortExpr> = ordering.iter().cloned().collect();
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            order_by,
            properties,
        })
    }

    /// The ORDER BY expressions the tap will sample, resolved from the
    /// input's output ordering at construction time.
    pub fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }
}

impl Debug for ValueIndexExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueIndexExec").finish()
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
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!(
                "ValueIndexExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(ValueIndexExec::try_new(input.clone())?))
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
        let input = self.input.execute(partition, ctx)?;
        let stream = ValueIndexStream {
            schema: schema.clone(),
            input,
            order_by: self.order_by.clone(),
            partition,
            rows_seen: 0,
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

struct ValueIndexStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    order_by: Vec<PhysicalSortExpr>,
    partition: usize,
    rows_seen: u64,
}

impl Stream for ValueIndexStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) if batch.num_rows() > 0 => {
                let first_row = self.rows_seen;
                let values = match sample_first_row(&batch, &self.order_by) {
                    Ok(v) => v,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                debug!(
                    target: "value_index",
                    "partition={} first_row={} values={:?}",
                    self.partition, first_row, values
                );
                self.rows_seen += batch.num_rows() as u64;
                Poll::Ready(Some(Ok(batch)))
            }
            other => Poll::Ready(other),
        }
    }
}

impl RecordBatchStream for ValueIndexStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Evaluate each ORDER BY expression on `batch` and return the row-0 value
/// of each, in order.
fn sample_first_row(
    batch: &RecordBatch,
    order_by: &[PhysicalSortExpr],
) -> Result<Vec<ScalarValue>> {
    let mut out = Vec::with_capacity(order_by.len());
    for sort_expr in order_by {
        let arr = sort_expr
            .expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        out.push(ScalarValue::try_from_array(&arr, 0)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobId;
    use crate::execution_plans::{
        OrderedRangeRepartitionExec, RuntimeStatsExec, ShuffleWriterExec,
    };
    use datafusion::arrow::array::{
        Float64Array, Int64Array, RecordBatch, StructArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::PhysicalSortExpr;
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

    /// Two input partitions of pre-sorted (v2, id) rows through
    /// Sort/preserve → RuntimeStatsExec → ORRE → ValueIndexExec →
    /// ShuffleWriter(passthrough). Drives all K output shuffle partitions
    /// concurrently and asserts total rows written equal total rows in.
    #[test]
    fn try_new_rejects_unordered_input() {
        let schema = schema_v2_id();
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema, None).unwrap();
        let err = ValueIndexExec::try_new(source)
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
        let tap = ValueIndexExec::try_new(sorted).unwrap();
        assert_eq!(tap.order_by().len(), 1);
        assert_eq!(tap.order_by()[0].expr.as_ref(), sort_expr.expr.as_ref());
    }

    #[tokio::test]
    async fn pipeline_with_value_index_preserves_rows() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let schema = schema_v2_id();

        // Two pre-sorted input partitions.
        let input_batches: Vec<Vec<RecordBatch>> = vec![
            vec![batch(&schema, vec![1.0, 2.0, 3.0, 4.0], vec![1, 2, 3, 4])],
            vec![batch(&schema, vec![1.5, 2.5, 3.5, 4.5], vec![5, 6, 7, 8])],
        ];
        let expected_rows: usize = input_batches
            .iter()
            .flat_map(|p| p.iter().map(|b| b.num_rows()))
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

        let tap: Arc<dyn ExecutionPlan> = Arc::new(ValueIndexExec::try_new(orre)?);

        let work_dir = TempDir::new()?;
        let writer = Arc::new(ShuffleWriterExec::try_new(
            JobId::new("value-index-test"),
            0,
            tap,
            work_dir.path().to_str().unwrap().to_owned(),
        )?);

        // Drive all K output partitions in parallel — matches production
        // dispatch and avoids the scatter-side deadlock documented on
        // ShuffleWriterExec::execute_shuffle_write.
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
                Ok::<_, datafusion::error::DataFusionError>(batches)
            }));
        }
        let mut metadata_batches = Vec::new();
        for h in handles {
            metadata_batches.extend(h.await.unwrap()?);
        }

        // ShuffleWriter emits one metadata batch per output partition; the
        // `num_rows` column of the struct sums to total rows written.
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
        assert_eq!(
            expected_rows as u64, total_written,
            "row conservation violated through the pipeline"
        );
        Ok(())
    }
}
