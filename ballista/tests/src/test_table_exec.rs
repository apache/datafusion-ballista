use crate::proto;
use crate::test_table::TestTable;
use ballista_core::circuit_breaker::model::CircuitBreakerStageKey;
use ballista_core::circuit_breaker::model::CircuitBreakerTaskKey;
use ballista_executor::circuit_breaker::client::CircuitBreakerClient;
use ballista_executor::circuit_breaker::client::CircuitBreakerMetadataExtension;
use ballista_executor::circuit_breaker::stream::CircuitBreakerCalculation;
use ballista_executor::circuit_breaker::stream::CircuitBreakerStream;
use ballista_scheduler::scheduler_server::timestamp_millis;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use futures::task::WakerRef;
use futures::Stream;
use std::any::Any;
use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, Clone)]
pub(crate) struct TestTableExec {
    pub(crate) table: Arc<TestTable>,
    pub(crate) limit: Option<usize>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) global_limit: u64,
}

impl TestTableExec {
    pub(crate) fn new(
        table: Arc<TestTable>,
        limit: Option<usize>,
        projection: Option<Vec<usize>>,
        global_limit: u64,
    ) -> Self {
        Self {
            table,
            limit,
            projection,
            global_limit,
        }
    }
}

impl DisplayAs for TestTableExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "TestTableExec")
    }
}

impl ExecutionPlan for TestTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.table.parallelism)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        panic!("Can't add children to TestTableExec")
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let record_batch = RecordBatch::try_new(
            self.schema(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;

        let stream = TestDataStream {
            batch: record_batch,
            schema: self.schema(),
            last_sent: 0,
            delay_ms: 1000,
        };

        let client_opt = context
            .session_config()
            .get_extension::<CircuitBreakerClient>();

        let task_id_opt = context.task_id();

        let metadata = context
            .session_config()
            .get_extension::<CircuitBreakerMetadataExtension>();

        if let (Some(client), Some(task_id), Some(metadata)) =
            (client_opt, task_id_opt, metadata)
        {
            let boxed: Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(stream);

            let stage_key = CircuitBreakerStageKey {
                job_id: metadata.job_id.clone(),
                shared_state_id: metadata.stage_id.to_string(),
                stage_id: metadata.stage_id,
                attempt_num: metadata.attempt_number,
            };

            let key = CircuitBreakerTaskKey {
                stage_key,
                partition: partition as u32,
                task_id,
            };

            let limit = self.global_limit;

            let calc = Box::new(TestCalculation { limit })
                as Box<dyn CircuitBreakerCalculation + Send>;

            let labels = vec!["test".to_owned(), format!("partition-{}", partition)];

            let limited_stream =
                CircuitBreakerStream::new(boxed, calc, key, client, labels)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            return Ok(Box::pin(limited_stream));
        }

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

impl TryFrom<proto::TestTableExec> for TestTableExec {
    type Error = DataFusionError;

    fn try_from(value: proto::TestTableExec) -> Result<Self> {
        let table = Arc::new(TestTable::from(value.table.unwrap()));
        let limit = value.limit.map(|x| x as usize);
        let global_limit = value.global_limit;
        let projection = value
            .projection
            .into_iter()
            .map(|x| x as usize)
            .collect::<Vec<_>>();

        let projection_opt = if projection.is_empty() {
            None
        } else {
            Some(projection)
        };

        Ok(Self {
            table,
            limit,
            projection: projection_opt,
            global_limit,
        })
    }
}

impl From<TestTableExec> for proto::TestTableExec {
    fn from(value: TestTableExec) -> Self {
        Self {
            table: Some(value.table.as_ref().clone().into()),
            limit: value.limit.map(|x| x as u64),
            projection: value
                .projection
                .unwrap_or_default()
                .into_iter()
                .map(|x| x as u64)
                .collect(),
            global_limit: value.global_limit,
        }
    }
}

struct TestCalculation {
    limit: u64,
}

impl CircuitBreakerCalculation for TestCalculation {
    fn calculate_delta(&mut self, f: &Poll<Option<Result<RecordBatch>>>) -> f64 {
        if let Poll::Ready(Some(Ok(batch))) = f {
            batch.num_rows() as f64 / self.limit as f64
        } else {
            0.0
        }
    }
}

struct TestDataStream {
    batch: RecordBatch,
    schema: SchemaRef,
    last_sent: u64,
    delay_ms: u64,
}

impl Stream for TestDataStream {
    type Item = Result<RecordBatch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let current_time = timestamp_millis();

        if self.last_sent + self.delay_ms > current_time {
            let waker = WakerRef::new(cx.waker()).clone();

            let sleep_for = self.last_sent + self.delay_ms - current_time;

            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(sleep_for)).await;
                waker.wake_by_ref();
            });

            return Poll::Pending;
        }

        let self_mut = self.get_mut();

        self_mut.last_sent = current_time;

        Poll::Ready(Some(Ok(self_mut.batch.clone())))
    }
}

impl RecordBatchStream for TestDataStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
