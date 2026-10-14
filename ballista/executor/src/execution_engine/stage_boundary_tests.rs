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

//! A worker-side consumer test for DataFusion's `StageBoundary` API.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ballista_core::JobId;
use ballista_core::execution_plans::{ShuffleWriterExec, create_shuffle_path};
use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, assert_eq_or_internal_err, internal_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coop::make_cooperative;
use datafusion::physical_plan::execution_plan::{
    EmissionType, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream, StageBoundary,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{StreamExt, stream};
use tempfile::TempDir;
use tokio::sync::watch;
use tokio::time::timeout;

use super::{DefaultExecutionEngine, ExecutionEngine, QueryStageExecutor};

#[derive(Debug, Default)]
struct BoundaryObservations {
    streams_waiting_for_release: AtomicUsize,
    emitted_batches: AtomicUsize,
}

struct StageBoundaryTestHarness {
    boundary: Arc<dyn StageBoundary>,
    observations: Arc<BoundaryObservations>,
    session_context: SessionContext,
    stage_executor: Arc<dyn QueryStageExecutor>,
    job_id: JobId,
    stage_id: usize,
    work_dir: TempDir,
    expected_batches: Vec<RecordBatch>,
}

impl StageBoundaryTestHarness {
    /// Builds two partition inputs and the Ballista stage that writes their shuffle output.
    fn new() -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt32,
            false,
        )]));
        // Separate batches let the test verify readiness and output for both partitions.
        let input_partitions = vec![
            vec![RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(UInt32Array::from(vec![1, 2]))],
            )?],
            vec![RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(UInt32Array::from(vec![3, 4]))],
            )?],
        ];
        let expected_batches = input_partitions.iter().flatten().cloned().collect();
        let source = Arc::new(MemorySourceConfig::try_new(
            &input_partitions,
            Arc::clone(&schema),
            None,
        )?);
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(source));
        let observations = Arc::new(BoundaryObservations::default());
        // The test driver controls this node while the shuffle writer consumes its output.
        let boundary = Arc::new(WorkerStageBoundaryExec::new(
            input,
            Arc::clone(&observations),
        ));
        let boundary: Arc<dyn StageBoundary> = boundary;

        let job_id = JobId::new("stage-boundary-worker-test");
        let stage_id = 1;
        let work_dir = TempDir::new()?;
        // Place the boundary under Ballista's real shuffle writer.
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ShuffleWriterExec::try_new(
            job_id.clone(),
            stage_id,
            boundary.clone(),
            work_dir.path().display().to_string(),
        )?);
        let session_context = SessionContext::new();
        // Exercise that plan through Ballista's default worker-stage executor.
        let stage_executor = DefaultExecutionEngine::new().create_query_stage_exec(
            job_id.clone(),
            stage_id,
            0,
            vec![0, 1],
            writer,
            work_dir.path().to_str().unwrap(),
            &SessionConfig::new(),
        )?;

        Ok(Self {
            boundary,
            observations,
            session_context,
            stage_executor,
            job_id,
            stage_id,
            work_dir,
            expected_batches,
        })
    }
}

#[derive(Debug)]
struct PartitionState {
    buffered: Arc<Mutex<Option<Vec<Result<RecordBatch>>>>>,
    drain_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    ready: Arc<AtomicBool>,
    executed: AtomicBool,
    release_signal: watch::Sender<bool>,
}

#[derive(Debug)]
struct WorkerStageBoundaryExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    partitions: Vec<PartitionState>,
    observations: Arc<BoundaryObservations>,
}

impl WorkerStageBoundaryExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        observations: Arc<BoundaryObservations>,
    ) -> Self {
        let partition_count = input.properties().output_partitioning().partition_count();
        let properties = PlanProperties::clone(input.properties())
            .with_emission_type(EmissionType::Final)
            .with_evaluation_type(EvaluationType::Eager)
            .with_scheduling_type(SchedulingType::Cooperative);
        let partitions = (0..partition_count)
            .map(|_| PartitionState {
                buffered: Arc::new(Mutex::new(None)),
                drain_task: Mutex::new(None),
                ready: Arc::new(AtomicBool::new(false)),
                executed: AtomicBool::new(false),
                release_signal: watch::channel(false).0,
            })
            .collect();
        Self {
            input,
            properties: Arc::new(properties),
            partitions,
            observations,
        }
    }
}

impl StageBoundary for WorkerStageBoundaryExec {
    fn prime(&self, partition: usize, context: Arc<TaskContext>) -> Result<()> {
        let Some(state) = self.partitions.get(partition) else {
            return internal_err!(
                "WorkerStageBoundaryExec invalid partition {partition}"
            );
        };
        let mut drain_task = state.drain_task.lock().unwrap();
        if drain_task.is_some() {
            return Ok(());
        }

        let mut input = self.input.execute(partition, context)?;
        let buffered = Arc::clone(&state.buffered);
        let ready = Arc::clone(&state.ready);
        *drain_task = Some(tokio::spawn(async move {
            let mut drained_batch_results = Vec::new();
            while let Some(batch_result) = input.next().await {
                let terminal_error = batch_result.is_err();
                drained_batch_results.push(batch_result);
                if terminal_error {
                    break;
                }
            }
            *buffered.lock().unwrap() = Some(drained_batch_results);
            // Under Rust's memory model, this Release store publishes the batches
            // above: a caller whose Acquire load in `is_ready()` reads `true`
            // observes those writes, even when the tasks run on different threads.
            ready.store(true, Ordering::Release);
        }));
        Ok(())
    }

    fn is_ready(&self, partition: usize) -> bool {
        self.partitions
            .get(partition)
            .is_some_and(|state| state.ready.load(Ordering::Acquire))
    }

    fn release(&self) {
        for state in &self.partitions {
            state.release_signal.send_replace(true);
        }
    }
}

impl DisplayAs for WorkerStageBoundaryExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WorkerStageBoundaryExec")
    }
}

impl ExecutionPlan for WorkerStageBoundaryExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq_or_internal_err!(
            children.len(),
            1,
            "WorkerStageBoundaryExec expected one child"
        );
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            Arc::clone(&self.observations),
        )))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(state) = self.partitions.get(partition) else {
            return internal_err!(
                "WorkerStageBoundaryExec invalid partition {partition}"
            );
        };
        if state.executed.swap(true, Ordering::AcqRel) {
            return internal_err!(
                "WorkerStageBoundaryExec partition {partition} executed twice"
            );
        }
        let release_rx = state.release_signal.subscribe();
        let buffered = Arc::clone(&state.buffered);
        let observations = Arc::clone(&self.observations);
        let output = stream::unfold(
            (release_rx, None::<std::vec::IntoIter<Result<RecordBatch>>>),
            move |(mut release_rx, mut pending_batch_results)| {
                let buffered = Arc::clone(&buffered);
                let observations = Arc::clone(&observations);
                async move {
                    if pending_batch_results.is_none() {
                        if !*release_rx.borrow() {
                            // Record that this downstream stream has reached the gate.
                            observations
                                .streams_waiting_for_release
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        while !*release_rx.borrow() {
                            if release_rx.changed().await.is_err() {
                                // A closed sender cannot release this stream, so it ends without output.
                                return None;
                            }
                        }
                        let buffered_batch_results = buffered
                            .lock()
                            .unwrap()
                            .take()
                            .expect("driver releases only ready partitions");
                        pending_batch_results = Some(buffered_batch_results.into_iter());
                    }

                    // Yield one buffered result; keep the iterator for the next poll.
                    let batch_result = pending_batch_results.as_mut().unwrap().next()?;
                    if batch_result.is_ok() {
                        observations.emitted_batches.fetch_add(1, Ordering::Relaxed);
                    }
                    Some((batch_result, (release_rx, pending_batch_results)))
                }
            },
        );
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(self.schema(), output));
        Ok(make_cooperative(stream))
    }
}

/// A test driver drains both boundary inputs while the shuffle writer waits, then releases it.
#[tokio::test]
async fn driver_can_drain_inputs_before_releasing_shuffle_writer() -> Result<()> {
    let harness = StageBoundaryTestHarness::new()?;

    // Start Ballista's worker-stage executor and wait until both output streams reach the gate.
    let stage_task = tokio::spawn({
        let stage_executor = Arc::clone(&harness.stage_executor);
        let task_context = harness.session_context.task_ctx();
        async move { stage_executor.execute_query_stage(0, task_context).await }
    });
    wait_until(|| {
        harness
            .observations
            .streams_waiting_for_release
            .load(Ordering::Relaxed)
            == 2
    })
    .await;

    // Prime both inputs and verify the writer remains blocked with no batches.
    for partition in 0..2 {
        harness
            .boundary
            .prime(partition, harness.session_context.task_ctx())?;
    }
    wait_until(|| (0..2).all(|partition| harness.boundary.is_ready(partition))).await;

    assert!(!stage_task.is_finished());
    assert_eq!(
        harness.observations.emitted_batches.load(Ordering::Relaxed),
        0
    );

    // Release the buffer, then verify the shuffle output.
    harness.boundary.release();
    let writes = timeout(Duration::from_secs(5), stage_task)
        .await
        .expect("Ballista query stage remained blocked after boundary release")
        .expect("Ballista query stage task panicked")?;

    assert_eq!(writes.iter().map(|write| write.num_rows).sum::<u64>(), 4);
    assert_eq!(
        harness.observations.emitted_batches.load(Ordering::Relaxed),
        2
    );

    let mut actual_batches = Vec::new();
    for write in writes {
        let path = create_shuffle_path(
            harness.work_dir.path(),
            &harness.job_id,
            harness.stage_id,
            write.partition_id as usize,
            write.file_id,
            write.is_sort_shuffle,
        )?;
        let reader = StreamReader::try_new(std::fs::File::open(path)?, None)?;
        actual_batches.extend(reader.collect::<std::result::Result<Vec<_>, _>>()?);
    }
    assert_eq!(actual_batches, harness.expected_batches);
    Ok(())
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition did not become true");
}
