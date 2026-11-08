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

//! A job's root operator that runs on the scheduler, once the rest of the job
//! has succeeded. See [`SessionConfigExt::with_ballista_scheduler_root`].

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use ballista_core::JobId;
use ballista_core::config::BALLISTA_CLIENT_GRPC_MAX_MESSAGE_SIZE;
use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, Partitioning, ReplaceChildrenOptions,
    execute_stream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// A job's root operator, removed from its plan to run on the scheduler.
#[derive(Clone)]
pub struct SchedulerRoot {
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
}

impl SchedulerRoot {
    /// Splits `plan` into the plan the executors run and the root the
    /// scheduler runs after them, if the session's predicate picks the root.
    ///
    /// Fails if it does, but the job builds a cache (see [`CacheBuild`]).
    pub(crate) fn split(
        plan: Arc<dyn ExecutionPlan>,
        ctx: &SessionContext,
    ) -> Result<(Arc<dyn ExecutionPlan>, Option<Self>)> {
        let state = ctx.state();
        let config = state.config();
        if !picks_root(&plan, config) {
            return Ok((plan, None));
        }
        if config.get_extension::<CacheBuild>().is_some() {
            return Err(BallistaError::General(format!(
                "{} is to run on the scheduler, which a job building a cache \
                 cannot do, as the cache would hold the root's input",
                plan.name()
            )));
        }
        let [input] = plan.children()[..] else {
            return Err(BallistaError::General(format!(
                "a scheduler root must have one input, but {} has {}",
                plan.name(),
                plan.children().len()
            )));
        };
        // The root reads the whole of its input as one partition, so merging
        // the input's partitions first would only add a stage.
        let input = match input.downcast_ref::<CoalescePartitionsExec>() {
            Some(coalesce) if coalesce.fetch().is_none() => coalesce.input(),
            _ => input,
        };
        let input = Arc::clone(input);

        // The scheduler holds no shuffle files, so it reads the job's output
        // from the executors.
        let config = config
            .clone()
            .with_ballista_shuffle_reader_force_remote_read(true);
        let task_ctx = TaskContext::from(&state).with_session_config(config);

        let root = Self {
            plan,
            task_ctx: Arc::new(task_ctx),
        };
        Ok((input, Some(root)))
    }

    /// Fails a pipelined job whose root the session's predicate picks: a
    /// pipelined job streams its result while it runs, so its root runs as a
    /// task, and could run more than once.
    pub(crate) fn refuse(
        plan: &Arc<dyn ExecutionPlan>,
        ctx: &SessionContext,
    ) -> Result<()> {
        if picks_root(plan, ctx.state().config()) {
            return Err(BallistaError::General(format!(
                "{} is to run on the scheduler, which a pipelined job cannot do; \
                 run the job staged",
                plan.name()
            )));
        }
        Ok(())
    }

    /// The plan the scheduler runs: the root over the job's output, the
    /// partitions at `locations`, read as one partition, since the root
    /// gathers it in one place.
    pub(crate) fn over_output(
        &self,
        locations: Vec<PartitionLocation>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Each location is read remotely, from the stage it names, so the
        // reader's stage id only labels it, and its work dir is unused.
        let stage_id = locations.first().map_or(0, |l| l.partition_id.stage_id);
        let input = ShuffleReaderExec::try_new(
            stage_id,
            vec![locations],
            self.plan.children()[0].schema(),
            Partitioning::UnknownPartitioning(1),
        )?
        .with_work_dir(String::new());
        self.over(Arc::new(input))
    }

    /// Runs the root over the job's output, the partitions at `locations`,
    /// and returns the root's output as an Arrow IPC stream.
    ///
    /// Fails as soon as the output is too big for the client to receive in
    /// the job's status, which a commit's never is: this guards against a
    /// predicate picking a root that returns rows, before the scheduler has
    /// gathered them all.
    pub(crate) async fn run(&self, locations: Vec<PartitionLocation>) -> Result<Vec<u8>> {
        let plan = self.over_output(locations)?;
        let limit = self
            .task_ctx
            .session_config()
            .ballista_grpc_client_max_message_size();
        let too_big = || {
            BallistaError::General(format!(
                "{} returned more than a client accepts in a message \
                 ({BALLISTA_CLIENT_GRPC_MAX_MESSAGE_SIZE} = {limit} bytes)",
                self.plan.name()
            ))
        };

        let mut output = execute_stream(Arc::clone(&plan), Arc::clone(&self.task_ctx))?;
        let mut writer = StreamWriter::try_new(Vec::new(), &plan.schema())?;
        while let Some(batch) = output.next().await {
            writer.write(&batch?)?;
            if writer.get_ref().len() > limit {
                return Err(too_big());
            }
        }
        let result = writer.into_inner()?;
        if result.len() > limit {
            return Err(too_big());
        }
        Ok(result)
    }

    /// The root's operator name, as `CommitExec`.
    pub(crate) fn name(&self) -> &str {
        self.plan.name()
    }

    /// The root over `input`.
    pub(crate) fn over(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::clone(&self.plan).replace_children(
            vec![input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?)
    }
}

/// Marks the session of a job that builds a `DataFrame::cache()`, whose root
/// cannot run on the scheduler: the cache keeps the job's shuffle output,
/// which would be the root's input rather than the job's result.
pub(crate) struct CacheBuild;

/// Whether the session's predicate picks `plan`'s root to run on the
/// scheduler.
fn picks_root(plan: &Arc<dyn ExecutionPlan>, config: &SessionConfig) -> bool {
    config
        .ballista_scheduler_root()
        .is_some_and(|predicate| predicate(plan.as_ref()))
}

impl Debug for SchedulerRoot {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SchedulerRoot")
            .field(&self.plan.name())
            .finish()
    }
}

/// A job's scheduler root, held by its graph, which reports the job running
/// until the root has run.
#[derive(Clone, Debug)]
pub struct PendingRoot {
    root: SchedulerRoot,
    /// Whether the root has been handed to the scheduler to run.
    started: bool,
    /// The root's output, once it has run, until the job succeeds with it.
    result: Option<Vec<u8>>,
}

impl PendingRoot {
    pub(crate) fn new(root: SchedulerRoot) -> Self {
        Self {
            root,
            started: false,
            result: None,
        }
    }

    pub(crate) fn root(&self) -> &SchedulerRoot {
        &self.root
    }

    /// Whether the root has been handed to the scheduler to run.
    #[cfg(feature = "rest-api")]
    pub(crate) fn is_started(&self) -> bool {
        self.started
    }

    /// Keeps the root's output, an Arrow IPC stream, for the job to succeed
    /// with.
    pub(crate) fn finish(&mut self, result: Vec<u8>) {
        self.result = Some(result);
    }

    /// Takes the root's output, if it has run.
    pub(crate) fn take_result(&mut self) -> Option<Vec<u8>> {
        self.result.take()
    }

    /// The event that runs the root over the job's output once every stage
    /// has succeeded: the first time only, as task updates can arrive after
    /// the last stage has finished.
    pub(crate) fn start(
        &mut self,
        job_id: &JobId,
        queued_at: u64,
    ) -> Option<QueryStageSchedulerEvent> {
        if std::mem::replace(&mut self.started, true) {
            return None;
        }
        Some(QueryStageSchedulerEvent::RunSchedulerRoot {
            job_id: job_id.clone(),
            queued_at,
        })
    }
}

/// A root for tests: an `INSERT`'s sink.
#[cfg(test)]
pub(crate) async fn test_root() -> SchedulerRoot {
    let ctx = tests::context(SessionConfig::new_with_ballista());
    let plan = tests::plan(&ctx, "INSERT INTO sink SELECT id FROM source").await;
    SchedulerRoot::split(plan, &ctx).unwrap().1.unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::datasource::MemTable;
    use datafusion::datasource::sink::DataSinkExec;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, displayable,
    };
    use std::time::Duration;
    use tokio::sync::Semaphore;

    /// A context from `config` whose scheduler root is an `INSERT`'s sink or
    /// a [`TestRootExec`], with a table `source` of two partitions and an
    /// empty table `sink`.
    pub(super) fn context(config: SessionConfig) -> SessionContext {
        let config = config
            .with_target_partitions(4)
            .with_ballista_scheduler_root(Arc::new(|plan: &dyn ExecutionPlan| {
                plan.is::<DataSinkExec>() || plan.is::<TestRootExec>()
            }));
        let ctx = SessionContext::new_with_config(config);
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = |ids: Vec<i32>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))])
                .unwrap()
        };
        let source = MemTable::try_new(
            schema.clone(),
            vec![vec![batch(vec![1, 2])], vec![batch(vec![3])]],
        )
        .unwrap();
        let sink = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        ctx.register_table("source", Arc::new(source)).unwrap();
        ctx.register_table("sink", Arc::new(sink)).unwrap();
        ctx
    }

    /// The physical plan of `sql`.
    pub(super) async fn plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    /// Splits `plan`, returning it and what the executors run, both
    /// displayed, and the root.
    fn split(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> (String, String, Option<SchedulerRoot>) {
        let display = |plan: &Arc<dyn ExecutionPlan>| {
            displayable(plan.as_ref()).indent(true).to_string()
        };
        let planned = display(&plan);
        let (input, root) = SchedulerRoot::split(plan, ctx).unwrap();
        (planned, display(&input), root)
    }

    /// Where a [`TestRootExec`] that is to wait does so until the test
    /// releases it.
    #[derive(Debug)]
    struct Gate {
        /// A permit for each time a root reaches the gate.
        reached: Semaphore,
        /// A permit for each root the test lets through.
        released: Semaphore,
    }

    impl Gate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                reached: Semaphore::new(0),
                released: Semaphore::new(0),
            })
        }

        /// How many roots have reached the gate.
        fn runs(&self) -> usize {
            self.reached.available_permits()
        }

        /// Waits for a root to reach the gate, leaving it counted.
        async fn wait_until_reached(&self) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.runs() == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("no root reached the gate");
        }
    }

    /// What a [`TestRootExec`] does when it runs.
    #[derive(Debug, Clone)]
    enum Behavior {
        /// Waits at the gate, then returns `ran = 1`.
        Gate(Arc<Gate>),
        /// Returns rows without end.
        Endless,
        /// Panics.
        Panic,
    }

    /// A root that does what its [`Behavior`] says, returning a column
    /// `ran`. It ignores its inputs, which virtual executors only pretend to
    /// write.
    #[derive(Debug)]
    struct TestRootExec {
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        behavior: Behavior,
        properties: Arc<PlanProperties>,
    }

    impl TestRootExec {
        fn new(inputs: Vec<Arc<dyn ExecutionPlan>>, behavior: Behavior) -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "ran",
                DataType::UInt64,
                false,
            )]));
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ));
            Self {
                inputs,
                behavior,
                properties,
            }
        }
    }

    impl DisplayAs for TestRootExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
            write!(f, "TestRootExec")
        }
    }

    impl ExecutionPlan for TestRootExec {
        fn name(&self) -> &str {
            "TestRootExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.inputs.iter().collect()
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &Arc<dyn PhysicalExpr>,
            )
                -> datafusion::common::Result<TreeNodeRecursion>,
        ) -> datafusion::common::Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self::new(children, self.behavior.clone())))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            let schema = self.schema();
            let ran = move |rows: usize| {
                let ran = UInt64Array::from(vec![1; rows]);
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(ran)],
                )?)
            };
            let output = match self.behavior.clone() {
                Behavior::Gate(gate) => futures::stream::once(async move {
                    gate.reached.add_permits(1);
                    gate.released.acquire().await.unwrap().forget();
                    ran(1)
                })
                .boxed(),
                // Yields between batches, as a real source would, so a
                // run that does not stop can still time out.
                Behavior::Endless => futures::stream::unfold(ran, |ran| async move {
                    tokio::task::yield_now().await;
                    Some((ran(1024), ran))
                })
                .boxed(),
                Behavior::Panic => {
                    futures::stream::once(async { panic!("root broke") }).boxed()
                }
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                output,
            )))
        }
    }

    #[tokio::test]
    async fn should_split_off_the_root_without_merging_its_input() {
        let ctx = context(SessionConfig::new_with_ballista());
        let (planned, input, root) = split(
            &ctx,
            plan(&ctx, "INSERT INTO sink SELECT id + 1 AS id FROM source").await,
        );

        assert_eq!(
            planned,
            "DataSinkExec: sink=MemoryTable (partitions=1)\n  \
               CoalescePartitionsExec\n    \
                 ProjectionExec: expr=[CAST(CAST(id@0 AS Int64) + 1 AS Int32) as id]\n      \
                   DataSourceExec: partitions=2, partition_sizes=[1, 1]\n"
        );
        assert_eq!(
            input,
            "ProjectionExec: expr=[CAST(CAST(id@0 AS Int64) + 1 AS Int32) as id]\n  \
               DataSourceExec: partitions=2, partition_sizes=[1, 1]\n"
        );
        assert_eq!(format!("{root:?}"), "Some(SchedulerRoot(\"DataSinkExec\"))");
    }

    /// A merge with a limit picks the rows the root sees, so it runs.
    #[tokio::test]
    async fn should_keep_a_merge_that_limits_the_roots_input() {
        let ctx = context(SessionConfig::new_with_ballista());
        let scan = plan(&ctx, "SELECT id FROM source").await;
        let limited = Arc::new(CoalescePartitionsExec::new(scan).with_fetch(Some(1)));
        let (_, input, root) = split(
            &ctx,
            Arc::new(TestRootExec::new(vec![limited], Behavior::Endless)),
        );

        assert_eq!(
            input,
            "CoalescePartitionsExec: fetch=1\n  \
               DataSourceExec: partitions=2, partition_sizes=[1, 1]\n"
        );
        assert_eq!(format!("{root:?}"), "Some(SchedulerRoot(\"TestRootExec\"))");
    }

    #[tokio::test]
    async fn should_keep_a_root_the_predicate_does_not_pick() {
        let ctx = context(SessionConfig::new_with_ballista());
        let (planned, input, root) =
            split(&ctx, plan(&ctx, "SELECT id + 1 AS id FROM source").await);

        assert_eq!(
            input,
            "ProjectionExec: expr=[CAST(id@0 AS Int64) + 1 as id]\n  \
               DataSourceExec: partitions=2, partition_sizes=[1, 1]\n"
        );
        assert_eq!(input, planned);
        assert_eq!(format!("{root:?}"), "None");
    }

    #[tokio::test]
    async fn should_refuse_a_root_with_more_than_one_input() {
        let ctx = context(SessionConfig::new_with_ballista());
        let scan = plan(&ctx, "SELECT id FROM source").await;
        let root = TestRootExec::new(vec![Arc::clone(&scan), scan], Behavior::Endless);

        let error = SchedulerRoot::split(Arc::new(root), &ctx)
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "General error: a scheduler root must have one input, but TestRootExec has 2"
        );
    }

    #[tokio::test]
    async fn should_refuse_a_root_for_a_job_building_a_cache() {
        let mut config = SessionConfig::new_with_ballista();
        config.set_extension(Arc::new(CacheBuild));
        let ctx = context(config);
        let plan = plan(&ctx, "INSERT INTO sink SELECT id FROM source").await;

        let error = SchedulerRoot::split(plan, &ctx).unwrap_err().to_string();

        assert_eq!(
            error,
            "General error: DataSinkExec is to run on the scheduler, which a job \
             building a cache cannot do, as the cache would hold the root's input"
        );
    }

    /// The root's output never ends, so only stopping at the limit ends the
    /// run, before the scheduler has gathered more than a client accepts.
    #[tokio::test]
    async fn should_stop_a_root_once_its_result_is_too_big_for_a_client() {
        let ctx = context(
            SessionConfig::new_with_ballista()
                .with_ballista_grpc_client_max_message_size(64 * 1024),
        );
        let scan = plan(&ctx, "SELECT id FROM source").await;
        let root = Arc::new(TestRootExec::new(vec![scan], Behavior::Endless));
        let (_, root) = SchedulerRoot::split(root, &ctx).unwrap();

        let ran =
            tokio::time::timeout(Duration::from_secs(10), root.unwrap().run(vec![]))
                .await
                .expect("the root ran past the limit");

        assert_eq!(
            ran.unwrap_err().to_string(),
            "General error: TestRootExec returned more than a client accepts in a \
             message (ballista.client.grpc_max_message_size = 65536 bytes)"
        );
    }

    /// A job's life with a root that runs on the scheduler, driven through
    /// the scheduler's event loop against virtual executors.
    mod lifecycle {
        use super::*;
        use crate::config::SchedulerConfig;
        use crate::scheduler_server::event::QueryStageSchedulerEvent;
        use crate::test_utils::{
            SchedulerTest, TestMetricsCollector, scan_with_file_groups,
        };
        use ballista_core::config::TaskSchedulingPolicy;
        use ballista_core::serde::protobuf::job_status::Status;
        use ballista_core::utils::decode_ipc_stream;
        use datafusion::arrow::util::pretty::pretty_format_batches;

        /// Submits a job of one scan task whose root is a [`TestRootExec`]
        /// that runs on the scheduler as `behavior` says.
        async fn job_with_root(behavior: Behavior) -> (SchedulerTest, JobId) {
            let mut test = SchedulerTest::new(
                SchedulerConfig::default()
                    .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
                Arc::new(TestMetricsCollector::default()),
                1,
                1,
                None,
            )
            .await
            .unwrap();
            test.set_scheduler_root(Arc::new(|plan: &dyn ExecutionPlan| {
                plan.is::<TestRootExec>()
            }));
            let plan = TestRootExec::new(vec![scan_with_file_groups(1)], behavior);
            let job_id = test.submit_physical("", Arc::new(plan)).await.unwrap();
            (test, job_id)
        }

        /// The job's status, in short: the inline result in full if it
        /// succeeded, and the error if it failed.
        async fn status(test: &SchedulerTest, job_id: &JobId) -> String {
            match test
                .job_status(job_id)
                .await
                .unwrap()
                .and_then(|s| s.status)
            {
                Some(Status::Running(_)) => "Running".to_owned(),
                Some(Status::Successful(job)) => {
                    let (_, batches) =
                        decode_ipc_stream(job.inline_result.as_deref().unwrap_or(&[]))
                            .unwrap();
                    format!(
                        "Successful, with {} partitions to fetch:\n{}",
                        job.partition_location.len(),
                        pretty_format_batches(&batches).unwrap()
                    )
                }
                Some(Status::Failed(job)) => format!("Failed: {}", job.error),
                other => format!("{other:?}"),
            }
        }

        /// Waits for the job to stop running, and returns its status.
        async fn settled(test: &SchedulerTest, job_id: &JobId) -> String {
            let stopped = test
                .await_job_status(job_id, |s| !matches!(s, Status::Running(_)))
                .await
                .unwrap();
            assert!(stopped, "the job is still running");
            status(test, job_id).await
        }

        #[tokio::test]
        async fn should_report_the_job_running_until_its_root_has_run() {
            let gate = Gate::new();
            let (mut test, job_id) = job_with_root(Behavior::Gate(gate.clone())).await;
            test.apply_task_statuses();

            gate.wait_until_reached().await;
            assert_eq!(status(&test, &job_id).await, "Running");

            gate.released.add_permits(1);
            assert_eq!(
                settled(&test, &job_id).await,
                "Successful, with 0 partitions to fetch:\n\
                 +-----+\n\
                 | ran |\n\
                 +-----+\n\
                 | 1   |\n\
                 +-----+"
            );
            assert_eq!(gate.runs(), 1);
        }

        /// The root runs user code, so a panic in it must fail the job rather
        /// than leave it running.
        #[tokio::test]
        async fn should_fail_the_job_when_its_root_panics() {
            let (mut test, job_id) = job_with_root(Behavior::Panic).await;
            test.apply_task_statuses();

            assert_eq!(
                settled(&test, &job_id).await,
                "Failed: Error running the job's scheduler root: \
                 Internal Ballista error: panicked: root broke"
            );
        }

        #[tokio::test]
        async fn should_not_run_the_root_of_a_job_cancelled_before_it_starts() {
            let gate = Gate::new();
            let (test, job_id) = job_with_root(Behavior::Gate(gate.clone())).await;

            // Planned, with its one task launched but not yet reported.
            let running = test
                .await_job_status(&job_id, |s| matches!(s, Status::Running(_)))
                .await
                .unwrap();
            assert!(running, "the job never ran");
            test.cancel(&job_id).await.unwrap();
            assert_eq!(settled(&test, &job_id).await, "Failed: Cancelled");
            // As if the job's stages had succeeded just before it was
            // cancelled, so the event to run its root comes after.
            test.post_scheduler_event(QueryStageSchedulerEvent::RunSchedulerRoot {
                job_id: job_id.clone(),
                queued_at: 0,
            })
            .await
            .unwrap();

            // Nothing signals a root that did not run, so give it the time
            // one that did would take.
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(gate.runs(), 0);
            assert_eq!(status(&test, &job_id).await, "Failed: Cancelled");
        }

        #[tokio::test]
        async fn should_discard_the_root_of_a_job_cancelled_while_it_runs() {
            let gate = Gate::new();
            let (mut test, job_id) = job_with_root(Behavior::Gate(gate.clone())).await;
            test.apply_task_statuses();
            gate.wait_until_reached().await;

            test.cancel(&job_id).await.unwrap();
            assert_eq!(settled(&test, &job_id).await, "Failed: Cancelled");

            // The root still finishes: a commit cannot be stopped midway.
            gate.released.add_permits(1);
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(gate.runs(), 1);
            assert_eq!(status(&test, &job_id).await, "Failed: Cancelled");
        }
    }
}
