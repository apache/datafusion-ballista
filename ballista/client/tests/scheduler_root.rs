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

//! End-to-end tests for running a job's root on the scheduler, the way a
//! table format's commit runs once on Spark's driver.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod scheduler_root_tests {
    use crate::common::{
        collect_text, setup_test_cluster_with_split_state, state_with_config,
        two_stage_physical_plan,
    };
    use ballista::extension::SessionContextExt;
    use ballista_core::config::{BALLISTA_CLIENT_PULL, BALLISTA_EXECUTION_MODE};
    use ballista_core::execution_plans::execute_physical_plan;
    use ballista_core::extension::SessionConfigExt;
    use ballista_core::serde::BallistaPhysicalExtensionCodec;
    use datafusion::arrow::array::{RecordBatch, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::common::{Result, exec_err};
    use datafusion::datasource::sink::DataSinkExec;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
        PlanProperties,
    };
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_proto::physical_plan::{
        PhysicalExtensionCodec, PhysicalProtoConverterExtension,
    };
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use futures::TryStreamExt;
    use rstest::rstest;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    /// Stands in for a table format's commit: gathers what every writer
    /// wrote, here a count of rows, and publishes it once.
    #[derive(Debug)]
    struct CommitExec {
        input: Arc<dyn ExecutionPlan>,
        /// Whether the commit is rejected, as by a conflicting commit.
        reject: bool,
        /// How many times a commit has run, shared by every copy.
        commits: Arc<AtomicUsize>,
        properties: Arc<PlanProperties>,
    }

    impl CommitExec {
        fn new(
            input: Arc<dyn ExecutionPlan>,
            reject: bool,
            commits: Arc<AtomicUsize>,
        ) -> Self {
            let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                "count",
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
                input,
                reject,
                commits,
                properties,
            }
        }
    }

    impl DisplayAs for CommitExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "CommitExec")
        }
    }

    impl ExecutionPlan for CommitExec {
        fn name(&self) -> &str {
            "CommitExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn required_input_distribution(&self) -> Vec<Distribution> {
            vec![Distribution::SinglePartition]
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

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self::new(
                children.swap_remove(0),
                self.reject,
                Arc::clone(&self.commits),
            )))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let input = self.input.execute(partition, context)?;
            let reject = self.reject;
            let commits = Arc::clone(&self.commits);
            let schema = self.schema();
            let commit = futures::stream::once(async move {
                let rows: usize = input
                    .map_ok(|batch| batch.num_rows())
                    .try_fold(0, |total, rows| async move { Ok(total + rows) })
                    .await?;
                commits.fetch_add(1, Ordering::SeqCst);
                if reject {
                    return exec_err!("commit rejected");
                }
                let count = UInt64Array::from(vec![rows as u64]);
                Ok(RecordBatch::try_new(schema, vec![Arc::new(count)])?)
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                commit,
            )))
        }
    }

    /// Ballista's codec, plus [`CommitExec`]. Only the scheduler and the
    /// client have it: an executor sent a commit cannot decode it.
    #[derive(Debug)]
    struct CommitCodec {
        commits: Arc<AtomicUsize>,
        inner: BallistaPhysicalExtensionCodec,
    }

    const COMMIT: u8 = 0;
    const REJECTED_COMMIT: u8 = 1;

    impl PhysicalExtensionCodec for CommitCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            ctx: &TaskContext,
            proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            match buf {
                [tag @ (COMMIT | REJECTED_COMMIT)] => Ok(Arc::new(CommitExec::new(
                    Arc::clone(&inputs[0]),
                    *tag == REJECTED_COMMIT,
                    Arc::clone(&self.commits),
                ))),
                _ => self.inner.try_decode(buf, inputs, ctx, proto_converter),
            }
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
            proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            match node.downcast_ref::<CommitExec>() {
                Some(commit) if commit.reject => buf.push(REJECTED_COMMIT),
                Some(_) => buf.push(COMMIT),
                None => self.inner.try_encode(node, buf, proto_converter)?,
            }
            Ok(())
        }
    }

    /// A session planned by the static or the `adaptive` planner, whose
    /// client polls for the job's status if `pull` rather than having it
    /// pushed.
    fn session(adaptive: bool, pull: bool) -> SessionConfig {
        SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_CLIENT_PULL, pull)
            .with_ballista_adaptive_query_planner(adaptive)
    }

    /// A failed job's `error`, with its job id as `<job>`: the scheduler
    /// picks the id, so the client cannot know it in advance.
    fn without_job_id(error: &str) -> String {
        const PREFIX: &str = "Execution error: Job ";
        let (_, reason) = error
            .strip_prefix(PREFIX)
            .and_then(|rest| rest.split_once(' '))
            .unwrap_or_else(|| panic!("not a failed job's error: {error}"));
        format!("{PREFIX}<job> {reason}")
    }

    /// Starts a cluster whose scheduler runs [`CommitExec`] roots itself and
    /// whose executor cannot decode them, runs a commit on it in `session`,
    /// and returns its result and how many commits ran.
    async fn run_commit(
        session: SessionConfig,
        reject: bool,
    ) -> (Result<Vec<RecordBatch>>, usize) {
        let commits = Arc::new(AtomicUsize::new(0));
        let codec = Arc::new(CommitCodec {
            commits: Arc::clone(&commits),
            inner: BallistaPhysicalExtensionCodec::default(),
        });
        let config = session
            .with_ballista_physical_extension_codec(codec.clone())
            .with_ballista_scheduler_root(Arc::new(|plan: &dyn ExecutionPlan| {
                plan.is::<CommitExec>()
            }));
        let executor_config = SessionConfig::new_with_ballista();
        let (host, port) = setup_test_cluster_with_split_state(
            &state_with_config(config.clone()),
            // Without the codec, an executor sent a commit fails its task.
            &state_with_config(executor_config.clone()),
            1,
            executor_config.ballista_standalone_parallelism(),
        )
        .await;

        // Five rows in two partitions, hash-repartitioned: two stages.
        let (writers, _) = two_stage_physical_plan().await;
        let plan = Arc::new(CommitExec::new(writers, reject, commits.clone()));
        let result = async {
            let stream = execute_physical_plan::<PhysicalPlanNode>(
                format!("http://{host}:{port}"),
                &config.ballista_config(),
                plan,
                codec.as_ref(),
                uuid::Uuid::new_v4().to_string(),
                config,
            )
            .await?;
            datafusion::physical_plan::common::collect(stream).await
        }
        .await;
        (result, commits.load(Ordering::SeqCst))
    }

    #[rstest]
    #[tokio::test]
    async fn should_commit_once_on_the_scheduler(
        #[values(false, true)] adaptive: bool,
        #[values(false, true)] pull: bool,
    ) {
        let (result, commits) = run_commit(session(adaptive, pull), false).await;

        let result = pretty_format_batches(&result.unwrap()).unwrap().to_string();
        assert_eq!(
            result,
            "+-------+\n\
             | count |\n\
             +-------+\n\
             | 5     |\n\
             +-------+"
        );
        assert_eq!(commits, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn should_fail_the_job_when_its_commit_fails(
        #[values(false, true)] pull: bool,
    ) {
        let (result, commits) = run_commit(session(false, pull), true).await;

        assert_eq!(
            without_job_id(&result.unwrap_err().to_string()),
            "Execution error: Job <job> failed: Error running the job's scheduler root: \
             DataFusion error: Execution error: commit rejected"
        );
        assert_eq!(commits, 1);
    }

    /// A pipelined job streams its result while it runs, so it cannot hold
    /// its root back to run on the scheduler, and would run it as a task.
    #[tokio::test]
    async fn should_refuse_a_pipelined_job_with_a_scheduler_root() {
        let pipelined =
            session(false, false).set_str(BALLISTA_EXECUTION_MODE, "pipelined");
        let (result, commits) = run_commit(pipelined, false).await;

        assert_eq!(
            without_job_id(&result.unwrap_err().to_string()),
            "Execution error: Job <job> failed: General error: CommitExec is to run on \
             the scheduler, which a pipelined job cannot do; run the job staged"
        );
        assert_eq!(commits, 0);
    }

    /// A standalone context in `config` whose scheduler runs `INSERT` sinks
    /// itself, with a table `written` stored in the returned directory, and
    /// how many roots the scheduler has split off.
    async fn sink_context(
        config: SessionConfig,
    ) -> (SessionContext, TempDir, Arc<AtomicUsize>) {
        let roots = Arc::new(AtomicUsize::new(0));
        let picked = Arc::clone(&roots);
        let config = config
            .with_target_partitions(4)
            .with_ballista_scheduler_root(Arc::new(move |plan: &dyn ExecutionPlan| {
                let sink = plan.is::<DataSinkExec>();
                if sink {
                    picked.fetch_add(1, Ordering::SeqCst);
                }
                sink
            }));
        let ctx = SessionContext::standalone_with_state(state_with_config(config))
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        collect_text(
            &ctx,
            &format!(
                "CREATE EXTERNAL TABLE written (id INT) STORED AS PARQUET LOCATION '{}'",
                dir.path().display()
            ),
        )
        .await;
        (ctx, dir, roots)
    }

    /// Two writers, from the two sides of the union.
    const INSERT: &str = "INSERT INTO written \
        SELECT column1 AS id FROM (VALUES (1), (2), (3)) \
        UNION ALL SELECT column1 AS id FROM (VALUES (4), (5))";

    /// A SQL `INSERT` whose sink runs on the scheduler: the scheduler plans
    /// the job from its logical plan, then writes what the executors read.
    ///
    /// Picking the sink sends every row it writes through the scheduler,
    /// which a real deployment should not do: it picks a commit, whose input
    /// is the writers' small reports. No commit is built into DataFusion,
    /// so the sink stands in for one, to cover a job planned from SQL.
    #[rstest]
    #[tokio::test]
    async fn should_write_an_insert_on_the_scheduler(
        #[values(false, true)] adaptive: bool,
        #[values(false, true)] pull: bool,
    ) {
        let (ctx, _dir, roots) = sink_context(
            SessionConfig::new_with_ballista()
                .set_bool(BALLISTA_CLIENT_PULL, pull)
                .with_ballista_adaptive_query_planner(adaptive),
        )
        .await;

        let inserted = collect_text(&ctx, INSERT).await;
        let written = collect_text(&ctx, "SELECT id FROM written ORDER BY id").await;

        assert_eq!(
            inserted,
            "+-------+\n\
             | count |\n\
             +-------+\n\
             | 5     |\n\
             +-------+"
        );
        assert_eq!(
            written,
            "+----+\n\
             | id |\n\
             +----+\n\
             | 1  |\n\
             | 2  |\n\
             | 3  |\n\
             | 4  |\n\
             | 5  |\n\
             +----+"
        );
        assert_eq!(roots.load(Ordering::SeqCst), 1);
    }

    /// A pipelined job planned from SQL is refused like one submitted as a
    /// physical plan, before anything is written.
    #[tokio::test]
    async fn should_refuse_a_pipelined_insert_with_a_scheduler_root() {
        let (ctx, _dir, _) = sink_context(
            SessionConfig::new_with_ballista()
                .set_str(BALLISTA_EXECUTION_MODE, "pipelined"),
        )
        .await;

        let error = ctx.sql(INSERT).await.unwrap().collect().await.unwrap_err();
        let written = collect_text(&ctx, "SELECT count(*) AS rows FROM written").await;

        assert_eq!(
            without_job_id(&error.to_string()),
            "Execution error: Job <job> failed: General error: DataSinkExec is to run on \
             the scheduler, which a pipelined job cannot do; run the job staged"
        );
        assert_eq!(
            written,
            "+------+\n\
             | rows |\n\
             +------+\n\
             | 0    |\n\
             +------+"
        );
    }
}
