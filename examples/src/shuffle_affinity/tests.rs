use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::config::BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK;
use ballista_core::error::Result;
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::AvailableVcores;
use ballista_core::serde::scheduler::{PartitionId, PartitionLocation, PartitionStats};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::NullEquality;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec, PartitionMode};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::SessionConfig;

use super::locality::StageLocality;
use super::policy::ShuffleAffinityPolicy;
use super::scheduler_internals::{bind_one_from, stage_has_input_collapse};
use super::stats::{LocalityObserver, LocalityStats};
use ballista_core::serde::protobuf::{self, TaskStatus, task_status};
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorOperatingSystemSpecification, ExecutorSpecification,
};
use ballista_scheduler::cluster::ClusterState;
use ballista_scheduler::cluster::memory::InMemoryClusterState;
use ballista_scheduler::cluster::{BoundTask, DistributionPolicy};
use ballista_scheduler::config::TaskDistributionPolicy;
use ballista_scheduler::planner::DefaultDistributedPlanner;
use ballista_scheduler::state::execution_graph::StaticExecutionGraph;
use ballista_scheduler::state::execution_graph::{ExecutionGraph, TaskDescription};
use ballista_scheduler::state::execution_stage::ExecutionStage;
use ballista_scheduler::state::execution_stage::RunningStage;
use ballista_scheduler::state::task_manager::JobInfoCache;
use datafusion::functions_aggregate::sum::sum;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::col;
use datafusion::prelude::SessionContext;
use datafusion::test_util::scan_empty_with_partitions;
use mock_locality_executor as executor;
use std::collections::HashSet;

/// An executor large enough that its budget never masks a placement.
fn mock_locality_executor(executor_id: &str) -> ExecutorMetadata {
    ExecutorMetadata {
        id: executor_id.to_string(),
        host: "localhost".to_string(),
        port: 50051,
        grpc_port: 50052,
        specification: ExecutorSpecification::default().with_vcores(8),
        os_info: ExecutorOperatingSystemSpecification::default(),
    }
}

/// Available tasks in the map stage, the first running stage.
fn first_running_stage_tasks(graph: &StaticExecutionGraph) -> usize {
    graph
        .stages()
        .values()
        .filter_map(|stage| match stage {
            ExecutionStage::Running(stage) => Some(stage.available_tasks()),
            _ => None,
        })
        .find(|available| *available > 0)
        .unwrap()
}

/// Completes the map stage, leaving the consumer stage pending. `producer` and
/// `bytes` choose each map task's executor and its bytes per output partition.
fn complete_map_stage(
    graph: &mut StaticExecutionGraph,
    producer: impl Fn(usize) -> &'static str,
    bytes: impl Fn(usize, usize) -> u64,
) -> Result<()> {
    graph.revive();

    // Only the map stage: popping further would drain the consumer stage.
    let map_tasks = first_running_stage_tasks(graph);

    for map_task in 0..map_tasks {
        let Some(task) = pop_map_task(graph) else {
            break;
        };
        let executor = mock_locality_executor(producer(map_task));
        let status = mock_completed_task_with_partition_bytes(task, &executor.id, |p| {
            bytes(map_task, p)
        });
        graph.update_task_status(&executor, vec![status], 1, 1)?;
    }

    Ok(())
}

/// A job whose map stage has completed, with even map tasks on `executor_1` and
/// odd ones on `executor_2`.
async fn mock_shuffle_jobs(
    job_id: &JobId,
    num_partitions: usize,
    bytes: &(dyn Fn(usize, usize) -> u64 + Sync),
) -> Result<HashMap<JobId, JobInfoCache>> {
    let mut graph = aggregation_graph(job_id, num_partitions, vec![col("id")]).await;
    complete_map_stage(
        &mut graph,
        |map_task| {
            if map_task % 2 == 0 {
                "executor_1"
            } else {
                "executor_2"
            }
        },
        bytes,
    )?;
    let mut jobs = HashMap::new();
    jobs.insert(job_id.clone(), JobInfoCache::new(Box::new(graph)));
    Ok(jobs)
}

/// A two-stage aggregation, copied from the scheduler's `cfg(test)` utilities.
/// With no `group_by` the consumer stage collapses into one task.
async fn aggregation_graph(
    job_id: &JobId,
    partitions: usize,
    group_by: Vec<Expr>,
) -> StaticExecutionGraph {
    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let session_state = ctx.state();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("gmv", DataType::UInt64, false),
    ]);

    let logical_plan = scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), 2)
        .unwrap()
        .aggregate(group_by, vec![sum(col("gmv"))])
        .unwrap()
        .build()
        .unwrap();

    let optimized_plan = session_state.optimize(&logical_plan).unwrap();
    let plan = session_state
        .create_physical_plan(&optimized_plan)
        .await
        .unwrap();

    let mut planner = DefaultDistributedPlanner::new();
    StaticExecutionGraph::new(
        "localhost:50050",
        job_id,
        "",
        "session",
        plan,
        0,
        // No per-task cap, so tests control the layout.
        Arc::new(
            SessionConfig::new_with_ballista()
                .set_str(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, "0"),
        ),
        &mut planner,
        None,
    )
    .unwrap()
}

/// A successful status whose output partitions report `num_bytes(partition)`.
/// Copied from the scheduler's `cfg(test)` utilities.
fn mock_completed_task_with_partition_bytes(
    task: TaskDescription,
    executor_id: &str,
    num_bytes: impl Fn(usize) -> u64,
) -> TaskStatus {
    let partitions = (0..task.get_output_partition_number())
        .map(|partition_id| protobuf::ShuffleWritePartition {
            partition_id: partition_id as u64,
            num_batches: 1,
            num_rows: 1,
            num_bytes: num_bytes(partition_id),
            file_id: None,
            is_sort_shuffle: false,
        })
        .collect();

    TaskStatus {
        task_id: task.key.task_id as u32,
        job_id: task.key.job_id.clone().into(),
        stage_id: task.key.stage_id as u32,
        stage_attempt_num: task.stage_attempt_num as u32,
        launch_time: 0,
        start_exec_time: 0,
        end_exec_time: 0,
        metrics: vec![],
        status: Some(task_status::Status::Successful(protobuf::SuccessfulTask {
            executor_id: executor_id.to_owned(),
            partitions,
            runtime_stats: vec![],
            window_state: vec![],
        })),
    }
}

/// Binds one single-partition task off the running stage, standing in for the
/// scheduler's `cfg(test)` `pop_next_task`. The executor is a placeholder.
fn pop_map_task(graph: &mut StaticExecutionGraph) -> Option<TaskDescription> {
    let session_id = graph.session_id().to_string();
    let job_id = graph.job_id().clone();
    let stage = graph.fetch_running_stage(&[])?;
    let mut budget = AvailableVcores {
        executor_id: "unassigned".to_string(),
        vcores: 1,
    };
    // Never as a collapse, or one task would drain the whole stage.
    let partition = stage.pending.next_slice(1);
    bind_one_from(stage, &session_id, &job_id, &mut budget, partition, false)
        .map(|(_, task)| task)
}

/// Records every round an observer receives.
#[derive(Default)]
struct RecordingObserver {
    rounds: std::sync::Mutex<Vec<LocalityStats>>,
}

impl RecordingObserver {
    fn rounds(&self) -> Vec<LocalityStats> {
        super::lock(&self.rounds).clone()
    }

    /// Every round summed.
    fn total(&self) -> LocalityStats {
        let mut total = LocalityStats::default();
        for round in self.rounds() {
            total += round;
        }
        total
    }
}

impl LocalityObserver for RecordingObserver {
    fn observe(&self, round: &LocalityStats) {
        super::lock(&self.rounds).push(*round);
    }
}

fn location(executor_id: &str, partition: usize, bytes: u64) -> PartitionLocation {
    PartitionLocation {
        map_partition_id: 0,
        partition_id: PartitionId {
            job_id: "job".into(),
            stage_id: 1,
            partition_id: partition,
        },
        executor_meta: executor(executor_id),
        partition_stats: PartitionStats::new(None, None, Some(bytes)),
        file_id: None,
        is_sort_shuffle: false,
    }
}

/// A location whose producer reported no size.
fn unsized_location(executor_id: &str, partition: usize) -> PartitionLocation {
    PartitionLocation {
        partition_stats: PartitionStats::new(None, None, None),
        ..location(executor_id, partition, 0)
    }
}

/// The one-column schema every mock reader carries.
fn reader_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, false)]))
}

fn reader_over(partitions: Vec<Vec<PartitionLocation>>) -> Arc<dyn ExecutionPlan> {
    let n = partitions.len();
    Arc::new(
        ShuffleReaderExec::try_new(
            1,
            partitions,
            reader_schema(),
            Partitioning::UnknownPartitioning(n),
        )
        .unwrap(),
    )
}

/// A reader whose partition `p` is held by the executors in `layout[p]`.
fn reader(layout: Vec<Vec<(&str, u64)>>) -> Arc<dyn ExecutionPlan> {
    reader_over(
        layout
            .into_iter()
            .enumerate()
            .map(|(p, holders)| {
                holders
                    .into_iter()
                    .map(|(id, bytes)| location(id, p, bytes))
                    .collect()
            })
            .collect(),
    )
}

/// The same, for producers that reported no sizes at all.
fn reader_without_sizes(layout: Vec<Vec<&str>>) -> Arc<dyn ExecutionPlan> {
    reader_over(
        layout
            .into_iter()
            .enumerate()
            .map(|(p, holders)| {
                holders
                    .into_iter()
                    .map(|id| unsized_location(id, p))
                    .collect()
            })
            .collect(),
    )
}

/// A partitioned-mode hash join, which reads partition `k` of both sides.
fn partitioned_hash_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let on = vec![(
        physical_col("v", &left.schema()).unwrap(),
        physical_col("v", &right.schema()).unwrap(),
    )];
    Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    )
}

/// A broadcast reader: one logical partition holding every location.
fn broadcast_reader(holders: Vec<(&str, u64)>) -> Arc<dyn ExecutionPlan> {
    let locations = holders
        .into_iter()
        .map(|(id, bytes)| location(id, 0, bytes))
        .collect();
    Arc::new(
        ShuffleReaderExec::try_new_broadcast(1, locations, reader_schema(), 1).unwrap(),
    )
}

/// Clones share stats, so an embedder's handle sees the scheduler's rounds.
#[test]
fn clones_of_a_policy_share_their_state() {
    let policy = ShuffleAffinityPolicy::new();
    let clone = policy.clone();

    assert!(Arc::ptr_eq(&policy.stats_handle(), &clone.stats_handle()));

    clone.record(LocalityStats {
        tasks: 1,
        local_bytes: 10,
        total_bytes: 10,
        ..Default::default()
    });
    assert_eq!(
        1,
        policy.stats().tasks,
        "the clone's round is the original's"
    );
}

#[test]
fn preferred_executor_is_the_largest_byte_holder() {
    let plan = reader(vec![
        vec![("executor_1", 900), ("executor_2", 100)],
        vec![("executor_1", 100), ("executor_2", 900)],
        vec![("executor_2", 5)],
    ]);
    let locality = StageLocality::of(&plan);

    assert_eq!(Some("executor_1"), best_holder(&locality, 0));
    assert_eq!(Some("executor_2"), best_holder(&locality, 1));
    assert_eq!(Some("executor_2"), best_holder(&locality, 2));
    // executor_2 holds 1005 bytes against executor_1's 1000.
    assert_eq!(locality.dominant_executor(), Some("executor_2"));
}

/// A holder below the share bar is not preferred, but its bytes still count.
#[test]
fn a_holder_below_the_threshold_is_still_measured() {
    let holders: Vec<(&str, u64)> = vec![
        ("executor_1", 600),
        ("executor_2", 500),
        ("executor_3", 400),
        ("executor_4", 300),
        ("executor_5", 200),
    ];
    let plan = reader(vec![holders]);
    let locality = StageLocality::of(&plan);
    let partition = &locality.partitions[&0];

    assert_eq!(200, partition.bytes_on("executor_5"));
    assert_eq!(0, partition.bytes_on("executor_9"));

    // 200 of 2000 is under the bar, so the slot is not offered.
    let mut capacity = HashMap::from([("executor_5", 1)]);
    assert!(locality.assign(0..1, &mut capacity).is_empty());
}

/// An evenly spread partition has no preferred holder.
#[test]
fn an_evenly_spread_partition_has_no_home() {
    // Eight executors, an eighth of the partition each.
    let locality = StageLocality::of(&reader(vec![vec![
        ("executor_1", 125),
        ("executor_2", 125),
        ("executor_3", 125),
        ("executor_4", 125),
        ("executor_5", 125),
        ("executor_6", 125),
        ("executor_7", 125),
        ("executor_8", 125),
    ]]));

    let mut capacity = HashMap::from([("executor_1", 1), ("executor_2", 1)]);
    assert!(
        locality.assign(0..1, &mut capacity).is_empty(),
        "no executor holds enough to be worth preferring",
    );
    assert_eq!(
        Some(&1),
        capacity.get("executor_1"),
        "and no capacity was spent pretending otherwise",
    );

    // The bytes are still recorded.
    assert_eq!(125, locality.partitions[&0].bytes_on("executor_1"));
}

/// The share bar is inclusive.
#[test]
fn an_exact_threshold_share_counts_as_a_home() {
    let locality = StageLocality::of(&reader(vec![vec![
        ("executor_1", 200),
        ("executor_2", 200),
        ("executor_3", 200),
        ("executor_4", 200),
        ("executor_5", 200),
    ]]));

    let mut capacity = HashMap::from([("executor_5", 1)]);
    assert_eq!(
        HashMap::from([(0, "executor_5")]),
        locality.assign(0..1, &mut capacity),
    );
}

#[test]
fn a_stage_without_shuffle_input_has_no_locality() {
    let plan = reader(vec![]);
    let locality = StageLocality::of(&plan);

    assert!(locality.partitions.is_empty());
    assert!(locality.dominant_executor().is_none());
}

/// Every partition prefers the executor that wrote its bigger half.
#[tokio::test]
async fn affinity_binds_each_partition_to_its_home_executor() -> Result<()> {
    let bound = bind(
        &ShuffleAffinityPolicy::new(),
        mock_jobs(8).await?,
        &[("executor_1", 4), ("executor_2", 4)],
    )
    .await;

    // Even partitions were written large by executor_1, odd by executor_2.
    for (executor_id, task) in &bound {
        for partition in &task.global_input_partition_ids {
            let expected = if partition % 2 == 0 {
                "executor_1"
            } else {
                "executor_2"
            };
            assert_eq!(
                executor_id, expected,
                "partition {partition} should run where its bytes are",
            );
        }
    }
    assert_eq!(8, partitions_covered(&bound));

    Ok(())
}

/// A partition with no live holder is still bound, and scores zero locality.
#[tokio::test]
async fn affinity_falls_back_when_no_executor_holds_the_input() -> Result<()> {
    let policy = ShuffleAffinityPolicy::new();
    let bound = bind(&policy, mock_jobs(8).await?, &[("executor_9", 8)]).await;

    assert_eq!(8, partitions_covered(&bound));
    assert!(bound.iter().all(|(id, _)| id == "executor_9"));

    let stats = policy.stats();
    assert_eq!(0, stats.local_bytes);
    assert_eq!(0, stats.local_partitions);
    assert_eq!(8, stats.partitions);
    assert_eq!(8_000, stats.total_bytes, "all of it read over the network");
    assert_eq!(0.0, stats.local_byte_ratio());

    Ok(())
}

/// A broadcast counts toward stage bytes but steers no partition.
#[test]
fn broadcast_bytes_count_for_the_stage_but_not_for_a_partition() {
    let broadcast = broadcast_reader(vec![("executor_2", 5_000)]);
    let locality = StageLocality::of(&broadcast);

    assert!(
        locality.partitions.is_empty(),
        "a broadcast reader says nothing about which partition goes where",
    );
    assert_eq!(
        locality.dominant_executor(),
        Some("executor_2"),
        "its bytes still belong to the stage's totals",
    );
}

/// A `UnionExec` offsets each child's partitions into the stage's index space.
#[test]
fn union_children_occupy_disjoint_partition_ranges() {
    let left = reader(vec![vec![("executor_1", 100)], vec![("executor_1", 100)]]);
    let right = reader(vec![vec![("executor_2", 100)], vec![("executor_2", 100)]]);
    let union = UnionExec::try_new(vec![left, right]).unwrap();

    let locality = StageLocality::of(&union);

    assert_eq!(
        (0..4)
            .map(|p| best_holder(&locality, p))
            .collect::<Vec<_>>(),
        vec![
            Some("executor_1"),
            Some("executor_1"),
            Some("executor_2"),
            Some("executor_2"),
        ],
    );
}

/// Co-partitioned join sides share partition indexes, so their bytes add.
#[test]
fn co_partitioned_readers_sum_into_the_same_partition() {
    let build = reader(vec![vec![("executor_1", 60)], vec![("executor_1", 60)]]);
    let probe = reader(vec![vec![("executor_2", 100)], vec![("executor_2", 100)]]);
    let join = partitioned_hash_join(build, probe);

    let locality = StageLocality::of(&join);

    assert_eq!(
        Some("executor_2"),
        best_holder(&locality, 0),
        "partition 0 holds 60 bytes on executor_1 and 100 on executor_2",
    );
    assert_eq!(60, locality.partitions[&1].bytes_on("executor_1"));
    assert_eq!(120, locality.bytes_on("executor_1"));
    assert_eq!(200, locality.bytes_on("executor_2"));
}

/// A cross join reads its left side whole in every task, so those bytes count
/// for the stage and steer no partition.
#[test]
fn a_collected_join_side_counts_for_the_stage_but_not_for_a_partition() {
    let build = reader(vec![vec![("executor_1", 900)], vec![("executor_1", 900)]]);
    let probe = reader(vec![vec![("executor_2", 100)], vec![("executor_2", 100)]]);
    let join: Arc<dyn ExecutionPlan> = Arc::new(CrossJoinExec::new(build, probe));

    let locality = StageLocality::of(&join);

    assert_eq!(
        Some("executor_2"),
        best_holder(&locality, 0),
        "only the probe side steers a partition",
    );
    assert_eq!(
        0,
        locality.partitions[&0].bytes_on("executor_1"),
        "the collected side is not input to any single partition",
    );
    // The stage still counts them, so collapse placement ranks on them.
    assert_eq!(1800, locality.bytes_on("executor_1"));
    assert_eq!(200, locality.bytes_on("executor_2"));
    assert_eq!(Some("executor_1"), locality.dominant_executor());
}

/// A collect applies to every reader below it.
#[test]
fn a_collect_reaches_every_reader_below_it() {
    let build = Arc::new(CoalescePartitionsExec::new(reader(vec![
        vec![("executor_1", 900)],
        vec![("executor_1", 900)],
    ])));
    let probe = reader(vec![vec![("executor_2", 100)], vec![("executor_2", 100)]]);
    let join: Arc<dyn ExecutionPlan> = Arc::new(CrossJoinExec::new(build, probe));

    let locality = StageLocality::of(&join);

    assert_eq!(0, locality.partitions[&0].bytes_on("executor_1"));
    assert_eq!(1800, locality.bytes_on("executor_1"));
}

/// A collapse task goes to the largest stage holder, even when bias would pick
/// `executor_2` for its room and per-partition placement for its first location.
#[tokio::test]
async fn a_collapse_stage_is_placed_where_its_bytes_are() -> Result<()> {
    let bound = bind(
        &ShuffleAffinityPolicy::new(),
        mock_collapse_job().await?,
        &[("executor_1", 1), ("executor_2", 8)],
    )
    .await;

    assert_eq!(1, bound.len(), "a collapse stage binds exactly one task");
    assert_eq!(
        "executor_1", bound[0].0,
        "placed on the executor holding the stage's bytes, not the one \
         with the most room or the one holding its first partition",
    );

    Ok(())
}

/// A full collapse home falls to the next largest holder, not the roomiest one.
#[tokio::test]
async fn a_full_collapse_home_settles_for_the_next_biggest_holder() -> Result<()> {
    let bound = bind(
        &ShuffleAffinityPolicy::new(),
        mock_collapse_job().await?,
        &[("executor_1", 0), ("executor_2", 1), ("executor_3", 8)],
    )
    .await;

    assert_eq!(1, bound.len(), "a collapse stage binds exactly one task");
    assert_eq!(
        "executor_2", bound[0].0,
        "should settle for the second-biggest holder, not the emptiest executor",
    );

    Ok(())
}

/// Partitions a round cannot bind stay queued in their original order.
#[tokio::test]
async fn partitions_a_round_could_not_bind_stay_queued_in_order() -> Result<()> {
    let jobs = mock_jobs(8).await?;
    // Three vcores against eight partitions: five must survive the round.
    bind(
        &ShuffleAffinityPolicy::new(),
        jobs.clone(),
        &[("executor_1", 3)],
    )
    .await;

    let job = jobs.values().next().expect("one job");
    let mut graph = job.execution_graph.write().await;
    let stage = graph.fetch_running_stage(&[]).expect("stage still pending");
    assert_eq!(5, stage.pending.remaining());

    // Affinity took `executor_1`'s even partitions, so the rest remain in order.
    let left = stage.pending.next_slice(usize::MAX);
    assert_eq!(vec![1, 3, 5, 6, 7], left);
    assert!(
        left.windows(2).all(|w| w[0] < w[1]),
        "the unbound partitions kept their queue order: {left:?}",
    );

    Ok(())
}

/// Scores any policy's placement with the affinity policy's own measurement.
fn score(
    locality: &StageLocality,
    whole_stage: bool,
    bound: &[BoundTask],
) -> LocalityStats {
    let mut stats = LocalityStats::default();
    for (executor_id, task) in bound {
        stats +=
            locality.measure(executor_id, &task.global_input_partition_ids, whole_stage);
    }
    stats
}

/// Binds `jobs` with `policy` on a fresh cluster and scores the result against
/// the consumer stage as it was before binding.
async fn measure(
    policy: TaskDistributionPolicy,
    jobs: HashMap<JobId, JobInfoCache>,
    executors: &[(&str, u32)],
) -> Result<LocalityStats> {
    let plan = {
        let job = jobs.values().next().expect("one job");
        let mut graph = job.execution_graph.write().await;
        graph
            .fetch_running_stage(&[])
            .map(|stage| stage.plan.clone())
    };
    let plan = plan.expect("a consumer stage to bind");
    let locality = StageLocality::of(&plan);
    let whole_stage = stage_has_input_collapse(&plan);

    let cluster_state = InMemoryClusterState::default();
    for (executor_id, vcores) in executors {
        register(&cluster_state, executor_id, *vcores).await?;
    }
    let bound = cluster_state
        .bind_schedulable_tasks(policy, Arc::new(jobs), None)
        .await?;

    Ok(score(&locality, whole_stage, &bound))
}

/// The shuffle layouts the benchmark compares policies over.
#[derive(Clone, Copy)]
enum Layout {
    /// Each partition has 900 bytes on one executor and 100 on the other.
    Split,
    /// Every partition split evenly across two executors.
    Even,
    /// One collapse task; `executor_1` holds 900 bytes per partition, `executor_2` 100.
    Collapse,
}

impl Layout {
    async fn jobs(self) -> Result<HashMap<JobId, JobInfoCache>> {
        match self {
            Layout::Split => mock_jobs(8).await,
            Layout::Even => mock_shuffle_jobs(&"job_even".into(), 8, &|_, _| 500).await,
            Layout::Collapse => mock_collapse_job().await,
        }
    }
}

/// Compares affinity with bias and round-robin on local byte share (#2319). The
/// shares are pinned because the contributors guide quotes them.
///
/// Run with `cargo test -p ballista-examples --lib locality_benchmark -- --nocapture`.
#[tokio::test]
async fn locality_benchmark_against_bias_and_round_robin() -> Result<()> {
    /// Label, layout, executor budgets and expected shares, in `POLICIES` order.
    type Scenario<'a> = (&'a str, Layout, &'a [(&'a str, u32)], [f64; 3]);

    const POLICIES: [&str; 3] = ["bias", "round-robin", "affinity"];

    let scenarios: Vec<Scenario> = vec![
        // A clear home per partition, whether or not capacity agrees.
        (
            "split, even capacity",
            Layout::Split,
            &[("executor_1", 8), ("executor_2", 8)],
            [0.5, 0.5, 0.9],
        ),
        (
            "split, capacity elsewhere",
            Layout::Split,
            &[("executor_1", 4), ("executor_2", 16)],
            [0.5, 0.5, 0.9],
        ),
        // Negative control: nothing to exploit, so every policy reads half.
        (
            "even shuffle",
            Layout::Even,
            &[("executor_1", 8), ("executor_2", 8)],
            [0.5, 0.5, 0.5],
        ),
        // A tied-capacity row is omitted: the built-ins would pick by listing order.
        (
            "collapse, capacity elsewhere",
            Layout::Collapse,
            &[("executor_1", 1), ("executor_2", 16)],
            [0.1, 0.1, 0.9],
        ),
    ];

    println!("\nshuffle-affinity vs bias vs round-robin");
    println!("share of shuffle input read from the executor running the task\n");
    println!(
        "{:<30} {:>8} {:>13} {:>10}",
        "layout", "bias", "round-robin", "affinity"
    );
    println!("{}", "-".repeat(65));

    for (label, layout, executors, expected) in &scenarios {
        let mut measured = vec![];
        for policy in [
            TaskDistributionPolicy::Bias,
            TaskDistributionPolicy::RoundRobin,
            TaskDistributionPolicy::Custom(Arc::new(ShuffleAffinityPolicy::new())),
        ] {
            let stats = measure(policy, layout.jobs().await?, executors).await?;
            measured.push(stats.local_byte_ratio());
        }
        println!(
            "{:<30} {:>7.1}% {:>12.1}% {:>9.1}%",
            label,
            measured[0] * 100.0,
            measured[1] * 100.0,
            measured[2] * 100.0,
        );
        for (i, policy) in POLICIES.iter().enumerate() {
            assert!(
                (measured[i] - expected[i]).abs() < 1e-9,
                "{label}: {policy} read {:.3} of its input locally, expected {:.3}. \
                 Update the table in docs/source/contributors-guide/shuffle.md \
                 alongside this scenario.",
                measured[i],
                expected[i],
            );
        }
    }
    println!();

    Ok(())
}

/// Repeated binding rounds reuse the scan; a new attempt invalidates it.
#[test]
fn locality_is_scanned_once_per_stage_attempt() {
    let policy = ShuffleAffinityPolicy::new();
    let job_id: JobId = "job_a".into();
    let plan = reader(vec![vec![("executor_1", 100)]]);

    let first = policy.locality_for(&job_id, &stage(plan.clone(), 0));
    let second = policy.locality_for(&job_id, &stage(plan.clone(), 0));
    assert!(
        Arc::ptr_eq(&first, &second),
        "the second bind of the same stage attempt reuses the scan",
    );

    let retried = policy.locality_for(&job_id, &stage(plan, 1));
    assert!(
        !Arc::ptr_eq(&first, &retried),
        "a new stage attempt can carry new locations, so it rescans",
    );
    assert_eq!(1, policy.cache_len(), "the retry replaces the old entry");
}

#[tokio::test]
async fn the_cache_drops_jobs_that_stopped_running() -> Result<()> {
    let policy = ShuffleAffinityPolicy::new();

    bind(&policy, mock_jobs(8).await?, &[("executor_1", 8)]).await;
    assert_eq!(1, policy.cache_len());

    // The job is gone from the running set on the next round.
    bind(&policy, HashMap::new(), &[]).await;
    assert_eq!(0, policy.cache_len());

    Ok(())
}

/// A policy shared by two schedulers loses its memo: each prunes the other's jobs.
#[tokio::test]
async fn schedulers_sharing_a_policy_evict_each_others_scans() -> Result<()> {
    let policy = ShuffleAffinityPolicy::new();
    let job_a: JobId = "job_a".into();
    let job_b: JobId = "job_b".into();
    let running = stage(reader(vec![vec![("executor_1", 100)]]), 0);

    let scanned_a = policy.locality_for(&job_a, &running);
    policy.locality_for(&job_b, &running);
    assert_eq!(2, policy.cache_len());

    // Scheduler B's round sees only its own jobs.
    policy.prune_cache(&mock_shuffle_jobs(&job_b, 2, &|_, _| 100).await?);
    assert_eq!(1, policy.cache_len(), "job_a's scan went with the prune");

    // So scheduler A rescans the same stage on its next round.
    let rescanned = policy.locality_for(&job_a, &running);
    assert!(!Arc::ptr_eq(&scanned_a, &rescanned));

    Ok(())
}

/// Scans of finished stages are dropped, per job.
#[test]
fn the_cache_drops_stages_that_stopped_running() {
    let policy = ShuffleAffinityPolicy::new();
    let job_id: JobId = "job_a".into();
    let other_job: JobId = "job_b".into();
    let running = stage(reader(vec![vec![("executor_1", 100)]]), 0);

    policy.locality_for(&job_id, &running);
    policy.locality_for(&other_job, &running);
    assert_eq!(2, policy.cache_len());

    // Still running: the scan is still needed.
    let stages = HashMap::from([(
        running.stage_id,
        ExecutionStage::Running(stage(reader(vec![vec![("executor_1", 100)]]), 0)),
    )]);
    policy.prune_finished_stages(&job_id, &stages);
    assert_eq!(2, policy.cache_len());

    // Succeeded: dropped, and only for the job it belongs to.
    let stages = HashMap::from([(
        running.stage_id,
        ExecutionStage::Successful(running.to_successful()),
    )]);
    policy.prune_finished_stages(&job_id, &stages);
    assert_eq!(
        1,
        policy.cache_len(),
        "job_b's scan of the same stage id must survive",
    );
}

/// A scarce vcore goes to the partition with the most bytes on that executor.
#[test]
fn a_scarce_slot_goes_to_the_strongest_locality() {
    let plan = reader(vec![
        vec![("executor_1", 10)],
        vec![("executor_1", 1_000)],
        vec![("executor_1", 100)],
    ]);
    let locality = StageLocality::of(&plan);
    let mut capacity = HashMap::from([("executor_1", 1)]);

    let assignment = locality.assign(0..3, &mut capacity);

    assert_eq!(
        HashMap::from([(1, "executor_1")]),
        assignment,
        "partition 1 has 100x the bytes on executor_1 that partition 0 has",
    );
    assert_eq!(Some(&0), capacity.get("executor_1"));
}

/// A partition whose best holder is full lands on its second best.
#[test]
fn a_full_first_choice_falls_to_the_next_best_holder() {
    let plan = reader(vec![
        vec![("executor_1", 1_000), ("executor_2", 500)],
        vec![("executor_1", 1_000), ("executor_2", 500)],
    ]);
    let locality = StageLocality::of(&plan);
    let mut capacity = HashMap::from([("executor_1", 1), ("executor_2", 1)]);

    let assignment = locality.assign(0..2, &mut capacity);

    assert_eq!(
        HashMap::from([(0, "executor_1"), (1, "executor_2"),]),
        assignment,
        "both partitions prefer executor_1, which only has room for one",
    );
}

/// End to end, a scarce vcore goes to the heaviest partition.
#[tokio::test]
async fn binding_spends_a_scarce_vcore_on_the_heaviest_partition() -> Result<()> {
    // executor_1 (map task 0) holds 100 bytes per partition index, rising
    // with the index; executor_2 holds a token byte of everything.
    let jobs = mock_shuffle_jobs(&"job_a".into(), 4, &|map_task, partition| {
        if map_task % 2 == 0 {
            100 * (partition as u64 + 1)
        } else {
            1
        }
    })
    .await?;

    let bound = bind(&ShuffleAffinityPolicy::new(), jobs, &[("executor_1", 2)]).await;

    let mut covered: Vec<usize> = bound
        .iter()
        .flat_map(|(_, task)| task.global_input_partition_ids.clone())
        .collect();
    covered.sort();
    assert_eq!(
        vec![2, 3],
        covered,
        "the two heaviest partitions, not the two at the front of the queue",
    );

    Ok(())
}

#[tokio::test]
async fn stats_report_the_locality_achieved() -> Result<()> {
    let policy = ShuffleAffinityPolicy::new();
    assert_eq!(LocalityStats::default(), policy.stats());

    bind(
        &policy,
        mock_jobs(8).await?,
        &[("executor_1", 4), ("executor_2", 4)],
    )
    .await;

    // Every partition holds 900 bytes on its home and 100 on the other,
    // and every one was bound home: 8 x 900 local of 8 x 1000 read.
    let stats = policy.stats();
    assert_eq!(
        LocalityStats {
            tasks: 2,
            partitions: 8,
            local_partitions: 8,
            local_bytes: 7_200,
            total_bytes: 8_000,
            imputed_bytes: false,
        },
        stats,
    );
    assert!((stats.local_byte_ratio() - 0.9).abs() < f64::EPSILON);

    Ok(())
}

/// An attached observer receives every round.
#[tokio::test]
async fn an_attached_observer_receives_each_round() -> Result<()> {
    let policy = ShuffleAffinityPolicy::new();
    let observer = Arc::new(RecordingObserver::default());
    assert!(policy.attach_observer(observer.clone()));

    for _ in 0..2 {
        bind(
            &policy,
            mock_jobs(8).await?,
            &[("executor_1", 4), ("executor_2", 4)],
        )
        .await;
    }

    // Each round is a delta, not a running total.
    let rounds = observer.rounds();
    assert_eq!(2, rounds.len());
    for round in &rounds {
        assert_eq!(
            LocalityStats {
                tasks: 2,
                partitions: 8,
                local_partitions: 8,
                local_bytes: 7_200,
                total_bytes: 8_000,
                imputed_bytes: false,
            },
            *round,
        );
    }
    // Summing the deltas is what the policy's own cumulative stats say.
    assert_eq!(policy.stats(), observer.total());

    Ok(())
}

/// A round that binds nothing is not reported.
#[test]
fn an_empty_round_is_not_reported() {
    let policy = ShuffleAffinityPolicy::new();
    let observer = Arc::new(RecordingObserver::default());
    policy.attach_observer(observer.clone());

    policy.record(LocalityStats::default());

    assert!(observer.rounds().is_empty());
    assert_eq!(
        LocalityStats::default(),
        policy.stats(),
        "an empty round should not disturb the cumulative total either",
    );
}

/// The observer is set once, so a second scheduler cannot take it over.
#[tokio::test]
async fn the_observer_is_attached_only_once() {
    let policy = ShuffleAffinityPolicy::new();
    let first = Arc::new(RecordingObserver::default());
    let second = Arc::new(RecordingObserver::default());

    assert!(policy.attach_observer(first.clone()));
    assert!(!policy.attach_observer(second.clone()));

    policy.record(LocalityStats {
        tasks: 1,
        partitions: 1,
        local_partitions: 1,
        local_bytes: 10,
        total_bytes: 10,
        imputed_bytes: false,
    });

    assert_eq!(1, first.rounds().len());
    assert!(second.rounds().is_empty());
}

/// A stage without shuffle input gets no assignments, yet is still bound.
#[tokio::test]
async fn a_stage_with_no_shuffle_input_is_still_bound() -> Result<()> {
    let job_id: JobId = "job_a".into();
    // The map stage reads the scan, so its plan has no partition locations.
    let mut graph = aggregation_graph(&job_id, 4, vec![col("id")]).await;
    graph.revive();
    let expected = first_running_stage_tasks(&graph);
    assert!(expected > 0, "the map stage should have work to bind");

    let mut jobs = HashMap::new();
    jobs.insert(job_id, JobInfoCache::new(Box::new(graph)));

    let bound = bind(&ShuffleAffinityPolicy::new(), jobs, &[("executor_1", 4)]).await;

    assert_eq!(
        expected,
        partitions_covered(&bound),
        "a stage with no locality to exploit still has to be scheduled",
    );

    Ok(())
}

/// With no sizes, ranking counts locations and the stage is flagged unmeasured.
#[test]
fn unsized_producers_still_rank_but_leave_the_stage_unmeasured() {
    let plan = reader_without_sizes(vec![
        vec!["executor_1", "executor_1", "executor_2"],
        vec!["executor_2"],
    ]);
    let locality = StageLocality::of(&plan);

    // Two placeholder bytes against one: the order is still right.
    assert_eq!(Some("executor_1"), best_holder(&locality, 0));
    assert_eq!(Some("executor_2"), best_holder(&locality, 1));
    // But nothing here is a byte count.
    assert!(!locality.measured);
}

/// One unsized producer pads its partition's total and marks the whole stage.
#[test]
fn one_unsized_producer_marks_the_whole_stage_unmeasured() {
    let plan = reader_over(vec![
        vec![
            location("executor_1", 0, 900),
            unsized_location("executor_2", 0),
        ],
        vec![
            location("executor_1", 1, 100),
            location("executor_2", 1, 900),
        ],
    ]);
    let locality = StageLocality::of(&plan);

    assert_eq!(901, locality.partitions[&0].total, "900 plus a placeholder");
    assert_eq!(1_000, locality.partitions[&1].total, "fully reported");
    assert!(!locality.measured);
}

/// The flag is sticky: one padded round taints the cumulative total.
#[test]
fn imputed_bytes_survives_a_later_measured_round() {
    assert!(!LocalityStats::default().imputed_bytes);

    let mut stats = LocalityStats {
        tasks: 1,
        partitions: 2,
        local_partitions: 2,
        local_bytes: 900,
        total_bytes: 901,
        imputed_bytes: true,
    };

    stats += LocalityStats {
        tasks: 1,
        partitions: 2,
        local_partitions: 0,
        local_bytes: 0,
        total_bytes: 1_000,
        imputed_bytes: false,
    };
    assert!(stats.imputed_bytes);
    assert_eq!(4, stats.partitions);
}

/// A collapse task's local bytes include broadcast input, but its local
/// partitions do not.
#[test]
fn a_collapse_task_counts_only_partitions_it_holds() {
    let plan = UnionExec::try_new(vec![
        reader(vec![vec![("executor_1", 900)], vec![("executor_1", 900)]]),
        broadcast_reader(vec![("executor_2", 5_000)]),
    ])
    .unwrap();
    let locality = StageLocality::of(&plan);

    let stats = locality.measure("executor_2", &[0, 1], true);
    assert_eq!(5_000, stats.local_bytes, "the broadcast is read locally");
    assert_eq!(
        0, stats.local_partitions,
        "executor_2 holds no bytes of either partition",
    );
    assert_eq!(2, stats.partitions);

    // The executor that wrote them holds both.
    let stats = locality.measure("executor_1", &[0, 1], true);
    assert_eq!(2, stats.local_partitions);
    assert_eq!(1_800, stats.local_bytes);
}

fn stage(plan: Arc<dyn ExecutionPlan>, stage_attempt_num: usize) -> RunningStage {
    RunningStage::new(
        2,
        stage_attempt_num,
        plan,
        1,
        vec![],
        HashMap::new(),
        Arc::new(SessionConfig::default()),
    )
}

/// The executor holding the largest share of `partition`.
fn best_holder(locality: &StageLocality, partition: usize) -> Option<&str> {
    locality
        .partitions
        .get(&partition)
        .and_then(|partition| partition.holders.first())
        .map(|(executor_id, _)| &**executor_id)
}

/// Binds `jobs` with `policy` over executors given as `(id, vcores)`.
async fn bind(
    policy: &ShuffleAffinityPolicy,
    jobs: HashMap<JobId, JobInfoCache>,
    executors: &[(&str, u32)],
) -> Vec<BoundTask> {
    let mut budgets: Vec<AvailableVcores> = executors
        .iter()
        .map(|(executor_id, vcores)| AvailableVcores {
            executor_id: executor_id.to_string(),
            vcores: *vcores,
        })
        .collect();
    policy
        .bind_tasks(budgets.iter_mut().collect(), Arc::new(jobs))
        .await
        .unwrap()
}

fn partitions_covered(bound: &[BoundTask]) -> usize {
    bound
        .iter()
        .map(|(_, task)| task.global_input_partition_ids.len())
        .sum()
}

/// A collapse job: `executor_2` wrote the first map partition with a tenth of the
/// bytes, `executor_1` the rest.
async fn mock_collapse_job() -> Result<HashMap<JobId, JobInfoCache>> {
    let job_id: JobId = "job_collapse".into();
    let mut graph = aggregation_graph(&job_id, 2, vec![]).await;
    complete_map_stage(
        &mut graph,
        |map_task| {
            if map_task == 0 {
                "executor_2"
            } else {
                "executor_1"
            }
        },
        |map_task, _| if map_task == 0 { 100 } else { 900 },
    )?;

    let mut jobs = HashMap::new();
    jobs.insert(job_id, JobInfoCache::new(Box::new(graph)));
    Ok(jobs)
}

/// Even partitions are written large by `executor_1`, odd by `executor_2`.
async fn mock_jobs(num_partitions: usize) -> Result<HashMap<JobId, JobInfoCache>> {
    mock_shuffle_jobs(&"job_a".into(), num_partitions, &|map_task, partition| {
        if partition % 2 == map_task % 2 {
            900
        } else {
            100
        }
    })
    .await
}

/// Registers `executor_id` with `vcores` free.
async fn register(
    cluster_state: &InMemoryClusterState,
    executor_id: &str,
    vcores: u32,
) -> Result<()> {
    cluster_state
        .register_executor(
            executor(executor_id),
            ExecutorData {
                executor_id: executor_id.to_string(),
                total_vcores: vcores,
                available_vcores: vcores,
            },
        )
        .await
}

/// Which executor each bound partition landed on.
fn placements(bound: &[BoundTask]) -> HashMap<usize, String> {
    bound
        .iter()
        .flat_map(|(executor_id, task)| {
            task.global_input_partition_ids
                .iter()
                .map(move |p| (*p, executor_id.clone()))
        })
        .collect()
}

/// Where each partition lands when `policy` binds a fresh cluster.
async fn place_with(policy: TaskDistributionPolicy) -> Result<HashMap<usize, String>> {
    let cluster_state = InMemoryClusterState::default();
    register(&cluster_state, "executor_1", 4).await?;
    register(&cluster_state, "executor_2", 4).await?;

    let bound = cluster_state
        .bind_schedulable_tasks(policy, Arc::new(mock_jobs(4).await?), None)
        .await?;
    Ok(placements(&bound))
}

/// The push path dispatches to the configured policy instance. Bias packs onto
/// one executor here, so the placements tell the two apart.
#[tokio::test]
async fn the_push_path_dispatches_to_the_configured_policy() -> Result<()> {
    let policy = ShuffleAffinityPolicy::new();
    let affinity =
        place_with(TaskDistributionPolicy::Custom(Arc::new(policy.clone()))).await?;
    let bias = place_with(TaskDistributionPolicy::Bias).await?;

    assert_eq!(4, affinity.len(), "every partition should be bound");
    for (partition, executor_id) in &affinity {
        let expected = if partition % 2 == 0 {
            "executor_1"
        } else {
            "executor_2"
        };
        assert_eq!(
            expected, executor_id,
            "partition {partition} did not land on its home",
        );
    }
    assert_eq!(
        1,
        bias.values().collect::<HashSet<_>>().len(),
        "bias should pack every partition onto the first budget",
    );

    let stats = policy.stats();
    assert_eq!(4, stats.local_partitions);
    assert_eq!(3_600, stats.local_bytes, "4 partitions x 900 bytes local");
    assert_eq!(4_000, stats.total_bytes);

    Ok(())
}

/// A cluster with no room binds nothing and must not error.
#[tokio::test]
async fn the_push_path_with_no_vcores_binds_nothing() -> Result<()> {
    let cluster_state = InMemoryClusterState::default();
    register(&cluster_state, "executor_1", 0).await?;

    let jobs = mock_shuffle_jobs(&"job_a".into(), 4, &|_, _| 900).await?;
    let bound = cluster_state
        .bind_schedulable_tasks(
            TaskDistributionPolicy::Custom(Arc::new(ShuffleAffinityPolicy::new())),
            Arc::new(jobs),
            None,
        )
        .await?;

    assert!(bound.is_empty());

    Ok(())
}
