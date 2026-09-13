use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use ballista_core::JobId;
use ballista_core::config::BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK;
use ballista_core::error::Result;
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{self, AvailableVcores, TaskStatus, task_status};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorMetadata, ExecutorOperatingSystemSpecification,
    ExecutorSpecification, PartitionId, PartitionLocation, PartitionStats,
};
use ballista_scheduler::cluster::memory::InMemoryClusterState;
use ballista_scheduler::cluster::{BoundTask, ClusterState, DistributionPolicy};
use ballista_scheduler::config::TaskDistributionPolicy;
use ballista_scheduler::planner::DefaultDistributedPlanner;
use ballista_scheduler::state::execution_graph::{
    ExecutionGraph, StaticExecutionGraph, TaskDescription,
};
use ballista_scheduler::state::execution_stage::{ExecutionStage, RunningStage};
use ballista_scheduler::state::task_manager::JobInfoCache;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::NullEquality;
use datafusion::functions_aggregate::sum::sum;
use datafusion::logical_expr::{Expr, JoinType, col};
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec, PartitionMode};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::test_util::scan_empty_with_partitions;

use super::locality::StageLocality;
use super::lock;
use super::policy::ShuffleAffinityPolicy;
use super::scheduler_internals::{bind_one_from, stage_has_input_collapse};
use super::stats::{LocalityObserver, LocalityStats};

/// An executor large enough that its budget never masks a placement.
fn executor(executor_id: &str) -> ExecutorMetadata {
    ExecutorMetadata {
        id: executor_id.to_string(),
        host: "localhost".to_string(),
        port: 50051,
        grpc_port: 50052,
        specification: ExecutorSpecification::default().with_vcores(8),
        os_info: ExecutorOperatingSystemSpecification::default(),
    }
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
    let (_, task) =
        bind_one_from(stage, &session_id, &job_id, &mut budget, partition, false);
    Some(task)
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
        let meta = executor(producer(map_task));
        let status = mock_completed_task_with_partition_bytes(task, &meta.id, |p| {
            bytes(map_task, p)
        });
        graph.update_task_status(&meta, vec![status], 1, 1)?;
    }

    Ok(())
}

/// `executor_1` for even `n` and `executor_2` for odd: where map task `n` runs, and
/// where `mock_jobs` writes partition `n` large.
fn parity_executor(n: usize) -> &'static str {
    if n.is_multiple_of(2) {
        "executor_1"
    } else {
        "executor_2"
    }
}

/// A job whose map stage has completed, with map task `n` on `parity_executor(n)`.
async fn mock_shuffle_jobs(
    job_id: &JobId,
    num_partitions: usize,
    bytes: &(dyn Fn(usize, usize) -> u64 + Sync),
) -> Result<HashMap<JobId, JobInfoCache>> {
    let mut graph = aggregation_graph(job_id, num_partitions, vec![col("id")]).await;
    complete_map_stage(&mut graph, parity_executor, bytes)?;
    let mut jobs = HashMap::new();
    jobs.insert(job_id.clone(), JobInfoCache::new(Box::new(graph)));
    Ok(jobs)
}

/// Each partition is written large by its `parity_executor` and small by the other.
async fn mock_jobs(num_partitions: usize) -> Result<HashMap<JobId, JobInfoCache>> {
    mock_shuffle_jobs(&"job_a".into(), num_partitions, &|map_task, partition| {
        if parity_executor(map_task) == parity_executor(partition) {
            900
        } else {
            100
        }
    })
    .await
}

/// Binding `mock_jobs(8)` over two four-vcore executors: every partition bound home,
/// reading 900 of its 1000 bytes locally.
const SPLIT_ROUND: LocalityStats = LocalityStats {
    tasks: 2,
    partitions: 8,
    local_partitions: 8,
    local_bytes: 7_200,
    total_bytes: 8_000,
    imputed_bytes: false,
};

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

/// The executor holding the largest share of `partition`.
fn best_holder(locality: &StageLocality, partition: usize) -> Option<&str> {
    locality
        .partitions
        .get(&partition)
        .and_then(|partition| partition.holders.first())
        .map(|(executor_id, _)| &**executor_id)
}

/// Records every round an observer receives.
#[derive(Default)]
struct RecordingObserver {
    rounds: Mutex<Vec<LocalityStats>>,
}

impl RecordingObserver {
    fn rounds(&self) -> Vec<LocalityStats> {
        lock(&self.rounds).clone()
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
        lock(&self.rounds).push(*round);
    }
}

/// The shuffle layouts the benchmark compares policies over.
#[derive(Clone, Copy)]
enum Layout {
    /// Each partition has 90% of its bytes on one executor, alternating between the two.
    Skewed,
    /// Every partition split evenly between the two executors.
    Uniform,
    /// One task reads every partition, and `executor_1` holds 90% of the bytes.
    GlobalAggregate,
}

impl Layout {
    async fn jobs(self) -> Result<HashMap<JobId, JobInfoCache>> {
        match self {
            Layout::Skewed => mock_jobs(8).await,
            Layout::Uniform => {
                mock_shuffle_jobs(&"job_uniform".into(), 8, &|_, _| 500).await
            }
            Layout::GlobalAggregate => mock_collapse_job().await,
        }
    }
}

/// Binds `jobs` with `policy` on a fresh cluster and returns the local byte share,
/// scored by the affinity policy's own measurement of the stage before binding.
async fn local_share(
    policy: TaskDistributionPolicy,
    jobs: HashMap<JobId, JobInfoCache>,
    executors: &[(&str, u32)],
) -> Result<f64> {
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

    let mut stats = LocalityStats::default();
    for (executor_id, task) in &bound {
        stats +=
            locality.measure(executor_id, &task.global_input_partition_ids, whole_stage);
    }
    Ok(stats.local_byte_ratio())
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

/// A partition is offered to at most its three largest holders with room, but every
/// holder's bytes still count.
#[test]
fn only_the_three_largest_holders_with_room_are_offered() {
    let plan = reader(vec![
        vec![
            ("executor_1", 600),
            ("executor_2", 500),
            ("executor_3", 400),
            ("executor_4", 300),
            ("executor_5", 200),
        ],
        vec![("executor_1", 1_000)],
        vec![("executor_2", 1_000)],
        vec![("executor_3", 1_000)],
    ]);
    let locality = StageLocality::of(&plan);
    assert_eq!(200, locality.partitions[&0].bytes_on("executor_5"));

    // Partitions 1 to 3 take the top three holders' only vcores, and `executor_4`
    // is not offered partition 0 even though it has room.
    let mut capacity = HashMap::from([
        ("executor_1", 1),
        ("executor_2", 1),
        ("executor_3", 1),
        ("executor_4", 1),
    ]);
    let assignment = locality.assign(0..4, &mut capacity);
    assert!(!assignment.contains_key(&0), "{assignment:?}");
    assert_eq!(Some(&1), capacity.get("executor_4"));
}

/// The largest holder is preferred even with less than a fifth of the partition.
#[test]
fn a_holder_under_a_fifth_is_still_preferred() {
    let locality = StageLocality::of(&reader(vec![vec![
        ("executor_1", 120),
        ("executor_2", 160),
        ("executor_3", 120),
        ("executor_4", 120),
        ("executor_5", 120),
        ("executor_6", 120),
        ("executor_7", 120),
        ("executor_8", 120),
    ]]));

    let mut capacity = HashMap::from([("executor_1", 1), ("executor_2", 1)]);
    assert_eq!(
        HashMap::from([(0, "executor_2")]),
        locality.assign(0..1, &mut capacity),
    );
}

/// Holders without free vcores don't use up a partition's three offers.
#[test]
fn busy_holders_do_not_use_up_the_offers() {
    let locality = StageLocality::of(&reader(vec![vec![
        ("executor_1", 400),
        ("executor_2", 300),
        ("executor_3", 200),
        ("executor_4", 100),
    ]]));

    // Only the smallest holder has room.
    let mut capacity = HashMap::from([("executor_4", 1)]);
    assert_eq!(
        HashMap::from([(0, "executor_4")]),
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

    for (executor_id, task) in &bound {
        for &partition in &task.global_input_partition_ids {
            assert_eq!(
                executor_id,
                parity_executor(partition),
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

    Ok(())
}

/// Compares affinity with bias and round-robin on local byte share (#2319). The
/// shares are pinned because the contributors guide quotes them.
///
/// Run with `cargo test -p ballista-examples --lib locality_benchmark -- --nocapture`.
#[tokio::test]
async fn locality_benchmark_against_bias_and_round_robin() -> Result<()> {
    /// Label, layout, budgets, and expected bias, round-robin and affinity shares.
    type Scenario<'a> = (&'a str, Layout, &'a [(&'a str, u32)], [f64; 3]);

    let scenarios: Vec<Scenario> = vec![
        // Affinity should win whether or not the free vcores sit with the bytes.
        (
            "Skewed partitions",
            Layout::Skewed,
            &[("executor_1", 8), ("executor_2", 8)],
            [0.5, 0.5, 0.9],
        ),
        (
            "Skewed partitions",
            Layout::Skewed,
            &[("executor_1", 4), ("executor_2", 16)],
            [0.5, 0.5, 0.9],
        ),
        // Control: nothing to exploit, so every policy reads half.
        (
            "Uniform partitions",
            Layout::Uniform,
            &[("executor_1", 8), ("executor_2", 8)],
            [0.5, 0.5, 0.5],
        ),
        // Not measured with equal free vcores: the built-ins would pick by listing order.
        (
            "Global aggregate",
            Layout::GlobalAggregate,
            &[("executor_1", 1), ("executor_2", 16)],
            [0.1, 0.1, 0.9],
        ),
    ];

    println!("\nShare of shuffle bytes read on the executor running the task\n");
    println!(
        "{:<20} {:>11} {:>8} {:>13} {:>10}",
        "Scenario", "Free vcores", "Bias", "Round-robin", "Affinity"
    );
    println!("{}", "-".repeat(66));

    for (label, layout, executors, expected) in &scenarios {
        let vcores = executors
            .iter()
            .map(|(_, vcores)| vcores.to_string())
            .collect::<Vec<_>>()
            .join(" / ");
        let mut shares = vec![];
        for (name, policy) in [
            ("bias", TaskDistributionPolicy::Bias),
            ("round-robin", TaskDistributionPolicy::RoundRobin),
            (
                "affinity",
                TaskDistributionPolicy::Custom(Arc::new(ShuffleAffinityPolicy::new())),
            ),
        ] {
            shares.push((
                name,
                local_share(policy, layout.jobs().await?, executors).await?,
            ));
        }
        println!(
            "{:<20} {:>11} {:>7.1}% {:>12.1}% {:>9.1}%",
            label,
            vcores,
            shares[0].1 * 100.0,
            shares[1].1 * 100.0,
            shares[2].1 * 100.0,
        );
        for ((name, share), expected) in shares.iter().zip(expected) {
            assert!(
                (share - expected).abs() < 1e-9,
                "{label} with {vcores} free vcores: {name} read {share:.3} locally, expected \
                 {expected:.3}. Update the table in \
                 docs/source/contributors-guide/shuffle.md alongside this scenario.",
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

    let stats = policy.stats();
    assert_eq!(SPLIT_ROUND, stats);
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
    assert_eq!(vec![SPLIT_ROUND, SPLIT_ROUND], observer.rounds());
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
#[test]
fn the_observer_is_attached_only_once() {
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

/// The push path dispatches to the configured policy instance. Bias packs onto
/// one executor here, so the placements tell the two apart.
#[tokio::test]
async fn the_push_path_dispatches_to_the_configured_policy() -> Result<()> {
    let policy = ShuffleAffinityPolicy::new();
    let affinity =
        place_with(TaskDistributionPolicy::Custom(Arc::new(policy.clone()))).await?;
    let bias = place_with(TaskDistributionPolicy::Bias).await?;

    assert_eq!(4, affinity.len(), "every partition should be bound");
    for (&partition, executor_id) in &affinity {
        assert_eq!(
            parity_executor(partition),
            executor_id,
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
