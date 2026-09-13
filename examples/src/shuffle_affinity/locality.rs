//! The scan: where a stage's input bytes live, read from its shuffle readers.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use ballista_core::execution_plans::{RangeShuffleReaderExec, ShuffleReaderExec};
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;

use super::scheduler_internals::child_scopes;
use super::stats::LocalityStats;

/// An interned executor id, so a scan holds one allocation per executor.
type ExecutorId = Arc<str>;

/// Where a stage's input bytes live, per partition and per executor.
pub(super) struct StageLocality {
    /// Holders of each stage-global input partition.
    pub(super) partitions: HashMap<usize, PartitionLocality>,
    /// Bytes per executor across the whole stage, broadcast and collected inputs
    /// included, largest first.
    totals: Vec<(ExecutorId, u64)>,
    /// Whether every producer feeding the stage reported a real size.
    pub(super) measured: bool,
}

/// Who holds one partition's input, best first.
pub(super) struct PartitionLocality {
    /// Every byte a task for this partition will read.
    pub(super) total: u64,
    /// `(executor, bytes)`, largest first, ties by id.
    pub(super) holders: Vec<(ExecutorId, u64)>,
}

impl PartitionLocality {
    /// Bytes `executor_id` holds of this partition, or zero if it holds none.
    pub(super) fn bytes_on(&self, executor_id: &str) -> u64 {
        bytes_held(&self.holders, executor_id)
    }
}

/// Ranks byte counts largest first, ties by id for determinism, interning ids
/// through `ids`.
fn rank<'a>(
    bytes: HashMap<&'a str, u64>,
    ids: &mut HashMap<&'a str, ExecutorId>,
) -> Vec<(ExecutorId, u64)> {
    let mut ranked: Vec<(ExecutorId, u64)> = bytes
        .into_iter()
        .map(|(executor_id, held)| {
            let id = ids
                .entry(executor_id)
                .or_insert_with(|| ExecutorId::from(executor_id));
            (id.clone(), held)
        })
        .collect();
    ranked.sort_unstable_by(|(a_id, a), (b_id, b)| {
        Ord::cmp(b, a).then_with(|| Ord::cmp(a_id, b_id))
    });
    ranked
}

/// Bytes `executor_id` holds of a ranked list, or zero if it holds none.
fn bytes_held(ranked: &[(ExecutorId, u64)], executor_id: &str) -> u64 {
    ranked
        .iter()
        .find(|(id, _)| &**id == executor_id)
        .map(|(_, bytes)| *bytes)
        .unwrap_or(0)
}

/// How many of its largest holders with free vcores a partition is offered to.
/// Offering every holder placed no better, and fewer candidates keep the sort small.
const MAX_HOLDERS_OFFERED: usize = 3;

impl StageLocality {
    pub(super) fn of(plan: &Arc<dyn ExecutionPlan>) -> Self {
        let mut acc = LocalityAcc::default();
        acc.add_plan(plan, 0, false);
        let mut ids: HashMap<&str, ExecutorId> = HashMap::new();

        let mut partitions = HashMap::with_capacity(acc.per_partition.len());
        for (partition, bytes) in acc.per_partition {
            let total: u64 = bytes.values().sum();
            if total == 0 {
                continue;
            }
            let holders = rank(bytes, &mut ids);
            partitions.insert(partition, PartitionLocality { total, holders });
        }

        Self {
            partitions,
            totals: rank(acc.totals, &mut ids),
            measured: !acc.imputed,
        }
    }

    /// Executors holding stage input, most bytes first.
    pub(super) fn ranked_executors(&self) -> impl Iterator<Item = &str> {
        self.totals.iter().map(|(executor_id, _)| &**executor_id)
    }

    /// Every input byte of the stage, wherever it lives.
    pub(super) fn total_bytes(&self) -> u64 {
        self.totals.iter().map(|(_, bytes)| bytes).sum()
    }

    /// Bytes `executor_id` holds across the whole stage.
    pub(super) fn bytes_on(&self, executor_id: &str) -> u64 {
        bytes_held(&self.totals, executor_id)
    }

    /// What a task on `executor_id` over `partitions` reads locally versus in total.
    /// A collapse task (`whole_stage`) counts the whole stage; any other task counts
    /// only its partitions, not inputs every task reads in full.
    pub(super) fn measure(
        &self,
        executor_id: &str,
        partitions: &[usize],
        whole_stage: bool,
    ) -> LocalityStats {
        let mut stats = LocalityStats {
            tasks: 1,
            partitions: partitions.len() as u64,
            imputed_bytes: !self.measured,
            ..Default::default()
        };
        if whole_stage {
            // Its bytes are the stage's, but local partitions still count per partition.
            stats.local_partitions = self.local_partitions(executor_id, partitions);
            stats.local_bytes = self.bytes_on(executor_id);
            stats.total_bytes = self.total_bytes();
            return stats;
        }
        for partition in partitions {
            let Some(locality) = self.partitions.get(partition) else {
                continue;
            };
            let local = locality.bytes_on(executor_id);
            stats.local_bytes += local;
            stats.total_bytes += locality.total;
            if local > 0 {
                stats.local_partitions += 1;
            }
        }
        stats
    }

    /// Of `partitions`, how many the executor holds any bytes of.
    fn local_partitions(&self, executor_id: &str, partitions: &[usize]) -> u64 {
        partitions
            .iter()
            .filter(|&&partition| {
                self.partitions
                    .get(&partition)
                    .is_some_and(|locality| locality.bytes_on(executor_id) > 0)
            })
            .count() as u64
    }

    /// Assigns pending partitions to holders, largest holdings first, spending
    /// `capacity`. Each partition is offered to its [`MAX_HOLDERS_OFFERED`] largest
    /// holders with free vcores; unassigned partitions are left to the fallback pass.
    pub(super) fn assign<'a>(
        &'a self,
        pending: impl Iterator<Item = usize>,
        capacity: &mut HashMap<&str, u32>,
    ) -> HashMap<usize, &'a str> {
        let mut candidates: Vec<Candidate<'a>> = vec![];
        let mut placeable = 0;
        for partition in pending {
            let Some(locality) = self.partitions.get(&partition) else {
                continue;
            };
            let with_room = locality
                .holders
                .iter()
                .filter(|(executor_id, _)| {
                    capacity.get(&**executor_id).is_some_and(|&free| free > 0)
                })
                .take(MAX_HOLDERS_OFFERED);
            let offered = candidates.len();
            for (executor_id, held) in with_room {
                candidates.push(Candidate {
                    bytes: *held,
                    partition,
                    executor_id,
                });
            }
            placeable += usize::from(candidates.len() > offered);
        }
        // Pop the strongest candidates first. Stop once no executor has room or every
        // partition with a candidate is placed, since anything left could only be skipped.
        let mut room: u64 = capacity.values().map(|&vcores| u64::from(vcores)).sum();
        let mut candidates = BinaryHeap::from(candidates);
        let mut assignment = HashMap::new();
        while room > 0
            && assignment.len() < placeable
            && let Some(Candidate {
                partition,
                executor_id,
                ..
            }) = candidates.pop()
        {
            if assignment.contains_key(&partition) {
                continue;
            }
            let Some(free) = capacity.get_mut(executor_id) else {
                continue;
            };
            if *free == 0 {
                continue;
            }
            *free -= 1;
            room -= 1;
            assignment.insert(partition, executor_id);
        }
        assignment
    }
}

/// One executor that could take one pending partition, holding `bytes` of it.
#[derive(PartialEq, Eq)]
struct Candidate<'a> {
    bytes: u64,
    partition: usize,
    executor_id: &'a str,
}

/// Most bytes is greatest, then the lowest partition and executor id, so ties resolve
/// the same way every round.
impl Ord for Candidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes
            .cmp(&other.bytes)
            .then_with(|| other.partition.cmp(&self.partition))
            .then_with(|| other.executor_id.cmp(self.executor_id))
    }
}

impl PartialOrd for Candidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Running totals for one stage plan.
#[derive(Default)]
struct LocalityAcc<'a> {
    /// Bytes per executor per partition, which drives per-partition placement.
    per_partition: HashMap<usize, HashMap<&'a str, u64>>,
    /// Bytes per executor across the stage, including inputs every task reads.
    totals: HashMap<&'a str, u64>,
    /// Whether any producer reported no size.
    imputed: bool,
}

impl<'a> LocalityAcc<'a> {
    /// Adds every shuffle reader's bytes under `node`. `offset` maps a reader's
    /// partitions to stage-global ones under a `UnionExec`, as `task_builder` does,
    /// and `under_collect` marks inputs every task reads whole.
    fn add_plan(
        &mut self,
        node: &'a Arc<dyn ExecutionPlan>,
        offset: usize,
        under_collect: bool,
    ) {
        let reader = match node.downcast_ref::<ShuffleReaderExec>() {
            Some(reader) => Some((reader.partition.as_slice(), reader.broadcast)),
            None => node
                .downcast_ref::<RangeShuffleReaderExec>()
                .map(|reader| (reader.partition.as_slice(), false)),
        };
        if let Some((partitions, broadcast)) = reader {
            // Inputs every task reads whole say nothing about per-partition placement.
            if broadcast || under_collect {
                self.add_stage_bytes(partitions);
            } else {
                self.add_partition_bytes(partitions, offset);
            }
            return;
        }
        // Under a collect a union's partition ranges don't apply, as in `task_builder`.
        if !under_collect && node.is::<UnionExec>() {
            let mut child_offset = offset;
            for child in node.children() {
                self.add_plan(child, child_offset, false);
                child_offset +=
                    child.properties().output_partitioning().partition_count();
            }
            return;
        }
        for (child, collect) in node
            .children()
            .into_iter()
            .zip(child_scopes(node, under_collect))
        {
            self.add_plan(child, offset, collect);
        }
    }

    /// Counts a reader's bytes toward the stage and toward each partition.
    fn add_partition_bytes(
        &mut self,
        partitions: &'a [Vec<PartitionLocation>],
        offset: usize,
    ) {
        self.add_stage_bytes(partitions);
        for (partition, locations) in partitions.iter().enumerate() {
            let by_executor = self.per_partition.entry(offset + partition).or_default();
            for (executor_id, bytes, _) in held(locations) {
                *by_executor.entry(executor_id).or_insert(0) += bytes;
            }
        }
    }

    /// Counts a reader's bytes toward the stage totals only.
    fn add_stage_bytes(&mut self, partitions: &'a [Vec<PartitionLocation>]) {
        for locations in partitions {
            for (executor_id, bytes, measured) in held(locations) {
                *self.totals.entry(executor_id).or_insert(0) += bytes;
                self.imputed |= !measured;
            }
        }
    }
}

/// `(executor, bytes, measured)` per location. An unsized location counts as
/// one placeholder byte: enough to rank, but not measured.
fn held(locations: &[PartitionLocation]) -> impl Iterator<Item = (&str, u64, bool)> {
    locations.iter().map(|location| {
        let bytes = location.partition_stats.num_bytes();
        (
            location.executor_meta.id.as_str(),
            bytes.unwrap_or(1),
            bytes.is_some(),
        )
    })
}
