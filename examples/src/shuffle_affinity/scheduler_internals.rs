//! Copies of scheduler internals, kept here rather than widening its API while
//! the policy is evaluated (#2319). Keep them in sync with the originals.

use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{RangeShuffleReaderExec, ShuffleReaderExec};
use ballista_core::serde::protobuf::AvailableVcores;
use ballista_core::serde::scheduler::TaskKey;
use datafusion::physical_expr::Distribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use ballista_scheduler::cluster::BoundTask;
use ballista_scheduler::state::execution_graph::{TaskDescription, create_task_info};
use ballista_scheduler::state::execution_stage::RunningStage;

/// Whether each child of `plan` is read whole by every task rather than sliced.
/// Copied from `child_scopes` in `ballista_scheduler::state::task_builder`.
pub(super) fn child_scopes(
    plan: &Arc<dyn ExecutionPlan>,
    under_collect: bool,
) -> Vec<bool> {
    let children = plan.children();
    if under_collect {
        return vec![true; children.len()];
    }
    // These read every partition without declaring a distribution requirement.
    if plan.is::<CoalescePartitionsExec>() || plan.is::<SortPreservingMergeExec>() {
        return vec![true; children.len()];
    }
    let required = plan.input_distribution_requirements();
    let per_child = required.per_child_distributions();
    if per_child.len() != children.len() {
        // Mismatched requirements: stay partition-aligned, as upstream does.
        return vec![false; children.len()];
    }
    per_child
        .map(|d| matches!(d, Distribution::SinglePartition))
        .collect()
}

/// Whether one task must read every input partition of this stage. Copied from
/// `ballista_scheduler::cluster`; splitting such a stage gives wrong results.
pub(super) fn stage_has_input_collapse(plan_root: &Arc<dyn ExecutionPlan>) -> bool {
    fn walk(node: &Arc<dyn ExecutionPlan>) -> bool {
        if node.downcast_ref::<ShuffleReaderExec>().is_some()
            || node.downcast_ref::<RangeShuffleReaderExec>().is_some()
        {
            return false;
        }
        if node.properties().output_partitioning().partition_count() == 1 {
            return true;
        }
        match node.children().as_slice() {
            [child] => walk(child),
            _ => false,
        }
    }
    match plan_root.children().as_slice() {
        [child] => walk(child),
        _ => false,
    }
}

/// Partition cap per task from the session config, with zero meaning no cap.
/// Copied from `bind_one` in `ballista_scheduler::cluster`.
pub(super) fn max_partitions_per_task(running_stage: &RunningStage) -> usize {
    running_stage
        .session_config
        .options()
        .extensions
        .get::<BallistaConfig>()
        .map(|bc| bc.max_partitions_per_task())
        .filter(|&n| n > 0)
        .unwrap_or(usize::MAX)
}

/// Builds one task over `input_partition_ids` and charges `budget`. Copied from
/// `bind_one` in `ballista_scheduler::cluster`, with the caller choosing partitions.
pub(super) fn bind_one_from(
    running_stage: &mut RunningStage,
    session_id: &str,
    job_id: &JobId,
    budget: &mut AvailableVcores,
    input_partition_ids: Vec<usize>,
    is_collapse: bool,
) -> Option<BoundTask> {
    if input_partition_ids.is_empty() {
        return None;
    }
    let vcores_consumed = if is_collapse {
        1
    } else {
        input_partition_ids.len() as u32
    };
    let executor_id = budget.executor_id.clone();
    // The task id is its slot in `task_infos`.
    let task_id = running_stage.task_infos.len();
    let task_attempt = input_partition_ids
        .iter()
        .map(|pid| running_stage.task_failure_numbers[*pid])
        .max()
        .unwrap_or(0);
    let mut task_info = create_task_info(executor_id.clone(), task_id);
    task_info.global_input_partition_ids = input_partition_ids.clone();
    task_info.vcores_consumed = vcores_consumed;
    running_stage.task_infos.push(task_info);
    let task_desc = TaskDescription {
        session_id: session_id.to_string(),
        key: TaskKey {
            job_id: job_id.clone(),
            stage_id: running_stage.stage_id,
            task_id,
        },
        stage_attempt_num: running_stage.stage_attempt_num,
        task_attempt,
        global_input_partition_ids: input_partition_ids,
        vcores_consumed,
        plan: running_stage.plan.clone(),
        session_config: running_stage.session_config.clone(),
    };
    budget.vcores -= vcores_consumed;
    Some((executor_id, task_desc))
}
