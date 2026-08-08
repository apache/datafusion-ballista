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

//! Builds REST response DTOs from scheduler execution state.
//!
//! These functions are the only place the scheduler's `ExecutionGraph` /
//! `JobOverview` shapes are translated into the wire types in
//! [`ballista_history::dto`]. Keeping the translation here (rather than inline
//! in the axum handlers) means the same DTOs can be produced from state that
//! did not come from a live handler request.
//!
//! Everything in this module is pure: state in, DTO out, no I/O.

use crate::api::handlers::{JobQueryParams, PlanFormat};
use crate::display::format_stage_metrics;
use crate::state::execution_graph::{ExecutionGraphBox, ExecutionStage};
use crate::state::execution_stage::TaskInfo;
use crate::state::task_manager::JobOverview;
use ballista_core::serde::protobuf::failed_task::FailedReason::{
    ExecutionError, ExecutorLost, FetchPartitionError, IoError, ResultLost, TaskKilled,
};
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{FailedTask, task_status};
use ballista_core::utils::get_current_time;
use ballista_history::dto::{
    JobResponse, Percentiles, QueryStageSummary, QueryStagesResponse, TaskStatus,
    TaskSummary,
};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::{MetricsSet, Time};
use std::time::Duration;

/// Build the `JobResponse` list entry served by `GET /api/jobs`.
///
/// Plan fields are always `None` here: the job list is built from
/// [`JobOverview`] summaries, which do not carry plans.
pub fn job_overview_to_response(job: &JobOverview) -> JobResponse {
    let (plain_status, job_status) = format_job_status(
        &job.status.status,
        job_elapsed_ms(job.start_time, job.end_time),
    );

    // calculate progress based on completed stages for now, but we could use completed
    // tasks in the future to make this more accurate
    let percent_complete = if job.num_stages == 0 {
        0
    } else {
        ((job.completed_stages as f32 / job.num_stages as f32) * 100_f32) as u8
    };

    JobResponse {
        job_id: job.job_id.to_string(),
        job_name: job.job_name.to_owned(),
        job_status,
        status: plain_status,
        start_time: job.start_time,
        end_time: job.end_time,
        num_stages: job.num_stages,
        completed_stages: job.completed_stages,
        percent_complete,
        logical_plan: None,
        physical_plan: None,
        stage_plan: None,
    }
}

/// Build the `JobResponse` served by `GET /api/job/{job_id}`, including the
/// rendered logical, physical, and stage plans.
pub fn graph_to_job_response(
    graph: &ExecutionGraphBox,
    query: &JobQueryParams,
) -> JobResponse {
    let stage_plan = format!("{graph:?}");
    let job = graph.as_ref();

    let (plain_status, job_status) = format_job_status(
        &job.status().status,
        job_elapsed_ms(job.start_time(), job.end_time()),
    );

    let num_stages = job.stage_count();
    let completed_stages = job.completed_stages();
    let percent_complete =
        ((completed_stages as f32 / num_stages as f32) * 100_f32) as u8;

    let plan_format = query.plan_format.clone().unwrap_or_default();
    let physical_plan = match plan_format {
        PlanFormat::Default | PlanFormat::Metrics => {
            DisplayableExecutionPlan::new(job.physical_plan().as_ref())
                .indent(false)
                .to_string()
        }
        PlanFormat::Tree => displayable(job.physical_plan().as_ref())
            .tree_render()
            .to_string(),
    };

    JobResponse {
        job_id: job.job_id().to_string(),
        job_name: job.job_name().to_owned(),
        job_status,
        status: plain_status,
        start_time: job.start_time(),
        end_time: job.end_time(),
        num_stages,
        completed_stages,
        percent_complete,
        logical_plan: job.logical_plan().map(str::to_owned),
        physical_plan: Some(physical_plan),
        stage_plan: Some(stage_plan),
    }
}

/// Build the per-stage summaries served by `GET /api/job/{job_id}/stages`.
pub fn graph_to_query_stages(
    graph: &ExecutionGraphBox,
    query: &JobQueryParams,
) -> QueryStagesResponse {
    let plan_format = query.plan_format.clone().unwrap_or_default();

    let stages = graph
        .as_ref()
        .stages()
        .iter()
        .map(|(id, stage)| {
            let mut summary = QueryStageSummary {
                stage_id: id.to_string(),
                stage_status: stage.variant_name().to_string(),
                input_rows: 0,
                output_rows: 0,
                elapsed_compute: None,
                tasks: vec![],
                task_duration_percentiles: None,
                task_input_percentiles: None,
                stage_plan: None,
            };

            match stage {
                ExecutionStage::Running(running_stage) => {
                    let metrics = running_stage.stage_metrics.as_deref().unwrap_or(&[]);
                    summary.stage_plan = Some(render_stage_plan(
                        running_stage.plan.as_ref(),
                        metrics,
                        &plan_format,
                    ));
                    summary.input_rows = get_combined_count(metrics, "input_rows");
                    summary.output_rows = get_combined_count(metrics, "output_rows");
                    summary.elapsed_compute = get_running_stage_time(
                        &running_stage.task_infos,
                        get_current_time(),
                    );
                    summary.tasks = task_summaries(&running_stage.task_infos, metrics);
                }
                ExecutionStage::Successful(completed_stage) => {
                    let metrics = completed_stage.stage_metrics.as_slice();
                    summary.stage_plan = Some(render_stage_plan(
                        completed_stage.plan.as_ref(),
                        metrics,
                        &plan_format,
                    ));
                    summary.input_rows = get_combined_count(metrics, "input_rows");
                    summary.output_rows = get_combined_count(metrics, "output_rows");
                    summary.elapsed_compute =
                        get_finished_stage_time(&completed_stage.task_infos);
                    summary.tasks = task_summaries(&completed_stage.task_infos, metrics);
                }
                ExecutionStage::Failed(failed_stage) => {
                    let metrics = failed_stage.stage_metrics.as_deref().unwrap_or(&[]);
                    summary.stage_plan = Some(render_stage_plan(
                        failed_stage.plan.as_ref(),
                        metrics,
                        &plan_format,
                    ));
                    summary.input_rows = get_combined_count(metrics, "input_rows");
                    summary.output_rows = get_combined_count(metrics, "output_rows");
                    summary.elapsed_compute =
                        get_finished_stage_time(&failed_stage.task_infos);
                    summary.tasks = task_summaries(&failed_stage.task_infos, metrics);
                }
                _ => {}
            }

            summary.task_duration_percentiles = task_duration_percentiles(&summary.tasks);
            summary.task_input_percentiles = task_input_percentiles(&summary.tasks);
            summary
        })
        .collect();

    QueryStagesResponse { stages }
}

/// Render one stage's plan in the requested format. `Metrics` overlays the
/// stage's aggregated metrics onto the plan; the other formats ignore them.
fn render_stage_plan(
    plan: &dyn datafusion::physical_plan::ExecutionPlan,
    metrics: &[MetricsSet],
    plan_format: &PlanFormat,
) -> String {
    match plan_format {
        PlanFormat::Default => displayable(plan).indent(false).to_string(),
        PlanFormat::Tree => displayable(plan).tree_render().to_string(),
        PlanFormat::Metrics => format_stage_metrics(plan, metrics),
    }
}

/// Build one [`TaskSummary`] per task in a stage. Row counts are summed over
/// the global partitions each task owns.
///
/// The `Option` wrapper on each entry is part of the wire format and is always
/// `Some` here; it predates multi-partition tasks, when a stage's task list was
/// indexed by partition and could be sparse.
fn task_summaries(
    task_infos: &[TaskInfo],
    metrics: &[MetricsSet],
) -> Vec<Option<TaskSummary>> {
    task_infos
        .iter()
        .map(|info| {
            let (input_rows, output_rows) =
                get_partition_counts(metrics, &info.global_input_partition_ids);

            let start_exec_time = info.start_exec_time as u64;
            let end_exec_time = info.end_exec_time as u64;

            Some(TaskSummary {
                id: info.task_id,
                partition_id: info
                    .global_input_partition_ids
                    .iter()
                    .map(|&p| p as u32)
                    .collect(),
                scheduled_time: info.scheduled_time as u64,
                launch_time: info.launch_time as u64,
                start_exec_time,
                end_exec_time,
                exec_duration: end_exec_time.saturating_sub(start_exec_time),
                finish_time: info.finish_time as u64,
                input_rows,
                output_rows,
                status: task_status_to_dto(&info.task_status),
            })
        })
        .collect()
}

/// Map a protobuf task status onto the wire enum.
///
/// A free function rather than a `From` impl: both types are foreign to this
/// crate now that [`TaskStatus`] lives in `ballista-history`.
pub fn task_status_to_dto(value: &task_status::Status) -> TaskStatus {
    match value {
        task_status::Status::Running(_) => TaskStatus::Running,
        task_status::Status::Failed(failed) => TaskStatus::Failed {
            reason: failed_reason(failed),
            error: failed.error.clone(),
        },
        task_status::Status::Successful(_) => TaskStatus::Successful,
    }
}

fn percentile_duration(sorted: &[u64], pct: f64) -> u64 {
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn percentiles_of(mut values: Vec<u64>) -> Option<Percentiles> {
    if values.is_empty() {
        return None;
    }

    values.sort_unstable();

    Some(Percentiles {
        min: values[0],
        p25: percentile_duration(&values, 25.0),
        median: percentile_duration(&values, 50.0),
        p75: percentile_duration(&values, 75.0),
        max: *values.last().unwrap(),
    })
}

fn task_input_percentiles(tasks: &[Option<TaskSummary>]) -> Option<Percentiles> {
    percentiles_of(
        tasks
            .iter()
            .flatten()
            .map(|t| t.input_rows as u64)
            .collect(),
    )
}

fn task_duration_percentiles(tasks: &[Option<TaskSummary>]) -> Option<Percentiles> {
    percentiles_of(tasks.iter().flatten().map(|t| t.exec_duration).collect())
}

/// Returns elapsed wall time in milliseconds for API formatting.
///
/// Uses saturating subtraction so inconsistent timestamps (e.g. failed jobs, or
/// `end_time` still zero while `start_time` is set) do not panic on subtract.
fn job_elapsed_ms(start_time: u64, end_time: u64) -> u64 {
    end_time.saturating_sub(start_time)
}

fn format_job_status(status: &Option<Status>, elapsed_ms: u64) -> (String, String) {
    match status {
        Some(Status::Queued(_)) => ("Queued".to_string(), "Queued".to_string()),
        Some(Status::Running(_)) => ("Running".to_string(), "Running".to_string()),
        Some(Status::Failed(error)) => {
            ("Failed".to_string(), format!("Failed: {}", error.error))
        }
        Some(Status::Successful(completed)) => {
            let num_rows = completed
                .partition_location
                .iter()
                .map(|p| p.partition_stats.as_ref().map(|s| s.num_rows).unwrap_or(0))
                .sum::<i64>();
            let num_rows_term = if num_rows == 1 { "row" } else { "rows" };
            let num_partitions = completed.partition_location.len();
            let num_partitions_term = if num_partitions == 1 {
                "partition"
            } else {
                "partitions"
            };
            (
                "Completed".to_string(),
                format!(
                    "Completed. Produced {} {} containing {} {}. Elapsed time: {} ms.",
                    num_partitions,
                    num_partitions_term,
                    num_rows,
                    num_rows_term,
                    elapsed_ms
                ),
            )
        }
        _ => ("Invalid".to_string(), "Invalid State".to_string()),
    }
}

fn get_running_stage_time(task_infos: &[TaskInfo], current_time: u128) -> Option<String> {
    let min_start = task_infos
        .iter()
        .map(|t| t.start_exec_time)
        .filter(|t| *t > 0)
        .min();

    match (min_start, current_time) {
        (Some(start), end) if end >= start => Some(format_millis(end - start)),
        _ => None,
    }
}

fn get_finished_stage_time(task_infos: &[TaskInfo]) -> Option<String> {
    let min_start = task_infos
        .iter()
        .map(|t| t.start_exec_time)
        .filter(|t| *t > 0)
        .min();

    let max_end = task_infos
        .iter()
        .map(|t| t.end_exec_time)
        .filter(|t| *t > 0)
        .max();

    match (min_start, max_end) {
        (Some(start), Some(end)) if end >= start => Some(format_millis(end - start)),
        _ => None,
    }
}

/// Format a millisecond duration the way DataFusion renders elapsed-time
/// metrics, so stage timings match plan output.
fn format_millis(millis: u128) -> String {
    let time = Time::new();
    time.add_duration(Duration::from_millis(millis as u64));
    time.to_string()
}

fn failed_reason(failed: &FailedTask) -> String {
    match &failed.failed_reason {
        Some(ExecutionError(_)) => "ExecutionError",
        Some(FetchPartitionError(_)) => "FetchPartitionError",
        Some(IoError(_)) => "IoError",
        Some(ExecutorLost(_)) => "ExecutorLost",
        Some(ResultLost(_)) => "ResultLost",
        Some(TaskKilled(_)) => "TaskKilled",
        None => "Failed",
    }
    .to_string()
}

/// Sum a task's `input_rows` / `output_rows` across the global partitions the
/// task owns. Metrics are keyed by global partition id — for single-partition
/// tasks `partitions` is a one-element slice; for multi-partition tasks it is
/// the task's `global_input_partition_ids`.
fn get_partition_counts(metrics: &[MetricsSet], partitions: &[usize]) -> (usize, usize) {
    let input_rows = get_partition_count(metrics, partitions, "input_rows");
    let output_rows = get_partition_count(metrics, partitions, "output_rows");
    (input_rows, output_rows)
}

fn get_partition_count(
    metrics: &[MetricsSet],
    partitions: &[usize],
    name: &str,
) -> usize {
    metrics
        .iter()
        .flat_map(|vec| {
            vec.iter().map(|metric| {
                let metric_value = metric.value();
                let owned_by_task = metric
                    .partition()
                    .map(|p| partitions.contains(&p))
                    .unwrap_or(false);
                if owned_by_task && metric_value.name() == name {
                    metric_value.as_usize()
                } else {
                    0
                }
            })
        })
        .sum()
}

fn get_combined_count(metrics: &[MetricsSet], name: &str) -> usize {
    metrics
        .iter()
        .flat_map(|vec| {
            vec.iter().map(|metric| {
                let metric_value = metric.value();
                if metric_value.name() == name {
                    metric_value.as_usize()
                } else {
                    0
                }
            })
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::execution_stage::TaskInfo;
    use ballista_core::serde::protobuf::task_status;

    fn make_task_info(start: u128, end: u128) -> TaskInfo {
        TaskInfo {
            task_id: 0,
            scheduled_time: 0,
            launch_time: 0,
            start_exec_time: start,
            end_exec_time: end,
            finish_time: 0,
            task_status: task_status::Status::Running(Default::default()),
            global_input_partition_ids: vec![],
            vcores_consumed: 0,
        }
    }

    #[test]
    fn test_job_elapsed_saturates_when_end_precedes_start() {
        assert_eq!(job_elapsed_ms(900, 100), 0);
    }

    // --- get_finished_stage_time ---

    #[test]
    fn test_finished_empty_slice_returns_none() {
        assert_eq!(get_finished_stage_time(&[]), None);
    }

    #[test]
    fn test_finished_all_zero_timestamps_returns_none() {
        let tasks = vec![make_task_info(0, 0), make_task_info(0, 0)];
        assert_eq!(get_finished_stage_time(&tasks), None);
    }

    #[test]
    fn test_finished_single_task_elapsed() {
        // 600 - 100 = 500 ms → "500.00ms"
        let tasks = vec![make_task_info(100, 600)];
        assert_eq!(
            get_finished_stage_time(&tasks),
            Some("500.00ms".to_string())
        );
    }

    #[test]
    fn test_finished_picks_earliest_start_and_latest_end() {
        // min start = 100, max end = 900 → 800 ms
        let tasks = vec![
            make_task_info(100, 500),
            make_task_info(200, 900),
            make_task_info(300, 700),
        ];
        assert_eq!(
            get_finished_stage_time(&tasks),
            Some("800.00ms".to_string())
        );
    }

    #[test]
    fn test_finished_end_before_start_returns_none() {
        let tasks = vec![make_task_info(900, 100)];
        assert_eq!(get_finished_stage_time(&tasks), None);
    }

    // --- get_running_stage_time ---

    #[test]
    fn test_running_empty_slice_returns_none() {
        assert_eq!(get_running_stage_time(&[], 1000), None);
    }

    #[test]
    fn test_running_all_zero_start_returns_none() {
        let tasks: Vec<TaskInfo> = vec![make_task_info(0, 0), make_task_info(0, 0)];
        assert_eq!(get_running_stage_time(&tasks, 1000), None);
    }

    #[test]
    fn test_running_future_start_returns_none() {
        // start_exec_time beyond current time → elapsed clamped to 0
        let tasks = vec![make_task_info(u128::MAX, 0)];
        assert_eq!(get_running_stage_time(&tasks, 1000), None);
    }

    #[test]
    fn test_running_past_start_returns_some() {
        let now = 4_000;
        let start = 1_000;
        let tasks = vec![make_task_info(start, 0)];
        assert_eq!(
            get_running_stage_time(&tasks, now),
            Some("3.00s".to_string())
        );
    }

    #[test]
    fn test_running_mixed_zero_start_uses_earliest_nonzero() {
        let now = 3_000;
        let earlier = 1_000;
        let later = 2_000;
        let tasks = vec![
            make_task_info(0, 0),
            make_task_info(later, 0),
            make_task_info(earlier, 0),
            make_task_info(0, 0),
        ];
        let result = get_running_stage_time(&tasks, now);
        assert_eq!(result, Some("2.00s".to_string()));
    }

    #[test]
    fn test_job_elapsed_ms_normal() {
        assert_eq!(super::job_elapsed_ms(100, 500), 400);
    }

    #[test]
    fn test_job_elapsed_ms_end_before_start_saturates_to_zero() {
        assert_eq!(super::job_elapsed_ms(500, 100), 0);
    }
}
