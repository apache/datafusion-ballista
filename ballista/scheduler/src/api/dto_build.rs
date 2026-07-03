// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Builds the shared REST DTOs (`ballista_history::dto`) from the scheduler's
//! internal execution-graph state. These builders back both the live REST API
//! handlers (`api::handlers`) and, eventually, the event-log writer, so both
//! serialize byte-identical JSON for the same graph state.

use crate::api::handlers::{JobQueryParams, PlanFormat};
use crate::display::format_stage_metrics;
use crate::state::execution_graph::{ExecutionGraphBox, ExecutionStage};
use crate::state::execution_graph_dot::ExecutionGraphDot;
use crate::state::execution_stage::TaskInfo;
use crate::state::task_manager::JobOverview;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{OperatorMetricsSet, task_status};
use ballista_core::utils::get_current_time;
use ballista_history::dto::{
    JobResponse, Percentiles, QueryStageSummary, QueryStagesResponse, TaskStatus,
    TaskSummary,
};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::{MetricsSet, Time};
use std::fmt;
use std::time::Duration;

/// Builds the `JobResponse` DTO for a job's execution graph.
///
/// `with_plans` controls whether `logical_plan`/`physical_plan`/`stage_plan` are
/// populated (the single-job detail view, and eventually the history detail
/// view) or left `None` (list views, which use
/// [`build_job_response_from_overview`] instead). `plan_format` selects how the
/// physical plan is rendered when `with_plans` is true; it is ignored otherwise.
pub(crate) fn build_job_response(
    graph: &ExecutionGraphBox,
    with_plans: bool,
    plan_format: PlanFormat,
) -> JobResponse {
    let job = graph.as_ref();
    let (plain_status, job_status) = format_job_status(
        &job.status().status,
        job_elapsed_ms(job.start_time(), job.end_time()),
    );

    let num_stages = job.stage_count();
    let completed_stages = job.completed_stages();
    let percent_complete =
        ((completed_stages as f32 / num_stages as f32) * 100_f32) as u8;

    let (logical_plan, physical_plan, stage_plan) = if with_plans {
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
        (
            job.logical_plan().map(str::to_owned),
            Some(physical_plan),
            Some(format!("{:?}", graph)),
        )
    } else {
        (None, None, None)
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
        logical_plan,
        physical_plan,
        stage_plan,
    }
}

/// Builds the `JobResponse` DTO for a job's cached [`JobOverview`] (no
/// execution graph, and hence no plans available).
pub(crate) fn build_job_response_from_overview(job: &JobOverview) -> JobResponse {
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

/// Builds the `QueryStagesResponse` DTO for a job's execution graph.
pub(crate) fn build_query_stages_response(
    graph: &ExecutionGraphBox,
    params: &JobQueryParams,
) -> QueryStagesResponse {
    let plan_format = params.plan_format.clone().unwrap_or_default();

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
                    summary.stage_plan = Some(match plan_format {
                        PlanFormat::Default => displayable(running_stage.plan.as_ref())
                            .indent(false)
                            .to_string(),
                        PlanFormat::Tree => displayable(running_stage.plan.as_ref())
                            .tree_render()
                            .to_string(),
                        PlanFormat::Metrics => {
                            format_stage_metrics(running_stage.plan.as_ref(), metrics)
                        }
                    });
                    summary.input_rows = running_stage
                        .stage_metrics
                        .as_ref()
                        .map(|m| get_combined_count(m.as_slice(), "input_rows"))
                        .unwrap_or(0);
                    summary.output_rows = running_stage
                        .stage_metrics
                        .as_ref()
                        .map(|m| get_combined_count(m.as_slice(), "output_rows"))
                        .unwrap_or(0);
                    summary.elapsed_compute = get_running_stage_time(
                        &running_stage.task_infos,
                        get_current_time(),
                    );
                    summary.tasks = running_stage
                        .task_infos
                        .iter()
                        .enumerate()
                        .map(|(partition_id, task_info)| {
                            task_info.as_ref().map(|info| {
                                let (input_rows, output_rows) = running_stage
                                    .stage_metrics
                                    .as_deref()
                                    .map(|metrics| {
                                        get_partition_counts(metrics, partition_id)
                                    })
                                    .unwrap_or((0, 0));

                                let start_exec_time = info.start_exec_time as u64;
                                let end_exec_time = info.end_exec_time as u64;

                                let task_status = to_api_task_status(&info.task_status);

                                TaskSummary {
                                    id: info.task_id,
                                    partition_id: partition_id as u32,
                                    scheduled_time: info.scheduled_time as u64,
                                    launch_time: info.launch_time as u64,
                                    start_exec_time,
                                    end_exec_time,
                                    exec_duration: end_exec_time
                                        .saturating_sub(start_exec_time),
                                    finish_time: info.finish_time as u64,
                                    input_rows,
                                    output_rows,
                                    status: task_status,
                                }
                            })
                        })
                        .collect();
                }
                ExecutionStage::Successful(completed_stage) => {
                    summary.stage_plan = Some(match plan_format {
                        PlanFormat::Default => displayable(completed_stage.plan.as_ref())
                            .indent(false)
                            .to_string(),
                        PlanFormat::Tree => displayable(completed_stage.plan.as_ref())
                            .tree_render()
                            .to_string(),
                        PlanFormat::Metrics => format_stage_metrics(
                            completed_stage.plan.as_ref(),
                            &completed_stage.stage_metrics,
                        ),
                    });
                    summary.input_rows =
                        get_combined_count(&completed_stage.stage_metrics, "input_rows");
                    summary.output_rows =
                        get_combined_count(&completed_stage.stage_metrics, "output_rows");
                    summary.elapsed_compute =
                        get_finished_stage_time(&completed_stage.task_infos);

                    summary.tasks = completed_stage
                        .task_infos
                        .iter()
                        .enumerate()
                        .map(|(partition_id, task_info)| {
                            let (input_rows, output_rows) = get_partition_counts(
                                &completed_stage.stage_metrics,
                                partition_id,
                            );

                            let start_exec_time = task_info.start_exec_time as u64;
                            let end_exec_time = task_info.end_exec_time as u64;
                            let task_status = to_api_task_status(&task_info.task_status);
                            Some(TaskSummary {
                                id: task_info.task_id,
                                partition_id: partition_id as u32,
                                scheduled_time: task_info.scheduled_time as u64,
                                launch_time: task_info.launch_time as u64,
                                start_exec_time,
                                end_exec_time,
                                exec_duration: end_exec_time
                                    .saturating_sub(start_exec_time),
                                finish_time: task_info.finish_time as u64,
                                input_rows,
                                output_rows,
                                status: task_status,
                            })
                        })
                        .collect();
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

/// Builds the DOT-format graph for a job's execution graph.
pub(crate) fn build_job_dot(graph: &ExecutionGraphBox) -> Result<String, fmt::Error> {
    ExecutionGraphDot::generate(graph.as_ref())
}

/// Converts a task's protobuf status into the shared `TaskStatus` DTO.
pub(crate) fn to_api_task_status(status: &task_status::Status) -> TaskStatus {
    match status {
        task_status::Status::Running(_) => TaskStatus::Running,
        task_status::Status::Failed(_) => TaskStatus::Failed,
        task_status::Status::Successful(_) => TaskStatus::Successful,
    }
}

/// Sums a single task's raw operator metrics into
/// `(input_rows, output_rows, elapsed_compute_nanos)`.
///
/// Note: this is *not* a literal extraction of `get_partition_counts`/
/// `get_combined_count` below. Those operate on the stage's already-merged
/// per-partition [`MetricsSet`]s (DataFusion's own metrics type) and filter by
/// partition id, returning `usize` row counts only. `task_row_counts` instead
/// converts a single task's raw protobuf [`OperatorMetricsSet`]s (as reported
/// by an executor for one task, e.g. `TaskStatus::metrics`) directly, with no
/// partition filtering (a task already corresponds to exactly one partition),
/// and additionally sums `elapsed_compute` (nanoseconds) alongside the row
/// counts, matching `ballista_history::event::TaskEndMetrics` for Task 5's
/// timeline events.
///
/// Not yet called from production code — the event-log writer that will
/// consume it lands in a later task. Silence `dead_code` until then.
#[allow(dead_code)]
pub(crate) fn task_row_counts(metrics: &[OperatorMetricsSet]) -> (u64, u64, u64) {
    let mut input_rows: u64 = 0;
    let mut output_rows: u64 = 0;
    let mut elapsed_compute_nanos: u64 = 0;

    for operator_metrics in metrics {
        let Ok(metrics_set) = TryInto::<MetricsSet>::try_into(operator_metrics.clone())
        else {
            continue;
        };
        for metric in metrics_set.iter() {
            let value = metric.value();
            match value.name() {
                "input_rows" => input_rows += value.as_usize() as u64,
                "output_rows" => output_rows += value.as_usize() as u64,
                "elapsed_compute" => elapsed_compute_nanos += value.as_usize() as u64,
                _ => {}
            }
        }
    }

    (input_rows, output_rows, elapsed_compute_nanos)
}

fn percentile_duration(sorted: &[u64], pct: f64) -> u64 {
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn task_input_percentiles(tasks: &[Option<TaskSummary>]) -> Option<Percentiles> {
    let mut durations: Vec<u64> = tasks
        .iter()
        .flatten()
        .map(|t| t.input_rows as u64)
        .collect();

    if durations.is_empty() {
        return None;
    }

    durations.sort_unstable();

    Some(Percentiles {
        min: durations[0],
        p25: percentile_duration(&durations, 25.0),
        median: percentile_duration(&durations, 50.0),
        p75: percentile_duration(&durations, 75.0),
        max: *durations.last().unwrap(),
    })
}

fn task_duration_percentiles(tasks: &[Option<TaskSummary>]) -> Option<Percentiles> {
    let mut durations: Vec<u64> =
        tasks.iter().flatten().map(|t| t.exec_duration).collect();

    if durations.is_empty() {
        return None;
    }

    durations.sort_unstable();

    Some(Percentiles {
        min: durations[0],
        p25: percentile_duration(&durations, 25.0),
        median: percentile_duration(&durations, 50.0),
        p75: percentile_duration(&durations, 75.0),
        max: *durations.last().unwrap(),
    })
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

fn get_running_stage_time(
    task_infos: &[Option<TaskInfo>],
    current_time: u128,
) -> Option<String> {
    let min_start = task_infos
        .iter()
        .flat_map(|t| t.as_ref().map(|t| t.start_exec_time))
        .filter(|t| *t > 0)
        .min();

    match (min_start, current_time) {
        (Some(start), end) if end >= start => {
            let time = Time::new();
            time.add_duration(Duration::from_millis((end - start) as u64));
            Some(time.to_string())
        }
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
        (Some(start), Some(end)) if end >= start => {
            let time = Time::new();
            time.add_duration(Duration::from_millis((end - start) as u64));
            Some(time.to_string())
        }
        _ => None,
    }
}

fn get_partition_counts(metrics: &[MetricsSet], partition_id: usize) -> (usize, usize) {
    let input_rows = get_partition_count(metrics, partition_id, "input_rows");
    let output_rows = get_partition_count(metrics, partition_id, "output_rows");
    (input_rows, output_rows)
}

fn get_partition_count(metrics: &[MetricsSet], partition_id: usize, name: &str) -> usize {
    metrics
        .iter()
        .flat_map(|vec| {
            vec.iter().map(|metric| {
                let metric_value = metric.value();
                if metric.partition() == Some(partition_id) && metric_value.name() == name
                {
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
    use crate::state::execution_graph_dot::tests::test_graph;
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
    fn test_running_all_none_returns_none() {
        let tasks: Vec<Option<TaskInfo>> = vec![None, None];
        assert_eq!(get_running_stage_time(&tasks, 1000), None);
    }

    #[test]
    fn test_running_future_start_returns_none() {
        // start_exec_time beyond current time → elapsed clamped to 0
        let tasks = vec![Some(make_task_info(u128::MAX, 0))];
        assert_eq!(get_running_stage_time(&tasks, 1000), None);
    }

    #[test]
    fn test_running_past_start_returns_some() {
        let now = 4_000;
        let start = 1_000;
        let tasks = vec![Some(make_task_info(start, 0))];
        assert_eq!(
            get_running_stage_time(&tasks, now),
            Some("3.00s".to_string())
        );
    }

    #[test]
    fn test_running_mixed_some_none_uses_earliest_some() {
        let now = 3_000;
        let earlier = 1_000;
        let later = 2_000;
        let tasks = vec![
            None,
            Some(make_task_info(later, 0)),
            Some(make_task_info(earlier, 0)),
            None,
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

    #[test]
    fn test_task_row_counts_empty_is_zero() {
        assert_eq!(task_row_counts(&[]), (0, 0, 0));
    }

    #[tokio::test]
    async fn test_dto_builders_plan_string() {
        let graph = test_graph().await.unwrap();
        let graph: ExecutionGraphBox = Box::new(graph);

        // build_query_stages_response: sanity-check the stage topology survives
        // the DTO builder + JSON serialization round trip. test_graph() is a
        // freshly built (never executed) graph, so every stage is
        // Resolved/Unresolved and `stage_plan` is intentionally left `None`
        // (only Running/Successful stages populate it) -- so this builder alone
        // can't be used for the plan-string assertion below.
        let params = JobQueryParams::default();
        let stages_response = build_query_stages_response(&graph, &params);
        let stages_json = serde_json::to_string(&stages_response).unwrap();
        assert!(
            stages_json.contains(r#""stage_status":"Resolved""#),
            "expected a resolved stage in response, got: {stages_json}"
        );

        // build_job_dot: assert on the embedded physical-plan string, following
        // the repo's plan-string assertion convention (mirrors
        // `execution_graph_dot::tests::dot()`, which renders the same graph).
        let dot = build_job_dot(&graph).unwrap();
        assert!(
            dot.contains("DataSourceExec: (Memory)"),
            "expected plan string in dot graph, got: {dot}"
        );
    }
}
