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
//! [`ballista_api_types::dto`]. Keeping the translation here (rather than inline
//! in the axum handlers) means the same DTOs can be produced from state that
//! did not come from a live handler request.
//!
//! Everything in this module is pure: state in, DTO out, no I/O and no
//! wall-clock reads. `graph_to_query_stages` takes `now` from its caller so a
//! replay of a stored log renders the same elapsed times every time.

use crate::display::format_stage_metrics;
use crate::state::execution_graph::{ExecutionGraphBox, ExecutionStage};
use crate::state::execution_graph_dot::ExecutionGraphDot;
use crate::state::execution_stage::TaskInfo;
use crate::state::task_manager::JobOverview;
use ballista_api_types::dto::{
    JobConfig, JobResponse, Percentiles, PlanFormat, QueryStageSummary,
    QueryStagesResponse, TaskStatus, TaskSummary,
};
use ballista_core::serde::protobuf::failed_task::FailedReason::{
    ExecutionError, ExecutorLost, FetchPartitionError, IoError, ResultLost, TaskKilled,
};
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{FailedTask, OperatorMetricsSet, task_status};
use datafusion::execution::context::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
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

    JobResponse {
        job_id: job.job_id.to_string(),
        job_name: job.job_name.to_owned(),
        job_status,
        status: plain_status,
        start_time: job.start_time,
        end_time: job.end_time,
        num_stages: job.num_stages,
        completed_stages: job.completed_stages,
        percent_complete: percent_complete(job.completed_stages, job.num_stages),
        logical_plan: None,
        physical_plan: None,
        stage_plan: None,
    }
}

/// Build the `JobResponse` served by `GET /api/job/{job_id}`, including the
/// rendered logical, physical, and stage plans.
pub fn graph_to_job_response(
    graph: &ExecutionGraphBox,
    plan_format: PlanFormat,
) -> JobResponse {
    let stage_plan = format!("{graph:?}");
    let job = graph.as_ref();

    let (plain_status, job_status) = format_job_status(
        &job.status().status,
        job_elapsed_ms(job.start_time(), job.end_time()),
    );

    let num_stages = job.stage_count();
    let completed_stages = job.completed_stages();

    let physical_plan = match plan_format {
        PlanFormat::Default | PlanFormat::Metrics => {
            displayable(job.physical_plan().as_ref())
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
        percent_complete: percent_complete(completed_stages, num_stages),
        logical_plan: job.logical_plan().map(str::to_owned),
        physical_plan: Some(physical_plan),
        stage_plan: Some(stage_plan),
    }
}

/// Flatten a session config into the sorted key/value map served by
/// `GET /api/job/{job_id}/config`.
pub fn session_config_to_job_config(config: &SessionConfig) -> JobConfig {
    config.to_props().into_iter().collect()
}

/// Build the per-stage summaries served by `GET /api/job/{job_id}/stages`.
pub fn graph_to_query_stages(
    graph: &ExecutionGraphBox,
    plan_format: PlanFormat,
    now: u128,
) -> QueryStagesResponse {
    let stages = graph
        .as_ref()
        .stages()
        .iter()
        .map(|(id, stage)| {
            // Every started stage contributes the same three things; only how
            // it reaches its metrics and which elapsed-time rule applies
            // differ. Stages that have not started yet contribute nothing.
            let started: Option<(&[MetricsSet], &[TaskInfo], Option<String>)> =
                match stage {
                    ExecutionStage::Running(s) => Some((
                        s.stage_metrics.as_deref().unwrap_or(&[]),
                        &s.task_infos,
                        get_running_stage_time(&s.task_infos, now),
                    )),
                    ExecutionStage::Successful(s) => Some((
                        &s.stage_metrics,
                        &s.task_infos,
                        get_finished_stage_time(&s.task_infos),
                    )),
                    ExecutionStage::Failed(s) => Some((
                        s.stage_metrics.as_deref().unwrap_or(&[]),
                        &s.task_infos,
                        get_finished_stage_time(&s.task_infos),
                    )),
                    _ => None,
                };

            let has_started = started.is_some();
            let (metrics, task_infos, elapsed_compute) = started.unwrap_or_default();
            let slots = StageMetricSlots::of(stage.plan());
            let (input_rows, output_rows) = slots.row_counts(metrics, None);
            let tasks = task_summaries(task_infos, metrics, &slots);

            QueryStageSummary {
                stage_id: id.to_string(),
                stage_status: stage.variant_name().to_string(),
                input_rows,
                output_rows,
                elapsed_compute,
                stage_plan: has_started
                    .then(|| render_stage_plan(stage.plan(), metrics, plan_format)),
                task_duration_percentiles: task_duration_percentiles(&tasks),
                task_input_percentiles: task_input_percentiles(&tasks),
                tasks,
            }
        })
        .collect();

    QueryStagesResponse { stages }
}

/// Render one stage's plan in the requested format. `Metrics` overlays the
/// stage's aggregated metrics onto the plan; the other formats ignore them.
fn render_stage_plan(
    plan: &dyn ExecutionPlan,
    metrics: &[MetricsSet],
    plan_format: PlanFormat,
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
    slots: &StageMetricSlots,
) -> Vec<Option<TaskSummary>> {
    task_infos
        .iter()
        .map(|info| {
            let (input_rows, output_rows) =
                slots.row_counts(metrics, Some(&info.global_input_partition_ids));

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

/// Render a job's stage DAG in DOT format.
pub fn build_job_dot(graph: &ExecutionGraphBox) -> Result<String, std::fmt::Error> {
    ExecutionGraphDot::generate(graph.as_ref())
}

/// Reduce one task's raw operator metrics to
/// `(input_rows, output_rows, elapsed_compute_nanos)`.
///
/// Distinct from [`StageMetricSlots::row_counts`] on a stage's already-merged
/// [`MetricsSet`]s: this takes the raw protobuf [`OperatorMetricsSet`]s an
/// executor reports for a single task, so there is no partition to filter on,
/// and it also sums `elapsed_compute` over every operator for the event log's
/// per-task timeline records. The row counts still need the stage plan to
/// tell the writer and the leaves apart from the operators in between; without
/// `slots` they are reported as zero.
pub fn task_row_counts(
    metrics: &[OperatorMetricsSet],
    slots: Option<&StageMetricSlots>,
) -> (u64, u64, u64) {
    let metrics: Vec<MetricsSet> = metrics
        .iter()
        .filter_map(|operator_metrics| operator_metrics.clone().try_into().ok())
        .collect();

    let elapsed_compute_nanos = metrics
        .iter()
        .map(|operator| sum_metric(operator, "elapsed_compute", None) as u64)
        .sum();
    let (input_rows, output_rows) = slots
        .map(|slots| slots.row_counts(&metrics, None))
        .unwrap_or((0, 0));

    (input_rows as u64, output_rows as u64, elapsed_compute_nanos)
}

/// Map a protobuf task status onto the wire enum.
///
/// A free function rather than a `From` impl: both types are foreign to this
/// crate now that [`TaskStatus`] lives in `ballista-api-types`.
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

/// Progress as a percentage of stages completed. Zero-stage jobs report 0
/// rather than dividing by zero.
fn percent_complete(completed_stages: usize, num_stages: usize) -> u8 {
    if num_stages == 0 {
        return 0;
    }
    ((completed_stages as f32 / num_stages as f32) * 100_f32) as u8
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

/// Earliest non-zero task start in a stage. Zero means "not started yet", so
/// those entries are ignored rather than dragging the minimum to 0.
fn min_start_time(task_infos: &[TaskInfo]) -> Option<u128> {
    task_infos
        .iter()
        .map(|t| t.start_exec_time)
        .filter(|t| *t > 0)
        .min()
}

fn get_running_stage_time(task_infos: &[TaskInfo], current_time: u128) -> Option<String> {
    match (min_start_time(task_infos), current_time) {
        (Some(start), end) if end >= start => Some(format_millis(end - start)),
        _ => None,
    }
}

fn get_finished_stage_time(task_infos: &[TaskInfo]) -> Option<String> {
    let max_end = task_infos
        .iter()
        .map(|t| t.end_exec_time)
        .filter(|t| *t > 0)
        .max();

    match (min_start_time(task_infos), max_end) {
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

/// Where a stage plan's root and leaf operators sit in the flat metrics vector
/// executors report for the stage.
///
/// Executors flatten a task's metrics with
/// [`ballista_core::utils::collect_plan_metrics`]: a pre-order walk of the
/// stage plan that appends one [`MetricsSet`] per operator that reports
/// metrics. The scheduler holds the same plan, so walking it the same way
/// recovers which slot belongs to which operator; the wire format carries no
/// operator names.
///
/// A stage's rows in are the rows its leaves (shuffle readers, data sources)
/// produce and its rows out are the rows its root (the shuffle writer)
/// produces. Summing every operator's `input_rows` / `output_rows` instead
/// counts a row once per operator that touches it: a hash join reports its
/// probe side as `input_rows`, so a scan-join-write stage looked like it read
/// its input twice and wrote more rows than it scanned.
pub struct StageMetricSlots {
    /// Slot of the root operator; `None` if it reports no metrics.
    root: Option<usize>,
    /// Slots of the leaf operators, in plan order.
    leaves: Vec<usize>,
    /// Number of slots the plan produces. A metrics vector of any other
    /// length was collected from a different plan shape and cannot be mapped.
    len: usize,
}

impl StageMetricSlots {
    pub fn of(plan: &dyn ExecutionPlan) -> Self {
        let mut slots = Self {
            root: None,
            leaves: Vec::new(),
            len: 0,
        };
        slots.visit(plan, true);
        slots
    }

    /// Mirrors `collect_plan_metrics`: pre-order, and an operator without
    /// metrics takes no slot.
    fn visit(&mut self, plan: &dyn ExecutionPlan, is_root: bool) {
        if plan.metrics().is_some() {
            if is_root {
                self.root = Some(self.len);
            }
            if plan.children().is_empty() {
                self.leaves.push(self.len);
            }
            self.len += 1;
        }
        for child in plan.children() {
            self.visit(child.as_ref(), false);
        }
    }

    /// `(input_rows, output_rows)` over the whole stage, or over the global
    /// partitions in `partitions` when given (the per-task view; for
    /// multi-partition tasks that is the task's `global_input_partition_ids`).
    ///
    /// Returns zeros when `metrics` does not line up with the plan (nothing
    /// reported yet, or a shape mismatch that `print_stage_metrics` already
    /// logs when the stage finishes): attributing the slots to the wrong
    /// operators would silently produce plausible-looking wrong numbers.
    pub fn row_counts(
        &self,
        metrics: &[MetricsSet],
        partitions: Option<&[usize]>,
    ) -> (usize, usize) {
        if metrics.len() != self.len {
            return (0, 0);
        }
        let output_rows_of =
            |slot: usize| sum_metric(&metrics[slot], "output_rows", partitions);
        let input_rows = self.leaves.iter().map(|&slot| output_rows_of(slot)).sum();
        let output_rows = self.root.map(output_rows_of).unwrap_or(0);
        (input_rows, output_rows)
    }
}

/// Sum the metrics named `name` in one operator's [`MetricsSet`], restricted to
/// the given global partitions when `partitions` is `Some`. Metrics with no
/// partition only count towards the unrestricted total.
fn sum_metric(metrics: &MetricsSet, name: &str, partitions: Option<&[usize]>) -> usize {
    metrics
        .iter()
        .filter(|metric| metric.value().name() == name)
        .filter(|metric| match partitions {
            None => true,
            Some(partitions) => {
                metric.partition().is_some_and(|p| partitions.contains(&p))
            }
        })
        .map(|metric| metric.value().as_usize())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::execution_stage::TaskInfo;
    use ballista_core::serde::protobuf::{OperatorMetric, operator_metric, task_status};
    use ballista_core::utils::collect_plan_metrics;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::{Count, Metric, MetricValue};
    use datafusion::physical_plan::union::UnionExec;
    use std::sync::Arc;

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

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    fn scan() -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap()
    }

    /// `CoalescePartitionsExec <- UnionExec <- [scan, scan]`. Every operator
    /// reports metrics, so pre-order gives slots root=0, union=1, scans=2,3.
    fn union_plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalescePartitionsExec::new(
            UnionExec::try_new(vec![scan(), scan()]).unwrap(),
        ))
    }

    /// One operator's merged metrics: an `output_rows` count per
    /// `(partition, rows)` pair.
    fn output_rows(per_partition: &[(usize, usize)]) -> MetricsSet {
        let mut set = MetricsSet::new();
        for &(partition, rows) in per_partition {
            let count = Count::new();
            count.add(rows);
            set.push(Arc::new(Metric::new(
                MetricValue::OutputRows(count),
                Some(partition),
            )));
        }
        set
    }

    /// Merged metrics for `union_plan()` with two partitions. Per partition
    /// the stage reads `scan_a + scan_b` rows and writes `root` rows; the
    /// union's own count must not leak into either.
    fn union_plan_metrics() -> Vec<MetricsSet> {
        vec![
            output_rows(&[(0, 5), (1, 7)]),   // root
            output_rows(&[(0, 12), (1, 70)]), // union
            output_rows(&[(0, 10), (1, 30)]), // scan a
            output_rows(&[(0, 2), (1, 40)]),  // scan b
        ]
    }

    // --- StageMetricSlots ---

    #[test]
    fn test_slots_follow_collect_plan_metrics_order() {
        let plan = union_plan();
        let slots = StageMetricSlots::of(plan.as_ref());
        assert_eq!(slots.len, collect_plan_metrics(plan.as_ref()).len());
        assert_eq!(slots.root, Some(0));
        assert_eq!(slots.leaves, vec![2, 3]);
    }

    #[test]
    fn test_operators_without_metrics_take_no_slot() {
        // `EmptyExec` reports no metrics, so the plan has a single slot (the
        // root) and no leaf slot at all.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(
            Arc::new(EmptyExec::new(test_schema())),
        ));
        let slots = StageMetricSlots::of(plan.as_ref());
        assert_eq!(slots.len, collect_plan_metrics(plan.as_ref()).len());
        assert_eq!(slots.len, 1);
        assert_eq!(slots.root, Some(0));
        assert!(slots.leaves.is_empty());
        let (input_rows, output_rows) = slots.row_counts(&[output_rows(&[(0, 3)])], None);
        assert_eq!((input_rows, output_rows), (0, 3));
    }

    #[test]
    fn test_stage_rows_are_leaf_input_and_root_output() {
        let plan = union_plan();
        let slots = StageMetricSlots::of(plan.as_ref());
        let (input_rows, output_rows) = slots.row_counts(&union_plan_metrics(), None);
        assert_eq!(input_rows, 10 + 30 + 2 + 40);
        assert_eq!(output_rows, 5 + 7);
    }

    #[test]
    fn test_task_rows_are_restricted_to_owned_partitions() {
        let plan = union_plan();
        let slots = StageMetricSlots::of(plan.as_ref());
        let metrics = union_plan_metrics();
        assert_eq!(slots.row_counts(&metrics, Some(&[0])), (10 + 2, 5));
        assert_eq!(slots.row_counts(&metrics, Some(&[1])), (30 + 40, 7));
        assert_eq!(slots.row_counts(&metrics, Some(&[0, 1])), (82, 12));
        assert_eq!(slots.row_counts(&metrics, Some(&[9])), (0, 0));
    }

    #[test]
    fn test_unpartitioned_metrics_only_count_stage_wide() {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(scan()));
        let slots = StageMetricSlots::of(plan.as_ref());
        let mut root = MetricsSet::new();
        let count = Count::new();
        count.add(4);
        root.push(Arc::new(Metric::new(MetricValue::OutputRows(count), None)));
        let metrics = vec![root, output_rows(&[(0, 9)])];
        assert_eq!(slots.row_counts(&metrics, None), (9, 4));
        assert_eq!(slots.row_counts(&metrics, Some(&[0])), (9, 0));
    }

    #[test]
    fn test_mismatched_metrics_report_zero_rows() {
        let plan = union_plan();
        let slots = StageMetricSlots::of(plan.as_ref());
        // Nothing reported yet.
        assert_eq!(slots.row_counts(&[], None), (0, 0));
        // Collected from a different plan shape.
        let short = union_plan_metrics()[..3].to_vec();
        assert_eq!(slots.row_counts(&short, None), (0, 0));
    }

    // --- task_row_counts ---

    fn raw_operator_metrics(output_rows: u64, elapsed_nanos: u64) -> OperatorMetricsSet {
        OperatorMetricsSet {
            metrics: vec![
                OperatorMetric {
                    metric: Some(operator_metric::Metric::OutputRows(output_rows)),
                    partition: Some(0),
                },
                OperatorMetric {
                    metric: Some(operator_metric::Metric::ElapseTime(elapsed_nanos)),
                    partition: Some(0),
                },
            ],
        }
    }

    #[test]
    fn test_task_row_counts_use_stage_plan_when_known() {
        let plan = union_plan();
        let slots = StageMetricSlots::of(plan.as_ref());
        let raw = vec![
            raw_operator_metrics(5, 100),  // root
            raw_operator_metrics(12, 200), // union
            raw_operator_metrics(10, 300), // scan a
            raw_operator_metrics(2, 400),  // scan b
        ];
        assert_eq!(task_row_counts(&raw, Some(&slots)), (12, 5, 1000));
        // Elapsed compute is still summed when the plan is unknown; the row
        // counts cannot be attributed and are reported as zero.
        assert_eq!(task_row_counts(&raw, None), (0, 0, 1000));
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
