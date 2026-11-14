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

//! Logs how the adaptive planner's rules change a job's plan.
//!
//! With `debug` logging on for this module, each pass of rules over a job's
//! plan logs every rule that changed the plan, with a diff of the plan before
//! and after the rule, and then the plan the pass ended with. A rule that
//! leaves the plan as it was logs nothing, as with LLVM's `-print-changed` or
//! Spark's plan change log. A change a later rule of the pass undoes, such as
//! the node DataFusion's `OutputRequirements` adds and removes again, logs no
//! diff, only a line naming the rule that undid it and the rules it undid:
//!
//! ```text
//! RUST_LOG=ballista_scheduler::state::aqe::plan_change_log=debug
//! ```
//!
//! What a rule decided, and why it skipped an optimization, the rule logs
//! under its own module, so `RUST_LOG=ballista_scheduler::state::aqe=debug`
//! shows both.
//!
//! Some rules change an exchange in place, so plans are compared by their
//! rendering, which is only taken while this log is enabled. A pass logs its
//! changes once its rules have run, or have failed.

use std::fmt::{self, Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use ballista_core::JobId;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use log::{Level, debug, log_enabled};
use similar::{Algorithm, DiffOp, DiffTag, capture_diff_slices, group_diff_ops};

/// Unchanged lines a diff keeps on each side of a change.
const CONTEXT: usize = 3;

/// What the planner runs a pass of rules over a job's plan for.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Pass {
    /// The first optimization of the job's plan.
    Initial,
    /// The replan after a stage finished.
    StageFinished(usize),
    /// The replan after a stage about to launch read a finished stage instead.
    StageReused,
    /// The rules run on a stage's own plan as it launches, with the stage's
    /// id if it has one.
    Stage(Option<usize>),
}

impl Display for Pass {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initial => write!(f, "initial optimization"),
            Self::StageFinished(stage_id) => {
                write!(f, "replan after stage {stage_id} finished")
            }
            Self::StageReused => write!(f, "replan after reusing a finished stage"),
            Self::Stage(Some(stage_id)) => write!(f, "stage {stage_id}"),
            Self::Stage(None) => write!(f, "stage ?"),
        }
    }
}

/// Logs the rules of one [`Pass`] that changed a job's plan. See the module
/// docs.
pub(crate) struct PlanChangeLog<'a> {
    job_id: &'a JobId,
    pass: Pass,
    /// What the rules of the pass changed so far, or `None` while the log is
    /// disabled.
    changes: Option<Changes>,
    /// The time spent rendering and diffing plans.
    overhead: Duration,
}

/// The changes the rules of a pass made to a plan.
struct Changes {
    /// The plan as the last rule left it, rendered.
    plan: String,
    /// Each change no later rule undid: the rule's name and its diff.
    made: Vec<(String, String)>,
    /// Each rule that undid changes, and the rules whose changes it undid.
    undone: Vec<(String, Vec<String>)>,
    /// A digest of each plan the pass went through, `seen[k]` being the plan
    /// the first `k` changes of `made` led to, so a rule that brings a plan
    /// back drops the changes it undid.
    seen: Vec<u64>,
}

impl<'a> PlanChangeLog<'a> {
    /// Starts the log of `pass` over `plan`.
    pub(crate) fn new(job_id: &'a JobId, pass: Pass, plan: &dyn ExecutionPlan) -> Self {
        Self::with_enabled(log_enabled!(Level::Debug), job_id, pass, plan)
    }

    fn with_enabled(
        enabled: bool,
        job_id: &'a JobId,
        pass: Pass,
        plan: &dyn ExecutionPlan,
    ) -> Self {
        let start = Instant::now();
        let changes = enabled.then(|| {
            let plan = render(plan);
            Changes {
                seen: vec![digest(&plan)],
                plan,
                made: Vec::new(),
                undone: Vec::new(),
            }
        });
        Self {
            job_id,
            pass,
            changes,
            overhead: start.elapsed(),
        }
    }

    /// The time the log spent rendering and diffing plans so far, which is
    /// zero while it is disabled.
    pub(crate) fn overhead(&self) -> Duration {
        self.overhead
    }

    /// Records the change `rule` made, given the `plan` it returned, if it
    /// changed the plan.
    pub(crate) fn record(&mut self, rule: &str, plan: &dyn ExecutionPlan) {
        let Some(changes) = &mut self.changes else {
            return;
        };
        let start = Instant::now();
        let after = render(plan);
        let after_digest = digest(&after);
        match changes.seen.iter().position(|seen| *seen == after_digest) {
            // The rule undid every change since the plan was last like this,
            // or changed nothing.
            Some(back) => {
                if back < changes.made.len() {
                    let undone =
                        changes.made.drain(back..).map(|(rule, _)| rule).collect();
                    changes.undone.push((rule.to_owned(), undone));
                }
                changes.seen.truncate(back + 1);
            }
            None => {
                changes
                    .made
                    .push((rule.to_owned(), diff(&changes.plan, &after)));
                changes.seen.push(after_digest);
            }
        }
        changes.plan = after;
        self.overhead += start.elapsed();
    }
}

impl Drop for PlanChangeLog<'_> {
    /// Logs the changes of the pass, the changes later rules undid, and the
    /// plan they left, if any. A pass that fails logs what its rules changed
    /// before it failed.
    fn drop(&mut self) {
        let Some(changes) = &self.changes else {
            return;
        };
        for (rule, diff) in &changes.made {
            debug!(
                "Job {}, {}: {rule} changed the plan:\n{diff}",
                self.job_id, self.pass
            );
        }
        for (rule, undone) in &changes.undone {
            debug!(
                "Job {}, {}: {rule} undid the changes of {}",
                self.job_id,
                self.pass,
                undone.join(", ")
            );
        }
        if changes.made.is_empty() {
            return;
        }
        let rules: Vec<&str> =
            changes.made.iter().map(|(rule, _)| rule.as_str()).collect();
        debug!(
            "Job {}, {}: plan changed by {}:\n{}",
            self.job_id,
            self.pass,
            rules.join(", "),
            changes.plan.trim_end()
        );
    }
}

fn render(plan: &dyn ExecutionPlan) -> String {
    displayable(plan).indent(false).to_string()
}

fn digest(rendering: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    rendering.hash(&mut hasher);
    hasher.finish()
}

/// A line diff of two renderings of a plan: `-` marks a removed line and `+`
/// an added one, and up to [`CONTEXT`] unchanged lines are kept on each side
/// of a change, with `...` for those left out. Lines match ignoring their
/// indentation, so a subtree a rule moves under a new node shows as the one
/// added line.
fn diff(before: &str, after: &str) -> String {
    let before: Vec<&str> = before.lines().collect();
    let after: Vec<&str> = after.lines().collect();
    let before_keys: Vec<&str> = before.iter().map(|line| line.trim_start()).collect();
    let after_keys: Vec<&str> = after.iter().map(|line| line.trim_start()).collect();
    let mut ops = capture_diff_slices(Algorithm::Myers, &before_keys, &after_keys);
    if ops.iter().all(|op| op.tag() == DiffTag::Equal) {
        // Only the indentation changed: a node moved to another parent without
        // moving in the rendering.
        ops = capture_diff_slices(Algorithm::Myers, &before, &after);
    }

    let mut out = Vec::new();
    // The end, in `after`, of the lines shown so far.
    let mut shown_to = 0;
    for group in group_diff_ops(ops, CONTEXT) {
        let Some(first) = group.first() else {
            continue;
        };
        if first.new_range().start > shown_to {
            out.push("...".to_owned());
        }
        for op in &group {
            push_lines(&mut out, op, &before, &after);
        }
        shown_to = group.last().map_or(shown_to, |op| op.new_range().end);
    }
    if shown_to < after.len() {
        out.push("...".to_owned());
    }
    out.join("\n")
}

/// Appends the lines of `op`, tagged as [`diff`] does: an unchanged line as it
/// is in `after`.
fn push_lines(out: &mut Vec<String>, op: &DiffOp, before: &[&str], after: &[&str]) {
    let (tag, old, new) = op.as_tag_tuple();
    let mut push = |tag: char, lines: &[&str]| {
        out.extend(lines.iter().map(|line| format!("{tag}{line}")));
    };
    match tag {
        DiffTag::Equal => push(' ', &after[new]),
        DiffTag::Delete => push('-', &before[old]),
        DiffTag::Insert => push('+', &after[new]),
        DiffTag::Replace => {
            push('-', &before[old]);
            push('+', &after[new]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::execution_plan::ExchangeExec;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;
    use std::sync::Arc;

    #[test]
    fn a_diff_keeps_the_lines_around_each_change() {
        let before = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl";
        let after = "a\nc\nd\ne\nf\ng\nh\ni\nj\nK\nl";
        assert_eq!(
            diff(before, after),
            " a\n-b\n c\n d\n e\n...\n h\n i\n j\n-k\n+K\n l"
        );
    }

    #[test]
    fn a_diff_marks_where_lines_are_left_out() {
        let before = "a\nb\nc\nd\ne\nf\ng\nh\ni";
        let after = "a\nb\nc\nd\nE\nf\ng\nh\ni";
        assert_eq!(
            diff(before, after),
            "...\n b\n c\n d\n-e\n+E\n f\n g\n h\n..."
        );
    }

    #[test]
    fn a_subtree_moved_under_a_new_node_shows_as_that_node() {
        let before = "Projection\n  Filter\n    Scan";
        let after = "Projection\n  Repartition\n    Filter\n      Scan";
        assert_eq!(
            diff(before, after),
            " Projection\n+  Repartition\n     Filter\n       Scan"
        );
    }

    #[test]
    fn a_node_moved_to_another_parent_shows_its_new_indentation() {
        let before = "Union\n  Projection\n    Scan";
        let after = "Union\n  Projection\n  Scan";
        assert_eq!(
            diff(before, after),
            " Union\n   Projection\n-    Scan\n+  Scan"
        );
    }

    #[test]
    fn a_large_diff_shows_only_the_lines_around_its_changes() {
        let before: Vec<String> = (0..600).map(|i| format!("line {i}")).collect();
        let mut after = before.clone();
        after[0] = "first".to_owned();
        after[599] = "last".to_owned();
        let diff = diff(&before.join("\n"), &after.join("\n"));
        assert_eq!(
            diff,
            "-line 0\n+first\n line 1\n line 2\n line 3\n...\n \
             line 596\n line 597\n line 598\n-line 599\n+last"
        );
    }

    fn exchange(plan_id: usize) -> ExchangeExec {
        let input = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        ExchangeExec::new(input, None, plan_id)
    }

    fn rules_with_changes(log: &PlanChangeLog) -> Vec<String> {
        let changes = log.changes.as_ref().expect("the log is enabled");
        changes.made.iter().map(|(rule, _)| rule.clone()).collect()
    }

    fn undone(log: &PlanChangeLog) -> Vec<(String, Vec<String>)> {
        log.changes
            .as_ref()
            .expect("the log is enabled")
            .undone
            .clone()
    }

    #[test]
    fn only_rules_that_change_the_plan_are_recorded() {
        let job_id: JobId = "job".into();
        let plan = exchange(0);
        let mut log = PlanChangeLog::with_enabled(true, &job_id, Pass::Initial, &plan);

        log.record("A", &plan);
        // Some rules change an exchange in place.
        plan.set_stage_id(1);
        log.record("B", &plan);
        log.record("C", &plan);

        assert_eq!(rules_with_changes(&log), ["B"]);
    }

    #[test]
    fn changes_a_later_rule_undoes_are_noted_without_a_diff() {
        let job_id: JobId = "job".into();
        let (start, other) = (exchange(0), exchange(1));
        let mut log = PlanChangeLog::with_enabled(true, &job_id, Pass::Initial, &start);

        log.record("Add", &other);
        log.record("Remove", &start);
        assert!(rules_with_changes(&log).is_empty());

        log.record("A", &other);
        log.record("B", &exchange(2));
        log.record("C", &other);
        assert_eq!(rules_with_changes(&log), ["A"]);

        // A rule that changes nothing undoes nothing.
        log.record("D", &other);
        assert_eq!(
            undone(&log),
            [
                ("Remove".to_owned(), vec!["Add".to_owned()]),
                ("C".to_owned(), vec!["B".to_owned()]),
            ]
        );
    }

    #[test]
    fn a_disabled_log_records_nothing() {
        let job_id: JobId = "job".into();
        let plan = exchange(0);
        let mut log = PlanChangeLog::with_enabled(false, &job_id, Pass::Initial, &plan);

        plan.set_stage_id(1);
        log.record("A", &plan);

        assert!(log.changes.is_none());
        assert!(log.overhead().is_zero());
    }
}
