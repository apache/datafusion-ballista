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
//! Spark's plan change log, and neither does a change a later rule of the
//! pass undoes, such as the node DataFusion's `OutputRequirements` adds and
//! removes again:
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

use ballista_core::JobId;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use log::{Level, debug, log_enabled};

/// Unchanged lines a diff keeps on each side of a change.
const CONTEXT: usize = 3;

/// The largest table of line matches a diff builds, `(m + 1) * (n + 1)` for
/// the `m` and `n` lines between the first and last change of each plan, about
/// 500 lines each. Past it, all of those lines show as removed and added.
const MAX_MATCH_TABLE: usize = 1 << 18;

/// What the planner runs a pass of rules over a job's plan for.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Pass {
    /// The first optimization of the job's plan.
    Initial,
    /// The replan after a stage finished.
    StageFinished(usize),
    /// The replan after a stage about to launch read a finished stage instead.
    StageReused,
    /// The rules run on a stage's own plan as it launches.
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
            Self::Stage(None) => write!(f, "stage"),
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
}

/// The changes the rules of a pass made to a plan.
struct Changes {
    /// The plan as the last rule left it, rendered.
    plan: String,
    /// Each change no later rule undid: the rule's name and its diff.
    made: Vec<(String, String)>,
    /// A digest of each plan the pass went through, and how many changes of
    /// `made` led to it, so a rule that brings a plan back drops the changes
    /// it undid.
    seen: Vec<(u64, usize)>,
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
        let changes = enabled.then(|| {
            let plan = render(plan);
            Changes {
                seen: vec![(digest(&plan), 0)],
                plan,
                made: Vec::new(),
            }
        });
        Self {
            job_id,
            pass,
            changes,
        }
    }

    /// Records the change `rule` made, given the `plan` it returned, if it
    /// changed the plan.
    pub(crate) fn record(&mut self, rule: &str, plan: &dyn ExecutionPlan) {
        let Some(changes) = &mut self.changes else {
            return;
        };
        let after = render(plan);
        if after == changes.plan {
            return;
        }
        let after_digest = digest(&after);
        match changes
            .seen
            .iter()
            .position(|(seen, _)| *seen == after_digest)
        {
            // The rule undid every change since the plan was last like this.
            Some(back) => {
                changes.made.truncate(changes.seen[back].1);
                changes.seen.truncate(back + 1);
            }
            None => {
                changes
                    .made
                    .push((rule.to_owned(), diff(&changes.plan, &after)));
                changes.seen.push((after_digest, changes.made.len()));
            }
        }
        changes.plan = after;
    }

    /// Logs the changes of the pass, and the plan they left, if any.
    pub(crate) fn finish(mut self) {
        self.flush();
    }

    fn flush(&mut self) {
        let Some(changes) = self.changes.take() else {
            return;
        };
        if changes.made.is_empty() {
            return;
        }
        for (rule, diff) in &changes.made {
            debug!(
                "Job {}, {}: {rule} changed the plan:\n{diff}",
                self.job_id, self.pass
            );
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

impl Drop for PlanChangeLog<'_> {
    /// Logs what the rules of a pass that failed changed before it failed.
    fn drop(&mut self) {
        self.flush();
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
    let mut lines = edit_script(&before, &after, str::trim_start);
    if lines.iter().all(|(tag, _)| *tag == ' ') {
        // Only the indentation changed: a node moved to another parent without
        // moving in the rendering.
        lines = edit_script(&before, &after, |line| line);
    }

    let mut kept = vec![false; lines.len()];
    for (i, (tag, _)) in lines.iter().enumerate() {
        if *tag != ' ' {
            let end = (i + CONTEXT + 1).min(lines.len());
            kept[i.saturating_sub(CONTEXT)..end].fill(true);
        }
    }
    let mut out = Vec::new();
    let mut skipped = false;
    for ((tag, line), kept) in lines.into_iter().zip(kept) {
        if !kept {
            skipped = true;
        } else {
            if std::mem::take(&mut skipped) {
                out.push("...".to_owned());
            }
            out.push(format!("{tag}{line}"));
        }
    }
    if skipped {
        out.push("...".to_owned());
    }
    out.join("\n")
}

/// The lines of `before` and `after` in order, each tagged ` ` when kept, as
/// it is in `after`, `-` when removed and `+` when added. Two lines match when
/// their `key`s are equal.
fn edit_script<'a>(
    before: &[&'a str],
    after: &[&'a str],
    key: fn(&str) -> &str,
) -> Vec<(char, &'a str)> {
    let shorter = before.len().min(after.len());
    let prefix = (0..shorter)
        .take_while(|&i| key(before[i]) == key(after[i]))
        .count();
    let suffix = (0..shorter - prefix)
        .take_while(|&i| {
            key(before[before.len() - 1 - i]) == key(after[after.len() - 1 - i])
        })
        .count();
    let old = &before[prefix..before.len() - suffix];
    let new = &after[prefix..after.len() - suffix];

    let mut script: Vec<_> = after[..prefix].iter().map(|line| (' ', *line)).collect();
    let width = new.len() + 1;
    if (old.len() + 1).saturating_mul(width) > MAX_MATCH_TABLE {
        script.extend(old.iter().map(|line| ('-', *line)));
        script.extend(new.iter().map(|line| ('+', *line)));
    } else {
        // `common[i * width + j]`: how many lines `old[i..]` and `new[j..]`
        // have in common, in order.
        let mut common = vec![0u32; (old.len() + 1) * width];
        for i in (0..old.len()).rev() {
            for j in (0..new.len()).rev() {
                common[i * width + j] = if key(old[i]) == key(new[j]) {
                    common[(i + 1) * width + j + 1] + 1
                } else {
                    common[(i + 1) * width + j].max(common[i * width + j + 1])
                };
            }
        }
        let (mut i, mut j) = (0, 0);
        while i < old.len() || j < new.len() {
            if i < old.len() && j < new.len() && key(old[i]) == key(new[j]) {
                script.push((' ', new[j]));
                i += 1;
                j += 1;
            } else if j == new.len()
                || (i < old.len()
                    && common[(i + 1) * width + j] >= common[i * width + j + 1])
            {
                script.push(('-', old[i]));
                i += 1;
            } else {
                script.push(('+', new[j]));
                j += 1;
            }
        }
    }
    script.extend(
        after[after.len() - suffix..]
            .iter()
            .map(|line| (' ', *line)),
    );
    script
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
    fn a_diff_too_large_to_match_shows_every_line_between_the_changes() {
        let before: Vec<String> = (0..600).map(|i| format!("line {i}")).collect();
        let mut after = before.clone();
        after[0] = "first".to_owned();
        after[599] = "last".to_owned();
        let diff = diff(&before.join("\n"), &after.join("\n"));
        assert_eq!(
            diff.lines().filter(|line| line.starts_with('-')).count(),
            600
        );
        assert_eq!(
            diff.lines().filter(|line| line.starts_with('+')).count(),
            600
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
    fn changes_a_later_rule_undoes_are_dropped() {
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
    }

    #[test]
    fn a_disabled_log_records_nothing() {
        let job_id: JobId = "job".into();
        let plan = exchange(0);
        let mut log = PlanChangeLog::with_enabled(false, &job_id, Pass::Initial, &plan);

        plan.set_stage_id(1);
        log.record("A", &plan);

        assert!(log.changes.is_none());
    }
}
