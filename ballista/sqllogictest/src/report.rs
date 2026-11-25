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

//! The Markdown report of a run.

use std::fmt::Write as _;
use std::time::Duration;

use crate::expected::Check;
use crate::{Engine, FileResult, Outcome};

pub struct Report {
    /// Counts, unexpected failures and listed records that now pass.
    pub summary: String,
    /// The summary, then comparisons between engines and every failure.
    pub full: String,
}

pub fn render(
    results: &[FileResult],
    engines: &[Engine],
    elapsed: Duration,
    check: &Check,
) -> Report {
    let mut out = String::new();
    let _ = writeln!(out, "# DataFusion sqllogictest on Ballista\n");
    let _ = writeln!(
        out,
        "{} files, {:.0}s wall time.\n",
        results.len(),
        elapsed.as_secs_f64()
    );
    counts(&mut out, results, engines);

    if check.unexpected.is_empty() {
        let _ = writeln!(out, "\nNo unexpected failures.");
    } else {
        let _ = writeln!(
            out,
            "\n## Unexpected failures ({})\n\nThese aren't in `expected_failures.txt`.\n",
            check.unexpected.len()
        );
        for u in &check.unexpected {
            let at = u.line.map(|l| format!(":{l}")).unwrap_or_default();
            let _ = writeln!(
                out,
                "### {}{at} on {}\n\n```text\n{}\n```\n",
                u.file,
                u.engine.name(),
                u.message.trim_end()
            );
        }
    }
    if !check.now_passing.is_empty() {
        let _ = writeln!(
            out,
            "\n## Listed records that passed\n\nRun with `--update-expected` to \
             remove them from `expected_failures.txt` if they pass consistently.\n"
        );
        let _ = writeln!(out, "| file | engine | lines |\n|---|---|---|");
        for p in &check.now_passing {
            let _ = writeln!(
                out,
                "| {} | {} | {} |",
                p.file,
                p.engine.name(),
                format_lines(&p.lines)
            );
        }
    }
    let summary = out.clone();

    comparisons(&mut out, results);
    file_table(&mut out, results, engines);
    all_failures(&mut out, results);
    Report { summary, full: out }
}

fn counts(out: &mut String, results: &[FileResult], engines: &[Engine]) {
    let _ = writeln!(
        out,
        "| engine | {} | failed records |",
        Outcome::KINDS.join(" | ")
    );
    let _ = writeln!(out, "|---|{}---|", "---|".repeat(Outcome::KINDS.len()));
    for &engine in engines {
        let outcomes: Vec<&Outcome> =
            results.iter().filter_map(|r| r.outcome(engine)).collect();
        let _ = write!(out, "| {} |", engine.name());
        for kind in Outcome::KINDS {
            let n = outcomes.iter().filter(|o| o.kind() == kind).count();
            let _ = write!(out, " {n} |");
        }
        let failed: usize = outcomes.iter().map(|o| o.failures().len()).sum();
        let _ = writeln!(out, " {failed} |");
    }
}

/// Records that fail on a Ballista engine but pass on DataFusion, and records
/// where the two planners disagree. Only runs that finished are compared.
fn comparisons(out: &mut String, results: &[FileResult]) {
    let mut ballista_only = String::new();
    let mut planner_diff = String::new();
    for result in results {
        let lines = |e: Engine| match result.outcome(e) {
            Some(outcome @ Outcome::Ran { .. }) => Some(outcome.failed_lines()),
            _ => None,
        };
        let (datafusion, st, aqe) = (
            lines(Engine::Datafusion),
            lines(Engine::BallistaStatic),
            lines(Engine::BallistaAqe),
        );
        if let Some(datafusion) = &datafusion {
            for (engine, ballista) in
                [(Engine::BallistaStatic, &st), (Engine::BallistaAqe, &aqe)]
            {
                let only = ballista
                    .as_ref()
                    .map(|b| difference(b, datafusion))
                    .unwrap_or_default();
                if !only.is_empty() {
                    let _ = writeln!(
                        ballista_only,
                        "| {} | {} | {} |",
                        result.file,
                        engine.name(),
                        format_lines(&only)
                    );
                }
            }
        }
        if let (Some(st), Some(aqe)) = (&st, &aqe) {
            let (aqe_only, static_only) = (difference(aqe, st), difference(st, aqe));
            if !aqe_only.is_empty() || !static_only.is_empty() {
                let _ = writeln!(
                    planner_diff,
                    "| {} | {} | {} |",
                    result.file,
                    format_lines(&aqe_only),
                    format_lines(&static_only)
                );
            }
        }
    }
    if !ballista_only.is_empty() {
        let _ = writeln!(
            out,
            "\n## Records that fail on Ballista but pass on DataFusion\n"
        );
        let _ = writeln!(out, "| file | engine | lines |\n|---|---|---|");
        out.push_str(&ballista_only);
    }
    if !planner_diff.is_empty() {
        let _ = writeln!(out, "\n## Records where the planners disagree\n");
        let _ = writeln!(
            out,
            "| file | fail on AQE only | fail on static only |\n|---|---|---|"
        );
        out.push_str(&planner_diff);
    }
}

/// The outcome on every engine, for files that aren't clean everywhere.
fn file_table(out: &mut String, results: &[FileResult], engines: &[Engine]) {
    let _ = writeln!(out, "\n## Files that don't pass on every engine\n");
    let _ = write!(out, "| file |");
    for engine in engines {
        let _ = write!(out, " {} |", engine.name());
    }
    let _ = write!(out, "\n|---|");
    for _ in engines {
        let _ = write!(out, "---|");
    }
    out.push('\n');
    for result in results
        .iter()
        .filter(|r| !r.outcomes.iter().all(|(_, o)| o.clean()))
    {
        let _ = write!(out, "| {} |", result.file);
        for (_, outcome) in &result.outcomes {
            let _ = write!(out, " {} |", outcome.short());
        }
        out.push('\n');
    }
}

fn all_failures(out: &mut String, results: &[FileResult]) {
    let _ = writeln!(out, "\n## Failures\n");
    for result in results {
        for (engine, outcome) in &result.outcomes {
            let body = match outcome {
                Outcome::Ran { failures } if !failures.is_empty() => failures
                    .iter()
                    .map(|f| format!("line {}:\n{}\n", f.line, f.message))
                    .collect::<Vec<_>>()
                    .join("\n"),
                Outcome::Error(e) => e.clone(),
                Outcome::TimedOut => "timed out".to_string(),
                _ => continue,
            };
            let _ = writeln!(
                out,
                "### {} on {}\n\n```text\n{}\n```\n",
                &result.file,
                engine.name(),
                body.trim_end()
            );
        }
    }
}

fn difference(a: &[u32], b: &[u32]) -> Vec<u32> {
    a.iter().filter(|l| !b.contains(l)).copied().collect()
}

fn format_lines(lines: &[u32]) -> String {
    if lines.is_empty() {
        return "-".to_string();
    }
    lines
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
