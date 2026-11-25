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

//! The checked-in list of records that are known to fail.
//!
//! Each line of `expected_failures.txt` is
//!
//! ```text
//! <engine> <file> <lines> <reason>
//! ```
//!
//! - `engine` is `datafusion`, `ballista-static`, `ballista-aqe`, or
//!   `ballista` for both Ballista engines.
//! - `file` is the path under `test_files/`.
//! - `lines` is a comma-separated list of the lines the failing records start
//!   on, or `skip` to not run the file on that engine at all (for files that
//!   hang or can't run). Line `0` stands for an error about the whole file,
//!   such as a configuration it leaves changed.
//! - `reason` is free text, usually an issue number.
//!
//! A listed record may fail, but doesn't have to: a run fails only on records
//! that aren't listed. `--update-expected` rewrites the list from a run,
//! keeping the reasons of records that still fail.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use clap::ValueEnum;
use datafusion::common::{DataFusionError, Result};

use crate::{Engine, FileResult, Outcome};

const HEADER: &str = "\
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# \"License\"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Records of DataFusion's sqllogictest files that are known to fail, by engine.
#
#   <engine> <file> <lines> <reason>
#
# engine: datafusion, ballista-static, ballista-aqe, or ballista for both
#         Ballista engines.
# lines:  the lines the failing records start on, or `skip` to not run the
#         file on that engine (it hangs or can't run). 0 is an error about
#         the whole file.
#
# A listed record may fail but doesn't have to. Regenerate this file with
# `--update-expected`; reasons of records that still fail are kept. See
# docs/source/contributors-guide/development.md.
";

/// What is expected of one file on one engine.
#[derive(Debug, Default, Clone)]
struct Expectation {
    /// Don't run the file on this engine, for this reason.
    skip: Option<String>,
    /// Lines whose records may fail, with the reason.
    lines: BTreeMap<u32, String>,
}

#[derive(Debug, Default)]
pub struct ExpectedFailures {
    by_file: BTreeMap<(String, Engine), Expectation>,
}

/// A failure the list doesn't allow.
#[derive(Debug, PartialEq)]
pub struct Unexpected {
    pub file: String,
    pub engine: Engine,
    /// `None` for the whole file (an error or a timeout).
    pub line: Option<u32>,
    pub message: String,
}

/// Listed records that passed in this run.
#[derive(Debug, PartialEq)]
pub struct NowPassing {
    pub file: String,
    pub engine: Engine,
    pub lines: Vec<u32>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Check {
    pub unexpected: Vec<Unexpected>,
    pub now_passing: Vec<NowPassing>,
}

impl ExpectedFailures {
    /// Reads the list; a missing file is an empty list.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text).map_err(|e| {
                DataFusionError::Configuration(format!("{}: {e}", path.display()))
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    fn parse(text: &str) -> std::result::Result<Self, String> {
        let mut list = Self::default();
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let error = |what: &str| format!("line {}: {what}: {line}", index + 1);
            let (engines, rest) = field(line).ok_or_else(|| error("no engine"))?;
            let (file, rest) = field(rest).ok_or_else(|| error("no file"))?;
            let (lines, rest) = field(rest).ok_or_else(|| error("no lines"))?;
            let reason = rest.trim().to_string();
            let engines = match engines {
                "ballista" => vec![Engine::BallistaStatic, Engine::BallistaAqe],
                name => vec![
                    Engine::from_str(name, false).map_err(|_| error("unknown engine"))?,
                ],
            };
            for engine in engines {
                let expectation =
                    list.by_file.entry((file.to_string(), engine)).or_default();
                if lines == "skip" {
                    expectation.skip = Some(reason.clone());
                    continue;
                }
                for number in lines.split(',') {
                    let number = number.parse().map_err(|_| error("bad line number"))?;
                    expectation.lines.insert(number, reason.clone());
                }
            }
        }
        Ok(list)
    }

    /// Why `file` isn't run on `engine`, if it isn't.
    pub fn skip_reason(&self, file: &str, engine: Engine) -> Option<&str> {
        self.by_file
            .get(&(file.to_string(), engine))
            .and_then(|e| e.skip.as_deref())
    }

    /// Compares a run's results with the list.
    pub fn check(&self, results: &[FileResult]) -> Check {
        let mut check = Check::default();
        for result in results {
            let file = &result.file;
            for (engine, outcome) in &result.outcomes {
                let allowed =
                    self.by_file.get(&(file.clone(), *engine)).map(|e| &e.lines);
                let allows = |line: u32| allowed.is_some_and(|a| a.contains_key(&line));
                let whole_file = |message: String| Unexpected {
                    file: file.clone(),
                    engine: *engine,
                    line: None,
                    message,
                };
                match outcome {
                    Outcome::Ran { failures } => {
                        let failed: BTreeSet<u32> =
                            failures.iter().map(|f| f.line).collect();
                        check.unexpected.extend(
                            failures.iter().filter(|f| !allows(f.line)).map(|f| {
                                Unexpected {
                                    file: file.clone(),
                                    engine: *engine,
                                    line: Some(f.line),
                                    message: f.message.clone(),
                                }
                            }),
                        );
                        let passing: Vec<u32> = allowed
                            .into_iter()
                            .flat_map(|a| a.keys())
                            .filter(|line| !failed.contains(line))
                            .copied()
                            .collect();
                        if !passing.is_empty() {
                            check.now_passing.push(NowPassing {
                                file: file.clone(),
                                engine: *engine,
                                lines: passing,
                            });
                        }
                    }
                    Outcome::Error(e) => check.unexpected.push(whole_file(e.clone())),
                    Outcome::TimedOut => {
                        check.unexpected.push(whole_file("timed out".to_string()))
                    }
                    Outcome::Skipped | Outcome::NotRun(_) => {}
                }
            }
        }
        check
    }

    /// The list with the expectations of every file and engine in `results`
    /// replaced by what happened. Reasons of records that still fail are kept.
    pub fn updated(&self, results: &[FileResult]) -> Self {
        let mut by_file = self.by_file.clone();
        for result in results {
            let file = &result.file;
            let datafusion_failed = result
                .outcome(Engine::Datafusion)
                .map(Outcome::failed_lines)
                .unwrap_or_default();
            for (engine, outcome) in &result.outcomes {
                let key = (file.clone(), *engine);
                let old = by_file.remove(&key).unwrap_or_default();
                let new = match outcome {
                    Outcome::NotRun(_) => old,
                    Outcome::Skipped => Expectation::default(),
                    Outcome::TimedOut => Expectation {
                        skip: Some(old.skip.unwrap_or_else(|| match engine {
                            Engine::BallistaAqe => {
                                "#2955 stalls on the adaptive planner".to_string()
                            }
                            _ => "times out".to_string(),
                        })),
                        lines: BTreeMap::new(),
                    },
                    Outcome::Error(e) => Expectation {
                        skip: Some(format!(
                            "fails to run: {}",
                            e.lines().next().unwrap_or_default()
                        )),
                        lines: BTreeMap::new(),
                    },
                    Outcome::Ran { failures } => {
                        let mut lines = BTreeMap::new();
                        for failure in failures {
                            lines.entry(failure.line).or_insert_with(|| {
                                old.lines.get(&failure.line).cloned().unwrap_or_else(
                                    || {
                                        if datafusion_failed.contains(&failure.line) {
                                            ALSO_ON_DATAFUSION.to_string()
                                        } else {
                                            reason(file, &failure.message).to_string()
                                        }
                                    },
                                )
                            });
                        }
                        Expectation { skip: None, lines }
                    }
                };
                if new.skip.is_some() || !new.lines.is_empty() {
                    by_file.insert(key, new);
                }
            }
        }
        Self { by_file }
    }

    /// The list as text, with records that fail the same way on both
    /// Ballista engines merged under `ballista`.
    pub fn render(&self) -> String {
        let mut out = HEADER.to_string();
        let files: BTreeSet<&str> =
            self.by_file.keys().map(|(f, _)| f.as_str()).collect();
        for file in files {
            let items = |engine: Engine| {
                self.by_file
                    .get(&(file.to_string(), engine))
                    .map(Expectation::items)
                    .unwrap_or_default()
            };
            let (st, aqe) = (items(Engine::BallistaStatic), items(Engine::BallistaAqe));
            let groups = [
                ("datafusion", items(Engine::Datafusion)),
                ("ballista", st.intersection(&aqe).copied().collect()),
                ("ballista-static", st.difference(&aqe).copied().collect()),
                ("ballista-aqe", aqe.difference(&st).copied().collect()),
            ];
            out.push('\n');
            for (engine, items) in groups {
                // One row per reason, in the order of their first lines.
                let mut rows: Vec<(&str, Vec<u32>)> = vec![];
                for item in items {
                    match item {
                        Item::Skip(reason) => {
                            let _ = writeln!(out, "{engine} {file} skip {reason}");
                        }
                        Item::Line(line, reason) => {
                            match rows.iter_mut().find(|(r, _)| *r == reason) {
                                Some((_, lines)) => lines.push(line),
                                None => rows.push((reason, vec![line])),
                            }
                        }
                    }
                }
                for (reason, lines) in rows {
                    let lines = lines.iter().map(u32::to_string).collect::<Vec<_>>();
                    let _ = writeln!(out, "{engine} {file} {} {reason}", lines.join(","));
                }
            }
        }
        out
    }
}

/// One entry of an [`Expectation`]: a skip, or a line that may fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Item<'a> {
    Skip(&'a str),
    Line(u32, &'a str),
}

impl Expectation {
    /// A skipped file has no lines to list.
    fn items(&self) -> BTreeSet<Item<'_>> {
        match &self.skip {
            Some(reason) => BTreeSet::from([Item::Skip(reason)]),
            None => self
                .lines
                .iter()
                .map(|(line, reason)| Item::Line(*line, reason))
                .collect(),
        }
    }
}

/// The next whitespace-separated field of `s`, and the rest.
fn field(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }
    let end = s.find(char::is_whitespace).unwrap_or(s.len());
    Some((&s[..end], &s[end..]))
}

/// Files whose `TestContext` registers tables before Ballista starts.
const PREREGISTERED_TABLES: &[&str] = &[
    "arrow_field.slt",
    "dictionary_struct.slt",
    "metadata.slt",
    "parquet_missing_bounds.slt",
    "range_partitioning.slt",
    "range_sorted_time_bin_agg.slt",
];

/// The reason of a record that fails on DataFusion itself: the published
/// crate's features or Ballista's dependency versions differ from DataFusion's
/// own test setup.
const ALSO_ON_DATAFUSION: &str = "also fails on plain DataFusion";

/// A reason for a newly listed Ballista failure, from its file and message.
/// Only patterns whose cause is known get an issue; the rest say what failed.
fn reason(file: &str, message: &str) -> &'static str {
    let has = |s: &str| message.contains(s);
    let expected_error = has("is expected to fail with error");
    let table_missing = (has("not found") && has("table")) || has("doesn't exist");
    if has("left modified configuration") {
        "RESET restores DataFusion's defaults, not Ballista's"
    } else if has("Invalid function") || file.starts_with("spark/") {
        "#2959 session functions are dropped"
    } else if table_missing && PREREGISTERED_TABLES.contains(&file) {
        "#2960 tables registered before Ballista starts are dropped"
    } else if table_missing {
        "table missing after an earlier failure"
    } else if expected_error && has("Task failed due to runtime execution error") {
        "#2967 runtime errors are wrapped"
    } else if has("RecursiveQueryExec") || has("WorkTableExec") || has("MATERIALIZED CTE")
    {
        "#2968 CTE can't be serialized"
    } else if has("with_null_aware_value_keys") {
        "#2953 multi-key null-aware anti join"
    } else if has("cannot restrict a `UnionExec` branch") {
        "#2958 union branch can't be restricted"
    } else if has("DistributedExchangeRule") {
        "#2942 sanity check after DistributedExchangeRule"
    } else if has("ordered_partition_by_indices") {
        "#2957 window input loses its ordering"
    } else if has("Dictionary(") || has("dictionary") {
        "#2963 dictionary columns come back decoded"
    } else if has("Float16") {
        "#2964 Float16 comes back as Float32"
    } else if file.contains("timezone") {
        "#2965 session time zone isn't applied on executors"
    } else if has("Utf8View") {
        "#1294 view types are disabled"
    } else if has("GROUPING SETS (())") {
        "#2962 GROUPING SETS (()) returns no rows"
    } else if has("MemUpdateExec") || has("MemDeleteExec") {
        "UPDATE / DELETE on a memory table can't be serialized"
    } else if has("Unsupported plan node") {
        "plan node can't be serialized"
    } else if has("failed to serialize logical plan") {
        "logical plan can't be serialized"
    } else if has("Schema mismatch") || has("Mismatch between schema and batches") {
        "batches don't match the plan's schema"
    } else if expected_error {
        "fails with a different error"
    } else if has("is expected to fail") {
        "doesn't fail"
    } else if has("result mismatch") || has("columns mismatch") {
        "wrong result"
    } else {
        "fails"
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::Failure;

    fn result(file: &str, outcomes: Vec<(Engine, Outcome)>) -> FileResult {
        FileResult {
            file: file.to_string(),
            outcomes,
            elapsed: Duration::ZERO,
        }
    }

    fn ran(lines: &[(u32, &str)]) -> Outcome {
        Outcome::Ran {
            failures: lines
                .iter()
                .map(|(line, message)| Failure {
                    line: *line,
                    message: message.to_string(),
                })
                .collect(),
        }
    }

    /// The rendered list without its header.
    fn body(list: &ExpectedFailures) -> String {
        list.render()[HEADER.len()..].to_string()
    }

    const LIST: &str = "\
# a comment
ballista a.slt 10,20 #2959 functions
ballista-aqe a.slt 30 wrong result
datafusion b.slt 0 config
ballista-aqe c.slt skip #2955 stalls
ballista f.slt skip hangs
";

    #[test]
    fn parse_and_render() {
        let list = ExpectedFailures::parse(LIST).unwrap();
        assert_eq!(
            body(&list),
            "\n\
             ballista a.slt 10,20 #2959 functions\n\
             ballista-aqe a.slt 30 wrong result\n\
             \n\
             datafusion b.slt 0 config\n\
             \n\
             ballista-aqe c.slt skip #2955 stalls\n\
             \n\
             ballista f.slt skip hangs\n"
        );
        assert_eq!(
            list.skip_reason("c.slt", Engine::BallistaAqe),
            Some("#2955 stalls")
        );
        assert_eq!(list.skip_reason("c.slt", Engine::BallistaStatic), None);
    }

    #[test]
    fn parse_errors() {
        assert_eq!(
            ExpectedFailures::parse("ballista-x a.slt 1 r").unwrap_err(),
            "line 1: unknown engine: ballista-x a.slt 1 r"
        );
        assert_eq!(
            ExpectedFailures::parse("ballista a.slt 1,x r").unwrap_err(),
            "line 1: bad line number: ballista a.slt 1,x r"
        );
        assert_eq!(
            ExpectedFailures::parse("ballista a.slt").unwrap_err(),
            "line 1: no lines: ballista a.slt"
        );
    }

    #[test]
    fn check_finds_unlisted_failures_and_listed_passes() {
        let list = ExpectedFailures::parse(LIST).unwrap();
        let results = vec![
            result(
                "a.slt",
                vec![
                    (Engine::BallistaStatic, ran(&[(10, "x"), (40, "new")])),
                    (Engine::BallistaAqe, ran(&[(10, "x"), (20, "y"), (30, "z")])),
                ],
            ),
            result("c.slt", vec![(Engine::BallistaStatic, Outcome::TimedOut)]),
        ];
        assert_eq!(
            list.check(&results),
            Check {
                unexpected: vec![
                    Unexpected {
                        file: "a.slt".to_string(),
                        engine: Engine::BallistaStatic,
                        line: Some(40),
                        message: "new".to_string(),
                    },
                    Unexpected {
                        file: "c.slt".to_string(),
                        engine: Engine::BallistaStatic,
                        line: None,
                        message: "timed out".to_string(),
                    },
                ],
                now_passing: vec![NowPassing {
                    file: "a.slt".to_string(),
                    engine: Engine::BallistaStatic,
                    lines: vec![20],
                }],
            }
        );
    }

    #[test]
    fn update_keeps_reasons_and_files_that_did_not_run() {
        let list = ExpectedFailures::parse(LIST).unwrap();
        let results = vec![
            result(
                "a.slt",
                vec![
                    (Engine::Datafusion, ran(&[(50, "query result mismatch")])),
                    (
                        Engine::BallistaStatic,
                        ran(&[(10, "x"), (50, "query result mismatch")]),
                    ),
                    (
                        Engine::BallistaAqe,
                        ran(&[(10, "x"), (50, "query result mismatch")]),
                    ),
                ],
            ),
            result(
                "c.slt",
                vec![(Engine::BallistaAqe, Outcome::NotRun("#2955 stalls".into()))],
            ),
            result("d.slt", vec![(Engine::BallistaStatic, Outcome::TimedOut)]),
            result(
                "spark/e.slt",
                vec![(Engine::Datafusion, ran(&[(7, "Invalid function 'pmod'")]))],
            ),
        ];
        assert_eq!(
            body(&list.updated(&results)),
            "\n\
             datafusion a.slt 50 also fails on plain DataFusion\n\
             ballista a.slt 10 #2959 functions\n\
             ballista a.slt 50 also fails on plain DataFusion\n\
             \n\
             datafusion b.slt 0 config\n\
             \n\
             ballista-aqe c.slt skip #2955 stalls\n\
             \n\
             ballista-static d.slt skip times out\n\
             \n\
             ballista f.slt skip hangs\n\
             \n\
             datafusion spark/e.slt 7 also fails on plain DataFusion\n"
        );
    }
}
