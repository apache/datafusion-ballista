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

//! Regenerates the marker-delimited configuration tables in
//! `docs/source/**/*.md` from the `ballista-core` config registry.
//!
//! Run via `./dev/update_config_docs.sh`. Pass `--check` to report drift
//! without writing, which is what CI does.

use ballista_core::config::{BallistaConfig, ConfigEntry};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Column headers for every generated table.
const HEADER: [&str; 4] = ["key", "type", "default", "description"];

/// A configuration key flattened into the cells of a docs table row.
struct Setting {
    key: String,
    cells: [String; 4],
}

/// Escapes text for use inside a markdown table cell. A raw pipe would
/// terminate the cell early and corrupt the table.
fn escape_cell(text: &str) -> String {
    text.replace('|', r"\|")
}

/// Flattens a registry entry into the four cells of a docs table row.
fn setting_cells(entry: &ConfigEntry) -> [String; 4] {
    [
        escape_cell(entry.name()),
        format!("{:?}", entry.data_type()),
        escape_cell(entry.doc_default().unwrap_or("(none)")),
        escape_cell(entry.description()),
    ]
}

/// Reads every registered configuration key into renderable form.
fn settings_from_registry() -> Vec<Setting> {
    BallistaConfig::valid_entries()
        .values()
        .map(|entry| Setting {
            key: entry.name().to_string(),
            cells: setting_cells(entry),
        })
        .collect()
}

/// Renders `rows` as a markdown table, padding each cell to the widest cell in
/// its column so the raw markdown stays readable.
///
/// Callers emit a `<!-- prettier-ignore -->` directive ahead of the table, so
/// this formatting does not need to match prettier's own table algorithm.
///
/// Rows are sorted by their first cell. The registry is a `HashMap`, so
/// without this the output would differ between runs.
fn render_table(rows: &[[String; 4]]) -> String {
    let mut rows = rows.to_vec();
    rows.sort_by(|a, b| a[0].cmp(&b[0]));

    let widths: [usize; 4] = std::array::from_fn(|i| {
        rows.iter()
            .map(|row| row[i].chars().count())
            .chain(std::iter::once(HEADER[i].chars().count()))
            .max()
            .unwrap_or(0)
    });

    let mut out = render_row(&HEADER.map(str::to_string), &widths);
    out.push_str(&render_separator(&widths));
    for row in &rows {
        out.push_str(&render_row(row, &widths));
    }
    out
}

fn render_row(cells: &[String; 4], widths: &[usize; 4]) -> String {
    let mut line = String::from("|");
    for (cell, width) in cells.iter().zip(widths) {
        line.push(' ');
        line.push_str(cell);
        line.extend(std::iter::repeat_n(' ', width - cell.chars().count()));
        line.push_str(" |");
    }
    line.push('\n');
    line
}

fn render_separator(widths: &[usize; 4]) -> String {
    let mut line = String::from("|");
    for width in widths {
        line.push(' ');
        line.extend(std::iter::repeat_n('-', *width));
        line.push_str(" |");
    }
    line.push('\n');
    line
}

/// Opening marker. The rest of the line may carry a `prefix=` attribute.
const BEGIN: &str = "<!-- BEGIN GENERATED CONFIG REFERENCE";
/// Closing marker, matched exactly.
const END: &str = "<!-- END GENERATED CONFIG REFERENCE -->";
/// Stops prettier reformatting the generated table. Without it the prettier CI
/// job and the config-docs CI job would disagree about the table's layout and
/// neither could pass.
const PRETTIER_IGNORE: &str = "<!-- prettier-ignore -->";

/// Returns whether `line` opens or closes a fenced code block, i.e. its
/// trimmed content starts with a ``` or ~~~ fence delimiter. Fences are
/// recognised regardless of indentation, since a fenced block nested inside
/// a list item is still a fence as far as markdown rendering is concerned.
fn is_fence_delimiter(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("```") || trimmed.starts_with("~~~")
}

/// Extracts the key prefixes selected by a `BEGIN` marker line. An empty
/// result means the region selects every key.
fn parse_prefixes(line: &str) -> Result<Vec<String>, String> {
    let attributes = line
        .strip_prefix(BEGIN)
        .and_then(|rest| rest.strip_suffix("-->"))
        .ok_or_else(|| format!("malformed marker: `{line}`"))?
        .trim();

    if attributes.is_empty() {
        return Ok(Vec::new());
    }

    let value = attributes.strip_prefix("prefix=").ok_or_else(|| {
        format!("unrecognised marker attribute in `{line}`; only `prefix=` is supported")
    })?;

    Ok(value
        .split(',')
        .map(|prefix| prefix.trim().to_string())
        .filter(|prefix| !prefix.is_empty())
        .collect())
}

/// Rewrites the body of every generated region in `content`.
///
/// Returns the new content and the keys rendered into it. Callers union those
/// key lists across files to check that no configuration key is undocumented.
fn splice(content: &str, settings: &[Setting]) -> Result<(String, Vec<String>), String> {
    let mut out = String::with_capacity(content.len());
    let mut rendered: Vec<String> = Vec::new();
    let mut lines = content.lines();
    // Whether the line currently being scanned is inside a fenced code
    // block. Markers only trimmed of trailing whitespace (never leading) so
    // that a marker must start at column 0 to be live; combined with the
    // fence tracking, this keeps a marker shown as a syntax example -
    // fenced, or indented inside a list item - from being mistaken for a
    // real one and silently overwritten or closed early.
    let mut in_fence = false;

    while let Some(line) = lines.next() {
        let trimmed = line.trim_end();

        if is_fence_delimiter(line) {
            in_fence = !in_fence;
        }

        if !in_fence && trimmed == END {
            return Err(format!("found `{END}` with no matching BEGIN marker"));
        }

        out.push_str(line);
        out.push('\n');

        if in_fence || !trimmed.starts_with(BEGIN) {
            continue;
        }

        let prefixes = parse_prefixes(trimmed)?;

        // Discard the old body.
        let mut closed = false;
        for inner in lines.by_ref() {
            let inner_trimmed = inner.trim_end();

            if is_fence_delimiter(inner) {
                in_fence = !in_fence;
            }

            if in_fence {
                continue;
            }

            if inner_trimmed.starts_with(BEGIN) {
                return Err(format!(
                    "nested BEGIN marker inside the region opened by `{trimmed}`"
                ));
            }
            if inner_trimmed == END {
                closed = true;
                break;
            }
        }
        if !closed {
            return Err(format!("unterminated marker: `{trimmed}`"));
        }

        let selected: Vec<&Setting> = settings
            .iter()
            .filter(|setting| {
                prefixes.is_empty()
                    || prefixes
                        .iter()
                        .any(|prefix| setting.key.starts_with(prefix.as_str()))
            })
            .collect();

        if selected.is_empty() {
            return Err(format!("marker `{trimmed}` matched no configuration keys"));
        }

        rendered.extend(selected.iter().map(|setting| setting.key.clone()));

        let rows: Vec<[String; 4]> = selected
            .iter()
            .map(|setting| setting.cells.clone())
            .collect();

        // The prettier-ignore directive keeps prettier from reformatting the
        // table. The blank line before END is required even so: prettier
        // inserts one between a table and a following HTML comment.
        out.push('\n');
        out.push_str(PRETTIER_IGNORE);
        out.push('\n');
        out.push_str(&render_table(&rows));
        out.push('\n');
        out.push_str(END);
        out.push('\n');
    }

    Ok((out, rendered))
}

/// Configuration keys that no generated region rendered.
///
/// A non-empty result is a hard error: it means a key exists in the registry
/// but appears in no documentation, which is the drift this tool exists to
/// prevent.
fn orphan_keys<'a>(settings: &'a [Setting], covered: &BTreeSet<String>) -> Vec<&'a str> {
    let mut orphans: Vec<&str> = settings
        .iter()
        .map(|setting| setting.key.as_str())
        .filter(|key| !covered.contains(*key))
        .collect();
    orphans.sort_unstable();
    orphans
}

/// Collects every `.md` file under `dir`, recursively.
fn markdown_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            markdown_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "md") {
            out.push(path);
        }
    }
    Ok(())
}

fn run(check: bool) -> Result<(), String> {
    // Resolved at compile time so the tool works from any working directory.
    let docs = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/source");

    let mut files = Vec::new();
    markdown_files(&docs, &mut files)
        .map_err(|e| format!("failed to walk {}: {e}", docs.display()))?;
    files.sort();

    let settings = settings_from_registry();
    let mut covered: BTreeSet<String> = BTreeSet::new();
    let mut stale: Vec<PathBuf> = Vec::new();

    for file in &files {
        let content = fs::read_to_string(file)
            .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
        if !content.contains(BEGIN) {
            continue;
        }

        let (updated, keys) = splice(&content, &settings)
            .map_err(|e| format!("{}: {e}", file.display()))?;
        covered.extend(keys);

        if updated != content {
            stale.push(file.clone());
            if !check {
                fs::write(file, updated)
                    .map_err(|e| format!("failed to write {}: {e}", file.display()))?;
            }
        }
    }

    let orphans = orphan_keys(&settings, &covered);
    if !orphans.is_empty() {
        let list = orphans
            .iter()
            .map(|key| format!("  - {key}"))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "these configuration keys are rendered by no generated region:\n{list}\n\
             Add them to a marker region under docs/source, or widen an existing \
             region's `prefix=` list."
        ));
    }

    if check && !stale.is_empty() {
        let list = stale
            .iter()
            .map(|file| format!("  - {}", file.display()))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "the generated configuration tables are out of date:\n{list}\n\
             Run ./dev/update_config_docs.sh and commit the result."
        ));
    }

    if check {
        println!("configuration tables are up to date");
    } else if stale.is_empty() {
        println!("configuration tables already up to date");
    } else {
        for file in &stale {
            println!("updated {}", file.display());
        }
    }

    Ok(())
}

fn main() -> ExitCode {
    let check = std::env::args().any(|arg| arg == "--check");

    match run(check) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, ty: &str, default: &str, description: &str) -> [String; 4] {
        [
            key.to_string(),
            ty.to_string(),
            default.to_string(),
            description.to_string(),
        ]
    }

    #[test]
    fn render_table_aligns_columns() {
        // Every cell is padded to the widest cell in its column, and the
        // separator cell is exactly that many dashes. This is for readability
        // of the raw markdown; correctness does not depend on matching any
        // particular formatter. The first row's key is wider than the "key"
        // header, which forces the column past its header width.
        let rows = vec![
            row("ballista.a.very.long.key.name", "Boolean", "false", "short"),
            row(
                "b",
                "UInt64",
                "268435456",
                "a much longer description here that goes on",
            ),
        ];

        // Sorted order, not insertion order: "b" is a proper prefix of
        // "ballista.a.very.long.key.name", and standard lexicographic string
        // ordering sorts the shorter prefix first (`"b" < "ballista..."`).
        let expected = "\
| key                           | type    | default   | description                                 |
| ----------------------------- | ------- | --------- | ------------------------------------------- |
| b                             | UInt64  | 268435456 | a much longer description here that goes on |
| ballista.a.very.long.key.name | Boolean | false     | short                                       |
";

        assert_eq!(render_table(&rows), expected);
    }

    #[test]
    fn render_table_sorts_by_key() {
        // The registry is a HashMap, so unsorted output would never converge.
        let rows = vec![
            row("ballista.z", "Utf8", "-", "last"),
            row("ballista.a", "Utf8", "-", "first"),
        ];

        let rendered = render_table(&rows);
        let a = rendered.find("ballista.a").expect("row present");
        let z = rendered.find("ballista.z").expect("row present");
        assert!(a < z, "rows should be sorted by key:\n{rendered}");
    }

    #[test]
    fn settings_from_registry_covers_every_key() {
        let settings = settings_from_registry();
        let keys: Vec<&str> = settings.iter().map(|s| s.key.as_str()).collect();

        assert_eq!(settings.len(), BallistaConfig::valid_entries().len());
        assert!(keys.contains(&"ballista.shuffle.sort_based.batch_size"));
        assert!(keys.contains(&"ballista.testing.chaos_execution.seed"));
    }

    #[test]
    fn setting_cells_prefers_doc_default() {
        let entries = BallistaConfig::valid_entries();

        // The real default is the empty string, which would render as a blank
        // cell; doc_default supplies something readable instead.
        let seed = entries
            .get("ballista.testing.chaos_execution.seed")
            .expect("entry is registered");
        assert_eq!(setting_cells(seed)[2], "(empty)");

        // Entries with no override show their real default.
        let batch_size = entries
            .get("ballista.shuffle.sort_based.batch_size")
            .expect("entry is registered");
        assert_eq!(setting_cells(batch_size)[2], "8192");
    }

    #[test]
    fn escape_cell_escapes_pipes() {
        // A raw pipe would terminate the markdown table cell early.
        assert_eq!(escape_cell("a | b"), r"a \| b");
        assert_eq!(escape_cell("no pipes here"), "no pipes here");
    }

    #[test]
    fn setting_cells_escapes_pipes_in_descriptions() {
        // No registered description contains a pipe today. Assert that
        // invariant directly, so this fails loudly if one is ever added
        // without going through escape_cell.
        for entry in BallistaConfig::valid_entries().values() {
            let description = setting_cells(entry)[3].clone();
            assert!(
                !description.contains('|') || description.contains(r"\|"),
                "unescaped pipe in description of {}: {description}",
                entry.name()
            );
        }
    }

    fn settings() -> Vec<Setting> {
        vec![
            Setting {
                key: "ballista.shuffle.one".to_string(),
                cells: row("ballista.shuffle.one", "Utf8", "-", "shuffle one"),
            },
            Setting {
                key: "ballista.shuffle.two".to_string(),
                cells: row("ballista.shuffle.two", "Utf8", "-", "shuffle two"),
            },
            Setting {
                key: "ballista.testing.one".to_string(),
                cells: row("ballista.testing.one", "Utf8", "-", "testing one"),
            },
        ]
    }

    fn doc(body: &str) -> String {
        format!("# Title\n\n{body}\ntail\n")
    }

    #[test]
    fn splice_fills_an_empty_region() {
        let content = doc(
            "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let (out, keys) = splice(&content, &settings()).expect("splice succeeds");

        assert_eq!(keys, vec!["ballista.testing.one".to_string()]);
        assert!(out.contains("| ballista.testing.one |"), "got:\n{out}");
        assert!(!out.contains("ballista.shuffle.one"), "got:\n{out}");
        // Prose outside the region survives.
        assert!(out.starts_with("# Title\n"), "got:\n{out}");
        assert!(out.ends_with("tail\n"), "got:\n{out}");
    }

    #[test]
    fn splice_emits_prettier_ignore_and_blank_lines() {
        // Without the directive the prettier CI job and the config-docs CI job
        // would fight over the table's formatting and neither could pass. The
        // blank line before END is required even with the directive: prettier
        // inserts one there regardless.
        let content = doc(
            "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let (out, _) = splice(&content, &settings()).expect("splice succeeds");

        assert!(
            out.contains(
                "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\n<!-- prettier-ignore -->\n| key"
            ),
            "missing blank line or prettier-ignore after BEGIN:\n{out}"
        );
        assert!(
            out.contains(" |\n\n<!-- END GENERATED CONFIG REFERENCE -->"),
            "missing blank line before END:\n{out}"
        );
    }

    #[test]
    fn splice_is_idempotent() {
        let content = doc(
            "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.shuffle. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let (once, _) = splice(&content, &settings()).expect("first splice");
        let (twice, _) = splice(&once, &settings()).expect("second splice");

        assert_eq!(once, twice, "splice must converge");
    }

    #[test]
    fn splice_replaces_stale_content() {
        let content = doc(
            "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\n\
             | key | type |\n| --- | ---- |\n| ballista.gone | Utf8 |\n\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let (out, _) = splice(&content, &settings()).expect("splice succeeds");

        assert!(!out.contains("ballista.gone"), "stale row survived:\n{out}");
        assert!(out.contains("ballista.testing.one"), "got:\n{out}");
    }

    #[test]
    fn splice_handles_multiple_regions() {
        let content = doc(
            "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.shuffle. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n\n\
             ## Testing\n\n\
             <!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let (out, mut keys) = splice(&content, &settings()).expect("splice succeeds");
        keys.sort();

        assert_eq!(
            keys,
            vec![
                "ballista.shuffle.one".to_string(),
                "ballista.shuffle.two".to_string(),
                "ballista.testing.one".to_string(),
            ]
        );
        assert!(
            out.contains("## Testing"),
            "prose between regions lost:\n{out}"
        );
    }

    #[test]
    fn splice_ignores_marker_pair_inside_fenced_code_block() {
        // A marker pair shown as a syntax example inside a fenced code block
        // must not be mistaken for a live region: there is nothing to
        // render into, so the fenced block passes through untouched and no
        // keys are reported.
        let content = doc("```\n\
             <!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n\
             ```\n");

        let (out, keys) = splice(&content, &settings()).expect("splice succeeds");

        assert!(
            keys.is_empty(),
            "fenced markers must not be treated as live: {keys:?}"
        );
        assert_eq!(out, content, "fenced block must be left untouched");
    }

    #[test]
    fn splice_ignores_indented_begin_marker() {
        // A BEGIN marker that does not start at column 0 - e.g. indented
        // inside a list item as a syntax example - is not live. With no
        // live BEGIN to match it, the following unindented END is an
        // orphan.
        let content = doc(
            "  <!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let err = splice(&content, &settings()).expect_err("must fail");

        assert!(err.contains("no matching BEGIN"), "got: {err}");
    }

    #[test]
    fn splice_handles_live_region_alongside_fenced_marker_example() {
        // A fenced block documenting the marker syntax elsewhere in the
        // same file must not interfere with a genuine, unindented live
        // region.
        let content = doc("Example syntax:\n\n\
             ```\n\
             <!-- BEGIN GENERATED CONFIG REFERENCE prefix=... -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n\
             ```\n\n\
             <!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n");

        let (out, keys) = splice(&content, &settings()).expect("splice succeeds");

        assert_eq!(keys, vec!["ballista.testing.one".to_string()]);
        assert!(out.contains("| ballista.testing.one |"), "got:\n{out}");
        assert!(
            out.contains("<!-- BEGIN GENERATED CONFIG REFERENCE prefix=... -->"),
            "fenced example must survive untouched:\n{out}"
        );
    }

    #[test]
    fn splice_fenced_end_does_not_close_a_live_region_early() {
        // The region-body-consuming inner loop must also track fence state:
        // an END shown inside a fenced example within a live region must
        // not close that region early.
        let content = doc(
            "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->\n\
             Example of the closing marker:\n\n\
             ```\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n\
             ```\n\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let (out, keys) = splice(&content, &settings()).expect("splice succeeds");

        assert_eq!(keys, vec!["ballista.testing.one".to_string()]);
        assert!(out.contains("| ballista.testing.one |"), "got:\n{out}");
        // The old body, including the fenced example, was discarded along
        // with the rest of the stale region.
        assert!(
            !out.contains("Example of the closing marker"),
            "got:\n{out}"
        );
    }

    #[test]
    fn splice_without_prefix_selects_every_key() {
        let content = doc("<!-- BEGIN GENERATED CONFIG REFERENCE -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n");

        let (_, keys) = splice(&content, &settings()).expect("splice succeeds");

        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn splice_rejects_unterminated_region() {
        let content = doc("<!-- BEGIN GENERATED CONFIG REFERENCE -->\n");

        let err = splice(&content, &settings()).expect_err("must fail");

        assert!(err.contains("unterminated"), "got: {err}");
    }

    #[test]
    fn splice_rejects_nested_begin() {
        let content = doc("<!-- BEGIN GENERATED CONFIG REFERENCE -->\n\
             <!-- BEGIN GENERATED CONFIG REFERENCE -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n");

        let err = splice(&content, &settings()).expect_err("must fail");

        assert!(err.contains("nested"), "got: {err}");
    }

    #[test]
    fn splice_rejects_orphan_end() {
        let content = doc("<!-- END GENERATED CONFIG REFERENCE -->\n");

        let err = splice(&content, &settings()).expect_err("must fail");

        assert!(err.contains("no matching BEGIN"), "got: {err}");
    }

    #[test]
    fn splice_rejects_prefix_matching_nothing() {
        // Catches typos in a prefix attribute.
        let content = doc(
            "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.typo. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let err = splice(&content, &settings()).expect_err("must fail");

        assert!(err.contains("matched no configuration keys"), "got: {err}");
    }

    #[test]
    fn splice_rejects_unknown_attribute() {
        let content = doc(
            "<!-- BEGIN GENERATED CONFIG REFERENCE suffix=ballista. -->\n\
             <!-- END GENERATED CONFIG REFERENCE -->\n",
        );

        let err = splice(&content, &settings()).expect_err("must fail");

        assert!(err.contains("unrecognised marker attribute"), "got: {err}");
    }

    #[test]
    fn parse_prefixes_reads_a_comma_separated_list() {
        let prefixes = parse_prefixes(
            "<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.a.,ballista.b. -->",
        )
        .expect("parses");

        assert_eq!(prefixes, vec!["ballista.a.", "ballista.b."]);
    }

    #[test]
    fn orphan_keys_reports_undocumented_settings() {
        let settings = settings();
        let mut covered = std::collections::BTreeSet::new();
        covered.insert("ballista.shuffle.one".to_string());
        covered.insert("ballista.shuffle.two".to_string());

        assert_eq!(
            orphan_keys(&settings, &covered),
            vec!["ballista.testing.one"]
        );
    }

    #[test]
    fn orphan_keys_is_empty_when_all_covered() {
        let settings = settings();
        let covered: std::collections::BTreeSet<String> =
            settings.iter().map(|s| s.key.clone()).collect();

        assert!(orphan_keys(&settings, &covered).is_empty());
    }
}
