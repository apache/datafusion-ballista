---
name: pr-review
description: Use when reviewing a pull request in the datafusion-ballista repo, including a request to review PR #N, review the current branch, or give feedback on a contributor's change.
---

<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Ballista PR Review

A full review of a Ballista pull request. The core is a line-level correctness review of
the code. On top of that sit three Ballista-specific gates that a generic review misses:
the user-persona registry, the public API surface, and benchmark evidence for changes to
plan shape or execution.

The correctness review (Steps 3 to 5) is the main job. The gates (Steps 6 to 8) are
additions to it, never a replacement for it. A report that only covers personas, API and
benchmarks is an incomplete review.

Stay quiet where the PR is clean. Do not manufacture findings to look useful.

## Step 1: Gather the PR

Resolve the target. If the user gave a PR number, use it. Otherwise use the PR for the
current branch. If the user asked for a batch of PRs, drop drafts (`isDraft: true`) and say
which were skipped. A PR the user names explicitly is reviewed even if it is a draft.

```sh
gh pr view <N> --json title,body,labels,files,url,isDraft,baseRefName,headRefOid
gh pr diff <N>
gh pr checks <N>
```

Read the full diff, not just the file list. Note any failing CI check, since a red check
often points straight at a bug.

## Step 2: Understand the intent

Read the PR body and the linked issue. Write down, in one or two sentences, what the change
is supposed to do and what behavior must stay the same. Every later step judges the code
against that statement. If the description and the diff disagree, that is the first finding.

## Step 3: Read the code in context

Do not review the diff in isolation. For each changed function, read the whole function and
its callers, and for each changed type, read its other users. Check out the PR head in a
worktree (`gh pr checkout <N>` in a scratch worktree) when the diff alone is not enough.

Trace the changed path end to end:

- Follow data through the whole path, not just the hunk. For a scheduler change that means
  from job submission through stage creation to task launch and completion. For an executor
  change it means from task receipt through execution to shuffle write and status report.
- For anything that crosses a process boundary, find both ends. A new plan node or
  expression needs the codec (`ballista/core/src/serde`) and proto
  (`ballista/core/proto/`) updated on the encode and the decode side, plus a round trip.
- For each new branch or early return, ask what state it leaves behind.

## Step 4: Review for correctness

Work through the diff hunk by hunk against the checklist. Every finding needs a file and
line, a concrete input or state that triggers it, and the wrong result that follows. If you
cannot construct that scenario, it is a question for the author, not a finding.

**Logic**

- Off-by-one and boundary cases: zero rows, zero partitions, one partition, empty stage,
  the last partition, partition counts that do not divide evenly.
- Wrong operator, inverted condition, swapped arguments of the same type, a variable
  shadowed or reused across loop iterations.
- Integer casts and arithmetic that can overflow or truncate (`as` casts, `usize` to `u32`,
  sums of byte or row counts).
- Behavior when an input is `None`, an `Err`, or an unknown enum variant.

**Distributed execution**

- Plan rewriting that changes partition count, operator count, or output partitioning. Check
  that the rewritten plan still satisfies each operator's required distribution and ordering.
- Anything per task. Task restriction and stage splitting must not make a leaf scan repeat
  or drop rows across tasks.
- Re-run and failure paths: a stage re-run after executor loss, stale shuffle locations,
  retried tasks writing the same output twice, state that is updated on success but not
  rolled back on failure.
- Behavior with multiple consumers of one stage, and with a stage that produces zero rows.
- Shuffle files, ids and paths: collisions between jobs, stages, attempts or executors, and
  cleanup that can delete files another consumer still needs.

**Async and concurrency**

- A lock or `RefCell` borrow held across an `.await`, and lock ordering between the
  scheduler state maps.
- Blocking or CPU-heavy work on an async runtime thread instead of `cpu_bound_executor`.
- Cancellation: a dropped future leaving a task slot, a reservation or a file handle behind.
- Spawned tasks whose `JoinHandle` and error are discarded.
- Shutdown and drain ordering.

**Errors and robustness**

- `unwrap`, `expect`, indexing or slicing that can panic on a scheduler or executor path.
  A panic there takes down the whole process.
- Errors swallowed with `let _ =`, `.ok()` or `unwrap_or_default()` where the failure should
  surface, including a config parse failure that silently resets to a default.
- Error messages that lose the underlying cause or the job, stage and task ids.

**Resources**

- Memory held longer than before, buffers or writers that now stay open per partition, and
  allocations the executor memory pool does not track. A PR that raises memory overhead
  gets `REQUEST_CHANGES`, even when it is a refactor and even when the regression only shows
  up under a non-default config. Put numbers in the comment where you can measure them.
- Unbounded channels, queues or caches, and caches keyed on too little (for example missing
  a credential or option that changes the result).
- Open file, socket or stream handles on error paths.

**Configuration and compatibility**

- A new setting needs a default, docs, and the config docs check (`ci/scripts/rust_config_docs_check.sh`).
- Defaults that override DataFusion's stock config. Clients that send DataFusion's own
  config can bypass guards the scheduler does not enforce.
- Wire compatibility between scheduler, executor and client versions during a rolling
  upgrade, and behavior of the Python bindings.

## Step 5: Review the tests

- Does a test fail without the change? A test that passes on `main` proves nothing.
- Does it cover the boundary cases from Step 4, the failure path, and the multi-partition
  case, or only the happy path on one partition?
- Does it assert results, not just that the query ran? Prefer checking rows or the plan.
- Are there sleeps, fixed ports, or ordering assumptions that will make it flaky in CI?

Suggest missing tests rather than stating that they are missing.

Lint locally if the PR is checked out (`cargo fmt --check`, `cargo clippy` on the touched
crates). Leave the full workspace build and test run to CI, and read the CI results from
Step 1 instead.

## Step 6: Read the persona contract

Read `docs/source/contributors-guide/user-personas.md` **every time**. Do not work from
memory of it and do not copy its contents into this skill. That page is the source of
truth, it is append-only, and it may have grown a persona since this skill was written.

For each persona it defines, note the "Depends on" list and the "Red flags in a pull
request" list.

Walk each persona's red-flag list against the diff. A red flag is not a veto. It means the
change needs explicit discussion, maintainer sign-off, and usually a migration path,
rather than landing silently.

Report each hit as:

- which persona is affected
- the exact red flag it trips, quoted from the doc
- the file and line in the diff that trips it
- what would let it land anyway (discussion, sign-off, an entry in `docs/source/upgrading/`)

Watch in particular for changes that serve one persona at the cost of another. The
personas are deliberately in tension: one wants distribution to be an invisible
implementation detail, one wants the execution model exposed and tunable, one wants the
internals to stay a stable composable library. A change that delights one can quietly
break another, and that is the single most valuable thing this check catches.

## Step 7: Check for avoidable breaking API changes

Ballista's library users are a documented persona. They depend on stable, composable
public APIs. Scan the diff for changes to the public surface of the four crates that
enable `#![warn(missing_docs)]` (`ballista-core`, `ballista-scheduler`,
`ballista-executor`, `ballista`):

- a removed `pub fn`, or one whose signature changed
- a trait method added, removed, or re-signed (any change breaks external implementors)
- a `pub` struct field removed or retyped
- a variant added to or removed from a public enum
- a proto field renumbered, removed, or type-changed in `ballista/core/proto/`
- an extension point narrowed: a codec argument dropped, a `RuntimeProducer` or
  `ConfigProducer` override removed, session-config injection bypassed

For every break you find, the important question is not "is this a break" but **could this
have been additive?** Say so, and name the alternative when one exists:

- add a new method and deprecate the old one, rather than changing a signature in place
- put the new parameter in config or a builder, rather than in the signature
- mark the enum `#[non_exhaustive]`, rather than adding a variant that breaks downstream
  `match` arms
- use a new proto field number, rather than reusing or renumbering an existing one
- keep the old trait method with a default implementation that delegates to the new one

Some breaks are genuinely unavoidable, and that is fine. Call out the ones that are not.

Then confirm the paperwork: a breaking change needs the `api-change` label (with a hyphen,
check `gh label list` if unsure) and an entry in the upgrade guide for the unreleased
version (`docs/source/upgrading/<workspace version>.md`). Flag either if missing.

## Step 8: Ask for TPC-H numbers when plan shape or execution changed

The change touches plan shape or execution if the diff includes any of:

- `ballista/core/src/planner.rs` or `ballista/scheduler/src/planner.rs`
- `ballista/core/src/execution_plans/` (shuffle reader/writer, unresolved shuffle,
  distributed query exec)
- `ballista/scheduler/src/physical_optimizer/` (for example join selection)
- `ballista/scheduler/src/state/execution_graph.rs` (stage DAG construction)
- the executor task-execution loop (`execution_loop.rs`, `executor_server.rs`) or
  `cpu_bound_executor.rs`
- the Arrow Flight shuffle service or the shuffle read/write path
- default configuration affecting `target_partitions`, task-slot concurrency, or the AQE
  and adaptive-planner path

If it triggers **and the PR body already contains benchmark numbers**, say so and move on.
Do not ask twice.

If it triggers and there are no numbers, draft a request for the author. Name the exact
command, ask for before/after against `main` at a stated scale factor, and ask for an
`EXPLAIN` diff when the plan shape itself changed (a shape change can be a win on wall
clock and still be wrong).

The command lives in `benchmarks/`:

```sh
cargo run --release --bin tpch -- benchmark ballista \
  --host localhost --port 50050 \
  --iterations 3 --path <tpch parquet dir> --format parquet --query <n>
```

## Step 9: Report

Write the report in chat. Order findings by severity and group them as:

1. **Blocking**: wrong results, panics, data loss, deadlocks, memory regressions, an
   unmitigated persona break.
2. **Should fix**: error handling gaps, missing tests for a risky path, avoidable API
   breaks, missing label or upgrade note.
3. **Nits and questions**: naming, comments, small simplifications, things you could not
   confirm.

Give each finding a file and line, the scenario that triggers it, and a suggested fix.
End with a verdict (approve, comment, or request changes) and one line stating what was
checked and came back clean. Say plainly if a step could not be done, for example CI had
not finished or the PR could not be checked out.

**Do not post anything to GitHub.** No `gh pr comment`, no `gh pr review`, no inline
comments, unless the user explicitly asks you to in this conversation. Present the drafted
text and let them decide.

When the user does ask you to post, make one inline comment per issue, attached to the
line it is about, using the review API with a `comments` array
(`gh api -X POST repos/{owner}/{repo}/pulls/{N}/reviews`). Do not bundle findings into one
large comment.

Any text drafted for the author (a finding, the benchmark request, a comment on a red flag)
is paste-ready and written in the maintainer's voice:

- friendly and conversational, in short separate sentences
- no em dashes and no semicolons
- acknowledge what the PR does well before raising concerns
- frame concerns as questions or suggestions where possible
- use backticks around function names, paths, types and config keys
- no AI attribution footer, and no mention of AI unless the user asks for a "first pass AI
  review" framing, in which case say so in the review summary only

If the PR is clean, the report reads like:

> Read the change in context and traced the new path through the scheduler and executor, no
> correctness issues found. Tests cover the new path. Checked all personas against the
> diff, no red flags. No public API breaks. No changes to plan shape or execution, so no
> benchmark numbers needed. Approve.
