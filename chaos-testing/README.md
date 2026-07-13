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

# ballista-chaos

A fault-injection harness that runs real, multi-process Ballista clusters and
injects faults into real queries, to exercise Ballista's high-availability
(HA) machinery end to end.

**This is a bug-hunting harness, not a regression suite in the usual sense.**
Its job is to surface real defects in Ballista's HA behavior. Where it finds
one, the corresponding test is *expected to fail* until the underlying bug is
fixed in a production crate — the failure is the deliverable, not something to
be papered over. See [Findings](#findings) below for the three confirmed bugs
this harness has found so far, each with a test that documents it by failing.

## Why this crate exists

Ballista's HA state machine — stage/task retry, executor-loss recovery,
map-stage resubmission — lives in
`ballista/scheduler/src/state/execution_graph.rs`. Before this crate, that code
was exercised only by unit tests that hand-construct `TaskStatus` protobufs and
feed them directly into `ExecutionGraph` methods. Those tests are useful for
pinning the state machine's transition logic, but nothing drove it end to end:
no test ran a real query against a real multi-process cluster, killed a real
executor process, and checked that the *result* was still correct. That gap is
exactly where the bugs in [Findings](#findings) were hiding — they only show up
when a real executor process dies mid-task, a real gRPC connection is refused,
or a real DataFusion error is really propagated through the real serialization
path, none of which a hand-built `TaskStatus` reproduces.

`ballista-chaos` closes that gap: it spawns a real `ballista-scheduler` and
one or more real `ballista-executor` processes, runs a real multi-stage query
against them through the `ballista` client, and injects faults or kills
processes while the query is in flight.

## Why fault injection uses UDFs, not `ChaosExec`

Ballista's AQE planner already has a fault-injection mechanism:
`ChaosCreatingRule` (`ballista/scheduler/src/state/aqe/planner.rs:542`), which
wraps a plan node in `ChaosExec` when `chaos_execution_enabled` is set. It was
deliberately not reused here, because it cannot do what this harness needs:

- It is wired into the **AQE physical-optimizer pipeline only**
  (`plan_preparation_optimizers` in `planner.rs`). It does not run at all when
  AQE is off, and every scenario in this crate must run under *both* AQE
  settings — the two planners have materially different join and retry
  behavior, and a bug that only reproduces on one side is easy to miss if you
  only test the other.
- It picks a **uniformly random plan node** to wrap
  (`ballista/scheduler/src/state/aqe/optimizer_rule/chaos_exec.rs`), not a node
  the test chooses. A scenario that wants to fault "the scan of `facts`" or
  "the shared join build side" specifically has no way to target it.
- It fires **probabilistically** (`chaos_execution_probability`), not
  deterministically. A test built on it would need to loop-and-retry until the
  fault happened to fire the right number of times, which is exactly the kind
  of flakiness this harness is trying to avoid introducing.

A SQL-level UDF (`chaos_fail`, `chaos_delay`, in `src/udf.rs`) sidesteps all
three problems: it lives in the query text itself, so it plans identically
(modulo AQE's own re-planning) whether AQE is on or off; its `guard` argument
lets a scenario target specific rows (and therefore specific partitions/tasks)
by writing an ordinary predicate; and it fires on every row where the guard is
true, subject only to the fault budget below — no probability, no retries of
the test itself.

## How determinism works

Every chaos scenario needs two things to be true: which rows/tasks fault must
be controlled, and how many attempts fault (across the whole cluster, across
retries and executor restarts) must be bounded. Two mechanisms provide these:

- **The `guard` predicate.** The fixture (`src/fixture.rs`) is a small, fully
  deterministic dataset: `facts(key, value)` joined to `dims(key, name)`, with
  a known key distribution. A scenario passes a boolean expression over that
  data as `chaos_fail`'s/`chaos_delay`'s first argument (e.g. `f.key = 7`);
  since the data is fixed, this expression deterministically selects which
  partitions the fault can fire in.
- **The filesystem fault budget** (`src/budget.rs`). A budget is a directory
  of token files, created with a fixed token count. Consuming a token is
  `fs::remove_file`, which is atomic across processes, so a budget of `n`
  bounds the fault to firing at most `n` times *cluster-wide* — across every
  executor process, every task attempt, and every retry or restart — not `n`
  times per process or per attempt. This is what makes "exactly one retryable
  fault, then it must succeed" (Scenario A) and "faults never stop, so retries
  must exhaust" (Scenario B) both expressible and deterministic.

## The `OR TRUE` trap

`Fixture::chaos_query` splices the injection expression into the query as
`WHERE {injection} IS NOT NULL`, not the more obvious `WHERE {injection} OR
TRUE`. This is deliberate and load-bearing: DataFusion's optimizer
constant-folds `expr OR TRUE` to the literal `TRUE` during logical
optimization, and once the predicate is a literal, the plan no longer
references the UDF call at all — it is dropped, not merely skipped. Every
fault-injection scenario built on `OR TRUE` would silently become a no-op: the
budget would never be consumed, the fault would never fire, and the suite
would report green while testing nothing.

`chaos_fail`/`chaos_delay` always return `Some(guard)` (never `NULL`), so
`... IS NOT NULL` is always true but is not foldable to a constant without
evaluating the call — the optimizer has no way to know the result is always
non-null without invoking the (volatile) UDF. Two regression tests in
`src/fixture.rs` pin this:

- `or_true_predicate_is_optimized_away_and_never_fires` proves the bad form
  is eliminated from the plan and never consumes a budget token — pinning the
  trap so it cannot silently return if someone "simplifies" the predicate back
  to `OR TRUE`.
- `chaos_query_predicate_survives_optimization_and_fires` proves the
  `IS NOT NULL` form the harness actually uses survives into the physical plan
  and does fire.

## How to run

Native binaries must be built before the integration tests, because each test
spawns them as real child processes rather than running in-process:

```sh
cargo build -p ballista-chaos --bins
cargo test -p ballista-chaos -- --test-threads=1
```

`--test-threads=1` is **mandatory**, not a style preference. Every test in
`tests/ha.rs` spawns a whole scheduler-plus-executors cluster on its own ports;
running scenarios concurrently exhausts ports and CPU and produces spurious,
unrelated failures.

Unit tests only (fast, no process spawning, safe to parallelize):

```sh
cargo test -p ballista-chaos --lib
```

Each cluster's child-process logs (`scheduler.log`, `executor-0.log`, ...) are
written under that cluster's own temp directory, in a `logs/` subdirectory
(`TestCluster::log_dir()`). When a scenario fails, those logs are the first
place to look for what the scheduler and executors were actually doing.

## Scenarios

Every scenario runs under both `ballista.planner.adaptive.enabled=false` (AQE
off, the default, static `DefaultDistributedPlanner`) and `=true` (AQE on, the
experimental dynamic-join-selection planner) — 14 test cases total across the
7 scenarios below, plus a non-lettered `baseline_matches_local_datafusion`
sanity check that every other scenario's assertions depend on.

| Scenario | Test | What it does | Expected result |
|---|---|---|---|
| A | `retryable_fault_is_retried_and_result_is_correct` | Injects one retryable IO fault (budget 1); the retry must succeed and match baseline. | **aqe_off: pass.** **aqe_on: FAIL — Finding 2.** |
| B | `exhausted_retries_fail_the_job_and_leave_the_cluster_healthy` | Injects an inexhaustible IO fault (budget 99 ≫ `task_max_failures`); job must fail, cluster must stay usable after. | Pass (both). |
| C | `panicking_task_fails_the_job_but_the_executor_survives` | Injects a task panic; job must fail non-retryably, both executor processes must survive, cluster must stay usable after. | Pass (both). |
| D | `executor_killed_mid_stage_is_recovered` | SIGKILLs an executor while its tasks are genuinely running (held open by `chaos_delay`); scheduler must reschedule onto the survivor and return the correct result. | **FAIL (both) — Finding 1.** |
| E | `executor_killed_after_shuffle_write_is_recovered` | SIGKILLs the map-side executor *after* it wrote shuffle output, with a long executor timeout to bias toward the fetch-failure path rather than heartbeat expiry; downstream stage must re-run the map stage. | Pass (both) — see the note below. |
| F | `restarted_executor_rejoins_and_serves_queries` | Kills an executor, waits for the scheduler to reap it, restarts it, asserts the registered count returns to 2 and the cluster still serves the baseline query. | Pass (both), after the race fix in this crate (see below). |
| G | `killing_every_executor_terminates_the_job` | SIGKILLs every executor mid-query; the only requirement is that the job *terminates* within 120s rather than hanging. | **FAIL (both) — Finding 3.** |

A scenario marked FAIL above is not a defect in this harness — see
[Findings](#findings).

### A note on Scenario F: the harness race that was fixed here

`restarted_executor_rejoins_and_serves_queries` used to kill executor 0 and
restart it immediately, then assert `registered_executors() == 2`. That is a
harness bug, not a Ballista bug: SIGKILL does not deregister the executor, so
the scheduler keeps listing it until its heartbeat times out
(`executor_timeout_seconds`, 5s in this harness's defaults). Restarting
immediately races the scheduler's own reap: the assertion could observe three
executors (the dead one, the untouched survivor, and the freshly restarted
one) depending on timing, and failed intermittently with `left: 3, right: 2`.
Ballista was behaving correctly; the test just hadn't waited for the state it
was asserting about. The fix adds `TestCluster::await_executor_count(n)` (an
exact-count analogue of the existing `await_executors(n)`, which only waits for
*at least* `n` — the right primitive for growing a cluster, but not for
observing a shrink) and has the scenario wait for the count to drop to 1
before restarting, so the final assertion tests what the scenario name
actually promises.

## Findings

Three scenarios above are expected to fail because they have found real bugs
in Ballista, not because the harness is broken. Each is left unignored and
unweakened deliberately: a red test here is the harness working as intended.

### Finding 1 — Shuffle-fetch failures lose their type, so the map-stage resubmit never fires

**Proven by:** Scenario D (`executor_killed_mid_stage_is_recovered`), both AQE
settings.

The shuffle reader (`ballista/core/src/execution_plans/shuffle_reader.rs`)
correctly produces a typed `BallistaError::FetchFailed(executor_id,
map_stage_id, map_partition_id, desc)` when it cannot reach a dead executor,
and `ballista/core/src/error.rs`'s `impl From<BallistaError> for FailedTask`
has a dedicated arm for exactly that variant (around line 205) which produces
`FailedReason::FetchPartitionError` — the signal
`ballista/scheduler/src/state/execution_graph.rs` (around line 826) uses to
resubmit the lost map stage rather than simply failing the job.

The type does not survive to that point, however. Two real, non-test code
paths erase it before the executor reports its `TaskStatus`:

- `ballista/executor/src/executor.rs:238-239`, in
  `Executor::execute_query_stage`, converts the stage's result with
  `result.map_err(|e| BallistaError::DataFusionError(Box::new(e)))` instead of
  `BallistaError::from(e)` / `e.into()`. That bypasses the very unwrapping
  logic `error.rs`'s `impl From<DataFusionError> for BallistaError` exists to
  provide (`DataFusionError::ArrowError(e, _) => Self::from(*e)`, which would
  otherwise recover a `FetchFailed` wrapped inside an `ArrowError::ExternalError`).
- `ballista/core/src/execution_plans/shuffle_writer.rs:245`, in
  `ShuffleWriterExec`'s unpartitioned write branch, Debug-formats a
  `BallistaError` into an opaque `DataFusionError::Execution(format!("{e:?}"))`
  — a conversion that can never be undone by any later `.into()`, because the
  original variant no longer exists, only its printed form.

Either path leaves the executor reporting something like
`BallistaError::DataFusionError(Execution("FetchFailed(\"<executor-id>\", ...,
\"...Connection refused...\")"))` — the `FetchFailed` information is present
only as inert text inside a string. `error.rs`'s `FetchFailed` arm cannot match
a `DataFusionError::Execution`, so the task falls to the catch-all arm
(around line 248) and is marked `retryable: false`, `FailedReason::ExecutionError`.

**Net effect:** when an executor dies after producing shuffle output that a
downstream stage still needs, Ballista fails the whole query instead of
re-running the map stage that produced it.

### Finding 2 — Retryable IO errors are misclassified when wrapped

**Proven by:** Scenario A, `aqe_on` case only.

`ballista/core/src/error.rs:237-238` classifies retryability with a shallow
match:

```rust
BallistaError::DataFusionError(e)
    if matches!(*e, DataFusionError::IoError(_)) =>
```

This recognizes only a *direct* `DataFusionError::IoError`. It does not see
through `DataFusionError::Shared(Arc<DataFusionError>)` (or any other wrapping
variant). Under AQE, this harness's join plans through a broadcast build side
that DataFusion collects once and shares across output partitions (the
`OnceFut`/`OnceAsync` pattern); when that shared collection fails, DataFusion
re-wraps the underlying error as `DataFusionError::Shared`. An `IoError`
raised there arrives as `Shared(IoError(..))`, misses the shallow match above,
falls to the catch-all arm, and is marked non-retryable.

**Net effect:** this misclassification is not specific to the injected fault —
it affects any genuine Parquet/object-store IO error that happens to occur on
a shared build side under AQE, turning what should be a retried task into an
immediate job failure.

### Finding 3 — Killing every executor hangs the job instead of failing it

**Proven by:** Scenario G (`killing_every_executor_terminates_the_job`), both
AQE settings.

With every executor dead mid-query, there is nothing left to schedule tasks
onto. The job does not terminate within the scenario's 120s timeout — the
scheduler waits rather than failing the query once it can determine no
executor can ever satisfy the remaining tasks. Note that this scenario
deliberately asserts only *termination*, not success or a particular error; it
is a hang detector, and what it detects is the hang itself.

### For comparison: Scenario E passes

`executor_killed_after_shuffle_write_is_recovered` currently **passes**, both
AQE settings, and is worth calling out precisely because it looks superficially
like Scenario D. The difference is the executor timeout: Scenario E raises it
to 60s specifically to bias the kill toward being detected as a heartbeat
expiry (`ExecutorLost`) rather than a downstream `FetchPartitionError` — a
different recovery path than the one Finding 1 breaks. That path does recover
correctly. This is not evidence against Finding 1; it shows that Ballista's HA
recovery is not uniformly broken, only broken specifically on the
fetch-failure path that Scenario D isolates.
