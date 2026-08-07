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
one, the corresponding test reproduces the bug rather than working around it.
Such a test is marked `#[ignore]` with the issue or follow-up path it
reproduces, so that it does not hold CI red on a bug it did not introduce, and
is un-ignored — not rewritten — when that issue is fixed, at which point it
becomes the regression test for the fix. Run them with
`cargo test -p ballista-chaos -- --ignored`.
See [Findings](#findings) below for the confirmed bugs this harness has
found so far, each with the test that reproduces it.

## Why this crate exists

Ballista's HA state machine — stage/task retry, executor-loss recovery,
map-stage resubmission — lives in
`ballista/scheduler/src/state/execution_graph.rs`. Before this crate, that code
was exercised only by unit tests that hand-construct `TaskStatus` protobufs and
feed them directly into `ExecutionGraph` methods. Those tests are useful for
pinning the state machine's transition logic, but nothing drove it end to end:
no test ran a real query against a real multi-process cluster, killed a real
executor process, and checked that the _result_ was still correct. That gap is
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
  AQE is off, and every scenario in this crate must run under _both_ AQE
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
  bounds the fault to firing at most `n` times _cluster-wide_ — across every
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

```sh
cargo test -p ballista-chaos              # active regression scenarios
cargo test -p ballista-chaos -- --ignored # any currently ignored known-bug scenarios
```

Every test that spawns a cluster does so through `TestCluster`, which holds
two locks for the cluster's lifetime: a process-wide mutex, and a
machine-wide `flock` (on a file in the system temp dir) for cluster-spawning
processes no in-process lock can see — a second cargo invocation in another
shell, or a runner like nextest that parallelizes test binaries. So scenarios
serialize themselves no matter how the test harness is invoked. This is not
cosmetic: each one starts a whole scheduler-plus-executors cluster, and
concurrent clusters exhaust ports and CPU and fail for reasons unrelated to
the scenario under test. `--test-threads=1` is therefore no longer required
(it will simply make the run marginally less confusing to read).

CI runners are slow enough that cluster startup alone has blown a 30-second
registration deadline ("timed out waiting for 2 executors to register"); the
deadline is now 120s, and on expiry the error message carries the tail of
every child process log, so a recurrence in CI is diagnosable from the test
output alone.

The `chaos-scheduler`/`chaos-executor` binaries are spawned as real child
processes rather than run in-process, but `cargo test` builds this crate's bin
targets along with its tests, so no separate build step is needed. The
harness locates them next to the running test executable, which is what makes
it work under any cargo profile (CI uses `--profile ci`, not `dev` or
`release`).

Unit tests only (fast; the ones that do not spawn a cluster):

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

| Scenario | Test                                                                | What it does                                                                                                                                                                                                 | Expected result                                                                                                                                |
| -------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| A        | `retryable_fault_is_retried_and_result_is_correct_{aqe_off,aqe_on}` | Injects one retryable IO fault (budget 1); the retry must succeed and match baseline.                                                                                                                        | Pass (both).                                                                                                                                   |
| B        | `exhausted_retries_fail_the_job_and_leave_the_cluster_healthy`      | Injects an inexhaustible IO fault (budget 99 ≫ `task_max_failures`); job must fail, cluster must stay usable after.                                                                                          | Pass (both).                                                                                                                                   |
| C        | `panicking_task_fails_the_job_but_the_executor_survives`            | Injects a task panic; job must fail non-retryably, both executor processes must survive, cluster must stay usable after.                                                                                     | Pass (both).                                                                                                                                   |
| D        | `executor_killed_mid_stage_is_recovered`                            | SIGKILLs an executor while its tasks are genuinely running (held open by `chaos_delay`); scheduler must reschedule onto the survivor and return the correct result.                                          | Pass (both), after stale task-attempt cancellation is classified as retryable cleanup.                                                         |
| E        | `executor_killed_after_shuffle_write_is_recovered`                  | SIGKILLs the map-side executor _after_ it wrote shuffle output, with a long executor timeout to bias toward the fetch-failure path rather than heartbeat expiry; downstream stage must re-run the map stage. | Pass (both).                                                                                                                                   |
| F        | `restarted_executor_rejoins_and_serves_queries`                     | Kills an executor, waits for the scheduler to reap it, restarts it, asserts the registered count returns to 2 and the cluster still serves the baseline query.                                               | Pass (both), after the race fix in this crate (see below).                                                                                     |
| G        | `killing_every_executor_terminates_the_job`                         | SIGKILLs every executor mid-query; asserts the job fails with an error naming the executor loss rather than hanging.                                                                                        | Regression test for [#2029](https://github.com/apache/datafusion-ballista/issues/2029) — Finding 3.                                 |

If a future scenario is ignored, it should stay tied to its tracking issue or
follow-up path, and its assertions should keep reproducing the underlying bug
rather than being weakened to pass.

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
_at least_ `n` — the right primitive for growing a cluster, but not for
observing a shrink) and has the scenario wait for the count to drop to 1
before restarting, so the final assertion tests what the scenario name
actually promises.

## Findings

The findings below are bugs this harness exposed. Fixed findings remain here
as context for the active regression scenarios; unfixed findings stay ignored
against their tracking issue or follow-up path so CI is not red on a known bug.

### Finding 1 — Shuffle-fetch failures lose their type, so the map-stage resubmit never fires

Tracked by [#2027](https://github.com/apache/datafusion-ballista/issues/2027).

**Regression coverage:** Scenario D
(`executor_killed_mid_stage_is_recovered`) and Scenario E
(`executor_killed_after_shuffle_write_is_recovered`), both AQE settings.

The shuffle reader produces a typed `BallistaError::FetchFailed(executor_id,
map_stage_id, map_partition_id, desc)` when it cannot reach a dead executor.
That type has to survive until task-failure classification, because the
scheduler's map-stage resubmit path is keyed on `FetchPartitionError`.

Before the fix, production code could turn that structured error into inert
text inside `DataFusionError::Execution`. Once that happened, the classifier
saw only a generic execution error and the scheduler never received the
`FetchPartitionError` signal.

The fix keeps the error structural across both places where it was being lost:
the executor now uses the existing `BallistaError` conversion when a stage
fails, and the shuffle-writer coordinator no longer Debug-formats the first
child error before handing it back through the output stream. The classifier
also looks through the DataFusion/Arrow wrapper stack so a wrapped
`FetchFailed` still becomes `FetchPartitionError`.

Scenario E is the direct fetch-failure regression: it kills the map-side
executor after shuffle output is written and uses a long executor timeout so a
downstream fetch is likely to hit the dead executor before heartbeat expiry.
Scenario D covers the adjacent executor-loss race while a stage is still
running. In that path, heartbeat expiry can win first, so executor-loss task
resets need to wake push scheduling with fresh offers.

### Scenario D note — Mid-stage executor loss can cancel stale task attempts

Scenario D also exposed an adjacent recovery edge. When executor-loss recovery
rolls back or resets stages, the scheduler can ask surviving executors to
cancel tasks from stale stage attempts. Those aborted task futures are reported
back as `BallistaError::Cancelled`; this is task attempt cleanup, not a
client-cancelled job.

The fix maps executor-reported `Cancelled` task attempts to retryable,
non-counting `TaskKilled` failures, so stale work cleanup does not fail a job
that executor-loss recovery is already rescheduling. Explicit job cancellation
is still handled by the scheduler by marking the job terminal before cancelling
running executor tasks.

### Finding 2 — Retryable IO errors are misclassified because the shuffle writer flattens them

Originally tracked by
[#2028](https://github.com/apache/datafusion-ballista/issues/2028) (now
fixed); the surviving flattening mechanism is the one
[#2027](https://github.com/apache/datafusion-ballista/issues/2027) tracks.

**Regression coverage:** Scenario A, both cases
(`retryable_fault_is_retried_and_result_is_correct_{aqe_off,aqe_on}`).

The history matters here because the failure mode moved underneath the
harness. As first found, only the `aqe_on` case failed: an `IoError` raised on
a join's shared broadcast build side arrived wrapped as
`DataFusionError::Shared(Arc<IoError>)`, and the classifier in
`ballista/core/src/error.rs` matched only a _direct_
`DataFusionError::IoError`. That was #2028, and it was fixed by classifying on
`find_root()` instead (#2119).

The sort-shuffle writer refactor (#2038, #2106) then re-broke both cases at an
earlier point in the pipeline: the shuffle write coordinator's error arm
(`ballista/core/src/execution_plans/shuffle_writer.rs`, in the coordinator
fan-out that distributes results to output-partition streams) converts any
task error with `DataFusionError::Execution(format!("{e:?}"))`. The injected
fault now reaches the classifier as
`Execution("IoError(Custom { .. })")` (`aqe_off`) or
`Execution("Shared(IoError(Custom { .. }))")` (`aqe_on`) — the variant exists
only as printed text, so `find_root()` has nothing to unwrap and the task is
marked non-retryable. Keeping the original `DataFusionError` through the
shuffle-writer handoff fixes this by giving the existing `find_root()` based
classifier the real IO error again.

### Finding 3 — Killing every executor hung the job instead of failing it (fixed)

Tracked by [#2029](https://github.com/apache/datafusion-ballista/issues/2029), fixed in that issue's PR.

**Regression test:** Scenario G (`killing_every_executor_terminates_the_job`),
both AQE settings, now enabled (no longer `#[ignore]`d).

With every executor dead mid-query, there is nothing left to schedule tasks
onto. Previously the job never terminated — the scheduler waited forever rather
than failing the query. The fix makes the scheduler wait a bounded grace period
(`no_executors_grace_period_seconds`) after losing its last executor and then
fail the job with a clear error. The scenario turns that grace down via the
cluster builder and asserts the query fails with an error naming the executor
loss.

**Second path, same hang** ([#2226](https://github.com/apache/datafusion-ballista/issues/2226)).
Scenario G kept failing intermittently after that fix, because the scheduler
has _two_ ways of discovering an executor is gone and only one of them armed
the grace timer. The heartbeat reaper posts `ExecutorLost`, which is where the
timer lives; a failing task launch instead took
`SchedulerState::remove_executor`, which rolled the graphs back inline and
posted nothing. That path also deletes the executor's heartbeat, so the reaper
could never rediscover it and post the event later — the job was left running
on an empty cluster with no further executor-loss event to come. Whether a kill
lands during a task launch or between heartbeats is a timing race, which is why
the scenario failed only sometimes. The fix routes both removal paths through
the same `ExecutorLost` event; the unit test
`test_running_job_fails_when_launch_failure_loses_last_executor` covers the
launch-failure path deterministically.

### For comparison: the heartbeat-expiry path also recovers

Killing an executor can be noticed in two ways: heartbeat expiry
(`ExecutorLost`) or a downstream shuffle fetch from the dead executor. Scenario
E biases toward the fetch-failure path. Scenario D exercises the broader
mid-stage executor-loss path, including stale task-attempt cancellation during
executor-loss recovery. Both paths now recover and return the baseline result.
