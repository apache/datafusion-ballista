# AQE Executor Failure Support — Design

**Status:** Draft
**Date:** 2026-04-27
**Tracking issue:** [#1359 — Adaptive Query Execution (AQE)](https://github.com/apache/datafusion-ballista/issues/1359), unchecked task "support executor failure"

## Problem

`AdaptiveExecutionGraph` already has `reset_stages_on_lost_executor` and `reset_stages_internal` (a near-line-for-line copy of `StaticExecutionGraph`). They correctly walk stage `inputs`, remove `PartitionLocation`s matching the lost executor, and roll back affected stages: Resolved→UnResolved, Running→UnResolved, Successful→Running.

The gap is in the **`AdaptivePlanner` side-state**, which is never told about executor loss:

1. `runnable_stage_cache: HashMap<usize, Arc<dyn ExecutionPlan>>` and `runnable_stage_output: HashMap<usize, StageOutput>` are removed inside `finalise_stage`. When a Successful stage is rolled back to Running and tasks complete on the rerun, `update_exchange_locations` returns `exec_err!("Can't find active stage to update stage outputs")`.
2. `ExchangeExec.shuffle_partitions: Arc<Mutex<Option<Vec<Vec<PartitionLocation>>>>>` (and the same field on `AdaptiveDatafusionExec`) in the `AdaptivePlanner.plan` tree still hold the original locations. There is no API to clear them. So `find_runnable_exchanges` keeps treating the stage as resolved (`shuffle_created() == true`) and skips it — the rerun stage will never be returned by `actionable_stages`.

The doc comment at `ballista/scheduler/src/state/aqe/mod.rs:67` says "it does not cover executor failure" — accurate, even though the surface code looks like it should.

## Goal

Achieve **parity with `StaticExecutionGraph`**'s executor-failure semantics for AQE jobs: when an executor is lost, in-flight stages whose data lived on it are correctly rolled back and re-run, late task status from the dead executor does not corrupt graph state, and the job completes successfully.

Out of scope: AQE-specific re-optimization triggered *by* failure (e.g., switching join strategy after losing data), partial per-partition re-execution beyond what static graph already does, dynamic shuffle coalescing.

## Architecture

```
AdaptiveExecutionGraph::reset_stages_on_lost_executor(executor_id)
  └── reset_stages_internal(executor_id)
       ├── (unchanged) walk stages, remove lost locations from stage.inputs
       ├── (unchanged) rollback Resolved/Running → UnResolved
       ├── (unchanged) Successful stages with lost outputs → Running
       └── NEW: planner.reset_on_lost_executor(executor_id)
                 ├── walk self.plan, find ExchangeExec / AdaptiveDatafusionExec
                 ├── for each affected exec node:
                 │     ├── exec.reset_locations_on_lost_executor(executor_id)
                 │     │     - clears shuffle_partitions to None
                 │     │     - returns Some(stage_id)
                 │     ├── re-insert into runnable_stage_cache (clone of plan node)
                 │     └── re-insert empty StageOutput into runnable_stage_output
                 └── replan_stages()
```

The graph-level rollback is responsible for *stage state* (UnResolved / Running / Successful). The planner reset is responsible for *plan-tree state* (which `ExchangeExec` nodes still count as "resolved" in `find_runnable_exchanges`) and *cache state* (which stages can accept `update_exchange_locations` and `finalise_stage`).

## Components

### 1. `ExchangeExec::reset_locations_on_lost_executor`

File: `ballista/scheduler/src/state/aqe/execution_plan.rs`

```rust
impl ExchangeExec {
    /// If this exec's resolved shuffle_partitions reference the given executor,
    /// clear shuffle_partitions back to None and return its stage_id so the
    /// caller can restore planner cache entries. Returns None if untouched.
    pub fn reset_locations_on_lost_executor(&self, executor_id: &str) -> Option<usize> {
        let mut guard = self.shuffle_partitions.lock();
        let affected = match guard.as_ref() {
            Some(parts) => parts
                .iter()
                .any(|locs| locs.iter().any(|loc| loc.executor_meta.id == executor_id)),
            None => false,
        };
        if affected {
            *guard = None;
            self.stage_id()
        } else {
            None
        }
    }
}
```

### 2. `AdaptiveDatafusionExec::reset_locations_on_lost_executor`

File: `ballista/scheduler/src/state/aqe/execution_plan.rs`

Same pattern as `ExchangeExec`. The final-stage wrapper can also have resolved shuffle metadata when it's the root (in plans where the final stage is itself a shuffle output).

### 3. `AdaptivePlanner::reset_on_lost_executor`

File: `ballista/scheduler/src/state/aqe/planner.rs`

```rust
impl AdaptivePlanner {
    pub fn reset_on_lost_executor(&mut self, executor_id: &str) -> common::Result<()> {
        let mut affected: Vec<(usize, Arc<dyn ExecutionPlan>)> = Vec::new();
        Self::collect_affected_stages(&self.plan, executor_id, &mut affected);

        if affected.is_empty() {
            return Ok(());
        }

        for (stage_id, plan_node) in affected {
            self.runnable_stage_cache.insert(stage_id, plan_node);
            self.runnable_stage_output
                .insert(stage_id, Default::default());
        }

        // Re-run optimizers since some shuffle_partitions just became None.
        self.replan_stages()?;
        Ok(())
    }

    fn collect_affected_stages(
        node: &Arc<dyn ExecutionPlan>,
        executor_id: &str,
        out: &mut Vec<(usize, Arc<dyn ExecutionPlan>)>,
    ) {
        if let Some(ex) = node.as_any().downcast_ref::<ExchangeExec>() {
            if let Some(stage_id) = ex.reset_locations_on_lost_executor(executor_id) {
                out.push((stage_id, node.clone()));
            }
        } else if let Some(ad) = node.as_any().downcast_ref::<AdaptiveDatafusionExec>() {
            if let Some(stage_id) = ad.reset_locations_on_lost_executor(executor_id) {
                out.push((stage_id, node.clone()));
            }
        }
        for child in node.children() {
            Self::collect_affected_stages(child, executor_id, out);
        }
    }
}
```

### 4. Wire-in at `AdaptiveExecutionGraph::reset_stages_internal`

File: `ballista/scheduler/src/state/aqe/mod.rs`

After the existing rollback logic (just before the final `Ok((reset_stage, all_running_tasks))` return at the end of `reset_stages_internal`), add:

```rust
// Synchronize planner state with the graph-level rollback we just did.
// Without this, re-running stages can't accept update_exchange_locations
// (cache entries were cleared in finalise_stage) and the plan tree still
// thinks affected exchanges are resolved.
self.planner
    .reset_on_lost_executor(executor_id)
    .map_err(|e| BallistaError::Internal(format!(
        "Failed to reset AdaptivePlanner state on lost executor {executor_id}: {e}"
    )))?;
```

### 5. Doc cleanup

Remove line 67 of `ballista/scheduler/src/state/aqe/mod.rs`:
```rust
/// - it does not cover executor failure
```

## Data Flow on Executor Loss

1. Heartbeat timeout fires `QueryStageSchedulerEvent::ExecutorLost(executor_id, ...)`.
2. `state::mod.rs` removes the executor from cluster, then calls `task_manager.executor_lost(executor_id)`.
3. For each active job, `task_manager` invokes `graph.reset_stages_on_lost_executor(executor_id)`.
4. `AdaptiveExecutionGraph::reset_stages_internal`:
   a. Iterates `self.stages`, removing locations matching `executor_id` from each stage's `inputs.partition_locations`.
   b. Calls `stage.reset_tasks(executor_id)` on Running stages (clears running task slots tied to the lost executor).
   c. Identifies which Resolved/Running stages now have incomplete inputs → marks them for rollback to UnResolved.
   d. Identifies Successful stages whose outputs included the lost executor → marks them for re-execution.
   e. Performs the rollbacks: `rollback_resolved_stage`, `rollback_running_stage`, `rerun_successful_stage`.
   f. **NEW**: Calls `self.planner.reset_on_lost_executor(executor_id)`.
5. `AdaptivePlanner::reset_on_lost_executor`:
   a. Walks `self.plan`, calls `reset_locations_on_lost_executor` on each `ExchangeExec` / `AdaptiveDatafusionExec`.
   b. For affected execs, restores `runnable_stage_cache` and `runnable_stage_output` entries so subsequent updates work.
   c. Calls `replan_stages()` so optimizer rules re-run with the now-unresolved shuffle metadata.
6. Returns `(reset_stage_ids, running_tasks_to_cancel)` to `task_manager`, which forwards `running_tasks_to_cancel` to `executor_manager.cancel_running_tasks`.
7. The next `update_task_status` cycle from a surviving executor finds Running stages with available tasks and proceeds normally.

## Error Handling

- `reset_locations_on_lost_executor` is infallible (lock + scan + assign).
- `planner.reset_on_lost_executor` only fails if `replan_stages` fails (e.g., a max-passes ceiling on the optimizer). Failure is propagated as `BallistaError::Internal`. The graph is already in a partially-rolled-back state at that point, so the caller path (`task_manager.executor_lost`) treats it like any other graph mutation failure — the existing job-failure machinery applies.
- **Idempotency**: a second call to `reset_stages_on_lost_executor` for the same `executor_id` must be a no-op. Verified by:
  - Stage iteration finds no remaining locations matching `executor_id` (already removed) → `resubmit_inputs` is empty → no rollbacks.
  - `collect_affected_stages` walks the plan tree, finds no `ExchangeExec` with locations on the now-removed executor (we cleared `shuffle_partitions` to `None` last time, and any new resolutions used surviving executors) → `affected` is empty → no replan.
  - `runnable_stage_cache` re-insertions are HashMap upserts, harmless if repeated.
- **Late task statuses**: a `TaskStatus` arriving from the dead executor after reset is handled by the existing `update_task_status` paths (which already filter / classify by stage state). No new handling required — covered by `test_long_delayed_failed_task_after_executor_lost`.

## Testing

Port the four executor-failure tests from `ballista/scheduler/src/state/execution_graph.rs` into a new file `ballista/scheduler/src/state/aqe/test/executor_failure.rs`:

| Test | What it verifies |
|------|------------------|
| `test_reset_completed_stage_executor_lost` | Successful stage's outputs lost → reset to Running → re-runs → job completes |
| `test_reset_resolved_stage_executor_lost` | Resolved (not-yet-running) stage with lost inputs → rolled back to UnResolved → re-resolves on input rerun |
| `test_task_update_after_reset_stage` | Out-of-order task status after reset doesn't corrupt state; second reset call returns empty set |
| `test_long_delayed_failed_task_after_executor_lost` | Late failed task status from dead executor doesn't break the rerun |

The static-graph tests use helpers `test_join_plan`, `test_aggregation_plan`, `revive_graph_and_complete_next_stage`, `revive_graph_and_complete_next_stage_with_executor`, `mock_completed_task`, `mock_failed_task`, `drain_tasks`, `mock_executor`. The AQE test module already has primitive helpers (`mock_partitions_with_statistics`, `mock_context`, `mock_memory_table`) but lacks the higher-level orchestration helpers. Two options for these:

1. **Add AQE-flavored helpers** in `ballista/scheduler/src/state/aqe/test/mod.rs` that build an `AdaptiveExecutionGraph` from a SQL-like plan and complete stages similarly to the static helpers. Preferred — keeps tests readable.
2. **Reuse static helpers via a small generic harness over `dyn ExecutionGraph`**. Cleaner but a larger change.

We will add AQE-flavored helpers (option 1) since that matches the existing AQE test module style, and we will not refactor the static helpers as part of this work.

Register the new test module in `ballista/scheduler/src/state/aqe/test/mod.rs` (`mod executor_failure;`).

## Migration / Compatibility

- New code paths are only exercised when `ballista_adaptive_query_planner_enabled()` is true (still off by default).
- No changes to the `ExecutionGraph` trait surface or to `StaticExecutionGraph`.
- No protocol / serialization changes.
- The new methods on `ExchangeExec` / `AdaptiveDatafusionExec` are `pub(crate)` (only the planner needs them).

## Files Changed

- `ballista/scheduler/src/state/aqe/execution_plan.rs` — add `reset_locations_on_lost_executor` on both wrappers.
- `ballista/scheduler/src/state/aqe/planner.rs` — add `reset_on_lost_executor` and `collect_affected_stages`.
- `ballista/scheduler/src/state/aqe/mod.rs` — call `planner.reset_on_lost_executor` from `reset_stages_internal`; remove the "does not cover executor failure" doc line.
- `ballista/scheduler/src/state/aqe/test/mod.rs` — register the new test module; possibly add helpers.
- `ballista/scheduler/src/state/aqe/test/executor_failure.rs` — new file, four ported tests.

## Open Questions

None at design time. Implementation may surface details about exactly which AQE test helpers to add — those decisions are local to the test file.
