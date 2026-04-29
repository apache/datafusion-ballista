# AQE Executor Failure Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring AQE (`AdaptiveExecutionGraph`) to parity with `StaticExecutionGraph` for executor-loss recovery, so re-running stages and rolled-back stages work end-to-end.

**Architecture:** The graph-level rollback (`reset_stages_internal`) already works structurally. The gap is `AdaptivePlanner` side-state: `runnable_stage_cache` and `runnable_stage_output` are removed in `finalise_stage`, and `ExchangeExec` / `AdaptiveDatafusionExec` keep `shuffle_partitions = Some(...)` even after their owning stage rolls back. This plan adds (a) per-exec methods to clear `shuffle_partitions` when affected by a lost executor, (b) a planner-level method that walks the live plan tree, restores cache entries, and replans, and (c) tests ported from the static-graph suite.

**Tech Stack:** Rust, DataFusion 53, Ballista scheduler crate. Tests use `tokio::test` and reuse helpers in `ballista/scheduler/src/test_utils.rs`.

**Spec:** `docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `ballista/scheduler/src/state/aqe/execution_plan.rs` | Modify | Add `reset_locations_on_lost_executor` to `ExchangeExec` and `AdaptiveDatafusionExec` |
| `ballista/scheduler/src/state/aqe/planner.rs` | Modify | Add `reset_on_lost_executor` and `collect_affected_stages` |
| `ballista/scheduler/src/state/aqe/mod.rs` | Modify | Call `planner.reset_on_lost_executor` from `reset_stages_internal`; remove the "does not cover executor failure" doc line |
| `ballista/scheduler/src/state/aqe/test/mod.rs` | Modify | Register the new test module; add `test_aqe_aggregation_plan` and `test_aqe_join_plan` helpers |
| `ballista/scheduler/src/state/aqe/test/executor_failure.rs` | Create | Four ported executor-failure tests |

---

### Task 1: Add `ExchangeExec::reset_locations_on_lost_executor` (TDD)

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/execution_plan.rs` — add method on `ExchangeExec` impl block (after `input` method, ~line 192)
- Test: same file, in a new `#[cfg(test)] mod tests` block at the bottom

- [ ] **Step 1: Write failing tests for ExchangeExec::reset_locations_on_lost_executor**

Append to `ballista/scheduler/src/state/aqe/execution_plan.rs` (new block at the very end of the file):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionStats,
    };
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::arrow::datatypes::{Field, Schema};

    fn loc(executor_id: &str) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: 0,
            partition_id: PartitionId {
                job_id: "j".to_string(),
                stage_id: 0,
                partition_id: 0,
            },
            executor_meta: ExecutorMetadata {
                id: executor_id.to_string(),
                host: "h".to_string(),
                port: 0,
                grpc_port: 0,
                specification: ExecutorSpecification { task_slots: 0 },
            },
            path: "p".to_string(),
            partition_stats: PartitionStats::new(Some(1), None, Some(1)),
        }
    }

    fn empty_input() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            datafusion::arrow::datatypes::DataType::Int32,
            true,
        )]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn exchange_exec_reset_clears_when_affected() {
        let exec = ExchangeExec::new(empty_input(), None, 0);
        exec.set_stage_id(7);
        exec.resolve_shuffle_partitions(vec![vec![loc("ex-1"), loc("ex-2")]]);
        assert!(exec.shuffle_created());

        let result = exec.reset_locations_on_lost_executor("ex-1");

        assert_eq!(result, Some(7));
        assert!(!exec.shuffle_created());
    }

    #[test]
    fn exchange_exec_reset_no_op_when_unrelated_executor() {
        let exec = ExchangeExec::new(empty_input(), None, 0);
        exec.set_stage_id(7);
        exec.resolve_shuffle_partitions(vec![vec![loc("ex-1"), loc("ex-2")]]);

        let result = exec.reset_locations_on_lost_executor("ex-99");

        assert_eq!(result, None);
        assert!(exec.shuffle_created());
    }

    #[test]
    fn exchange_exec_reset_no_op_when_unresolved() {
        let exec = ExchangeExec::new(empty_input(), None, 0);
        exec.set_stage_id(7);

        let result = exec.reset_locations_on_lost_executor("ex-1");

        assert_eq!(result, None);
        assert!(!exec.shuffle_created());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail (compilation error: method does not exist)**

Run: `cargo test -p ballista-scheduler --lib state::aqe::execution_plan::tests 2>&1 | tail -20`

Expected: compilation error — `no method named reset_locations_on_lost_executor found for struct ExchangeExec`.

- [ ] **Step 3: Implement `ExchangeExec::reset_locations_on_lost_executor`**

In `ballista/scheduler/src/state/aqe/execution_plan.rs`, inside the `impl ExchangeExec` block, after the `input(&self)` method (~line 192), insert:

```rust
    /// If this exec's resolved `shuffle_partitions` reference the given
    /// executor, clear `shuffle_partitions` back to `None` and return its
    /// `stage_id` so the planner can restore cache entries. Returns `None`
    /// if the exec is unaffected (no resolved partitions, or none on the
    /// lost executor).
    pub(crate) fn reset_locations_on_lost_executor(
        &self,
        executor_id: &str,
    ) -> Option<usize> {
        let mut guard = self.shuffle_partitions.lock();
        let affected = match guard.as_ref() {
            Some(parts) => parts.iter().any(|locs| {
                locs.iter().any(|loc| loc.executor_meta.id == executor_id)
            }),
            None => false,
        };
        if affected {
            *guard = None;
            self.stage_id()
        } else {
            None
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ballista-scheduler --lib state::aqe::execution_plan::tests 2>&1 | tail -10`

Expected: `test result: ok. 3 passed; 0 failed`.

- [ ] **Step 5: Commit**

```bash
git add ballista/scheduler/src/state/aqe/execution_plan.rs
git commit -m "$(cat <<'EOF'
feat(aqe): add ExchangeExec::reset_locations_on_lost_executor

Clears resolved shuffle_partitions when any location references the
lost executor; returns the stage_id so the planner can restore cache
entries downstream.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Add `AdaptiveDatafusionExec::reset_locations_on_lost_executor` (TDD)

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/execution_plan.rs` — add method on `AdaptiveDatafusionExec` impl block (after `input` method, ~line 405)
- Test: extend the `#[cfg(test)] mod tests` block from Task 1

- [ ] **Step 1: Append failing tests for AdaptiveDatafusionExec**

In the existing `#[cfg(test)] mod tests` block at the end of `ballista/scheduler/src/state/aqe/execution_plan.rs`, append:

```rust
    #[test]
    fn adaptive_datafusion_exec_reset_clears_when_affected() {
        let exec = AdaptiveDatafusionExec::new(0, empty_input());
        exec.set_stage_id(11);
        exec.resolve_shuffle_partitions(vec![vec![loc("ex-1")]]);
        assert!(exec.shuffle_created());

        let result = exec.reset_locations_on_lost_executor("ex-1");

        assert_eq!(result, Some(11));
        assert!(!exec.shuffle_created());
    }

    #[test]
    fn adaptive_datafusion_exec_reset_no_op_when_unrelated_executor() {
        let exec = AdaptiveDatafusionExec::new(0, empty_input());
        exec.set_stage_id(11);
        exec.resolve_shuffle_partitions(vec![vec![loc("ex-2")]]);

        let result = exec.reset_locations_on_lost_executor("ex-1");

        assert_eq!(result, None);
        assert!(exec.shuffle_created());
    }
```

- [ ] **Step 2: Run tests to verify they fail (compilation error)**

Run: `cargo test -p ballista-scheduler --lib state::aqe::execution_plan::tests 2>&1 | tail -10`

Expected: compilation error — `no method named reset_locations_on_lost_executor found for struct AdaptiveDatafusionExec`.

- [ ] **Step 3: Implement `AdaptiveDatafusionExec::reset_locations_on_lost_executor`**

In `ballista/scheduler/src/state/aqe/execution_plan.rs`, inside the `impl AdaptiveDatafusionExec` block, after the `input(&self)` method (~line 405), insert:

```rust
    /// If this exec's resolved `shuffle_partitions` reference the given
    /// executor, clear `shuffle_partitions` back to `None` and return its
    /// `stage_id` so the planner can restore cache entries. Returns `None`
    /// if unaffected.
    pub(crate) fn reset_locations_on_lost_executor(
        &self,
        executor_id: &str,
    ) -> Option<usize> {
        let mut guard = self.shuffle_partitions.lock();
        let affected = match guard.as_ref() {
            Some(parts) => parts.iter().any(|locs| {
                locs.iter().any(|loc| loc.executor_meta.id == executor_id)
            }),
            None => false,
        };
        if affected {
            *guard = None;
            self.stage_id()
        } else {
            None
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ballista-scheduler --lib state::aqe::execution_plan::tests 2>&1 | tail -10`

Expected: `test result: ok. 5 passed; 0 failed`.

- [ ] **Step 5: Commit**

```bash
git add ballista/scheduler/src/state/aqe/execution_plan.rs
git commit -m "$(cat <<'EOF'
feat(aqe): add AdaptiveDatafusionExec::reset_locations_on_lost_executor

Mirrors ExchangeExec; the final-stage wrapper can also carry resolved
shuffle metadata when the root stage produces shuffled output.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Add `AdaptivePlanner::reset_on_lost_executor` (TDD)

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/planner.rs` — add the public `reset_on_lost_executor` method and the private `collect_affected_stages` walker (after `update_exchange_locations`, ~line 209)
- Test: append to existing `mod tests` if any, or add a new `#[cfg(test)] mod tests` block at the bottom

- [ ] **Step 1: Confirm planner has no existing tests block, then write a failing test**

Run: `grep -n "#\[cfg(test)\]" ballista/scheduler/src/state/aqe/planner.rs`

Expected output: nothing (no existing test block).

Append to `ballista/scheduler/src/state/aqe/planner.rs` (very end of file):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::test::{mock_context, mock_memory_table};
    use ballista_core::extension::SessionConfigExt;
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionStats,
    };
    use datafusion::prelude::SessionConfig;

    fn loc(executor_id: &str) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: 0,
            partition_id: PartitionId {
                job_id: "j".to_string(),
                stage_id: 0,
                partition_id: 0,
            },
            executor_meta: ExecutorMetadata {
                id: executor_id.to_string(),
                host: "h".to_string(),
                port: 0,
                grpc_port: 0,
                specification: ExecutorSpecification { task_slots: 0 },
            },
            path: "p".to_string(),
            partition_stats: PartitionStats::new(Some(1), None, Some(1)),
        }
    }

    async fn build_planner() -> AdaptivePlanner {
        let ctx = mock_context();
        ctx.register_table("t", mock_memory_table()).unwrap();
        let df = ctx
            .sql("select c, count(*) from t group by c")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let session_config = SessionConfig::new_with_ballista();
        AdaptivePlanner::try_new(&session_config, plan, "test".into()).unwrap()
    }

    #[tokio::test]
    async fn reset_on_lost_executor_is_noop_when_no_locations_match() {
        let mut planner = build_planner().await;
        // Force at least one runnable stage to be tracked.
        let _ = planner.runnable_stages().unwrap();

        let res = planner.reset_on_lost_executor("never-existed");

        assert!(res.is_ok());
        // No cache disruption.
    }

    #[tokio::test]
    async fn reset_on_lost_executor_clears_affected_exchange_and_restores_cache() {
        let mut planner = build_planner().await;
        let runnable = planner.runnable_stages().unwrap().unwrap();
        assert!(!runnable.is_empty(), "planner should yield runnable stages");

        // Drive the first runnable stage to completion using a fake executor id
        // so its shuffle partitions reference 'ex-1'.
        let stage_id = runnable[0].plan.stage_id();
        let part = vec![vec![loc("ex-1")]];
        planner.update_exchange_locations(stage_id, vec![loc("ex-1")]).unwrap();
        let _resolved = planner.finalise_stage(stage_id).unwrap();

        // After finalise, the cache entries are gone and the exchange is resolved.
        assert!(!planner.runnable_stage_cache.contains_key(&stage_id));
        assert!(!planner.runnable_stage_output.contains_key(&stage_id));

        planner.reset_on_lost_executor("ex-1").unwrap();

        // Cache and output entries are restored for the affected stage.
        assert!(planner.runnable_stage_cache.contains_key(&stage_id));
        assert!(planner.runnable_stage_output.contains_key(&stage_id));
        // Suppress unused warning on `part` — the body above already created locations.
        let _ = part;
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ballista-scheduler --lib state::aqe::planner::tests 2>&1 | tail -25`

Expected: compilation error — `no method named reset_on_lost_executor found for struct AdaptivePlanner`.

- [ ] **Step 3: Implement `reset_on_lost_executor` and `collect_affected_stages`**

In `ballista/scheduler/src/state/aqe/planner.rs`, locate `update_exchange_locations` (~line 196) and insert this block immediately after its closing `}` (before the next `pub fn finalise_stage` definition):

```rust
    /// Reset planner state for stages whose resolved shuffle outputs include
    /// data on the lost executor.
    ///
    /// For each affected `ExchangeExec` / `AdaptiveDatafusionExec` in the live
    /// plan tree, this clears `shuffle_partitions` back to `None` (so the
    /// stage looks unresolved to `find_runnable_exchanges`) and restores the
    /// `runnable_stage_cache` / `runnable_stage_output` entries so subsequent
    /// `update_exchange_locations` and `finalise_stage` calls work on the
    /// re-running stage. Then re-runs `replan_stages()` because some
    /// `shuffle_partitions` just transitioned back to `None`.
    pub(super) fn reset_on_lost_executor(
        &mut self,
        executor_id: &str,
    ) -> common::Result<()> {
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
        } else if let Some(ad) =
            node.as_any().downcast_ref::<AdaptiveDatafusionExec>()
        {
            if let Some(stage_id) = ad.reset_locations_on_lost_executor(executor_id) {
                out.push((stage_id, node.clone()));
            }
        }
        for child in node.children() {
            Self::collect_affected_stages(child, executor_id, out);
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ballista-scheduler --lib state::aqe::planner::tests 2>&1 | tail -15`

Expected: `test result: ok. 2 passed; 0 failed`.

If the second test fails because `runnable_stage_cache` or `runnable_stage_output` are private and not accessible from the test:

- They are private struct fields. Tests inside the `tests` submodule of `planner.rs` should still see them via `super::*`. If access is denied because of `pub(crate)`/`pub(super)`/no visibility, change the assertions to call the existing `actionable_stages()` / `runnable_stages()` methods and verify the affected stage is reported as runnable again instead.

If `update_exchange_locations` is not visible from tests, verify the existing visibility — at the time of writing it is `pub`, so this should be fine.

- [ ] **Step 5: Commit**

```bash
git add ballista/scheduler/src/state/aqe/planner.rs
git commit -m "$(cat <<'EOF'
feat(aqe): add AdaptivePlanner::reset_on_lost_executor

Walks the live plan tree, clears resolved shuffle metadata on
ExchangeExec / AdaptiveDatafusionExec nodes that reference the lost
executor, restores runnable_stage_cache / runnable_stage_output
entries for affected stages, and re-runs replan_stages.

Without this the AdaptiveExecutionGraph rolls back stages but the
planner's plan tree still treats them as resolved, and re-running
stages can't accept update_exchange_locations.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Wire planner reset into `AdaptiveExecutionGraph::reset_stages_internal` and remove stale doc

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/mod.rs` — add planner call inside `reset_stages_internal` (~line 484, just before the final `Ok((reset_stage, all_running_tasks))`); remove `/// - it does not cover executor failure` from the module docstring (line 67)

- [ ] **Step 1: Read the current end of `reset_stages_internal` to confirm the insertion point**

Run: `sed -n '475,490p' ballista/scheduler/src/state/aqe/mod.rs`

Expected: see the lines `let mut reset_stage = HashSet::new(); reset_stage.extend(...)` ending with `Ok((reset_stage, all_running_tasks))`.

- [ ] **Step 2: Modify `reset_stages_internal` to call the planner**

In `ballista/scheduler/src/state/aqe/mod.rs`, find the lines (around line 479-484):

```rust
        let mut reset_stage = HashSet::new();
        reset_stage.extend(reset_running_stage);
        reset_stage.extend(rollback_resolved_stages);
        reset_stage.extend(rollback_running_stages);
        reset_stage.extend(resubmit_successful_stages);
        Ok((reset_stage, all_running_tasks))
```

Replace with:

```rust
        let mut reset_stage = HashSet::new();
        reset_stage.extend(reset_running_stage);
        reset_stage.extend(rollback_resolved_stages);
        reset_stage.extend(rollback_running_stages);
        reset_stage.extend(resubmit_successful_stages);

        // Synchronize planner state with the graph-level rollback above.
        // Without this, re-running stages can't accept
        // update_exchange_locations (cache entries were cleared in
        // finalise_stage) and the plan tree still treats affected
        // exchanges as resolved.
        self.planner
            .reset_on_lost_executor(executor_id)
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Failed to reset AdaptivePlanner state on lost executor {executor_id}: {e}"
                ))
            })?;

        Ok((reset_stage, all_running_tasks))
```

- [ ] **Step 3: Remove the stale doc line about executor failure**

In `ballista/scheduler/src/state/aqe/mod.rs`, find line 67 which says:

```rust
/// - it does not cover executor failure
```

Delete that single line. The lines before and after should remain untouched (`/// with many limitations, such as:` above and `/// - dynamically coalescing shuffle partitions, not supported yet` below).

- [ ] **Step 4: Verify the module still compiles and existing tests pass**

Run: `cargo test -p ballista-scheduler --lib state::aqe 2>&1 | tail -20`

Expected: All previously-passing AQE tests (12 baseline + 5 new from Task 1+2 + 2 new from Task 3 = 19) pass.

- [ ] **Step 5: Commit**

```bash
git add ballista/scheduler/src/state/aqe/mod.rs
git commit -m "$(cat <<'EOF'
feat(aqe): wire AdaptivePlanner::reset_on_lost_executor into rollback

reset_stages_internal now syncs planner state after rolling back
graph-level stages on executor loss; also drop the stale
"does not cover executor failure" doc line.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Add AQE plan-builder helpers to `aqe/test/mod.rs`

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/test/mod.rs` — add `test_aqe_aggregation_plan` and `test_aqe_join_plan` helpers, plus `mod executor_failure;` registration

- [ ] **Step 1: Add the test module registration and helpers**

In `ballista/scheduler/src/state/aqe/test/mod.rs`, append (after the existing `mod alter_stages;` and `mod plan_to_stages;` lines and after the existing helpers):

```rust
/// Tests for executor failure handling
mod executor_failure;

use crate::state::aqe::AdaptiveExecutionGraph;
use ballista_core::extension::SessionConfigExt;
use datafusion::common::JoinType;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::col;
use datafusion::physical_plan::displayable;

/// Creates an AdaptiveExecutionGraph for an aggregation plan
/// (count(*) grouped by c). Equivalent in spirit to test_aggregation_plan
/// for the static graph.
pub(crate) async fn test_aqe_aggregation_plan(
    partition: usize,
) -> AdaptiveExecutionGraph {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(partition)
        .with_round_robin_repartition(false);
    let state = SessionStateBuilder::new()
        .with_config(config.clone())
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("t", mock_memory_table()).unwrap();

    let df = ctx
        .sql("select c, count(*) from t group by c")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    println!("{}", displayable(plan.as_ref()).indent(false));

    AdaptiveExecutionGraph::try_new(
        "localhost:50050",
        "job",
        "",
        "session",
        plan,
        0,
        Arc::new(config),
        None,
        None,
    )
    .unwrap()
}

/// Creates an AdaptiveExecutionGraph for a self-join plan on column `a`.
pub(crate) async fn test_aqe_join_plan(partition: usize) -> AdaptiveExecutionGraph {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(partition)
        .with_round_robin_repartition(false);
    let state = SessionStateBuilder::new()
        .with_config(config.clone())
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("t", mock_memory_table()).unwrap();
    ctx.register_table("u", mock_memory_table()).unwrap();

    let left = ctx.table("t").await.unwrap();
    let right = ctx.table("u").await.unwrap();
    let df = left
        .join(right, JoinType::Inner, &["a"], &["a"], None)
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    println!("{}", displayable(plan.as_ref()).indent(false));

    AdaptiveExecutionGraph::try_new(
        "localhost:50050",
        "job",
        "",
        "session",
        plan,
        0,
        Arc::new(config),
        None,
        None,
    )
    .unwrap()
}
```

- [ ] **Step 2: Create the test file as an empty module so registration compiles**

Run: `touch ballista/scheduler/src/state/aqe/test/executor_failure.rs`

Then add a license header to the empty file by writing this content:

```rust
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

//! Tests for executor failure handling in AdaptiveExecutionGraph.
//! Mirrors the static-graph tests in
//! `ballista/scheduler/src/state/execution_graph.rs`.
```

- [ ] **Step 3: Verify the AQE module still compiles**

Run: `cargo check -p ballista-scheduler 2>&1 | tail -10`

Expected: `Finished ... target(s) in ...s` (no errors).

If you get unused-import warnings on `JoinType`, `col`, `displayable`, etc., that's fine — they're used by helpers and tests added in later tasks. Warnings should not block, but if `-D warnings` is in effect for the project, suppress per-import with `#[allow(unused_imports)]` only if necessary (verify by running `cargo build -p ballista-scheduler` without `--lib`).

- [ ] **Step 4: Verify existing AQE tests still pass**

Run: `cargo test -p ballista-scheduler --lib state::aqe 2>&1 | tail -10`

Expected: All AQE tests pass (no new tests yet, but the helpers must not break compilation).

- [ ] **Step 5: Commit**

```bash
git add ballista/scheduler/src/state/aqe/test/mod.rs ballista/scheduler/src/state/aqe/test/executor_failure.rs
git commit -m "$(cat <<'EOF'
test(aqe): add plan-builder helpers and executor_failure module

Adds test_aqe_aggregation_plan / test_aqe_join_plan that build
AdaptiveExecutionGraph from a SQL-shaped plan, mirroring the static
graph helpers. Registers an empty executor_failure module to be
filled in by subsequent tasks.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Port `test_reset_completed_stage_executor_lost`

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/test/executor_failure.rs`

- [ ] **Step 1: Append the ported test (failing initially because we want to verify the wire-in works)**

In `ballista/scheduler/src/state/aqe/test/executor_failure.rs`, append after the module docstring:

```rust
use crate::state::aqe::test::test_aqe_join_plan;
use crate::test_utils::{
    mock_completed_task, mock_executor,
    revive_graph_and_complete_next_stage_with_executor,
};
use ballista_core::error::Result;

#[tokio::test]
async fn test_reset_completed_stage_executor_lost() -> Result<()> {
    let executor1 = mock_executor("executor-id1".to_string());
    let executor2 = mock_executor("executor-id2".to_string());
    let mut join_graph = test_aqe_join_plan(4).await;

    // Initial state: graph has the leaf stages resolved.
    let initial_stage_count = join_graph.stage_count();
    assert!(initial_stage_count >= 2, "expected at least 2 leaf stages");

    join_graph.revive();

    // Complete the first leaf stage on executor1.
    revive_graph_and_complete_next_stage_with_executor(
        &mut join_graph,
        &executor1,
    )?;

    // Complete the second leaf stage on executor2.
    revive_graph_and_complete_next_stage_with_executor(
        &mut join_graph,
        &executor2,
    )?;

    join_graph.revive();

    // Pop and complete one task in the next stage on executor1.
    if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
        let task_status = mock_completed_task(task, &executor1.id);
        join_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
    }
    // Pop a second task on executor1 but don't report status (running).
    let _running = join_graph.pop_next_task(&executor1.id)?;

    let reset = join_graph.reset_stages_on_lost_executor(&executor1.id)?;

    // At least one stage was reset (combining rollback + rerun).
    assert!(
        !reset.0.is_empty(),
        "expected at least one stage to be reset"
    );

    // The graph still has tasks available to drive to completion.
    drain_aqe(&mut join_graph, &executor2)?;
    assert!(
        join_graph.is_successful(),
        "join plan failed to complete after executor loss"
    );

    Ok(())
}

/// Local equivalent of static `drain_tasks`: keep popping tasks and reporting
/// completion via executor2 (the surviving one) until no more available.
fn drain_aqe(
    graph: &mut crate::state::aqe::AdaptiveExecutionGraph,
    executor: &ballista_core::serde::scheduler::ExecutorMetadata,
) -> Result<()> {
    loop {
        graph.revive();
        let mut popped = false;
        while let Some(task) = graph.pop_next_task(&executor.id)? {
            popped = true;
            let task_status = mock_completed_task(task, &executor.id);
            graph.update_task_status(executor, vec![task_status], 4, 4)?;
        }
        if !popped {
            // No more tasks to pop. Try one more revive to flush state, then exit.
            if !graph.revive() {
                break;
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p ballista-scheduler --lib state::aqe::test::executor_failure::test_reset_completed_stage_executor_lost 2>&1 | tail -30`

Expected: PASS. If FAIL, the failure mode tells you something:
- "Can't find active stage to update stage outputs" → planner reset isn't running or isn't restoring cache entries; revisit Task 3 / Task 4.
- "Failed to reset AdaptivePlanner state" → Task 4 wire-in is propagating an error from `replan_stages`. Read the error to see if a specific optimizer rule is failing on the partially-rolled-back tree.
- "join plan failed to complete after executor loss" → drain_aqe loop terminated before all stages succeeded. May need to run multiple revive/drain cycles or to debug `actionable_stages()` after reset.

- [ ] **Step 3: Commit**

```bash
git add ballista/scheduler/src/state/aqe/test/executor_failure.rs
git commit -m "$(cat <<'EOF'
test(aqe): port test_reset_completed_stage_executor_lost

Verifies that when an executor is lost mid-stage, the graph rolls
back affected stages, the planner restores its cache, and the
job completes successfully on a surviving executor.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Port `test_reset_resolved_stage_executor_lost`

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/test/executor_failure.rs`

- [ ] **Step 1: Append the ported test**

Append to `ballista/scheduler/src/state/aqe/test/executor_failure.rs`:

```rust
#[tokio::test]
async fn test_reset_resolved_stage_executor_lost() -> Result<()> {
    let executor1 = mock_executor("executor-id1".to_string());
    let executor2 = mock_executor("executor-id2".to_string());
    let mut join_graph = test_aqe_join_plan(4).await;

    join_graph.revive();

    // Complete first leaf stage on executor1 (its outputs end up
    // referencing executor1).
    revive_graph_and_complete_next_stage_with_executor(
        &mut join_graph,
        &executor1,
    )?;

    // Complete second leaf stage on executor2.
    revive_graph_and_complete_next_stage_with_executor(
        &mut join_graph,
        &executor2,
    )?;

    // The downstream stage is now Resolved (waiting for revive) but no
    // tasks have been popped yet.
    let reset = join_graph.reset_stages_on_lost_executor(&executor1.id)?;

    // Either the resolved stage was rolled back, or the upstream successful
    // stage was reset for re-execution. We expect at least one of these.
    assert!(
        !reset.0.is_empty(),
        "expected at least one stage to be reset (rollback or rerun)"
    );

    // Plan must still complete on executor2.
    drain_aqe(&mut join_graph, &executor2)?;
    assert!(
        join_graph.is_successful(),
        "join plan failed to complete after Resolved-stage rollback"
    );

    Ok(())
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p ballista-scheduler --lib state::aqe::test::executor_failure::test_reset_resolved_stage_executor_lost 2>&1 | tail -30`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add ballista/scheduler/src/state/aqe/test/executor_failure.rs
git commit -m "$(cat <<'EOF'
test(aqe): port test_reset_resolved_stage_executor_lost

Verifies executor loss at the Resolved-stage point: the resolved
downstream stage is rolled back to UnResolved (or the upstream
successful stage is reset for rerun), and the job still completes.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Port `test_task_update_after_reset_stage`

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/test/executor_failure.rs`

- [ ] **Step 1: Append the ported test**

Append to `ballista/scheduler/src/state/aqe/test/executor_failure.rs`:

```rust
use crate::state::aqe::test::test_aqe_aggregation_plan;

#[tokio::test]
async fn test_task_update_after_reset_stage() -> Result<()> {
    let executor1 = mock_executor("executor-id1".to_string());
    let executor2 = mock_executor("executor-id2".to_string());
    let mut agg_graph = test_aqe_aggregation_plan(4).await;

    agg_graph.revive();

    // Complete the first stage on executor1 — outputs reference executor1.
    revive_graph_and_complete_next_stage_with_executor(
        &mut agg_graph,
        &executor1,
    )?;

    // First task in the second stage, on executor2.
    if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
        let task_status = mock_completed_task(task, &executor2.id);
        agg_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
    }

    // Second task in the second stage, on executor1.
    if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
        let task_status = mock_completed_task(task, &executor1.id);
        agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
    }

    // Third task — popped (running) but not completed, on executor1.
    let task = agg_graph.pop_next_task(&executor1.id)?;

    let reset = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;

    // The 3rd task's late status arrives after the reset.
    if let Some(t) = task {
        let task_status = mock_completed_task(t, &executor1.id);
        agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
    }

    assert!(!reset.0.is_empty(), "expected stages to be reset");

    // Idempotent: a second reset call must produce no further changes.
    let reset2 = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;
    assert!(
        reset2.0.is_empty(),
        "expected second reset call to be a no-op, got {:?}",
        reset2.0
    );

    drain_aqe(&mut agg_graph, &executor2)?;
    assert!(
        agg_graph.is_successful(),
        "agg plan failed to complete after reset/late-status"
    );

    Ok(())
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p ballista-scheduler --lib state::aqe::test::executor_failure::test_task_update_after_reset_stage 2>&1 | tail -30`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add ballista/scheduler/src/state/aqe/test/executor_failure.rs
git commit -m "$(cat <<'EOF'
test(aqe): port test_task_update_after_reset_stage

Verifies that:
1. A late task status arriving after reset_stages_on_lost_executor
   doesn't corrupt graph state.
2. A second reset call for the same executor is a no-op (idempotent).
3. The job still completes after the reset/late-status churn.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Port `test_long_delayed_failed_task_after_executor_lost`

**Files:**
- Modify: `ballista/scheduler/src/state/aqe/test/executor_failure.rs`

- [ ] **Step 1: Look up the static graph version for fidelity**

Run: `grep -n "test_long_delayed_failed_task_after_executor_lost" ballista/scheduler/src/state/execution_graph.rs`

Expected: a single hit around line 2127.

Run: `sed -n '2127,2230p' ballista/scheduler/src/state/execution_graph.rs`

Read the test body to understand exactly what it verifies (a failed-task status from the lost executor arriving long after reset; verify it doesn't break the rerun).

- [ ] **Step 2: Append the ported test**

Append to `ballista/scheduler/src/state/aqe/test/executor_failure.rs`:

```rust
use crate::test_utils::mock_failed_task;
use ballista_core::serde::protobuf::{FailedTask, failed_task};

#[tokio::test]
async fn test_long_delayed_failed_task_after_executor_lost() -> Result<()> {
    let executor1 = mock_executor("executor-id1".to_string());
    let executor2 = mock_executor("executor-id2".to_string());
    let mut agg_graph = test_aqe_aggregation_plan(4).await;

    agg_graph.revive();

    // Complete the first stage on executor1.
    revive_graph_and_complete_next_stage_with_executor(
        &mut agg_graph,
        &executor1,
    )?;

    // Pop one task in second stage on executor1, do not report.
    let stranded = agg_graph.pop_next_task(&executor1.id)?;

    // Lose executor1.
    let reset = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;
    assert!(!reset.0.is_empty());

    // Now the stranded task's failure status arrives long after the reset.
    if let Some(t) = stranded {
        let failed_status = mock_failed_task(
            t,
            FailedTask {
                error: "executor lost".to_string(),
                retryable: true,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::ExecutionError(
                    Default::default(),
                )),
            },
        );
        // Should not panic / not corrupt graph state.
        agg_graph.update_task_status(&executor1, vec![failed_status], 4, 4)?;
    }

    drain_aqe(&mut agg_graph, &executor2)?;
    assert!(
        agg_graph.is_successful(),
        "agg plan failed to complete after delayed failed-status from dead executor"
    );

    Ok(())
}
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p ballista-scheduler --lib state::aqe::test::executor_failure::test_long_delayed_failed_task_after_executor_lost 2>&1 | tail -30`

Expected: PASS.

If the failed_reason variant `ExecutionError` doesn't exist or its `Default` differs, run `grep -n "pub enum FailedReason" ballista/core/src/serde/generated/ballista.protobuf.rs` to find the actual variants and pick a `retryable: true` one (e.g., `IoError(IoError {})` from existing static-graph tests).

- [ ] **Step 4: Commit**

```bash
git add ballista/scheduler/src/state/aqe/test/executor_failure.rs
git commit -m "$(cat <<'EOF'
test(aqe): port test_long_delayed_failed_task_after_executor_lost

Verifies that a failed task status arriving long after the executor
was declared lost does not corrupt graph state — the rerun continues
on the surviving executor.

Refs: docs/superpowers/specs/2026-04-27-aqe-executor-failure-design.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Final verification — full test suite + clippy + fmt

**Files:** none (verification only)

- [ ] **Step 1: Run the full scheduler test suite**

Run: `cargo test -p ballista-scheduler 2>&1 | tail -20`

Expected: all previously-passing tests still pass; the four new executor-failure tests pass; the five new exec-wrapper tests pass; the two new planner tests pass.

- [ ] **Step 2: Run clippy (workspace-wide is overkill — scheduler crate only)**

Run: `cargo clippy -p ballista-scheduler --tests 2>&1 | tail -30`

Expected: no warnings (or only pre-existing ones unrelated to this work). If new warnings appear, fix them — typically dead-code or unused-import on the new test helpers.

- [ ] **Step 3: Format check**

Run: `cargo fmt -p ballista-scheduler -- --check 2>&1`

Expected: clean. If it complains, run `cargo fmt -p ballista-scheduler` and commit any formatting fixes.

- [ ] **Step 4: Commit any formatting / clippy fixes**

If Step 2 or Step 3 produced changes:

```bash
git add -u
git commit -m "$(cat <<'EOF'
chore(aqe): clippy / fmt cleanups for executor failure work

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

If neither step produced changes, skip this commit step.

- [ ] **Step 5: Print branch state**

Run: `git log --oneline origin/main..HEAD`

Expected: a clean linear history of the tasks above (3 spec/gitignore commits + 9 implementation/test commits + optional 1 fmt cleanup).

---

## Self-Review (already performed inline)

- **Spec coverage**: every spec component (1) ExchangeExec method (Task 1), (2) AdaptiveDatafusionExec method (Task 2), (3) AdaptivePlanner method (Task 3), (4) wire-in (Task 4), (5) doc cleanup (Task 4 step 3). All four ported tests have their own task (6/7/8/9). Test helpers prerequisite: Task 5.
- **Placeholder scan**: no TBD/TODO; every code block contains compilable Rust; every step shows the exact command and expected outcome.
- **Type consistency**: `reset_locations_on_lost_executor` signature is consistent across exec wrappers (Tasks 1, 2) and matches what the planner calls in Task 3. `reset_on_lost_executor` signature on the planner matches the call site in Task 4. The test helper name `drain_aqe` is local to `executor_failure.rs` and used consistently across Tasks 6/7/8/9.
- **Known fragility**: Task 6's `drain_aqe` assumes a finite revive/pop loop terminates. If a test deadlocks here, the planner is producing infinite re-runs — that's a real bug and the test exposes it correctly.
