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

# Parallel bounded-RANGE-frame windows — design doc

## The formula

1. Envision the end state.
2. Figure out where you actually are.
3. Plot a course.
4. Create a series of steps.
5. Each step's vector must have a dot product > 0.9 with the target direction.
6. Plan the nearby step very well.
7. Plan the farthest step barely at all.
8. Interpolate planning along the way.
9. Revisit at each step.
10. **Rope-bridge principle.** Fire an arrow with a string; pull twine; pull rope; pull larger rope; pull a floor. The bridge fulfills "bridge" from day one. Ship the _shape_, then thicken. Correct-and-slow-and-limited is a viable arrow; the way to a full bridge is not to design the floor first.

Corollary: a step is a candidate for skipping-and-back-filling if its output correctness holds without it. "Necessary for the final impl" ≠ "necessary for this step." Filtering files by ValueIndex range is important in the end state; it doesn't stop earlier steps from being correct without it.

## The end state (step 1)

On a cluster with E executors × V vcores each, scan the input across E×V partitions in parallel. Stage N collects per-partition stats via a runtime sketch and drives U/ORRE with `output_cnt == input_cnt`, keeping every vcore busy inside the task (local exchange is cheap). If the shuffle is ordered, write ValueIndex files so downstream doesn't have to over-sample or over-fetch. Cuts flow to the scheduler and become global.

Stage N+1 uses multi-partition tasks: each task claims `vcores` input partitions plus a file-halo overlap with the neighbouring task. Overlap is a _task-level_ concern, not a partition-level concern — inside a task, adjacent partitions borrow context via local memory (free); across tasks, file-halo is fetched over shuffle via ValueIndex-based partial reads. The task k-way merges its inputs into `vcores` sorted DF partitions, using the global cuts to distribute evenly. It then filters with row-halo, runs `PartitionedBoundedWindowAggExec`, filters without row-halo, and writes `vcores` output files.

Invariants:

- **Width invariant.** Partition count stays at E×V across stages. No funnel except where explicitly planted (not usually needed, done in ballista client).
- **Ordered-shuffle propagation.** When the writer declared an output ordering, the reader preserves it via k-way merge. Every ordered-shuffle consumer (BWAG, SortMergeJoin build side, …) benefits from the same primitive.
- **Two-level halo.** Stage-level file-halo crosses task boundaries at shuffle cost (partial-file read); task-level row-halo crosses local-partition boundaries at memory cost.

## The plan

Ordered chronologically. Ticked items are landed (or mostly landed). Unticked items may be skipped and back-filled per the rope-bridge principle.

- [x] **Multi-partition-task substrate.** #2038: `partition_slice` on task launch, K-drain ShuffleWriter, executor-side partition restriction. Every parallel operator downstream sits on this.
- [x] **T-Digest / KLL runtime stats.** #2180: sketch per partition, wire report to scheduler, merge into cuts. Foundation for any data-driven range op.
- [x] **URRE / ORRE.** #2169, #2196: N sorted overlapping → K sorted disjoint (ORRE via k-way merge internally) or unordered variant.
- [x] **RuntimeStatsExec, cut-discovery walker.** Same PR family. Late-binding cuts flow scheduler → downstream ops.
- [x] **RangeFilterExec + PartitionedBoundedWindowAggExec.** #2223: filter by resolved cuts + halo; BWAG wrapper that hides from tree walkers so `EnforceDistribution` doesn't collapse K→1.
- [x] **ParallelWindowRule.** #2223: match the `BWAG on Column Float64 ORDER BY, RANGE PRECEDING/FOLLOWING` shape, rewrite to insert `RuntimeStats → ORRE → RangeFilter(wide) → PartitionedBWAG → RangeFilter(narrow)`.
- [x] **Feature flag.** `ballista.planner.parallel_window.enabled=false` by default. Off is inert; on activates the rule.
- [x] **Ordered ShuffleReader.** New `RangeShuffleReaderExec` — keeps each upstream source as its own stream, feeds N into `StreamingMergeBuilder` on the child's declared ordering. Adapter picks it whenever `exchange.input().output_ordering().is_some()` (writer-driven gate). No permit governor and no per-source buffering; backpressure flows from the merge's demand through h2 / disk. Reusable — SortMergeJoin build side wants the same thing. Verified: h2o Q8 @ 1e7 SUM diff between `parallel_window.enabled=true` and `=false` agrees to 5e-14 relative (Float64 noise floor). If a wasted-merge cost surfaces in profiles later, tighten to demand-driven via a new consumer-side `requires_globally_sorted_input` bit.
- [x] **RangeFilter min/max fast-path + binary-search slice.** Post-ordered-ShuffleReader, batches are internally sorted. `min/max` fast paths (100% pass → Arc-clone; 0% overlap → skip) collapse the hot filter cost without touching correctness. Binary-search + `RecordBatch::slice` covers the mixed case with zero data copy. `sorted_on_key` derived at construction from `input.output_ordering()` — ascending on `routing_expr`, no config knob. Nullable routing columns fall back to `filter_record_batch` on a per-batch basis. Verified h2o Q8, 2 execs × 4 vcores, MPT=4: at 1e7 (2G cap) 7.6 s → 2.5 s = 3.0×; at 1e8 (4G cap) 143 s → 92 s = 1.55×. The 1e8 delta is smaller because the bottleneck shifts to shuffle/merge memory — see the next two items.
- [ ] **ValueIndex-based partial-file reads at shuffle-fetch time.** #2204 landed the write-side + reader primitives; consumer plumbing to translate value-range → byte-range at fetch has to hook in. Lets stage-N+1 halo reads pull only the halo slice from a neighbour file, not the whole file.
- [ ] **Per-task halo metadata on task-status.** New axis on task shipping: `own_files + halo_slice(file_ref, value_range)`. Composes with the ValueIndex plumbing — the scheduler emits `PartitionLocation`-with-value-range instead of `PartitionLocation`-whole-file. This is the rope. Enables inter-task halo without over-fetch.
- [ ] **Intra-task ORRE.** Once per-task input is one sorted merged stream (post-ordered-ShuffleReader), split it into `vcores` sub-partitions by task-local sub-cuts (derived from global cuts + vcore count). Task-level halo at the sub-partition boundaries is intra-task (local memory, free). Now cores stay busy inside every task without inter-task shuffle.
- [ ] **Two-level halo semantics in the rule.** RangeFilter at the stage boundary uses stage-level halo width (crosses tasks, shuffles); RangeFilter at the task-local boundary uses task-level halo width (crosses local partitions, memory). The rule plants both.
- [ ] **Symmetric halo (PRECEDING + FOLLOWING).** Generalize the halo direction on the shape. Plumbing extension; no new operators.
- [ ] **Range-partition invariant across stages.** E×V vcores → E×V in-flight partitions at every stage, funnel-free. Where an actual funnel is required, plant it explicitly. Everywhere else, keep width invariant.

Out of scope for this end state: **unbounded PRECEDING** (running SUM). Halo degenerates to "all preceding tasks" → serialism. Sibling design (prefix scan).

## Dot-product check on the near steps

- **RangeFilter fast paths.** Perf. Correctness-preserving. Doesn't unlock or block anything else, but pays for itself immediately on the wide filter (which sees 100% pass every batch given ORRE's exact-routing).
- **ValueIndex plumbing.** Skippable at the cost of over-fetching halo files whole. Everything after it correctness-holds without it.
- **Per-task halo metadata.** Skippable at the cost of stage-level halo remaining a single-width parameter on the shape. Everything after correctness-holds without it.
- **Intra-task ORRE.** Skippable at the cost of not saturating vcores when a task's input is ≤ vcores partitions.
