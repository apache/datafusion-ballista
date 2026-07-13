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

# Tuning Guide

## Partitions and Parallelism

The goal of any distributed compute engine is to parallelize work as much as possible, allowing the work to scale
by adding more compute resource.

The basic unit of concurrency and parallelism in Ballista is the concept of a partition. The leaf nodes of a query
are typically table scans that read from files on disk and Ballista currently treats each file within a table as a
single partition (in the future, Ballista will support splitting files into partitions but this is not implemented yet).

For example, if there is a table "customer" that consists of 200 Parquet files, that table scan will naturally have
200 partitions and the table scan and certain subsequent operations will also have 200 partitions. Conversely, if the
table only has a single Parquet file then there will be a single partition and the work will not be able to scale even
if the cluster has resource available. Ballista supports repartitioning within a query to improve parallelism.
The configuration setting `datafusion.execution.target_partitions`can be set to the desired number of partitions. This is
currently a global setting for the entire context. The default value for this setting is 16.

Note that Ballista will never decrease the number of partitions based on this setting and will only repartition if
the source operation has fewer partitions than this setting.

Example: Setting the desired number of shuffle partitions when creating a context.

```rust
use ballista::extension::{SessionConfigExt, SessionContextExt};

let session_config = SessionConfig::new_with_ballista()
    .with_target_partitions(200);

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_config(session_config)
    .build();

let ctx: SessionContext = SessionContext::remote_with_state(&url,state).await?;
```

## Configuring Executor Concurrency Levels

Each executor instance has a fixed number of tasks that it can process concurrently. This is specified by passing a
`concurrent_tasks` command-line parameter. The default setting is to use all available CPU cores.

Increasing this configuration setting will increase the number of tasks that each executor can run concurrently but
this will also mean that the executor will use more memory. If executors are failing due to out-of-memory errors then
decreasing the number of concurrent tasks may help.

## Configuring Executor Memory Pool

By default the executor uses DataFusion's unbounded memory pool, so spillable
operators (sort, hash join, hash aggregate) grow until the host runs out of
memory. To bound executor memory and let those operators spill to disk under
pressure, pass `--memory-pool-size` when starting the executor:

```sh
ballista-executor --memory-pool-size 8GB --concurrent-tasks 8
```

The argument accepts human-readable sizes (`8GB`, `512MiB`) or a plain byte
count. SI suffixes (`KB`/`MB`/`GB`) are powers of 10; IEC suffixes
(`KiB`/`MiB`/`GiB`) are powers of 2.

The total budget is divided equally across concurrent task slots: each task
receives its own `FairSpillPool` of size `memory_pool_size / concurrent_tasks`.
With `--memory-pool-size 8GB --concurrent-tasks 8`, every task sees a 1 GB
pool, fully isolated from other tasks. Idle slots do not lend their share to
busy ones, which keeps task memory predictable at the cost of some unused
capacity when the executor is under-utilized.

The executor refuses to start if the per-task share would round to zero (i.e.
`memory_pool_size < concurrent_tasks`).

When `--memory-pool-size` is not set, the executor behaves as before with no
memory pool installed.

## OOM Guard (Experimental)

The memory pool above only ever sees what DataFusion explicitly reserves
through it. Arrow buffer growth, join build-side scratch space, and several
expression kernels allocate native memory without ever going through a
`MemoryReservation`, so an executor's real memory use can run well above what
the pool believes is checked out. When that happens the process itself can be
OOM-killed by the kernel or by Kubernetes.

This is more damaging in Ballista than a single failed task. A dead executor
takes every shuffle file it had written with it, so every downstream stage
waiting to read from it fails with a fetch-partition error. That cascades into
stage rollbacks and re-execution, potentially affecting other jobs running
concurrently on the same executor.

The `oom-guard` feature adds a second, allocator-backed layer of protection on
top of `--memory-pool-size`. It is a **build-time** opt-in: it installs a
global allocator that tracks every byte the executor's process actually hands
out, so it is only available in binaries built with the feature enabled, and
it carries no cost otherwise:

```sh
cargo build --release --features oom-guard
```

A default build has no tracking allocator and no per-allocation overhead. The
feature must be compiled in, and it reuses `--memory-pool-size` as its budget.
If that flag is not set, the guard still tracks usage but never enforces
anything — with `oom-guard` compiled in but no memory pool configured, the
feature is a no-op.

Once armed, the tracked usage is compared against the same limit at two
points:

- A cooperative gate on the memory pool itself: once real usage plus a
  requested reservation would exceed the budget, the pool rejects the growth
  so DataFusion spills instead of continuing to allocate. This is the layer
  that does the useful work — most of the time, a spill is all that happens
  and no task fails.
- A circuit breaker that checks the same budget between batches of a running
  stage. If real usage is already over budget when the cooperative gate could
  not prevent it, this layer fails just the one task rather than letting the
  process die. A failed task reports a retriable `ResourcesExhausted`, so the
  scheduler reschedules it on another attempt, bounded by `--task-max-failures`
  (default 4), instead of failing the whole job.

A few things to keep in mind before enabling this.

Because the tracked figure is **every live allocation in the process**, not
just query memory, it also includes gRPC buffers, the Tokio runtime,
object-store client caches, and so on. With `oom-guard` enabled, an executor
therefore gates somewhat earlier than the same `--memory-pool-size` value
implies in a default build — queries may start spilling sooner than expected.
This is intentional: the whole point is to budget against what the OOM killer
sees, not just what the query engine reserves.

Enforcement is also batch-granular, not byte-granular: the circuit breaker
checks the budget between batches, so a single operator that allocates past
the limit within one batch can still bring down the process before the check
runs again. The cooperative gate is the layer that actually prevents this in
most cases; the circuit breaker only shrinks the blast radius once it hasn't.

Finally, the guard tracks bytes requested from the allocator, not resident set
size (RSS). The two can diverge, because allocators such as mimalloc do not
return freed pages to the OS immediately. The executor logs a warning when RSS
runs substantially above the tracked figure — that is the signal that the
allocator is holding on to freed memory rather than that a spill failed to
help.

This feature is experimental and disabled by default pending further
benchmark validation. Treat it as a safety net for production clusters that
have seen OOM-killed executors, not as a substitute for sizing
`--memory-pool-size` and `--concurrent-tasks` appropriately.

## Join Strategy

Ballista defaults to **sort-merge join** rather than hash join. This is the
opposite of DataFusion's standalone default and reflects two facts:

- DataFusion's hash join implementation does not yet support spilling: the
  full build side must fit in memory per task.
- Ballista executors run multiple tasks in parallel per host, so per-task
  build sides aggregate quickly under load and can OOM the executor.

Sort-merge join spills under memory pressure (via the executor's memory
pool, when configured), making it the safer default for distributed
execution.

If you know the build side of a particular query fits comfortably in
memory and you want hash-join performance, opt back in at the session
level:

```sql
SET datafusion.optimizer.prefer_hash_join = true;
```

or in code:

```rust
let session_config = SessionConfig::new_with_ballista()
    .set_bool("datafusion.optimizer.prefer_hash_join", true);
```

This setting applies per session and does not require restarting the
scheduler or executors.

## Shuffle Implementation

Ballista exchanges data between query stages by writing the output of each
upstream task to local files, which downstream tasks read either from disk
(when co-located) or over Arrow Flight. Two shuffle implementations are
available, with different trade-offs around file count, memory use, and
write latency.

### Sort-based shuffle (default)

The sort-based writer accumulates batches in a per-output-partition
in-memory buffer. When the total buffered size crosses a threshold, the
largest buffers are spilled to disk. After the input stream finishes, the
remaining in-memory data and any spilled batches are merged and written
into a single consolidated Arrow IPC file per input partition, alongside
an index file that lets readers seek directly to a given output partition.

This produces `2 × N` files instead of `N × M`, coalesces small batches
to a target size before writing, and bounds shuffle memory use via
spilling — at the cost of higher write latency than the hash writer.

### Hash-based shuffle (opt-in)

The hash-based writer hashes each incoming `RecordBatch` and immediately
encodes the per-partition slices to Arrow IPC, streaming them into one
file per `(input_partition, output_partition)` pair. Nothing is buffered
in memory across batches.

This is simple and low latency, but for `N` input partitions and `M`
output partitions it produces `N × M` files. Wide shuffles can therefore
generate a large number of small files. Consider switching to the
hash-based writer for narrow shuffles where the additional buffering
and merging of the sort-based writer is unnecessary overhead:

```rust
let session_config = SessionConfig::new_with_ballista()
    .set_bool("ballista.shuffle.sort_based.enabled", false);
```

The following session-level keys tune its behavior:

| key                                         | type    | default   | description                                                                                               |
| ------------------------------------------- | ------- | --------- | --------------------------------------------------------------------------------------------------------- |
| ballista.shuffle.sort_based.enabled         | Boolean | true      | Enables the sort-based shuffle writer.                                                                    |
| ballista.shuffle.sort_based.buffer_size     | UInt64  | 1048576   | Per-partition buffer size in bytes (1 MiB default).                                                       |
| ballista.shuffle.sort_based.memory_limit    | UInt64  | 268435456 | Total in-memory budget across all output-partition buffers (256 MiB default).                             |
| ballista.shuffle.sort_based.spill_threshold | Utf8    | "0.8"     | Fraction of `memory_limit` at which the largest buffers begin spilling to disk. Must be in the range 0–1. |
| ballista.shuffle.sort_based.batch_size      | UInt64  | 8192      | Target row count when coalescing buffered batches before they are written or spilled.                     |

## Adaptive Query Execution (Experimental)

Ballista has experimental support for adaptive query execution (AQE), where the
scheduler re-runs the DataFusion physical optimizer between query stages. This
lets the planner make decisions using statistics collected from completed
stages rather than relying solely on pre-execution estimates.

AQE is disabled by default. To enable it, set
`ballista.planner.adaptive.enabled` to `true` on your `SessionConfig`:

```rust
let session_config = SessionConfig::new_with_ballista()
    .set_bool("ballista.planner.adaptive.enabled", true);
```

When AQE is enabled, the scheduler logs a warning at job submission so it is
clear that AQE was used:

```
Adaptive Query Planning is EXPERIMENTAL, should be used for testing purposes only!
```

### Configuration

| key                               | type    | default | description                                 |
| --------------------------------- | ------- | ------- | ------------------------------------------- |
| ballista.planner.adaptive.enabled | Boolean | false   | Enables the adaptive planner. Experimental. |

### What AQE does today

When AQE is enabled, the scheduler builds the stage DAG incrementally. As each
shuffle stage completes, the planner re-optimizes the remaining plan and emits
the next set of runnable stages. Two adaptive optimizations are currently
implemented:

- **Join reordering.** Uses runtime row counts from completed stages so the
  smaller side drives the join.
- **Empty stage elimination.** When a completed stage produces zero rows, its
  downstream exchange is replaced with an empty execution node, and emptiness
  is propagated up the plan so downstream stages are skipped entirely.

### Current limitations

The implementation covers the happy path only. The following are known to be
missing or incomplete:

- Executor failure handling on the AQE path ([#1986](https://github.com/apache/datafusion-ballista/issues/1986))
- Dynamic coalescing of shuffle partitions ([#1987](https://github.com/apache/datafusion-ballista/issues/1987))
- Switching from hash join to sort-merge join based on runtime statistics ([#1988](https://github.com/apache/datafusion-ballista/issues/1988))
- Switching from streaming aggregation to hash aggregation based on runtime statistics ([#1989](https://github.com/apache/datafusion-ballista/issues/1989))

Until these gaps are closed, AQE should be used for testing and experimentation
rather than production workloads. See [issue #1359](https://github.com/apache/datafusion-ballista/issues/1359)
for the tracking epic and ongoing work.

## Push-based vs Pull-based Task Scheduling

Ballista supports both push-based and pull-based task scheduling. It is recommended that you try both to determine
which is the best for your use case.

Pull-based scheduling works in a similar way to Apache Spark and push-based scheduling can result in lower latency.

The scheduling policy can be specified in the `--scheduler-policy` parameter when starting the scheduler and executor
processes. The default is `pull-staged`.

## Viewing Query Plans and Metrics

The scheduler provides a REST API for monitoring jobs. See the
[scheduler documentation](scheduler.md) for more information.

> This is optional scheduler feature which should be enabled with rest-api feature

To download a query plan in dot format from the scheduler, submit a request to the following API endpoint

```
http://localhost:50050/api/job/{job_id}/dot
```

The resulting file can be converted into an image using `graphviz`:

```bash
dot -Tpng query.dot > query.png
```

Here is an example query plan:

![query plan](images/example-query-plan.png)
