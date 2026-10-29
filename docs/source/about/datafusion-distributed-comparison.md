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

# Comparison with DataFusion Distributed

[DataFusion Distributed] is another project that runs [Apache DataFusion] plans across many machines. Both
projects are open source, both are built on the same query engine, and both stream Arrow data over gRPC. Users
reasonably ask how they differ, which one to choose, and whether one could be built on top of the other. This page
answers those questions. It is likely biased, because the Ballista community maintains it.

In short: **Ballista is a distributed query engine, and DataFusion Distributed is a library for building one.**
Ballista ships a scheduler, executors, clients, and the machinery for running many jobs on a shared cluster and
surviving the loss of an executor. DataFusion Distributed adds a small set of operators and a planner to DataFusion
so that an existing system can spread one query over workers it already manages, and leaves deployment, worker
discovery, and cluster management to that system.

[datafusion distributed]: https://github.com/datafusion-contrib/datafusion-distributed
[apache datafusion]: https://datafusion.apache.org/

## Execution models

Ballista started with the execution model of Apache Spark. Each query stage runs to completion and writes its output
to local storage before the next stage starts (a _blocking_ shuffle). That output outlives the task that wrote it,
which is what lets Ballista recover from a lost executor by re-running only the lost tasks, run a query wider than the
cluster, and plan the rest of a query from exact statistics. [Shuffle Design](../contributors-guide/shuffle.md)
explains this model and its costs in detail.

Ballista is adding a second, _pipelined_ execution model, in the style of Trino, as an experimental option chosen per
job with `ballista.execution.mode = pipelined`. All of a job's tasks are placed up front and stages stream batches to
each other through bounded in-memory queues, with nothing written to disk. The configuration and the pipelined
exchange operators are on `main` ([#2670], [#2685]); the scheduler support that runs pipelined jobs end to end is in
review in [#2657].

DataFusion Distributed has one execution model, the pipelined one. Its documentation is clear about the trade-off:

> If any node fails mid-query, the whole query fails; there are no retries. There's no persistence of intermediate
> results, so queries can't checkpoint or resume from where they stopped.

So the choice between them is no longer "batch versus interactive". Once pipelined mode is complete, a Ballista
cluster will run long ETL jobs with the blocking model and low-latency analytical queries with the pipelined model,
side by side on the same scheduler and executors. DataFusion Distributed is designed for the second kind of workload.

[#2657]: https://github.com/apache/datafusion-ballista/pull/2657
[#2670]: https://github.com/apache/datafusion-ballista/pull/2670
[#2685]: https://github.com/apache/datafusion-ballista/pull/2685

## Comparison

|                          | Ballista                                                                                                                                  | DataFusion Distributed                                                                                                           |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| What it is               | A distributed query engine: scheduler and executor binaries                                                                               | A library: operators and a planner that extend DataFusion                                                                        |
| Governance               | Apache DataFusion subproject                                                                                                              | `datafusion-contrib` organization                                                                                                |
| Deployment               | Docker images, Docker Compose and Kubernetes guides, optional KEDA autoscaling                                                            | Left to the user, who hosts workers in their own services                                                                        |
| Cluster state            | Scheduler tracks executors (registration, heartbeats, expiry), jobs, and stages                                                           | None; the node that receives a query coordinates it, and its state lives for that query                                          |
| Concurrent jobs          | Many jobs share the cluster's vcores, with job status, cancellation, and metrics through gRPC and a REST API                              | Each query is coordinated independently by the node that receives it, with no shared job queue or admission control              |
| Exchange between stages  | Blocking (Arrow IPC files on executors, read locally or over Arrow Flight); pipelined is experimental and per job                         | Streaming over gRPC, using Arrow Flight encoding                                                                                 |
| Fault tolerance          | Task retry, and re-execution of lost stages when an executor is lost or a shuffle fetch fails                                             | Retries only when a task is first routed to a worker; a worker failure after that fails the query                                |
| Adaptive query execution | On by default: re-plans after each stage from exact statistics (join selection, broadcast, empty-input elimination, partition coalescing) | Opt-in: sizes the task count of later stages from sampled statistics                                                             |
| Memory                   | Each executor has a memory pool (70% of the container by default), split per task, with spilling                                          | Whatever the user configures in the worker's `RuntimeEnv`                                                                        |
| Clients                  | Rust `SessionContext` extension, Python, SQL CLI, Arrow Flight SQL                                                                        | The user's own `SessionContext`; the planner is registered on it                                                                 |
| Extension points         | Logical and physical codecs, query planner, config and runtime producers, executor and scheduler overrides                                | Physical codec, worker and channel resolvers, network boundaries, and event handlers for task counts, routing, and plan rewrites |
| Operations               | Event logs and a history server, Prometheus metrics, TUI                                                                                  | Distributed `EXPLAIN ANALYZE`, metrics collection, a TUI console                                                                 |

## Why Ballista does not build on DataFusion Distributed

The two projects overlap in one place: how a plan is split into stages and how data moves between them. Everything
else Ballista provides sits above that layer, and DataFusion Distributed deliberately leaves it out. Its stated goal is
to remain "a library, not an engine" that is "unopinionated about networking". Rebuilding Ballista on it would
therefore replace only the part that Ballista needs to own in order to offer both execution models.

**Ballista's blocking model needs a scheduler that sees stages.** Recovery in Ballista works because the scheduler
knows when each stage has completed, where its output lives, and which tasks to re-run when an executor disappears.
DataFusion Distributed's coordinator is scoped to a single query and is built around all stages running at once. In
its default mode it sets up every stage's tasks before the query's final stage runs, keeps only the worker addresses
of a stage once its plan has been sent, and treats a worker failure after routing as a query failure. Its transport is
pluggable through the `WorkerChannel` trait, and a disk-backed implementation has been suggested as a way to bring
Ballista's model to it. A channel can make a producer's output re-readable, but it cannot decide that a stage has
finished, keep the plan needed to re-run part of it, or move a task to another worker. Those are coordinator
responsibilities. Adding them would turn DataFusion Distributed into an engine, against its own design, or leave
Ballista running its own scheduler on top of a second coordinator.

**Exact statistics come from completed stages.** Ballista's adaptive planner re-optimizes the remaining plan after
each stage finishes, using measured row and byte counts. That includes decisions that need a final answer, such as
removing work below an input that turned out to be empty. DataFusion Distributed's adaptive execution samples batches
from running stages to size later ones. Both approaches are sound, but they need different contracts between the
planner and the exchange. See [Shuffle
Design](../contributors-guide/shuffle.md#completed-stages-yield-exact-statistics) for more on this distinction.

**The building blocks Ballista would keep are most of Ballista.** These include the executor lifecycle, vcore
accounting and placement across many jobs, per-task memory pools, multi-partition tasks, the DataFrame cache, the
clients, the REST API, event logs, and the history server. They would all remain, on top of a dependency whose
coordination model differs from theirs.

**Ballista's own APIs would gain a second source of churn.** Users extend Ballista through its codecs, planners, and
producers, and follow DataFusion's release cadence. DataFusion Distributed is moving quickly, with four major releases
so far, and marks `WorkerChannel` as unstable. That is reasonable for a young library, but building Ballista on it
would pass those changes on to Ballista's users.

The reverse does not work either. DataFusion Distributed's users choose it because it embeds into an existing system
as an extension of DataFusion, with no scheduler or executor processes to run. Ballista cannot offer that.

## Where the projects can share work

The overlap is real and likely to grow, as Ballista's pipelined mode moves into the space DataFusion Distributed
targets. The most useful place to share work is DataFusion itself. Several DataFusion and Arrow improvements have
already come from DataFusion Distributed, such as range partitioning that avoids unnecessary shuffles ([datafusion#23184],
[datafusion#24598]), dynamic filters that can be sent over the network ([datafusion#21807], [datafusion#24068]), and IPC
and Flight optimizations ([arrow-rs#10044], [arrow-rs#10273]). Both projects benefit from those. Other shared
pieces, such as an exchange abstraction or a common way to split a plan into stages, would also be best placed in
DataFusion, where any distributed engine can use them.

[datafusion#21807]: https://github.com/apache/datafusion/pull/21807
[datafusion#23184]: https://github.com/apache/datafusion/pull/23184
[datafusion#24068]: https://github.com/apache/datafusion/pull/24068
[datafusion#24598]: https://github.com/apache/datafusion/pull/24598
[arrow-rs#10044]: https://github.com/apache/arrow-rs/pull/10044
[arrow-rs#10273]: https://github.com/apache/arrow-rs/pull/10273

## Performance

The results below compare the two projects on the same hardware and data, with each given the same resources.
Both use the same query engine, both are open source, and both are improving quickly. Treat the gaps as a snapshot of
two moving targets rather than a fixed property of either design, and expect them to narrow as each project adopts
what works in the other.

**Setup.** TPC-H at scale factor 1000, Parquet files compressed with zstd, read from Amazon S3. The tables are
Hive-partitioned (`lineitem` and `orders` by date), and both projects declared the partition columns so that they
prune partitions. Kubernetes on AWS, with 32 workers (Ballista executors) of 8 vCPU and 64 GiB each. Each query ran
twice with no warm-up, so the first, cold run is included in the mean.

- **Ballista** was built from [#2657] at commit `cb49f30f` (DataFusion 55.2.0). Each executor ran 8 tasks with a
  memory pool of 70% of its memory, and `target_partitions` was 256. The same image ran the blocking and pipelined
  modes.
- **DataFusion Distributed** was built from `main` at commit `90350275` (DataFusion 55.0.0), using the worker from
  its `benchmarks/remote-worker` crate. Upstream's worker has an unbounded memory pool, so it was patched to give each
  worker a fair spill pool of 70% of its memory, like Ballista's executors. A separate 8 vCPU, 32 GiB pod ran the
  coordinator and the final stage of each query. Its settings were otherwise the defaults.

Mean time per query, in seconds:

| Query     | DataFusion Distributed | Ballista (pipelined) | Ballista (blocking) |
| --------- | ---------------------: | -------------------: | ------------------: |
| Q1        |                    8.2 |                 13.8 |                14.8 |
| Q2        |                   19.3 |                 13.5 |                36.3 |
| Q3        |                    8.3 |                  7.3 |                17.9 |
| Q4        |                    5.8 |                  6.1 |                10.9 |
| Q5        |                   16.2 |                 13.9 |                41.4 |
| Q6        |                    1.7 |                  1.4 |                 1.7 |
| Q7        |                   18.6 |                 18.7 |                35.1 |
| Q8        |                   20.9 |                 14.1 |                21.6 |
| Q9        |                   31.7 |                 24.2 |                35.8 |
| Q10       |                   25.5 |                 22.0 |                45.1 |
| Q11       |                    9.4 |                  4.4 |                18.7 |
| Q12       |                    5.0 |                  4.4 |                10.1 |
| Q13       |                    6.2 |                  5.8 |                10.8 |
| Q14       |                    2.9 |                  1.4 |                 7.0 |
| Q15       |                   11.5 |                  6.9 |                10.6 |
| Q16       |                   78.2 |                  5.1 |                13.3 |
| Q17       |                   20.0 |                 15.4 |                23.6 |
| Q18       |                   30.8 |                 35.4 |                55.4 |
| Q19       |                    8.1 |                  7.4 |                12.1 |
| Q20       |                   16.6 |                  6.3 |                15.6 |
| Q21       |                   34.6 |                 45.9 |                68.6 |
| Q22       |                    6.9 |                  5.4 |                10.5 |
| **Total** |              **386.5** |            **278.8** |           **517.0** |

**What the results show.**

- Pipelined Ballista and DataFusion Distributed are close, which is expected of two pipelined engines on the same
  query engine. Ballista was faster on 17 of the 22 queries and DataFusion Distributed on 5 (Q1, Q4, Q7, Q18, and
  Q21). The geometric mean of the per-query ratios favours pipelined Ballista by about 1.4x.
- Most of the difference in the total is Q16. DataFusion Distributed runs the final `COUNT(DISTINCT)` aggregation and
  anti join of Q16 on the coordinator, over 8 partitions. With the coordinator's 22.4 GiB memory pool, that stage
  spilled 7.2 GB, and it was the only stage in the run that spilled. Excluding Q16, the geometric mean favours
  pipelined Ballista by about 1.2x.
- Blocking Ballista was about 1.85x slower than pipelined Ballista in total, which is the latency cost of writing
  every stage's output to disk before the next stage starts.

**Caveats.**

- Each query ran only twice, so per-query differences of less than about 10% are within run-to-run variation.
- The setups are as close as we could make them, but not identical. DataFusion Distributed's coordinator is an extra
  pod that runs part of each query, while Ballista's scheduler runs no query work. The two projects were also on
  different patch releases of DataFusion.
- DataFusion Distributed's README publishes a comparison in which Ballista is slower than it by a wide margin, on a
  12-node cluster at scale factors up to 100. That comparison was last updated in July 2026 and used Ballista 53.0.0.
  Ballista has had two releases and more than 500 commits since. Those include adaptive query execution enabled by
  default ([#2315]) and tasks that cover several partitions ([#2038]). The pipelined mode was not available then.
- Corrections to either setup are welcome. Please open an issue.

[#2038]: https://github.com/apache/datafusion-ballista/pull/2038
[#2315]: https://github.com/apache/datafusion-ballista/pull/2315
