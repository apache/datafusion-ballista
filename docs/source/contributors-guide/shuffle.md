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

# Shuffle Design

Ballista uses a **blocking shuffle**: a query stage runs to completion and
materializes its output to local storage before any downstream stage starts. This
is the same model Apache Spark uses, and it is deliberately different from the
**pipelined shuffle** used by engines such as [Apache Flink],
[DataFusion Distributed], and [Sail], where a downstream stage streams data from
an upstream stage that is still running.

This page explains why Ballista made that choice, what it costs, and how to
think about the trade-off when proposing changes to the exchange layer. For the
mechanics of the shuffle writer and its tuning knobs, see the
[Shuffle Implementation section of the tuning guide](../user-guide/tuning-guide.md#shuffle-implementation).

[apache flink]: https://flink.apache.org/
[datafusion distributed]: https://datafusion-contrib.github.io/datafusion-distributed/
[sail]: https://github.com/lakehq/sail

## The barrier, concretely

Three pieces of the system implement the barrier:

1. **The write side.** `ShuffleWriterExec` (and `SortShuffleWriterExec`) drain
   their input partitions concurrently, streaming each to an Arrow IPC file
   under `{work_dir}/{job_id}/{stage_id}/{partition_id}/`. A partition's
   `ShuffleWritePartition` summary — the row, batch, and byte counts that the
   scheduler records — does not exist until that file is closed.

2. **The scheduler.** An `UnresolvedStage` becomes resolvable only when every
   input stage is marked complete:

   ```rust
   // ballista/scheduler/src/state/execution_stage.rs
   pub fn resolvable(&self) -> bool {
       self.inputs.iter().all(|(_, input)| input.is_complete())
   }
   ```

   Resolution then rewrites `UnresolvedShuffleExec` nodes into
   `ShuffleReaderExec` nodes carrying concrete `PartitionLocation`s, and runs
   Ballista's `JoinSelection` optimizer over the result.

3. **The read side.** `ShuffleReaderExec` fetches those locations, either
   directly from the local filesystem or from the producing executor's Arrow
   Flight service (`do_get`, or the raw-block `IO_BLOCK_TRANSPORT` action).

A stage's output is therefore a durable, addressable, re-readable artifact that
outlives the task that produced it. Almost every property below follows from
that one fact.

## Why the barrier is there

### Producers and consumers never compete for slots

Each executor advertises a fixed number of task slots. Under a blocking
shuffle, a stage's tasks hold slots only while that stage runs, so a stage with
1000 tasks executes perfectly well on a cluster with 32 slots — the scheduler
simply feeds tasks in as slots free up.

Pipelined shuffle removes that freedom. If a consumer task streams from a
producer task, both must be resident at the same time, which turns stage
scheduling into a co-scheduling (gang scheduling) problem. Get it wrong and the
cluster deadlocks: every slot is held by a consumer waiting on a producer that
cannot be scheduled. Engines that pipeline either require enough capacity for
the whole pipeline region, provision workers on demand, or fall back to
materialization under pressure.

Ballista's ability to run a query far wider than the cluster is a direct
consequence of blocking.

### Failures cost a task, not a query

Because shuffle output survives the task that wrote it, Ballista can recover
from a lost executor by re-running only what was actually lost. When a fetch
fails, the scheduler receives a `FetchPartitionError` and does three things: it
drops every input partition the downstream stage was sourcing from the failed
executor, rolls back that downstream stage, and resubmits only the map tasks
needed to regenerate the dropped partitions. Map output that lives on surviving
executors is not recomputed, and unrelated stages are not touched. The same
machinery handles executors disappearing outright
(`reset_stages_on_lost_executor`, `rollback_running_stage`,
`rerun_successful_stage` in `ballista/scheduler/src/state/execution_graph.rs`).
Repeated failures are capped: once a stage exceeds its retry budget the job
fails rather than looping.

A pipelined engine has to work harder for the same guarantee. Without a durable
copy of the producer's output, recovery means either re-running the producer or
restoring from a checkpoint, and checkpointing a distributed dataflow is a hard
problem in its own right — Flink got there with
[asynchronous barrier snapshotting], but it took years of work, and it recovers
by rewinding a whole pipeline region to the last snapshot rather than by
re-running the handful of tasks that were actually lost. Engines that have not
taken that on restart the query. DataFusion Distributed is explicit about this
in its own documentation:

> If any node fails mid-query, the whole query fails; there are no retries.
> There's no persistence of intermediate results, so queries can't checkpoint or
> resume from where they stopped.

For a query that runs for seconds, restarting is cheap and this hardly matters.
For an ETL job that runs for an hour on a cluster where a spot instance
disappears every few minutes, it is the difference between a job that finishes
and one that never does.

[asynchronous barrier snapshotting]: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/

### The filesystem absorbs producer/consumer skew

In a blocking shuffle, the buffer between stages is the disk. A producer never
waits for a consumer: it writes its output, reports, and releases its slot. That
decoupling is what lets the sort-based writer bound its own memory by spilling
and still guarantee forward progress.

In a pipelined shuffle, the buffer is memory plus network, and backpressure is
end-to-end. A slow consumer stalls its producer, which holds a slot and its
working set while stalled. Shuffles far larger than cluster memory — the case
Ballista is built for — need either a spill path or a remote shuffle service to
stay safe, which is most of the blocking machinery reintroduced.

Note that Ballista does apply backpressure on the _read_ side: the shuffle
governor (`ballista.shuffle.reader.max_bytes_in_flight` and
`max_blocks_in_flight_per_address`) bounds concurrent fetches. The point is that
this only shapes reader memory. It cannot stall a producer that has already
finished.

### Completed stages yield exact statistics

Stage resolution is where Ballista re-plans. `to_resolved()` runs
`JoinSelection` against the resolved plan, and with
`ballista.planner.adaptive.enabled` the `AdaptiveExecutionGraph` re-runs a set
of physical optimizer rules after each stage completes, using the exact row and
byte counts that stage produced. See
[Adaptive Query Execution](architecture.md#adaptive-query-execution-aqe).

It is worth being precise about what the barrier buys here, because pipelining
does not rule out adaptivity. DataFusion Distributed performs AQE without a
barrier by injecting a `SamplerExec` below the producer's exchange: it buffers
early batches, reports sampled row, byte, distinct, and null counts to the
coordinator, the coordinator sizes the next stage from that sample, and the
buffered batches then flow on into the consumer.

So the real distinction is **exact versus sampled**. Sampling is enough to size
a stage or pick a join side. It is not enough for decisions that depend on a
final answer — Ballista's empty-stage elimination rule, which replaces a
downstream exchange with an empty node and propagates emptiness up the plan,
requires knowing a stage produced zero rows, and no sample can establish that
before the stage ends.

Exact per-partition counts are also what makes skew mitigation tractable. The
AQE `CoalescePartitionsRule` already uses them to merge undersized shuffle
partitions before the next stage is scheduled, and the same information is what
a future rule would need to go the other way and split an oversized partition.
Skew is listed below as a cost of the barrier, but the barrier is also where the
statistics to fix it come from.

The two sources of statistics are not exclusive, either. Ballista's range
repartition operators already sample in flight: `RuntimeStatsExec` maintains a
T-Digest over the routing expression, and the repartition operator snapshots it
on the first batch to pick its quantile cuts, so range boundaries adapt to the
data without waiting for a stage to finish. The same mechanism is what would let
a hot key be spread across several downstream tasks.

### Executors can come and go between stages

Because a stage's inputs are files rather than live connections, cluster
membership only has to be stable for the duration of a single stage. Executors
can register between stages and pick up work immediately, which is what makes
autoscaling (including the KEDA scaler) straightforward. Shuffle files are
cleaned up after the job finishes, on the interval set by
`finished-job-data-clean-up-interval-seconds`.

For deployments that scale continuously with demand and run on spot capacity,
this and partial re-execution above are the same property seen twice: a fleet
whose size changes under the query only works if a departing executor costs a
few re-run tasks and an arriving one can be handed work with no setup. Both
follow from stage output being a file rather than a connection.

## What the barrier costs

The trade-off is real, and it is worth stating plainly.

- **Straggler stall.** A stage finishes when its slowest task finishes. Until
  then no downstream work can start, even if 99% of the input is ready. On a
  skewed join key or a slow node, most of the cluster sits idle waiting. This is
  the single largest source of avoidable latency in Ballista today — and the
  word to note is _avoidable_: waiting on the whole stage is how the scheduler
  works now, not something materialization forces (see
  [partial-input early start](#directions-that-do-not-require-abandoning-the-model)).
- **Write amplification.** Every intermediate byte is written, then read, then
  often served over the network — even when the intermediate result is a few
  kilobytes. For small queries this dominates the runtime.
- **Higher interactive latency.** The cost above is roughly fixed per stage
  boundary, so a query with many small stages pays it repeatedly. Pipelined
  engines report substantially better latency on interactive benchmarks, and the
  gap is structural, not an implementation detail. This is a gap relative to a
  pipelined engine, not an absolute verdict: Ballista is in production on
  workloads where queries return in around a second.
- **Local storage is required.** Executors need writable local storage sized for
  the largest shuffle they will handle, plus cleanup. It does not have to be a
  physical disk — deployments on memory-rich instances point `work_dir` at a
  RAM-backed filesystem and never touch one — but the space has to exist
  somewhere, and today that somewhere is attached to the executor rather than
  to a shuffle service.
- **No path to streaming.** A blocking barrier cannot express an unbounded
  input. Any future support for continuous queries would need a different
  exchange model.

## Choosing a model

| Workload                                          | Blocking (Ballista)  | Pipelined                            |
| ------------------------------------------------- | -------------------- | ------------------------------------ |
| Interactive queries, seconds, small intermediates | Slower               | Faster                               |
| Large batch and ETL, intermediates exceeding RAM  | Designed for it      | Needs spill or a shuffle service     |
| Cluster smaller than the query is wide            | Works                | Risks deadlock without co-scheduling |
| Unreliable or preemptible nodes                   | Partial re-execution | Query restart                        |
| Elastic or scale-to-zero clusters                 | Natural fit          | Requires resident workers            |
| Unbounded or streaming input                      | Not supported        | Natural fit                          |

Neither model is simply better. DataFusion Distributed reaches the same
conclusion from the other side, recommending Ballista for "large, long-running
batch or ETL that benefits from materializing intermediate results between
stages" while targeting interactive analytics itself.

## Directions that do not require abandoning the model

The barrier is not all-or-nothing, and several ideas would recover part of the
latency without giving up recovery or the ability to run wider than the cluster.
These are open directions rather than commitments, and where one changes
behavior it would be config-gated, leaving today's model as the default:

- **Partial-input early start.** Nothing about the design requires a consumer to
  wait for the _whole_ producer stage; that is just what `resolvable()` does
  today. A consumer task could begin once some producer tasks have finished,
  learning about additional `PartitionLocation`s incrementally. Where the
  cluster has spare capacity this overlaps stages and shortens the straggler
  stall, and it keeps files, retries, and slot accounting exactly as they are.
  The general form of this — deciding how much of the DAG is in flight at once
  rather than always running exactly one stage — is sometimes called bubble
  execution ([#2320], [#408]).
- **Hybrid exchange.** Stream to a consumer when one is already running and fall
  back to writing files otherwise, in the spirit of Flink's hybrid shuffle. This
  gets the latency win where capacity allows and degrades to today's behavior
  where it does not ([#1151], [#2003]).
- **In-memory shuffle.** Small intermediates are written and read back through
  the filesystem regardless of size. Keeping them in a bounded in-memory buffer,
  spilling to files only once the buffer is exceeded, would remove most of the
  write amplification for short queries while leaving the durable path in place
  for the ones that need it. This looks like the largest single win available:
  simply pointing `work_dir` at a RAM disk large enough to hold the whole
  shuffle already recovers a large share of the TPC-H gap against a pipelined
  engine, which says the cost being paid there is the filesystem round trip
  rather than the barrier itself ([#2318]).
- **A cheaper shuffle format.** The write side currently produces a file per
  output partition per task and a metadata round-trip per partition, which is a
  lot of small files and small reads at high partition counts. Writing one
  indexed file per task, coalescing batches across partitions, and trimming the
  per-partition metadata would cut the fixed cost of a stage boundary without
  changing where the data lives ([#660]).
- **Shuffle affinity.** Schedule a consumer task on the executor that already
  holds most of its input, turning Flight fetches into local reads. This one can
  be prototyped without touching the core: task distribution is already
  pluggable, so a `TaskDistributionPolicy::Custom` implementation can bind tasks
  to the executors holding their `PartitionLocation`s and be measured against
  the built-in bias and round-robin policies ([#2319]).
- **Remote shuffle service.** Offload shuffle storage to a service such as
  Apache Celeborn or Apache Uniffle ([#1539]), which decouples shuffle
  durability from executor lifetime and makes aggressive autoscaling safer.

Anyone proposing a change here should be explicit about which of the five
properties above it preserves and which it trades away.

[#408]: https://github.com/apache/datafusion-ballista/issues/408
[#660]: https://github.com/apache/datafusion-ballista/issues/660
[#1151]: https://github.com/apache/datafusion-ballista/issues/1151
[#1539]: https://github.com/apache/datafusion-ballista/issues/1539
[#2003]: https://github.com/apache/datafusion-ballista/issues/2003
[#2318]: https://github.com/apache/datafusion-ballista/issues/2318
[#2319]: https://github.com/apache/datafusion-ballista/issues/2319
[#2320]: https://github.com/apache/datafusion-ballista/issues/2320
