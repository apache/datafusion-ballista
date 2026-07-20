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

# Benchmarking

This page describes how Ballista is benchmarked at scale, and records the current
results. It is aimed at contributors who want to reproduce a number, understand why
a comparison is set up the way it is, or add a result of their own.

The benchmark used here is derived from TPC-H. For running TPC-H locally at small
scale factors, see [`benchmarks/README.md`][bench-readme] in the repository; this
page covers the multi-node, large-scale-factor setup and the cross-engine
comparisons.

[bench-readme]: https://github.com/apache/datafusion-ballista/blob/main/benchmarks/README.md

## What we measure, and why

Ballista's central performance question is **how evenly work is spread across
executor tasks**. A distributed query is only as fast as its slowest stage, and a
stage is only as fast as its slowest task, so a query whose work concentrates onto
a few partitions will underperform no matter how fast the underlying operators are.
Benchmarks are therefore run at a scale where that imbalance actually shows up.

These benchmarks are run with **adaptive query execution (AQE) enabled**
(`ballista.planner.adaptive.enabled=true`). This is the configuration Ballista is
being actively developed against, so it is the one measured here.

AQE selects the adaptive planner, which can re-plan stages using runtime statistics —
coalescing partitions, re-optimising joins, and promoting a small join side to a
broadcast at runtime. The alternative, AQE off, selects the static
`DefaultDistributedPlanner`; it is a different planner with materially different join
behaviour, and it is no longer benchmarked on this page.

## Environment

The results on this page currently come from a **small homelab cluster**: two
bare-metal nodes running Kubernetes, with the TPC-H data staged on node-local disk.
This is a deliberate starting point rather than a final destination. A two-node
cluster with local disk removes as many confounds as possible — no object-store
latency, no cloud network variance, no noisy neighbours — so that when a query is
slow, the cause is Ballista and not the environment.

The intent is to **move these benchmarks to AWS with data in S3** once the results
here are good. That environment is the one users actually run, and it exercises
things a homelab cannot: object-store reads instead of local disk, higher and more
variable network latency between executors, and larger executor counts. Some of
Ballista's behaviour is expected to change there — a shuffle that is cheap over a
local link is not cheap over a cloud network, and object-store reads make scan
parallelism matter differently.

So results on this page should be read as **relative comparisons on controlled
hardware**, useful for "did this change help", not as absolute throughput numbers
for a cloud deployment.

## Reference cluster

All results on this page use the following shape. It is a reference point, not a
requirement — the commands below work on any cluster, but numbers are only
comparable when the shape matches.

|                     |                                             |
| ------------------- | ------------------------------------------- |
| Executors           | 2, one per physical node                    |
| Per executor        | 16 cores, 56 GiB, `--memory-pool-size=48GB` |
| Per task slot       | 16 concurrent tasks → 3 GB pool each        |
| Scheduler           | 1                                           |
| Data                | TPC-H SF1000 Parquet, node-local disk       |
| `target_partitions` | 64                                          |

Two details matter more than they look:

- **Executors are spread one per node.** Packing two executors onto one node makes
  them contend for the same disk and memory bandwidth, which is measuring the host,
  not the engine.
- **Data is on node-local disk, not object storage.** With a modest network between
  nodes, reading from object storage makes the interconnect the bottleneck and the
  engine comparison becomes an I/O comparison.

The memory pool is deliberately set below the container limit. Ballista splits
`--memory-pool-size` into one `FairSpillPool` per task slot, and that accounting
does not cover every allocation, so leaving headroom between the pool and the
container limit avoids the container being killed outright instead of reporting a
graceful resource error.

## Queries

All engines run the **same SQL**, which is what makes a cross-engine comparison
apples-to-apples.

The shared set is the TPC-H queries from [apache/datafusion-benchmarks][dfb]
(SQLBench-H). Ballista's bundled queries in `benchmarks/queries/` are the classic
TPC-H phrasing and differ in places, so the shared set is overlaid over them for a
comparison run.

Spark and Comet are driven through Comet's benchmark harness, which reads its own
bundled copy of the queries (labelled CometBench-H). That copy is textually
identical to the SQLBench-H set — all 22 queries match once the licence header
comment is ignored — so the engines are executing the same statements even though
they load them from different paths. Worth re-checking if either set is ever
regenerated.

[dfb]: https://github.com/apache/datafusion-benchmarks

## Running the benchmark

### Ballista

Start a scheduler and one executor per node:

```sh
# scheduler
ballista-scheduler --bind-host 0.0.0.0 --bind-port 50050

# executor (one per node; --concurrent-tasks defaults to the detected core count)
ballista-executor \
  --bind-host 0.0.0.0 \
  --scheduler-host <scheduler> --scheduler-port 50050 \
  --memory-pool-size=48GB \
  --work-dir /work \
  --client-ttl=60
```

`--client-ttl=60` enables shuffle-client connection caching. Without it every
shuffle fetch opens a new connection, and a high `target_partitions` can exhaust
ephemeral ports.

Run a query:

```sh
tpch benchmark ballista \
  --host <scheduler> --port 50050 \
  --query 18 \
  --path /mnt/bigdata/tpch/sf1000 --format parquet \
  --partitions 64 --iterations 1 \
  -c datafusion.optimizer.prefer_hash_join=false \
  -c datafusion.optimizer.enable_dynamic_filter_pushdown=false \
  -c ballista.planner.adaptive.enabled=true
```

Omit `--query` to run all 22. `ballista.planner.adaptive.enabled=true` is the AQE-on
configuration these results use.

`prefer_hash_join=false` makes DataFusion plan joins as `SortMergeJoin`, which is the
join strategy this page compares across engines (Comet is configured to match, below).

Note `datafusion.optimizer.enable_dynamic_filter_pushdown=false`: DataFusion's
dynamic filter pushdown assumes single-process execution and can deadlock
distributed execution, so it is pinned off for benchmark runs.

### Spark

Spark runs the same queries via `tpcbench.py` from
[apache/datafusion-benchmarks][dfb]:

```sh
spark-submit \
  --master <master> \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=16 \
  --conf spark.executor.memory=32G \
  --conf spark.executor.memoryOverhead=8G \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=16g \
  --conf spark.comet.enabled=false \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.SortShuffleManager \
  tpcbench.py \
    --benchmark tpch \
    --data /mnt/bigdata/tpch/sf1000 \
    --format parquet \
    --iterations 1 \
    --query 18
```

`spark.comet.enabled=false` and the stock `SortShuffleManager` are what make this a
**vanilla** Spark baseline, in case the image being used ships Comet.

### Comet

[Apache DataFusion Comet][comet] accelerates Spark by translating supported
operators to DataFusion. Same queries, same sizing; the difference is the plugin,
the Comet shuffle manager, and the jar on the classpath:

```sh
spark-submit \
  --master <master> \
  --jars $COMET_JAR --driver-class-path $COMET_JAR \
  --conf spark.executor.extraClassPath=$COMET_JAR \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.comet.exec.replaceSortMergeJoin=false \
  --conf spark.comet.exec.memoryPool=fair_unified \
  --conf spark.comet.exec.memoryPool.fraction=0.8 \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=16 \
  --conf spark.executor.memory=32G \
  --conf spark.executor.memoryOverhead=8G \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=16g \
  tpcbench.py \
    --benchmark tpch \
    --data /mnt/bigdata/tpch/sf1000 \
    --format parquet \
    --iterations 1 \
    --query 18
```

`spark.comet.exec.replaceSortMergeJoin=false` keeps Comet on Spark's `SortMergeJoin`
instead of converting it to a shuffled hash join. This matches Ballista, which plans
`SortMergeJoin` (`prefer_hash_join=false`), so the two engines are compared on the
same join strategy rather than one being handed a different one.

[comet]: https://datafusion.apache.org/comet/

## Why compare against Comet

Comet is the most informative comparison available to Ballista, because **Comet and
Ballista execute DataFusion physical plans using the same DataFusion operators**.
The scan, filter, join, and aggregate implementations doing the work are largely
shared code. When Ballista and Comet diverge on a query, the difference therefore
points at what is _not_ shared — how work is distributed, scheduled, shuffled, and
bounded by memory — rather than at the speed of the operators themselves. That is
precisely the surface Ballista is trying to improve, which makes the comparison
diagnostic rather than merely competitive.

That said, **the two do not necessarily run the same plan shape**, and the numbers
should not be read as an operator-level A/B:

- **Different planners produce the plan.** Comet accelerates a plan that _Spark's_
  optimizer produced: Spark chooses the join order and join strategies, and
  **Spark's AQE** coalesces shuffle partitions, converts joins, and splits skewed
  partitions at runtime. Ballista plans with DataFusion's optimizer and, when
  enabled, its own experimental AQE. Spark's AQE is a mature implementation and
  Ballista's is not, so the same SQL can arrive at execution with materially
  different plans.
- **Comet falls back to Spark.** Operators and expressions Comet does not support
  stay on the JVM, so a Comet run is generally a mix of DataFusion and Spark
  execution rather than an all-DataFusion one.
- **The distribution models differ.** Comet executes within Spark's task model and
  shuffle service; Ballista has its own scheduler, stage/task model, and shuffle.

So a Comet-vs-Ballista gap is best read as a question — _what is Spark's planner or
execution model doing here that Ballista's is not?_ — rather than as a verdict on
DataFusion. Vanilla Spark is included as the third data point, since it isolates
how much of any Comet result comes from DataFusion acceleration versus from Spark's
planner.

## Results

TPC-H **SF1000**, reference cluster above, **AQE on**, 1 iteration,
`target_partitions=64`, `prefer_hash_join=false`,
`enable_dynamic_filter_pushdown=false`. All three engines plan `SortMergeJoin`. Times
in seconds; lower is better.

Versions under test:

| Engine   | Version                                          |
| -------- | ------------------------------------------------ |
| Ballista | `main` @ `49d1fec8`                              |
| Spark    | 3.5.3 (vanilla, Comet disabled)                  |
| Comet    | `main` @ `b0165552` (1.0.0-SNAPSHOT, Spark 3.5)  |

Pin the **exact commit** the numbers came from, not "main": `main` moves, and a row
that mixes numbers from different commits silently misattributes a regression.

Ballista Q1–Q17 come from a **full 22-query suite run** (one query after another on
freshly started executors); Q18 exhausts the memory pool and OOM-kills the executors
(see below), which currently wedges the rest of the suite, so **Q19–Q22 were run as
individual single-query jobs** against a fresh cluster of the same shape. Comet and
Spark each completed the full suite in one run.

|     Query |    Spark |      Comet | Ballista (AQE on) |   Rows |
| --------: | -------: | ---------: | ----------------: | -----: |
|         1 |    427.1 |       46.6 |              21.4 |      4 |
|         2 |     75.9 |       39.2 |              64.1 |    100 |
|         3 |    154.1 |      195.4 |             202.5 |     10 |
|         4 |     82.0 |       42.6 |              54.6 |      5 |
|         5 |    336.4 |      493.4 |             474.7 |      5 |
|         6 |     28.9 |       14.6 |              27.7 |      1 |
|         7 |    180.6 |      149.5 |             457.0 |      4 |
|         8 |    391.8 |      642.1 |             731.0 |      2 |
|         9 |    509.8 |      843.5 |             934.3 |    175 |
|        10 |    151.1 |      104.8 |             136.8 |     20 |
|        11 |     44.7 |       44.1 |              85.4 |  0 [1] |
|        12 |     74.1 |       49.9 |              70.9 |      2 |
|        13 |     98.7 |       58.3 |              92.5 |     30 |
|        14 |     43.0 |       27.4 |              38.9 |      1 |
|        15 |    121.5 |       65.6 |              84.5 |  1 [2] |
|        16 |     24.5 |       17.5 |              24.3 |  27840 |
|        17 |    406.7 |      285.8 |             355.3 |      1 |
|        18 |    428.8 |      370.5 |               OOM |    100 |
|        19 |     58.9 |       35.5 |             120.4 |      1 |
|        20 |    105.9 |       67.6 |             111.2 | 110759 |
|        21 |    562.2 |      460.1 |             694.7 |    100 |
|        22 |     36.8 |       21.5 |              35.6 |      7 |
| **Total (excl. Q18)** | **3914.7** | **3705.0** |        **4817.8** |        |

The **Total** row sums the 21 queries **excluding Q18**, because Q18 does not complete
on Ballista at this sizing (below), so a 22-query total would not be comparable across
engines. Q18's own times are in its row.

Row counts agree across all three engines on every query, including Q18 (Spark and
Comet return 100; Ballista OOMs before producing a result).

[1] Q11 returns 0 rows for every engine at this scale factor: the query's threshold
constant is tuned for SF1.

[2] Q15 is a multi-statement query (`CREATE VIEW` / `SELECT` / `DROP VIEW`); all three
engines report 1 row here. (A previous result set saw Spark report 0 for Q15 depending
on which statement the harness took as the result; it does not recur in this run.)

**Q18 OOMs on Ballista at this sizing.** Q18's hash-join build side is `Partitioned`
and does not spill; with 16 task slots sharing the 48 GB pool (3 GB per slot), the
per-task build side exceeds the container limit and the executor is OOM-killed
([#2025](https://github.com/apache/datafusion-ballista/issues/2025)). At the previous
8-slot sizing (6 GB per slot) Q18 completed, so this is a direct consequence of the
denser packing. The OOM is recorded as `OOM` rather than a time.

The `Rows` column is the row count the query returned, recorded so a time is never
read without the answer it produced.

This table records **one current result set**. When results are refreshed, the
table and the pinned versions above are replaced together — a row must never mix
numbers from different commits, because a stale row silently misattributes a
regression.

`TBD` means not yet measured on this cluster at this commit; a query that ran but
did not produce an answer is recorded as `FAIL`, or `OOM` where the failure is a
known memory exhaustion.

### Recording a result

- Pin the **exact commit** the numbers came from, not a branch name.
- Report the **AQE-on** number (`ballista.planner.adaptive.enabled=true`) — the
  configuration this page measures — from the same commit and cluster.
- Prefer the figure from a **full suite run**, not a standalone single-query run: a
  long-lived executor deep into a suite is not in the same state as a freshly started
  one. When a query cannot complete in-suite (e.g. Q18's OOM currently wedges the
  run), note that the remaining queries were run individually, as done here for
  Q19–Q22.
- Note the **row count** each query returned. A fast wrong answer is not a result,
  and distributed execution has produced silently wrong row counts before.
- Flag any stage whose runtime is dominated by a few partitions — that is the
  imbalance this page exists to surface.

## Known issues found by benchmarking

Benchmarking at SF1000 is how most of the following were found. They are worth
knowing about before interpreting a number:

| Issue                                                              | Summary                                                                                                                                                                                                  |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [#1359](https://github.com/apache/datafusion-ballista/issues/1359) | Umbrella issue for adaptive (AQE) query execution.                                                                                                                                                       |
| [#2025](https://github.com/apache/datafusion-ballista/issues/2025) | Q18's hash-join build side exhausts the memory pool at SF1000. DataFusion's hash-join build side does not spill, so a per-partition build side larger than one task slot's pool fails the task outright. |
| [#2063](https://github.com/apache/datafusion-ballista/issues/2063) | AQE can hang when a re-plan cancels an in-flight stage; the job never reports completion.                                                                                                                |
