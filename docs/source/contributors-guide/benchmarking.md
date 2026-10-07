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

Current TPC-H **SF1000** results for Ballista, compared against vanilla
**Spark 4.1.3** and **Spark 4.1.3 with Apache DataFusion Comet 1.1.0-rc2**,
running on the same cluster shape. A **Trino 483** column is included for
reference; it is not an apples-to-apples comparison (see
[Trino configuration](#trino-configuration)).

## Versions under test

| Engine   | Version                                                                                                                                                                                                                |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ballista | [`c76278cd`](https://github.com/apache/datafusion-ballista/commit/c76278cd48583504821ac4d71ea74b44e54ed4ea) (`main`, 2026-10-07), Cargo pkg `55.0.0`, DataFusion `55.1.0`                                              |
| Spark    | `4.1.3` (`apache/spark:4.1.3` image, vanilla, no acceleration plugin)                                                                                                                                                  |
| Comet    | [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) `1.1.0-rc2` ([`992c806a`](https://github.com/apache/datafusion-comet/commit/992c806a7e38c2e88bd018aa5774164b0850e1fa)) on the same Spark `4.1.3` |
| Trino    | `483` (`trinodb/trino:483` image, Hive connector)                                                                                                                                                                      |

## Environment

- **Cluster:** Kubernetes on AWS (`us-west-2`); one driver/scheduler pod and
  32 executor pods for each engine, launched on the same node pool.
- **K8s worker nodes:** a mix of `r6i.24xlarge` (96 vCPU, 768 GiB memory) and
  `r6i.16xlarge` (64 vCPU, 512 GiB memory), EBS-only — no local
  instance-store. Pods are not pinned to an instance type.
- **Executor pod (Ballista):** x86_64, 8 vCPU, 64 GiB memory, plus a
  dedicated 1000 GiB `gp3` EBS PVC mounted at `/data` for the executor's
  shuffle work-dir (see [Executor storage](#executor-storage)).
- **Executor pod (Spark):** x86_64, 8 vCPU, 64 GiB + 10 GiB overhead, plus a
  dedicated `gp3` PVC via `spark-local-dir-1`.
- **Executor pod (Spark + Comet):** the same as Spark, plus 32 GiB of
  off-heap memory for Comet's native execution (about 106 GiB per pod in
  total, compared with 64 GiB for a Ballista executor).
- **Worker pod (Trino):** x86_64, 8 vCPU, 64 GiB memory (the image's
  default JVM heap of 80 %, about 51 GiB), no spill volume. A separate
  coordinator pod plans and schedules but does not scan data.
- **Driver (Spark and Spark + Comet):** runs the queries through a Spark
  benchmark harness that is not in this repository yet. See
  [Spark and Spark + Comet](#spark-and-spark--comet) under Reproducing.
- **Client pod (Ballista):** the `tpch` Rust benchmark runner from
  [`benchmarks/`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks)
  in this repo (`cargo run --release --bin tpch -- benchmark ballista ...`),
  which submits SQL through a Ballista `SessionContext` and collects
  results locally.
- **Data:** TPC-H SF1000 Parquet on S3 (`us-west-2`), ZSTD compression,
  ~512 MiB row groups, one directory per table. `lineitem`, `orders`,
  `customer` and `part` are Hive-style partitioned (e.g.
  `lineitem/l_shipdate=YYYY-MM-DD/`). The `tpch` runner declares the
  partition columns (see
  [Hive-partitioned data](https://github.com/apache/datafusion-ballista/tree/main/benchmarks#hive-partitioned-data)),
  so Ballista skips the partitions a filter excludes, as Spark, Comet and
  Trino do. Results from before
  [#2553](https://github.com/apache/datafusion-ballista/pull/2553) were
  collected without partition columns and pruned these scans only through
  Parquet statistics.

## Executor storage

Each Ballista executor pod is attached to a **fresh 1000 GiB `gp3` EBS
volume** (generic-ephemeral PVC, `storageClassName: gp3`), mounted at
`/data`, and the executor is launched with `--work-dir /data`. All shuffle
temp files land on this dedicated volume.

Without it, `--work-dir` defaults to a random directory under `/tmp` on the
container overlay filesystem, i.e. onto the node's root EBS volume, which
is shared with container images, `kubelet`, and every other pod on the
same node. On `r6i.24xlarge` that shared bandwidth becomes the binding
constraint under sustained shuffle-write pressure — `EXPLAIN ANALYZE`
observed Q8's `SortShuffleWriter.write_time` inflate ~5× on the second
run of a suite compared to a fresh cluster, despite the same 313 GB of
shuffle output and zero spilling, because Q1–Q7's dirty pages force
synchronous flushes to EBS at cap. Attaching a dedicated PVC removes that
contention and brings in-suite per-query times in line with the standalone
number.

Spark on the same cluster has always used this pattern via
`spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1`.

## Ballista configuration

| Flag / config key                                           | Value                                                                   |
| ----------------------------------------------------------- | ----------------------------------------------------------------------- |
| `--vcores`                                                  | `8`                                                                     |
| `--memory-pool-size` (bytes; ≈70 % of the 64 GiB container) | `48103633715`                                                           |
| `--work-dir`                                                | `/data` (dedicated gp3 PVC — see [Executor storage](#executor-storage)) |
| `--grpc-server-max-decoding-message-size`                   | `134217728`                                                             |
| `--grpc-server-max-encoding-message-size`                   | `134217728`                                                             |
| `datafusion.execution.target_partitions`                    | `256`                                                                   |
| `datafusion.execution.collect_statistics`                   | `true`                                                                  |
| `ballista.planner.adaptive.enabled`                         | `true` (AQE; default)                                                   |
| `ballista.shuffle.sort_based.memory_limit_per_task_bytes`   | `268435456` (256 MiB; default)                                          |

`datafusion.optimizer.prefer_hash_join` is left at its default; under AQE the
join strategy is selected at runtime by `DelayJoinSelectionRule` /
`DynamicJoinSelectionExec` from runtime statistics and the broadcast /
`ballista.optimizer.hash_join_max_build_partition_bytes` thresholds.

The gRPC message-size ceiling is raised from the 16 MiB default to 128 MiB;
some SF1000 physical plans (Q11, Q21, Q22) encode above 16 MiB and hit
`OutOfRange` errors otherwise.

## Spark configuration (highlights)

Both Spark runs use these settings. The vanilla Spark run has no Comet
plugin and uses the stock `SortShuffleManager`.

| Key                                                        | Value                                |
| ---------------------------------------------------------- | ------------------------------------ |
| `spark.executor.instances`                                 | `32`                                 |
| `spark.executor.cores`                                     | `16` (task parallelism per executor) |
| `spark.kubernetes.executor.limit.cores` / `.request.cores` | `8` (physical vCPU)                  |
| `spark.executor.memory`                                    | `64G`                                |
| `spark.executor.memoryOverhead`                            | `10G`                                |
| `spark.memory.fraction`                                    | `0.6`                                |
| `spark.memory.storageFraction`                             | `0.2`                                |
| `spark.sql.shuffle.partitions`                             | `512`                                |
| `spark.sql.broadcastTimeout`                               | `900`                                |
| `spark.serializer`                                         | `KryoSerializer`                     |
| `spark.io.compression.codec`                               | `zstd`                               |

Spark AQE is left at its Spark 4.1 defaults. Shuffle spills to a `gp3`-backed
per-executor volume (`spark.kubernetes.executor.volumes...spark-local-dir-1`).

Note that `spark.executor.cores=16` is Spark's **task parallelism** setting,
not a CPU allocation — each executor pod is given only **8 physical vCPU**
via `spark.kubernetes.executor.limit.cores` / `.request.cores`, so Spark
schedules 16 concurrent tasks onto 8 physical cores (2× oversubscription).
The matching Ballista executor runs `--vcores=8` on the same
8 physical vCPU (1:1).

## Comet configuration

The Spark + Comet run adds these settings to the Spark configuration above:

| Key                                    | Value                                                              |
| -------------------------------------- | ------------------------------------------------------------------ |
| `spark.plugins`                        | `org.apache.spark.CometPlugin`                                     |
| `spark.comet.enabled`                  | `true`                                                             |
| `spark.shuffle.manager`                | `org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager` |
| `spark.memory.offHeap.enabled`         | `true`                                                             |
| `spark.memory.offHeap.size`            | `32G`                                                              |
| `spark.comet.exec.memoryPool.fraction` | `0.8`                                                              |

The run also enabled Comet's explain and fallback logging. Everything else is
left at Comet's defaults.

## Trino configuration

Trino is a useful reference point, but the setup differs from the other
engines in ways that matter:

- **Warm timings.** Each query runs once untimed, then the reported number
  is the mean of 3 timed iterations. The other engines include the cold
  first iteration in their mean.
- **In-memory exchanges.** Trino streams data between stages over the
  network and never writes shuffle files, while Ballista, Spark, and Comet
  materialize every shuffle on disk. Spilling was disabled, so a query
  that does not fit in memory would fail rather than slow down (none did).
- **Partition pruning.** Tables are external Hive tables over the same
  Parquet files with the partition columns declared, so Trino prunes
  partitions like Spark and Comet do.
- **No table statistics.** `ANALYZE` was not run, so the cost-based
  optimizer had no row counts or column statistics.
- **Queries.** The same SQL text as the Spark harness.

| Key                                                              | Value                                |
| ---------------------------------------------------------------- | ------------------------------------ |
| Workers                                                          | `32` (`include-coordinator=false`)   |
| `query.max-memory-per-node`                                      | `38GB`                               |
| `memory.heap-headroom-per-node`                                  | `6GB`                                |
| `spill-enabled`                                                  | `false`                              |
| `hive.metastore`                                                 | `file` (stored in S3)                |
| `hive.metastore-cache-ttl`, `hive.file-status-cache-expire-time` | `24h` (metadata cached across a run) |
| `hive.dynamic-filtering.wait-timeout`                            | `1s`                                 |

Everything else is left at Trino's defaults, with no session properties
set. No tuning was done beyond this, and the configuration has not been
reviewed by Trino experts.

## Queries

The SQLBench-H phrasing of the 22 TPC-H queries from
[apache/datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks).

The Spark harness uses the same queries with the same substitution
parameters. The only differences are in wording: it writes some dates as
arithmetic (for example `date '1998-12-01' - interval '90' day` instead of
`date '1998-09-02'`), and it phrases Q15 as a CTE instead of a view.

## Results

**Times in seconds; lower is better.** Ballista, Spark, and Spark + Comet:
mean of 2 iterations, including the cold first iteration. Trino: mean of 3
iterations after an untimed warm-up run (see
[Trino configuration](#trino-configuration)).

|     Query | Ballista (s) | Spark 4.1.3 (s) | Spark 4.1.3 + Comet 1.1.0-rc2 (s) | Trino 483 (s) |
| --------: | -----------: | --------------: | --------------------------------: | ------------: |
|         1 |        14.07 |           71.45 |                             10.66 |          5.32 |
|         2 |        25.81 |           37.98 |                             21.66 |         15.34 |
|         3 |        17.22 |           30.61 |                             16.05 |         13.47 |
|         4 |        15.31 |           23.86 |                              8.11 |          8.01 |
|         5 |        46.88 |           57.39 |                             38.32 |         25.98 |
|         6 |         1.59 |            1.73 |                              0.99 |          0.84 |
|         7 |        37.26 |           25.34 |                             16.60 |         16.19 |
|         8 |        20.13 |           58.17 |                             46.04 |         30.93 |
|         9 |        35.94 |           74.90 |                             55.78 |         41.71 |
|        10 |        43.36 |           35.31 |                             21.31 |         20.41 |
|        11 |        15.01 |           31.66 |                             14.79 |          5.73 |
|        12 |        10.49 |           13.24 |                              5.33 |          4.40 |
|        13 |        11.05 |           22.73 |                             11.31 |         10.56 |
|        14 |         5.54 |            7.25 |                              2.50 |          1.66 |
|        15 |        10.58 |           22.45 |                             11.85 |          8.30 |
|        16 |        12.78 |           24.34 |                              8.94 |          6.98 |
|        17 |        20.74 |           81.36 |                             25.14 |         24.76 |
|        18 |        45.63 |          134.11 |                             46.42 |         66.00 |
|        19 |        14.59 |           13.27 |                              9.07 |          5.93 |
|        20 |        14.46 |           16.08 |                              6.57 |          8.57 |
|        21 |        75.74 |           98.04 |                             68.43 |         83.22 |
|        22 |         8.25 |           16.90 |                              9.22 |          7.29 |
| **Total** |   **502.43** |      **898.17** |                        **455.09** |    **411.62** |

## Reproducing

The Ballista numbers come from the `tpch` runner in this repository. The Spark,
Spark + Comet, and Trino numbers were collected with a benchmark harness that
is not in this repository yet. We are working on adding scripts so that anyone
can reproduce all of these results.

### Ballista

Bring up the cluster (one scheduler, N executors), then run the suite from a
client. Executor sizing on each node:

```sh
ballista-executor \
  --bind-host 0.0.0.0 --bind-port 50051 \
  --scheduler-host <scheduler> --scheduler-port 50050 \
  --vcores 8 \
  --memory-pool-size 48103633715 \
  --work-dir /data \
  --grpc-server-max-decoding-message-size 134217728 \
  --grpc-server-max-encoding-message-size 134217728
```

`/data` should be a dedicated volume (e.g. a `gp3` PVC) sized for the
suite's shuffle output — see [Executor storage](#executor-storage).

Run all 22 queries with the `tpch` Rust runner from
[`benchmarks/`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks):

```sh
cargo run --release --bin tpch -- benchmark ballista \
  --host <scheduler> --port 50050 \
  --path s3://<bucket>/tpch/sf1000 --format parquet \
  --partitions 256 --iterations 2
```

The runner sets `target_partitions` from `--partitions` and enables
`collect_statistics`; everything else is left at its default.

### Spark and Spark + Comet

Until the harness is added, the closest public equivalent is
`tpcbench.py` from
[apache/datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks),
with the settings above and stock Spark 4.1 defaults for everything else:

```sh
spark-submit \
  --master <master> \
  --conf spark.executor.instances=32 \
  --conf spark.executor.cores=16 \
  --conf spark.executor.memory=64G \
  --conf spark.executor.memoryOverhead=10G \
  --conf spark.sql.shuffle.partitions=512 \
  tpcbench.py \
    --benchmark tpch \
    --data s3a://<bucket>/tpch/sf1000 \
    --format parquet \
    --iterations 2
```

For Spark + Comet, add the Comet JAR to the driver and executor classpath
(see the
[Comet installation guide](https://datafusion.apache.org/comet/user-guide/latest/installation.html))
and the settings in [Comet configuration](#comet-configuration):

```sh
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=32G \
  --conf spark.comet.exec.memoryPool.fraction=0.8
```

### Trino

The Trino numbers were collected with the same harness. It creates
external Hive tables over the Parquet files, syncs their partitions, and runs
each query once untimed and then three times, one query at a time, through the
Trino client, with the settings in [Trino configuration](#trino-configuration).

## Recording a new result set

- Pin the **exact commit** the numbers came from, not a branch name.
- Replace the version, environment, config, and results tables together — a
  row that mixes numbers from different commits silently misattributes a
  regression.
- Prefer a single continuous suite run: a long-lived executor deep into a
  suite is not in the same state as a freshly started one.
- Report `FAIL` for a query that ran but did not produce an answer, and
  `OOM` when the failure is a known memory exhaustion. See the tracker for
  open issues found by benchmarking: [#1359][aqe],
  [#2025][q18], [#2063][aqe-hang].

[aqe]: https://github.com/apache/datafusion-ballista/issues/1359
[q18]: https://github.com/apache/datafusion-ballista/issues/2025
[aqe-hang]: https://github.com/apache/datafusion-ballista/issues/2063
