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

Current TPC-H **SF1000** results for Ballista, compared against a vanilla
**Spark 3.4** baseline running on the same cluster shape.

## Versions under test

| Engine   | Version                                                                                                                          |
| -------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Ballista | [`696ca29b`](https://github.com/apache/datafusion-ballista/commit/696ca29b3c1fe3013238822b7f5d8cdb918fdaa3) (`main`, 2026-07-23), Cargo pkg `54.0.0`, DataFusion `54.1.0` |
| Spark    | 3.4 (vanilla, no acceleration plugin)                                                                                            |

## Environment

- **Cluster:** Kubernetes on AWS (`us-west-2`); one driver/scheduler pod and
  32 executor pods for each engine, launched on the same node pool.
- **Executor pod (Ballista):** x86_64, 8 vCPU, 32 GiB memory.
- **Executor pod (Spark):**    x86_64, 8 vCPU, 64 GiB + 10 GiB overhead.
- **Data:** TPC-H SF1000 Parquet on S3 (`us-west-2`), ZSTD compression,
  ~512 MiB row groups, one directory per table.

## Ballista configuration

| Flag / config key                                             | Value          |
| ------------------------------------------------------------- | -------------- |
| `--concurrent-tasks`                                          | `8`            |
| `--memory-pool-size` (bytes; ≈70 % of the 32 GiB container)   | `24051816858`  |
| `datafusion.execution.target_partitions`                      | `256`          |
| `datafusion.execution.collect_statistics`                     | `true`         |
| `datafusion.execution.listing_table_factory_infer_partitions` | `false`        |
| `datafusion.catalog.information_schema`                       | `true`         |
| `ballista.planner.adaptive.enabled`                           | `true` (AQE)   |
| `ballista.shuffle.sort_based.memory_limit_per_task_bytes`     | `0`            |

`datafusion.optimizer.prefer_hash_join` is left at its default; under AQE the
join strategy is selected at runtime by `DelayJoinSelectionRule` /
`DynamicJoinSelectionExec` from runtime statistics and the broadcast /
`ballista.optimizer.hash_join_max_build_partition_bytes` thresholds.

## Spark configuration (highlights)

Vanilla Spark 3.4 — no Comet plugin, stock `SortShuffleManager`.

| Key                                | Value                                       |
| ---------------------------------- | ------------------------------------------- |
| `spark.executor.instances`         | `32`                                        |
| `spark.executor.cores`             | `16` (task parallelism per executor)        |
| `spark.kubernetes.executor.limit.cores` / `.request.cores` | `8` (physical vCPU) |
| `spark.executor.memory`            | `64G`                                       |
| `spark.executor.memoryOverhead`    | `10G`                                       |
| `spark.memory.fraction`            | `0.6`                                       |
| `spark.memory.storageFraction`     | `0.2`                                       |
| `spark.sql.shuffle.partitions`     | `512`                                       |
| `spark.sql.broadcastTimeout`       | `900`                                       |
| `spark.serializer`                 | `KryoSerializer`                            |
| `spark.io.compression.codec`       | `zstd`                                      |

Spark AQE is left at its Spark 3.4 defaults. Shuffle spills to a `gp3`-backed
per-executor volume (`spark.kubernetes.executor.volumes...spark-local-dir-1`).

Note that `spark.executor.cores=16` is Spark's **task parallelism** setting,
not a CPU allocation — each executor pod is given only **8 physical vCPU**
via `spark.kubernetes.executor.limit.cores` / `.request.cores`, so Spark
schedules 16 concurrent tasks onto 8 physical cores (2× oversubscription).
The matching Ballista executor runs `--concurrent-tasks=8` on the same
8 physical vCPU (1:1).

## Queries

The SQLBench-H phrasing of the 22 TPC-H queries from
[apache/datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks).

## Results

**Times in seconds; lower is better.** Ballista: mean of 2 iterations. Spark:
mean of 3 iterations (cold iteration dropped by the harness). `FAIL` = query
did not complete on this Ballista configuration.

| Query | Ballista (s) | Spark 3.4 (s) |
| ----: | -----------: | ------------: |
|     1 |        19.55 |         67.58 |
|     2 |        32.84 |         29.80 |
|     3 |        40.52 |         25.13 |
|     4 |        27.98 |         21.19 |
|     5 |       202.84 |         54.12 |
|     6 |        12.25 |          1.23 |
|     7 |       253.60 |         19.57 |
|     8 |       281.02 |         48.60 |
|     9 |       323.02 |         69.38 |
|    10 |        66.71 |         35.92 |
|    11 |        29.76 |         30.88 |
|    12 |        23.74 |         10.78 |
|    13 |        15.46 |         20.45 |
|    14 |        25.53 |          7.00 |
|    15 |        24.83 |         23.75 |
|    16 |        16.25 |         23.41 |
|    17 |       161.60 |         82.30 |
|    18 |       399.72 |        129.40 |
|    19 |        23.43 |         11.26 |
|    20 |     **FAIL** |         19.22 |
|    21 |     **FAIL** |        101.53 |
|    22 |     **FAIL** |         12.71 |
| **Total (Q1–Q19)** | **1980.65** | **711.85** |

The total row sums Q1–Q19 only, because Ballista has no time for Q20–Q22 at
this commit.

## Reproducing

### Ballista

Bring up the cluster (one scheduler, N executors), then run the suite from a
client. Executor sizing on each node:

```sh
ballista-executor \
  --bind-host 0.0.0.0 --bind-port 50051 \
  --scheduler-host <scheduler> --scheduler-port 50050 \
  --concurrent-tasks 8 \
  --memory-pool-size 24051816858
```

Run all 22 queries:

```sh
tpch benchmark ballista \
  --host <scheduler> --port 50050 \
  --path s3://<bucket>/tpch/sf1000 --format parquet \
  --partitions 256 --iterations 2 \
  -c ballista.planner.adaptive.enabled=true \
  -c datafusion.execution.collect_statistics=true \
  -c ballista.shuffle.sort_based.memory_limit_per_task_bytes=0
```

### Spark

Runs the same queries via `tpcbench.py` from
[apache/datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks),
with the highlights above and stock Spark 3.4 defaults for everything else:

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
    --iterations 3
```

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
