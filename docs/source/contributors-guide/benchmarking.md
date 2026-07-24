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

| Engine   | Version                                                                                                                                                                   |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ballista | [`696ca29b`](https://github.com/apache/datafusion-ballista/commit/696ca29b3c1fe3013238822b7f5d8cdb918fdaa3) (`main`, 2026-07-23), Cargo pkg `54.0.0`, DataFusion `54.1.0` |
| Spark    | 3.4 (vanilla, no acceleration plugin)                                                                                                                                     |

## Environment

- **Cluster:** Kubernetes on AWS (`us-west-2`); one driver/scheduler pod and
  32 executor pods for each engine, launched on the same node pool.
- **K8s worker nodes:** `r6i.24xlarge` (96 vCPU, 768 GiB memory, 40 Gbps EBS
  bandwidth, EBS-only — no local instance-store).
- **Executor pod (Ballista):** x86_64, 8 vCPU, 64 GiB memory, plus a
  dedicated 1000 GiB `gp3` EBS PVC mounted at `/data` for the executor's
  shuffle work-dir (see [Executor storage](#executor-storage)).
- **Executor pod (Spark):** x86_64, 8 vCPU, 64 GiB + 10 GiB overhead, plus a
  dedicated `gp3` PVC via `spark-local-dir-1`.
- **Client pod (Ballista):** the Python benchmark runner submits SQL to
  Ballista via `BallistaSessionContext` and collects results locally.
  Requires 64 GiB memory limit to complete the full 22-query suite —
  smaller limits (16 GiB) OOM the client partway through, even though the
  executor cluster is healthy.
- **Data:** TPC-H SF1000 Parquet on S3 (`us-west-2`), ZSTD compression,
  ~512 MiB row groups, one directory per table.

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

| Flag / config key                                             | Value                                                                   |
| ------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `--concurrent-tasks`                                          | `8`                                                                     |
| `--memory-pool-size` (bytes; ≈70 % of the 64 GiB container)   | `48103633715`                                                           |
| `--work-dir`                                                  | `/data` (dedicated gp3 PVC — see [Executor storage](#executor-storage)) |
| `--grpc-server-max-decoding-message-size`                     | `134217728`                                                             |
| `--grpc-server-max-encoding-message-size`                     | `134217728`                                                             |
| `datafusion.execution.target_partitions`                      | `256`                                                                   |
| `datafusion.execution.collect_statistics`                     | `true`                                                                  |
| `datafusion.execution.listing_table_factory_infer_partitions` | `false`                                                                 |
| `datafusion.catalog.information_schema`                       | `true`                                                                  |
| `ballista.planner.adaptive.enabled`                           | `true` (AQE)                                                            |
| `ballista.shuffle.sort_based.memory_limit_per_task_bytes`     | `0`                                                                     |

`datafusion.optimizer.prefer_hash_join` is left at its default; under AQE the
join strategy is selected at runtime by `DelayJoinSelectionRule` /
`DynamicJoinSelectionExec` from runtime statistics and the broadcast /
`ballista.optimizer.hash_join_max_build_partition_bytes` thresholds.

The gRPC message-size ceiling is raised from the 16 MiB default to 128 MiB;
some SF1000 physical plans (Q11, Q21, Q22) encode above 16 MiB and hit
`OutOfRange` errors otherwise.

## Spark configuration (highlights)

Vanilla Spark 3.4 — no Comet plugin, stock `SortShuffleManager`.

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

**Times in seconds; lower is better.** Ballista: single iteration. Spark:
mean of 3 iterations (cold iteration dropped by the harness). `FAIL` = query
did not complete on this Ballista configuration.

|                              Query | Ballista (s) | Spark 3.4 (s) |
| ---------------------------------: | -----------: | ------------: |
|                                  1 |        21.71 |         67.58 |
|                                  2 |        26.73 |         29.80 |
|                                  3 |        34.77 |         25.13 |
|                                  4 |        20.73 |         21.19 |
|                                  5 |        42.26 |         54.12 |
|                                  6 |        13.78 |          1.23 |
|                                  7 |        47.17 |         19.57 |
|                                  8 |        45.74 |         48.60 |
|                                  9 |        67.27 |         69.38 |
|                                 10 |        52.99 |         35.92 |
|                                 11 |     **FAIL** |         30.88 |
|                                 12 |        25.97 |         10.78 |
|                                 13 |        15.30 |         20.45 |
|                                 14 |        21.13 |          7.00 |
|                                 15 |        27.30 |         23.75 |
|                                 16 |        17.38 |         23.41 |
|                                 17 |        47.29 |         82.30 |
|                                 18 |        78.32 |        129.40 |
|                                 19 |        18.71 |         11.26 |
|                                 20 |        97.13 |         19.22 |
|                                 21 |     **FAIL** |        101.53 |
|                                 22 |     **FAIL** |         12.71 |
| **Total (comparable subset, 19Q)** |   **721.68** |    **700.09** |

The total row sums Q1–Q10, Q12–Q20 — the queries that completed on both
engines. Q11, Q21, Q22 currently fail on Ballista with an `OutOfRange`
error (`decoded message length too large`); their physical plans encode
above the client's default 16 MiB gRPC ceiling. Bumping the ceiling to
128 MiB via `ballista.client.grpc_max_message_size` on the client side is
still under investigation — the scheduler and executor sides accept the
raised limit (see [Ballista configuration](#ballista-configuration)) but
the client-submission channel is separate and continues to enforce 16 MiB
in the current release.

Row counts agree across engines for every query Ballista returned.

## Reproducing

### Ballista

Bring up the cluster (one scheduler, N executors), then run the suite from a
client. Executor sizing on each node:

```sh
ballista-executor \
  --bind-host 0.0.0.0 --bind-port 50051 \
  --scheduler-host <scheduler> --scheduler-port 50050 \
  --concurrent-tasks 8 \
  --memory-pool-size 48103633715 \
  --work-dir /data \
  --grpc-server-max-decoding-message-size 134217728 \
  --grpc-server-max-encoding-message-size 134217728
```

`/data` should be a dedicated volume (e.g. a `gp3` PVC) sized for the
suite's shuffle output — see [Executor storage](#executor-storage).

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
