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
running on the same cluster shape.

## Versions under test

| Engine   | Version                                                                                                                                                                                                                |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ballista | [`d6d8bd91`](https://github.com/apache/datafusion-ballista/commit/d6d8bd91fceaae4fb39624f6f1083a5f0ad78fbd) (`main`, 2026-10-03), Cargo pkg `55.0.0`, DataFusion `55.1.0`                                              |
| Spark    | `4.1.3` (`apache/spark:4.1.3` image, vanilla, no acceleration plugin)                                                                                                                                                  |
| Comet    | [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) `1.1.0-rc2` ([`992c806a`](https://github.com/apache/datafusion-comet/commit/992c806a7e38c2e88bd018aa5774164b0850e1fa)) on the same Spark `4.1.3` |

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
- **Driver (Spark and Spark + Comet):** runs the queries through an internal
  Spark benchmark harness. See [Spark and Spark + Comet](#spark-and-spark--comet)
  under Reproducing.
- **Client pod (Ballista):** the `tpch` Rust benchmark runner from
  [`benchmarks/`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks)
  in this repo (`cargo run --release --bin tpch -- benchmark ballista ...`),
  which submits SQL through a Ballista `SessionContext` and collects
  results locally.
- **Data:** TPC-H SF1000 Parquet on S3 (`us-west-2`), ZSTD compression,
  ~512 MiB row groups, one directory per table. `lineitem`, `orders`,
  `customer` and `part` are Hive-style partitioned (e.g.
  `lineitem/l_shipdate=YYYY-MM-DD/`). The `tpch` runner registers tables
  without partition columns, so Ballista prunes these scans only through
  Parquet statistics, while Spark and Comet apply partition filters. This
  favours Spark and Comet on date-filtered queries such as Q6, Q12, Q14
  and Q20.

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

## Queries

The SQLBench-H phrasing of the 22 TPC-H queries from
[apache/datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks).

The Spark harness uses the same queries with the same substitution
parameters. The only differences are in wording: it writes some dates as
arithmetic (for example `date '1998-12-01' - interval '90' day` instead of
`date '1998-09-02'`), and it phrases Q15 as a CTE instead of a view.

## Results

**Times in seconds; lower is better.** Every engine: mean of 2 iterations,
including the cold first iteration.

|     Query | Ballista (s) | Spark 4.1.3 (s) | Spark 4.1.3 + Comet 1.1.0-rc2 (s) |
| --------: | -----------: | --------------: | --------------------------------: |
|         1 |        13.94 |           71.45 |                             10.66 |
|         2 |        28.35 |           37.98 |                             21.66 |
|         3 |        25.29 |           30.61 |                             16.05 |
|         4 |         8.80 |           23.86 |                              8.11 |
|         5 |        63.94 |           57.39 |                             38.32 |
|         6 |         4.68 |            1.73 |                              0.99 |
|         7 |        45.85 |           25.34 |                             16.60 |
|         8 |        28.93 |           58.17 |                             46.04 |
|         9 |        37.64 |           74.90 |                             55.78 |
|        10 |        38.18 |           35.31 |                             21.31 |
|        11 |        15.87 |           31.66 |                             14.79 |
|        12 |        10.18 |           13.24 |                              5.33 |
|        13 |         9.78 |           22.73 |                             11.31 |
|        14 |         8.88 |            7.25 |                              2.50 |
|        15 |        10.61 |           22.45 |                             11.85 |
|        16 |        14.50 |           24.34 |                              8.94 |
|        17 |        19.56 |           81.36 |                             25.14 |
|        18 |        54.43 |          134.11 |                             46.42 |
|        19 |        10.65 |           13.27 |                              9.07 |
|        20 |        20.40 |           16.08 |                              6.57 |
|        21 |        91.31 |           98.04 |                             68.43 |
|        22 |         9.78 |           16.90 |                              9.22 |
| **Total** |   **571.55** |      **898.17** |                        **455.09** |

## Reproducing

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

The Spark and Spark + Comet numbers above were collected with an internal
benchmark harness, which isn't public. The closest public equivalent is
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
