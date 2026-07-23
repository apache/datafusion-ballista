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

Current TPC-H **SF1000** results for Ballista. This page records one result set
from one commit on one cluster shape; refresh the commit, cluster, and table
together.

## Version under test

| Component  | Value                                                                                                     |
| ---------- | --------------------------------------------------------------------------------------------------------- |
| Ballista   | [`696ca29b`](https://github.com/apache/datafusion-ballista/commit/696ca29b3c1fe3013238822b7f5d8cdb918fdaa3) (`main`, 2026-07-23) |
| Cargo pkg  | `54.0.0`                                                                                                  |
| DataFusion | `54.1.0`                                                                                                  |

## Environment

- **Cluster:** Kubernetes on AWS (`us-west-2`), one scheduler pod and 32 executor pods.
- **Executor pod:** x86_64, 8 vCPU, 32 GiB memory, on-demand nodes.
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

## Queries

The SQLBench-H phrasing of the 22 TPC-H queries from
[apache/datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks).

## Results

**Mean of 2 iterations, seconds.** `FAIL` = query did not complete on this
configuration.

| Query | Ballista (s) |
| ----: | -----------: |
|     1 |        19.55 |
|     2 |        32.84 |
|     3 |        40.52 |
|     4 |        27.98 |
|     5 |       202.84 |
|     6 |        12.25 |
|     7 |       253.60 |
|     8 |       281.02 |
|     9 |       323.02 |
|    10 |        66.71 |
|    11 |        29.76 |
|    12 |        23.74 |
|    13 |        15.46 |
|    14 |        25.53 |
|    15 |        24.83 |
|    16 |        16.25 |
|    17 |       161.60 |
|    18 |       399.72 |
|    19 |        23.43 |
|    20 |     **FAIL** |
|    21 |     **FAIL** |
|    22 |     **FAIL** |
| **Total (Q1–Q19)** | **1980.65** |

The total row sums Q1–Q19 only, because Q20–Q22 have no time at this commit.

## Reproducing

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
