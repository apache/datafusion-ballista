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

# Benchmark Environment

How the TPC-H SF1000 results on the [benchmarking page](index.md) were
collected: cluster, data, and queries. Per-engine settings are on the
[Ballista](ballista.md), [Spark](spark.md), [Spark + Comet](comet.md) and
[Trino](trino.md) pages.

## Cluster

- **Cluster:** Kubernetes on AWS (`us-west-2`); one driver/scheduler pod and
  32 executor pods for each engine, launched on the same node pool.
- **K8s worker nodes:** a mix of `r6i.24xlarge` (96 vCPU, 768 GiB memory) and
  `r6i.16xlarge` (64 vCPU, 512 GiB memory), EBS-only — no local
  instance-store. Pods are not pinned to an instance type.
- **Executor pod (Ballista):** x86_64, 8 vCPU, 64 GiB memory, plus a
  dedicated 1000 GiB `gp3` EBS PVC mounted at `/data` for the executor's
  shuffle work-dir (see Executor storage below).
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
  [Spark](spark.md).
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

## Queries

The SQLBench-H phrasing of the 22 TPC-H queries from
[apache/datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks).

The Spark harness uses the same queries with the same substitution
parameters. The only differences are in wording: it writes some dates as
arithmetic (for example `date '1998-12-01' - interval '90' day` instead of
`date '1998-09-02'`), and it phrases Q15 as a CTE instead of a view.
