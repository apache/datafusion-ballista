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

# Ballista Benchmark Configuration

How Ballista was configured for the TPC-H SF1000 results on the
[benchmarking page](index.md). The cluster, data and queries are described in
[Benchmark Environment](environment.md).

## Ballista configuration

| Flag / config key                                           | Value                                                                |
| ----------------------------------------------------------- | -------------------------------------------------------------------- |
| `--vcores`                                                  | `8`                                                                  |
| `--memory-pool-size` (bytes; ≈70 % of the 64 GiB container) | `48103633715`                                                        |
| `--work-dir`                                                | `/data` (dedicated gp3 PVC — see [Executor storage](environment.md)) |
| `--grpc-server-max-decoding-message-size`                   | `134217728`                                                          |
| `--grpc-server-max-encoding-message-size`                   | `134217728`                                                          |
| `datafusion.execution.target_partitions`                    | `256`                                                                |
| `datafusion.execution.collect_statistics`                   | `true`                                                               |
| `ballista.planner.adaptive.enabled`                         | `true` (AQE; default)                                                |
| `ballista.shuffle.sort_based.memory_limit_per_task_bytes`   | `268435456` (256 MiB; default)                                       |

`datafusion.optimizer.prefer_hash_join` is left at its default; under AQE the
join strategy is selected at runtime by `DelayJoinSelectionRule` /
`DynamicJoinSelectionExec` from runtime statistics and the broadcast /
`ballista.optimizer.hash_join_max_build_partition_bytes` thresholds.

The gRPC message-size ceiling is raised from the 16 MiB default to 128 MiB;
some SF1000 physical plans (Q11, Q21, Q22) encode above 16 MiB and hit
`OutOfRange` errors otherwise.

## Reproducing

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
suite's shuffle output — see [Executor storage](environment.md).

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
