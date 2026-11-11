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

# Trino Benchmark Configuration

How Trino 483 was configured for the TPC-H SF1000 results on the
[benchmarking page](index.md). The cluster, data and queries are described in
[Benchmark Environment](environment.md).

## Differences from the other engines

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

## Reproducing

The Trino numbers were collected with the same harness as the Spark numbers.
[`benchmarks/cluster`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks/cluster)
does the same with the settings on this page: it creates external Hive tables
over the Parquet files, syncs their partitions, and runs each query once
untimed and then three times, one query at a time, through Trino's HTTP
protocol:

```sh
cd benchmarks/cluster
python3 run.py --engine trino
```
