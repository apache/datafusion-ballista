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

# Spark Benchmark Configuration

How vanilla Spark 4.1.3 was configured for the TPC-H SF1000 results on the
[benchmarking page](index.md). The cluster, data and queries are described in
[Benchmark Environment](environment.md).

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

## Reproducing

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
