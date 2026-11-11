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

# Spark + Comet Benchmark Configuration

How Spark 4.1.3 with [Apache DataFusion Comet](https://github.com/apache/datafusion-comet)
1.1.0-rc2 was configured for the TPC-H SF1000 results on the
[benchmarking page](index.md). The cluster, data and queries are described in
[Benchmark Environment](environment.md).

## Comet configuration

The Spark + Comet run adds these settings to the [Spark configuration](spark.md):

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

## Reproducing

Build the Comet image from `benchmarks/cluster/docker/comet.Dockerfile`, which
adds the Comet JAR to the Spark image (see the
[Comet installation guide](https://datafusion.apache.org/comet/user-guide/latest/installation.html)),
then run:

```sh
cd benchmarks/cluster
python3 run.py --engine comet
```

The settings above are in `benchmarks/cluster/conf/comet.conf`, on top of the
[Spark](spark.md) settings.
