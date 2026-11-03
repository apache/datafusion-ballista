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

Current TPC-H **SF1000** results for Ballista in its default staged mode and in
the opt-in pipelined mode, compared against vanilla
**Spark 4.1.3** and **Spark 4.1.3 with Apache DataFusion Comet 1.1.0-rc2**,
running on the same cluster shape. A **Trino 483** column is included for
reference; it is not an apples-to-apples comparison (see
[Trino configuration](trino.md)).

## Versions under test

| Engine   | Version                                                                                                                                                                                                                 |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ballista | [`0ef7399e`](https://github.com/apache/datafusion-ballista/commit/0ef7399eeac239b867e46f1c9bcd23d8f071589e) ([#2657](https://github.com/apache/datafusion-ballista/pull/2657)), Cargo pkg `55.0.0`, DataFusion `55.2.0` |
| Spark    | `4.1.3` (`apache/spark:4.1.3` image, vanilla, no acceleration plugin)                                                                                                                                                   |
| Comet    | [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) `1.1.0-rc2` ([`992c806a`](https://github.com/apache/datafusion-comet/commit/992c806a7e38c2e88bd018aa5774164b0850e1fa)) on the same Spark `4.1.3`  |
| Trino    | `483` (`trinodb/trino:483` image, Hive connector)                                                                                                                                                                       |

## Results

**Times in seconds; lower is better.** Ballista, Spark, and Spark + Comet:
mean of 2 iterations, including the cold first iteration. Trino: mean of 3
iterations after an untimed warm-up run (see
[Trino configuration](trino.md)).

Both Ballista columns come from the same image, cluster and runner, one suite
run each; they differ only in `ballista.execution.mode` (and the push-staged
scheduling that pipelined mode requires). The Spark, Comet and Trino columns
are from separate runs.

|     Query | Ballista staged (s) | Ballista pipelined (s) | Spark 4.1.3 (s) | Spark 4.1.3 + Comet 1.1.0-rc2 (s) | Trino 483 (s) |
| --------: | ------------------: | ---------------------: | --------------: | --------------------------------: | ------------: |
|         1 |               11.43 |                  12.66 |           71.45 |                             10.66 |          5.32 |
|         2 |               31.70 |                  12.49 |           37.98 |                             21.66 |         15.34 |
|         3 |               17.32 |                   7.14 |           30.61 |                             16.05 |         13.47 |
|         4 |               11.07 |                   5.75 |           23.86 |                              8.11 |          8.01 |
|         5 |               41.90 |                  14.35 |           57.39 |                             38.32 |         25.98 |
|         6 |                1.26 |                   1.32 |            1.73 |                              0.99 |          0.84 |
|         7 |               35.09 |                  18.29 |           25.34 |                             16.60 |         16.19 |
|         8 |               20.34 |                  16.19 |           58.17 |                             46.04 |         30.93 |
|         9 |               32.81 |                  25.20 |           74.90 |                             55.78 |         41.71 |
|        10 |               37.13 |                  24.84 |           35.31 |                             21.31 |         20.41 |
|        11 |               15.95 |                   4.08 |           31.66 |                             14.79 |          5.73 |
|        12 |               10.26 |                   3.80 |           13.24 |                              5.33 |          4.40 |
|        13 |               10.67 |                   5.52 |           22.73 |                             11.31 |         10.56 |
|        14 |                6.71 |                   1.35 |            7.25 |                              2.50 |          1.66 |
|        15 |               10.35 |                   6.04 |           22.45 |                             11.85 |          8.30 |
|        16 |               13.52 |                   5.07 |           24.34 |                              8.94 |          6.98 |
|        17 |               23.21 |                  14.94 |           81.36 |                             25.14 |         24.76 |
|        18 |               56.45 |                  36.92 |          134.11 |                             46.42 |         66.00 |
|        19 |               10.88 |                   7.08 |           13.27 |                              9.07 |          5.93 |
|        20 |               13.83 |                   5.66 |           16.08 |                              6.57 |          8.57 |
|        21 |               62.41 |                  47.01 |           98.04 |                             68.43 |         83.22 |
|        22 |                9.63 |                   5.37 |           16.90 |                              9.22 |          7.29 |
| **Total** |          **483.91** |             **281.06** |      **898.17** |                        **455.09** |    **411.62** |

**Staged** is the default: stages run one after another and exchange data
through shuffle files, with adaptive query execution (AQE). **Pipelined**
places every task of a job up front and streams record batches between stages
over Arrow Flight, with no shuffle files and no AQE. See the
[Ballista configuration](ballista.md) for the settings.

## Reproducing

[`benchmarks/cluster`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks/cluster)
has the scripts to run all four engines on your own Kubernetes cluster with
the settings described on these pages. You provide the cluster, the data in
S3, and the images, which the directory has Dockerfiles for.

The Ballista numbers above come from the `tpch` runner in this repository. The
Spark, Spark + Comet, and Trino numbers were collected before these scripts
were added, with a harness that used the same settings but its own phrasing of
the queries (see [Queries](environment.md#queries)). The scripts run the
queries in this repository on every engine.

## Details

How the cluster was set up and how each engine was configured:

```{toctree}
:maxdepth: 1

Environment <environment>
Ballista <ballista>
Spark <spark>
Spark + Comet <comet>
Trino <trino>
DataFusion Distributed <datafusion-distributed>
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
