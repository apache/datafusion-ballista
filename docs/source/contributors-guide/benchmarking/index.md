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

Current TPC-H **SF1000** results for Ballista in its default blocking mode and in
the opt-in pipelined mode, compared against vanilla
**Spark 4.1.3** and **Spark 4.1.3 with Apache DataFusion Comet 1.1.0-rc2**,
running on the same cluster shape. A **Trino 483** column is included for
reference; it is not an apples-to-apples comparison (see
[Trino configuration](trino.md)).

## Versions under test

| Engine   | Version                                                                                                                                                                                                                                              |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ballista | [`cb49f30f`](https://github.com/apache/datafusion-ballista/commit/cb49f30feb95524befa9e44e3b21bdeeef449ed6) ([#2657](https://github.com/apache/datafusion-ballista/pull/2657), on top of `main` `98d724b3`), Cargo pkg `55.0.0`, DataFusion `55.2.0` |
| Spark    | `4.1.3` (`apache/spark:4.1.3` image, vanilla, no acceleration plugin)                                                                                                                                                                                |
| Comet    | [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) `1.1.0-rc2` ([`992c806a`](https://github.com/apache/datafusion-comet/commit/992c806a7e38c2e88bd018aa5774164b0850e1fa)) on the same Spark `4.1.3`                               |
| Trino    | `483` (`trinodb/trino:483` image, Hive connector)                                                                                                                                                                                                    |

## Results

**Times in seconds; lower is better.** Ballista, Spark, and Spark + Comet:
mean of 2 iterations, including the cold first iteration. Trino: mean of 3
iterations after an untimed warm-up run (see
[Trino configuration](trino.md)).

Both Ballista columns come from the same image, cluster and runner, one suite
run each; they differ only in `ballista.execution.mode` (and the push-staged
scheduling that pipelined mode requires). The 22 queries return the same
number of rows in both modes. The Spark, Comet and Trino columns are from
separate runs.

|     Query | Ballista blocking (s) | Ballista pipelined (s) | Spark 4.1.3 (s) | Spark 4.1.3 + Comet 1.1.0-rc2 (s) | Trino 483 (s) |
| --------: | --------------------: | ---------------------: | --------------: | --------------------------------: | ------------: |
|         1 |                 14.82 |                  13.76 |           71.45 |                             10.66 |          5.32 |
|         2 |                 36.27 |                  13.51 |           37.98 |                             21.66 |         15.34 |
|         3 |                 17.94 |                   7.26 |           30.61 |                             16.05 |         13.47 |
|         4 |                 10.92 |                   6.06 |           23.86 |                              8.11 |          8.01 |
|         5 |                 41.38 |                  13.87 |           57.39 |                             38.32 |         25.98 |
|         6 |                  1.68 |                   1.36 |            1.73 |                              0.99 |          0.84 |
|         7 |                 35.09 |                  18.73 |           25.34 |                             16.60 |         16.19 |
|         8 |                 21.62 |                  14.06 |           58.17 |                             46.04 |         30.93 |
|         9 |                 35.82 |                  24.25 |           74.90 |                             55.78 |         41.71 |
|        10 |                 45.09 |                  22.03 |           35.31 |                             21.31 |         20.41 |
|        11 |                 18.75 |                   4.38 |           31.66 |                             14.79 |          5.73 |
|        12 |                 10.13 |                   4.40 |           13.24 |                              5.33 |          4.40 |
|        13 |                 10.76 |                   5.84 |           22.73 |                             11.31 |         10.56 |
|        14 |                  7.01 |                   1.43 |            7.25 |                              2.50 |          1.66 |
|        15 |                 10.60 |                   6.93 |           22.45 |                             11.85 |          8.30 |
|        16 |                 13.25 |                   5.11 |           24.34 |                              8.94 |          6.98 |
|        17 |                 23.59 |                  15.41 |           81.36 |                             25.14 |         24.76 |
|        18 |                 55.45 |                  35.36 |          134.11 |                             46.42 |         66.00 |
|        19 |                 12.09 |                   7.43 |           13.27 |                              9.07 |          5.93 |
|        20 |                 15.63 |                   6.28 |           16.08 |                              6.57 |          8.57 |
|        21 |                 68.61 |                  45.91 |           98.04 |                             68.43 |         83.22 |
|        22 |                 10.54 |                   5.45 |           16.90 |                              9.22 |          7.29 |
| **Total** |            **517.04** |             **278.84** |      **898.17** |                        **455.09** |    **411.62** |

**Blocking** is the default: stages run one after another and exchange data
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
