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
running on the same cluster shape. A **Trino 483** column is included for
reference; it is not an apples-to-apples comparison (see
[Trino configuration](trino.md)).

## Versions under test

| Engine   | Version                                                                                                                                                                                                                |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ballista | [`c76278cd`](https://github.com/apache/datafusion-ballista/commit/c76278cd48583504821ac4d71ea74b44e54ed4ea) (`main`, 2026-10-07), Cargo pkg `55.0.0`, DataFusion `55.1.0`                                              |
| Spark    | `4.1.3` (`apache/spark:4.1.3` image, vanilla, no acceleration plugin)                                                                                                                                                  |
| Comet    | [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) `1.1.0-rc2` ([`992c806a`](https://github.com/apache/datafusion-comet/commit/992c806a7e38c2e88bd018aa5774164b0850e1fa)) on the same Spark `4.1.3` |
| Trino    | `483` (`trinodb/trino:483` image, Hive connector)                                                                                                                                                                      |

## Results

**Times in seconds; lower is better.** Ballista, Spark, and Spark + Comet:
mean of 2 iterations, including the cold first iteration. Trino: mean of 3
iterations after an untimed warm-up run (see
[Trino configuration](trino.md)).

|     Query | Ballista (s) | Spark 4.1.3 (s) | Spark 4.1.3 + Comet 1.1.0-rc2 (s) | Trino 483 (s) |
| --------: | -----------: | --------------: | --------------------------------: | ------------: |
|         1 |        14.07 |           71.45 |                             10.66 |          5.32 |
|         2 |        25.81 |           37.98 |                             21.66 |         15.34 |
|         3 |        17.22 |           30.61 |                             16.05 |         13.47 |
|         4 |        15.31 |           23.86 |                              8.11 |          8.01 |
|         5 |        46.88 |           57.39 |                             38.32 |         25.98 |
|         6 |         1.59 |            1.73 |                              0.99 |          0.84 |
|         7 |        37.26 |           25.34 |                             16.60 |         16.19 |
|         8 |        20.13 |           58.17 |                             46.04 |         30.93 |
|         9 |        35.94 |           74.90 |                             55.78 |         41.71 |
|        10 |        43.36 |           35.31 |                             21.31 |         20.41 |
|        11 |        15.01 |           31.66 |                             14.79 |          5.73 |
|        12 |        10.49 |           13.24 |                              5.33 |          4.40 |
|        13 |        11.05 |           22.73 |                             11.31 |         10.56 |
|        14 |         5.54 |            7.25 |                              2.50 |          1.66 |
|        15 |        10.58 |           22.45 |                             11.85 |          8.30 |
|        16 |        12.78 |           24.34 |                              8.94 |          6.98 |
|        17 |        20.74 |           81.36 |                             25.14 |         24.76 |
|        18 |        45.63 |          134.11 |                             46.42 |         66.00 |
|        19 |        14.59 |           13.27 |                              9.07 |          5.93 |
|        20 |        14.46 |           16.08 |                              6.57 |          8.57 |
|        21 |        75.74 |           98.04 |                             68.43 |         83.22 |
|        22 |         8.25 |           16.90 |                              9.22 |          7.29 |
| **Total** |   **502.43** |      **898.17** |                        **455.09** |    **411.62** |

## Reproducing

The Ballista numbers come from the `tpch` runner in this repository. The Spark,
Spark + Comet, and Trino numbers were collected with a benchmark harness that
is not in this repository yet. We are working on adding scripts so that anyone
can reproduce all of these results. See the per-engine pages below for the
commands and settings.

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
