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

TPC-H **SF1000** results on Kubernetes for Ballista in its default staged mode
and in the opt-in pipelined mode, **DataFusion Distributed**, vanilla
**Spark 4.1.3**, **Spark 4.1.3 with Apache DataFusion Comet 1.1.0** and
**Trino 483**, all on the same cluster shape. The Trino and DataFusion
Distributed columns are not apples-to-apples comparisons (see
[Trino configuration](trino.md) and
[DataFusion Distributed configuration](datafusion-distributed.md)).

## Where the numbers come from

- **Ballista and DataFusion Distributed** were run with the scripts in
  [`benchmarks/cluster`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks/cluster)
  in this repository, as they are, against the commits below. Anyone with a
  Kubernetes cluster, the same data and the settings on these pages can repeat
  them (see [Reproducing](#reproducing)).
- **Spark, Spark + Comet and Trino** were run earlier, with proprietary scripts
  that are not part of this repository. They used the same settings as the
  scripts here, but their own phrasing of the queries (see
  [Queries](environment.md#queries)), so these three columns cannot be
  reproduced exactly from this repository, and differences between them and the
  other columns come from the scripts as well as the engines.

## Versions under test

| Engine                 | Version                                                                                                                                                                                                            |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Ballista               | [`d9872080`](https://github.com/apache/datafusion-ballista/commit/d98720804369917151052e1c280a1dae711817fb), Cargo pkg `55.0.0`, DataFusion `55.2.0`                                                               |
| DataFusion Distributed | [`90350275`](https://github.com/datafusion-contrib/datafusion-distributed/commit/903502750b99839515aa8af9982f84f9c8c2ee5f), DataFusion `55`, with the memory-limit patch                                           |
| Spark                  | `4.1.3` (`apache/spark:4.1.3` image, vanilla, no acceleration plugin)                                                                                                                                              |
| Comet                  | [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) `1.1.0` ([`992c806a`](https://github.com/apache/datafusion-comet/commit/992c806a7e38c2e88bd018aa5774164b0850e1fa)) on the same Spark `4.1.3` |
| Trino                  | `483` (`trinodb/trino:483` image, Hive connector)                                                                                                                                                                  |

## Results

**Times in seconds; lower is better.** Ballista, Spark, and Spark + Comet:
mean of 2 iterations, including the cold first iteration. Trino and DataFusion
Distributed: mean of 3 iterations after an untimed warm-up run (see
[Trino configuration](trino.md) and
[DataFusion Distributed configuration](datafusion-distributed.md)). For
Ballista and DataFusion Distributed every query returned the expected row
count.

The two Ballista columns come from the same image, cluster and runner, one
suite run each; they differ only in `ballista.execution.mode` (and the
push-staged scheduling that pipelined mode requires). The other columns are
from separate runs.

<!-- BEGIN GENERATED BENCHMARK RESULTS -->

|     Query | Ballista staged (s) | Ballista pipelined (s) | DataFusion Distributed (s) | Spark 4.1.3 (s) | Spark 4.1.3 + Comet 1.1.0 (s) | Trino 483 (s) |
| --------: | ------------------: | ---------------------: | -------------------------: | --------------: | ----------------------------: | ------------: |
|         1 |               14.59 |                  12.19 |                       7.85 |           71.45 |                         10.66 |          5.32 |
|         2 |               31.83 |                  13.32 |                      21.29 |           37.98 |                         21.66 |         15.34 |
|         3 |               17.32 |                   7.97 |                       8.85 |           30.61 |                         16.05 |         13.47 |
|         4 |               11.39 |                   5.54 |                       6.10 |           23.86 |                          8.11 |          8.01 |
|         5 |               34.79 |                  17.39 |                      18.89 |           57.39 |                         38.32 |         25.98 |
|         6 |                1.90 |                   1.29 |                       1.47 |            1.73 |                          0.99 |          0.84 |
|         7 |               17.22 |                   8.27 |                      19.31 |           25.34 |                         16.60 |         16.19 |
|         8 |               16.57 |                  18.20 |                      23.87 |           58.17 |                         46.04 |         30.93 |
|         9 |               34.09 |                  29.99 |                      35.92 |           74.90 |                         55.78 |         41.71 |
|        10 |               39.89 |                  21.85 |                      19.93 |           35.31 |                         21.31 |         20.41 |
|        11 |                9.62 |                   3.12 |                       7.47 |           31.66 |                         14.79 |          5.73 |
|        12 |                7.65 |                   3.52 |                       4.40 |           13.24 |                          5.33 |          4.40 |
|        13 |                9.93 |                   6.44 |                       6.38 |           22.73 |                         11.31 |         10.56 |
|        14 |                6.83 |                   1.40 |                       2.53 |            7.25 |                          2.50 |          1.66 |
|        15 |               13.13 |                   6.83 |                       7.68 |           22.45 |                         11.85 |          8.30 |
|        16 |               12.15 |                   5.37 |                      76.41 |           24.34 |                          8.94 |          6.98 |
|        17 |               21.81 |                  17.80 |                      22.06 |           81.36 |                         25.14 |         24.76 |
|        18 |               35.10 |                  28.98 |                      34.84 |          134.11 |                         46.42 |         66.00 |
|        19 |                8.84 |                   7.67 |                       8.26 |           13.27 |                          9.07 |          5.93 |
|        20 |               14.50 |                   6.44 |                      17.77 |           16.08 |                          6.57 |          8.57 |
|        21 |               35.71 |                  32.09 |                      39.06 |           98.04 |                         68.43 |         83.22 |
|        22 |               10.16 |                   5.22 |                       5.51 |           16.90 |                          9.22 |          7.29 |
| **Total** |          **405.00** |             **260.90** |                 **395.83** |      **898.17** |                    **455.09** |    **411.62** |

<!-- END GENERATED BENCHMARK RESULTS -->

**Staged** is the default: stages run one after another and exchange data
through shuffle files, with adaptive query execution (AQE). **Pipelined**
places every task of a job up front and streams record batches between stages
over Arrow Flight, with no shuffle files and no AQE. See the
[Ballista configuration](ballista.md) for the settings.

## Reproducing

[`benchmarks/cluster`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks/cluster)
has the scripts to run all five engines on your own Kubernetes cluster with
the settings described on these pages. You provide the cluster, the data in
S3, and the images, which the directory has Dockerfiles for. The Ballista and
DataFusion Distributed columns were produced with:

```sh
cd benchmarks/cluster
python3 run.py --engine ballista
python3 run.py --engine ballista --ballista-config ballista.execution.mode=pipelined
python3 run.py --engine datafusion-distributed
```

The scripts run the queries in this repository on every engine, so the Spark,
Spark + Comet and Trino columns can be rerun with them too
(`--engine spark`, `--engine comet`, `--engine trino`).

## Details

How the cluster was set up and how each engine was configured. [Memory usage](memory.md)
compares how much executor memory Ballista, Spark and Spark + Comet need for one
query, measured on a smaller cluster:

```{toctree}
:maxdepth: 1

Environment <environment>
Memory usage <memory>
Ballista <ballista>
Spark <spark>
Spark + Comet <comet>
Trino <trino>
DataFusion Distributed <datafusion-distributed>
```

## Recording a new result set

- Results are stored as JSON in
  [`benchmarks/cluster/published/tpch-sf1000`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks/cluster/published/tpch-sf1000),
  one file per column of the table, in the format `run.py` writes plus a
  `label` for the column heading. Copy a run's file over the column it
  replaces, leaving out anything private such as image registry names, then
  run `python3 benchmarks/cluster/publish.py`. It regenerates the table on
  this page and the charts and speedups in the top-level README, so do not
  edit them by hand.
- Pin the **exact commit** the numbers came from, not a branch name.
- Replace the version, environment, config, and results tables together — a
  row that mixes numbers from different commits silently misattributes a
  regression.
- Prefer a single continuous suite run: a long-lived executor deep into a
  suite is not in the same state as a freshly started one.
- Report `FAIL` for a query that ran but did not produce an answer, and
  `OOM` when the failure is a known memory exhaustion (set the query's
  `error` to `OOM` in the JSON). See the tracker for
  open issues found by benchmarking: [#1359][aqe],
  [#2025][q18], [#2063][aqe-hang].

[aqe]: https://github.com/apache/datafusion-ballista/issues/1359
[q18]: https://github.com/apache/datafusion-ballista/issues/2025
[aqe-hang]: https://github.com/apache/datafusion-ballista/issues/2063
