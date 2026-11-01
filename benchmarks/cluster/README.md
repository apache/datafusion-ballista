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

# Comparing Ballista, Spark, Comet and Trino on Kubernetes

These scripts run TPC-H (and, for Ballista and Spark, TPC-DS) on a Kubernetes
cluster with one of four engines, the same way for each, so the results can be
compared. They reproduce the setup behind the results on the
[benchmarking page](../../docs/source/contributors-guide/benchmarking/index.md):

| `--engine` | What runs                                                                                         |
| ---------- | ------------------------------------------------------------------------------------------------- |
| `ballista` | A scheduler, N executors, and the `tpch` or `tpcds` runner from `benchmarks/`, all from one image |
| `spark`    | A driver Job running `spark/tpcbench.py` in client mode, which starts N executor pods             |
| `comet`    | The same, with Apache DataFusion Comet on the classpath and the settings in `conf/comet.conf`     |
| `trino`    | A coordinator, N workers, and a Job running `trino/run_queries.py` (TPC-H only)                   |

Each run deploys a fresh cluster for one engine, runs the queries from a Job
inside it, reads the results from the Job's log, writes them to `results/`, and
deletes everything it created. Every engine runs the query files in
`benchmarks/queries` (TPC-H) or `benchmarks/queries-tpcds` (TPC-DS), against the
same Parquet files, with the same number of executor pods of the same size.

`run.py` uses only the Python standard library and `kubectl`.

## What you need

- **A Kubernetes cluster** and `kubectl` pointing at it. The defaults reproduce
  the published shape: 32 executor pods of 8 CPUs each. Ballista executors and
  Trino workers have 64 GiB, Spark executors 74 GiB (64 GiB heap plus 10 GiB
  overhead) and Spark + Comet executors 106 GiB (plus 32 GiB off-heap). The
  driver or coordinator needs 16 CPUs and 64 GiB more. Use `--executors`,
  `--executor-cpus` and `--executor-memory-gib` for a smaller cluster.
- **A StorageClass for executor volumes** (recommended). Each Ballista and Spark
  executor gets a volume of its own for shuffle files, provisioned with the pod
  and deleted with it, so shuffle I/O does not compete with the node's root disk
  (see [Executor storage](../../docs/source/contributors-guide/benchmarking/environment.md#executor-storage)).
- **Python 3.11 or later** on the machine you run `run.py` from.
- **The data in S3**, one directory of Parquet files per table, such as
  `s3://my-bucket/tpch/sf1000/lineitem/`. The tables may be Hive-partitioned
  (`lineitem/l_shipdate=1994-01-01/`); Ballista and Spark find partition
  directories themselves, and Trino is told with `tpch_partitioned` in
  `cluster.toml`. Partitioning changes how much of the data date-filtered
  queries read, so compare engines on the same layout.
- **A service account** that can read the data, for example through IRSA on EKS,
  or a Secret with AWS credentials (`env_secret` in `cluster.toml`). The Spark
  driver uses it to create its executor pods, so it also needs the Role in
  [`rbac.example.yaml`](rbac.example.yaml).
- **Images** in a registry the cluster can pull from (see below). Trino uses the
  official `trinodb/trino` image.
- For Trino, **a writable S3 directory** for its file metastore, where it keeps
  the table definitions, partitions and statistics between runs.

## Building the images

Build the Ballista image from the root of the repository, at the commit you
want to measure. The scheduler, executor and runners in it all come from that
commit:

```shell
docker build -f benchmarks/cluster/docker/ballista.Dockerfile \
  --build-arg GIT_COMMIT=$(git rev-parse HEAD) \
  -t <registry>/ballista-bench:$(git rev-parse --short HEAD) .
```

The Spark image adds the S3 connector to the official Apache Spark image, and
the Comet image adds the Comet JAR to it. The JAR must match the Spark and Scala
version (see the
[Comet installation guide](https://datafusion.apache.org/comet/user-guide/latest/installation.html)):

```shell
cd benchmarks/cluster/docker
docker build -f spark.Dockerfile -t <registry>/spark-bench:4.1.3 .
docker build -f comet.Dockerfile \
  --build-arg SPARK_IMAGE=<registry>/spark-bench:4.1.3 \
  --build-arg COMET_JAR_URL=https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/<version>/comet-spark-spark4.1_2.13-<version>.jar \
  -t <registry>/comet-bench:4.1.3-<version> .
```

Push them, and put their names in `cluster.toml`.

## Configuring

Copy [`cluster.example.toml`](cluster.example.toml) to `cluster.toml` (which git
ignores) and fill it in: the namespace, service account, region, storage class,
node selector and tolerations, the data and metastore locations, and the images.
Everything about the benchmark itself is a `run.py` flag; see
`python3 run.py --help`.

The Spark settings are in [`conf/spark.conf`](conf/spark.conf) and
[`conf/comet.conf`](conf/comet.conf). `--spark-conf KEY=VALUE` overrides them for
one run.

## Running

Start with one query to check the setup, then run the suite. `--dry-run` prints
the manifests without applying them.

```shell
cd benchmarks/cluster
python3 run.py --engine ballista --query 6 --iterations 1
python3 run.py --engine ballista
python3 run.py --engine spark
python3 run.py --engine comet
python3 run.py --engine trino
```

Each command prints how to follow the queries with `kubectl logs`, then a table
of the results. A run takes as long as its queries plus a few minutes to start
the pods; the full TPC-H suite at SF1000 takes 20 to 40 minutes per engine on the
default cluster.

For TPC-DS, add `--benchmark tpcds` (Ballista, Spark and Comet). Its query files
q14, q23, q24 and q39 each hold two queries, which are timed together.

Some useful options:

```shell
# A smaller cluster
python3 run.py --engine ballista --executors 8 --executor-cpus 4 --executor-memory-gib 32

# A Ballista setting for the run (repeatable)
python3 run.py --engine ballista --ballista-config ballista.execution.mode=pipelined

# Ballista without the tables' partition columns
python3 run.py --engine ballista --no-partition-cols

# Keep the cluster after the run to look at its logs; run.py prints how to delete it
python3 run.py --engine spark --query 18 --keep
```

## How each engine is measured

All engines run one query at a time, `--iterations` times each, before going on
to the next query. The time is the wall-clock time the runner sees, from
submitting the query to receiving the last row. A query file with several
statements (q15 creates a view, selects from it, and drops it) is timed as a
whole.

- **Ballista, Spark and Comet** report the mean of 2 iterations, including the
  cold first one.
- **Trino** runs every query once untimed, then reports the mean of 3 iterations
  (`--no-warmup` turns the untimed run off). It runs without table statistics
  unless given `--trino-analyze`, and without spilling unless given
  `--trino-spill`. See [Trino configuration](../../docs/source/contributors-guide/benchmarking/trino.md)
  for why its numbers are not directly comparable.
- **Spark and Trino check the row count** of every TPC-H result at SF1000
  against [`tpch_expected_rows.py`](tpch_expected_rows.py), and fail a query that
  returns the wrong number of rows. `--no-check-rows` turns that off; other
  scale factors are not checked.

The settings of each engine, and why, are on the
[Ballista](../../docs/source/contributors-guide/benchmarking/ballista.md),
[Spark](../../docs/source/contributors-guide/benchmarking/spark.md),
[Spark + Comet](../../docs/source/contributors-guide/benchmarking/comet.md) and
[Trino](../../docs/source/contributors-guide/benchmarking/trino.md) pages.

## Results

Each run writes `results/<run>.json` and the runner's log next to it. The JSON
holds the run's settings, the image, the Ballista commit (from the image), and
per query the time of each iteration, their mean, the row count and any error.
`compare.py` prints a Markdown table of several runs, like the one on the
benchmarking page:

```shell
python3 compare.py results/tpch-ballista-*.json results/tpch-spark-*.json \
  --label Ballista --label "Spark 4.1.3"
```

When you publish results, follow
[Recording a new result set](../../docs/source/contributors-guide/benchmarking/index.md#recording-a-new-result-set).

## Tests

The tests need neither a cluster nor Spark:

```shell
cd benchmarks/cluster
python3 -m unittest -v test_run
(cd spark && python3 -m unittest -v test_tpcbench)
(cd trino && python3 -m unittest -v test_run_queries)
```

## Limitations

- The data must be in Amazon S3 (`s3://`).
- Trino runs TPC-H only: its table definitions are in `trino/tpch_tables.py`.
- Iceberg tables are not supported.
