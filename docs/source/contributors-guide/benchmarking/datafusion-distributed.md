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

# DataFusion Distributed Benchmark Configuration

How [DataFusion Distributed](https://github.com/datafusion-contrib/datafusion-distributed)
is run by the scripts in
[`benchmarks/cluster`](https://github.com/apache/datafusion-ballista/tree/main/benchmarks/cluster).
The cluster, data and queries are described in
[Benchmark Environment](environment.md). For what the project is and how it
differs from Ballista, see
[Ballista and DataFusion Distributed](../../about/datafusion-distributed-comparison.md).

## What is measured

DataFusion Distributed is a library, so there is no engine to install. The
project benchmarks itself with a small server binary, `worker`, from its own
repository (`benchmarks/remote-worker`), and so do these scripts. Every pod
runs it. One pod is the coordinator: it takes the SQL over HTTP, plans each
query, sends its stages to the workers and runs the last stage itself. It is
not in the workers' list, so it gets no tasks. The workers are found through a
headless Kubernetes Service that the coordinator resolves every second;
nothing registers anywhere.

## Differences from the other engines

- **In-memory exchanges.** Stages stream batches to each other over the
  network and never write shuffle files, like Trino and unlike Ballista, Spark
  and Comet.
- **A memory limit that upstream does not have.** The upstream `worker` gives
  DataFusion an unbounded memory pool, so a query can use all of a pod's memory
  and one that needs more gets the pod OOM-killed. That would make it the only
  engine here without a limit. The image therefore applies
  [`worker-memory-limit.patch`](https://github.com/apache/datafusion-ballista/blob/main/benchmarks/cluster/datafusion-distributed/worker-memory-limit.patch)
  before it builds the worker, which reads the pool size and a spill directory
  from the environment. Each pod gets a `FairSpillPool` of 70% of its container,
  the fraction a Ballista executor gets, and operators that can spill write to a
  volume of their own. `run.py` stops if the coordinator does not log its pool,
  so an image built without the patch cannot produce results that look limited.
- **A shared pool.** Ballista gives each task slot its own share of the pool.
  DataFusion Distributed runs a pod's tasks in one DataFusion runtime, so they
  share one pool, which divides it among the operators using it.
- **Warm timings.** Each query runs once untimed, then the reported number is
  the mean of 3 timed iterations, as for Trino. `--no-warmup --iterations 2`
  makes the protocol the same as Ballista's and Spark's.
- **Task counts come from the data.** The planner splits each scan into
  partitions of about 16 MiB of Parquet and spreads them over as many tasks as
  there are workers. A small query may run on the coordinator alone.
- **Partition pruning.** The tables are registered with
  `CREATE EXTERNAL TABLE` over the same Parquet files, with the partition
  columns declared when `tpch_partitioned` is set, so a filter on them skips
  whole directories at planning time.
- **Tables live in the coordinator's memory.** There is no metastore. The
  runner registers the tables at the start of every run, and again after a
  failed query in case the coordinator restarted.
- **Table checks.** DataFusion reads a column that a file does not have as
  NULL, so a misnamed column would not fail. The runner fails setup if a table
  has no rows, if a sample of 10,000 rows has a NULL (TPC-H has none), or if a
  partition value is still percent-encoded.
- **Queries.** The same files as Ballista, Spark and Trino. A query file with
  several statements (q15) is sent as one request per statement and only the
  `SELECT` is timed.

| Setting             | Value                                                                  |
| ------------------- | ---------------------------------------------------------------------- |
| Workers             | `32`, 8 CPUs and 64 GiB each                                           |
| Worker memory pool  | `FairSpillPool`, 70% of the container (44.8 GiB)                       |
| Worker spill volume | 200 GiB per pod, from `storage_class`                                  |
| Coordinator         | 1 pod, 8 CPUs and 32 GiB (`--dfd-coordinator-memory-gib`)              |
| Scan partition size | 16 MiB of Parquet (`distributed.file_scan_config_bytes_per_partition`) |
| Tasks per stage     | one per worker (`distributed.max_tasks_per_stage=0`)                   |

Everything else is the `worker` binary's own configuration, the same as in the
project's benchmarks. `--dfd-config KEY=VALUE` changes a DataFusion or
`distributed.*` setting for a run (it is run as `SET` on the coordinator).
`distributed.*` settings are sent to the workers with each query; other
DataFusion settings apply to planning on the coordinator.

## Reproducing

Build the image from the root of the repository. The context is the directory
that holds the patch, and `DFD_REF` is the branch, tag or commit of
datafusion-distributed to measure:

```sh
docker build -f benchmarks/cluster/docker/datafusion-distributed.Dockerfile \
  --build-arg DFD_REF=main \
  -t <registry>/datafusion-distributed-bench:main benchmarks/cluster/datafusion-distributed
```

Put the image in `cluster.toml` as `images.datafusion_distributed`, then:

```sh
cd benchmarks/cluster
python3 run.py --engine datafusion-distributed --executors 4 --query 6 --iterations 1 --no-warmup
python3 run.py --engine datafusion-distributed
```

The results record the datafusion-distributed commit that the workers report.
Each query's distributed plan, with the metrics of every node, is saved in
`results/<run>-plans/`.
