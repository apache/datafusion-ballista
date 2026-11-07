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

# Ballista Python Bindings

Ballista provides Python bindings, allowing SQL and DataFrame queries to be executed
from Python. Like PySpark, you build a plan through SQL or a DataFrame API against
Parquet, CSV, JSON, and other file formats, run it in a distributed environment, and
collect the results back in Python.

## Installation

The `ballista` package is published on [PyPI](https://pypi.org/project/ballista/) and requires Python 3.10 or later. Installing into a virtual environment is recommended:

```bash
python -m venv .venv
source .venv/bin/activate
pip install ballista
```

or with [uv](https://docs.astral.sh/uv/):

```bash
uv add ballista          # in a uv project
uv pip install ballista  # in an existing environment
```

`ballista` depends on a matching `datafusion` release, for example `ballista` 55
requires `datafusion` 55, which is installed automatically. Queries are planned by DataFusion Python on the client and executed by the Ballista cluster, so the client should have the same major version as the cluster. To install a specific version:

```bash
pip install "ballista==55.*"
```

See [Versioning](../versioning.md) for the compatibility rules.

### Optional Extras

| Extra           | Install                                 | Provides                                                                                |
| --------------- | --------------------------------------- | --------------------------------------------------------------------------------------- |
| `jupyter`       | `pip install "ballista[jupyter]"`       | Jupyter magics, see [Jupyter Notebooks](jupyter.md)                                     |
| `python-client` | `pip install "ballista[python-client]"` | `BallistaClient` and `BallistaPythonSessionContext`, a cluster client written in Python |

### Verifying the Installation

Start an in-process cluster to check that the package and its native extension load:

```bash
python -c "from ballista import setup_test_cluster; print(setup_test_cluster())"
```

This prints the host and port of the test scheduler.

### Building from Source

If no wheel is available for your platform, or you want to use an unreleased version,
see the [contributors guide](../../contributors-guide/development.md) for building the
package from source with `maturin`.

## Connecting to a Cluster

`BallistaSessionContext` is the entry point for both remote and in-process clusters.

**Remote cluster** — connect to an already-running scheduler:

```python
from ballista import BallistaSessionContext

ctx = BallistaSessionContext("df://localhost:50050")
```

**In-process cluster** — start a scheduler and executor in the current Python process.
Useful for development, testing, and notebooks:

```python
from ballista import BallistaSessionContext, setup_test_cluster

host, port = setup_test_cluster()
ctx = BallistaSessionContext(f"df://{host}:{port}")
```

`BallistaSessionContext` returns a regular `datafusion.SessionContext`, so the whole
[DataFusion Python](https://datafusion.apache.org/python/) API is available. Any
operation which executes a plan, such as `collect`, `show`, `to_pandas` or
`write_parquet`, runs on the cluster.

The scheduler address is only used when a query is executed. Creating a context with
a wrong address succeeds, and the error is raised by the first query.

## Configuration

### Target Partitions

Set `datafusion.execution.target_partitions` to match your cluster capacity
(`executors × vcores_per_executor`) by passing `config` to the
constructor. Ballista sets the default to `16`, regardless of the client's CPU
count. A value set in a passed DataFusion `SessionConfig` is overridden with
the default, use `BallistaSessionConfig.with_target_partitions` or a dictionary
instead:

```python
from ballista import BallistaSessionConfig, BallistaSessionContext

executors = 4
vcores = 8
target_partitions = executors * vcores

ctx = BallistaSessionContext(
    "df://localhost:50050",
    config=BallistaSessionConfig().with_target_partitions(target_partitions),
)

# or
ctx = BallistaSessionContext(
    "df://localhost:50050",
    config={"datafusion.execution.target_partitions": target_partitions},
)
```

`config` can also be a dictionary of `datafusion.*` and `ballista.*` options, or a
DataFusion `SessionConfig`. It is applied to the local context, so settings consulted
during table registration (e.g. `datafusion.execution.listing_table_factory_infer_partitions`)
take effect before the plan is shipped, and it is propagated to the scheduler-side
session for distributed planning and execution.

This controls how many parallel tasks the scheduler creates per stage. Setting it too
low leaves executor capacity idle; setting it too high creates unnecessary scheduling
overhead.

### Ballista Settings

`BallistaSessionConfig` has typed setters for Ballista settings, and `set` for any
`datafusion.*` or `ballista.*` option. Unknown `ballista.*` keys raise `ValueError`:

```python
from ballista import BallistaSessionConfig, BallistaSessionContext

ctx = BallistaSessionContext(
    "df://localhost:50050",
    config=BallistaSessionConfig()
    .with_job_name("daily report")
    .with_adaptive_query_planner(True)
    .with_broadcast_join_threshold_bytes(10 * 1024 * 1024)
    .set("ballista.shuffle.compression.codec", "zstd"),
)
```

Ballista settings are registered as a DataFusion config extension, so they can be
changed with SQL `SET`:

```python
ctx.sql("SET ballista.job.name = 'nightly'")
```

Some DataFusion options are restricted by Ballista, for example
`datafusion.optimizer.prefer_hash_join`, and are always set to the Ballista value.
See [Configuration](../configs.md) for the available settings.

## SQL

### Registering Tables

Before running SQL queries, register tables with the context using a `register_*`
method or a `CREATE EXTERNAL TABLE` statement:

```python
ctx.register_parquet("trips", "/mnt/bigdata/trips")
```

```python
ctx.sql("CREATE EXTERNAL TABLE trips STORED AS PARQUET LOCATION '/mnt/bigdata/trips'")
```

Data in S3 needs credentials in two places. The client reads table schemas, so register an S3 object store on the context. The scheduler and executors read the data, so pass them the same settings with `SET s3.*`:

```python
from datafusion.object_store import AmazonS3

ctx.register_object_store("s3://", AmazonS3(bucket_name="my-bucket", region="us-west-2"))

ctx.sql("SET s3.region = 'us-west-2'")
ctx.sql("SET s3.access_key_id = '...'")
ctx.sql("SET s3.secret_access_key = '...'")

ctx.sql("CREATE EXTERNAL TABLE trips STORED AS PARQUET LOCATION 's3://my-bucket/trips/'")
```

The cluster processes can instead use the standard AWS environment variables. See [Querying S3 Data](cloud-storage.md) for all options.

### Executing Queries

The `sql` method returns a `DataFrame`. The query runs when you call an action like
`show` or `collect`:

```python
df = ctx.sql("SELECT count(*) FROM trips")
```

### Showing Query Results

```python
df.show()
+-----------------+
| COUNT(UInt8(1)) |
+-----------------+
| 9071244         |
+-----------------+
```

### Collecting Query Results

`collect` executes the query and returns the results as
[PyArrow](https://arrow.apache.org/docs/python/index.html) record batches:

```python
df.collect()
[pyarrow.RecordBatch
COUNT(UInt8(1)): int64]
```

### Viewing Query Plans

`explain` prints the logical, physical and distributed (per stage) plans for a
query, and `explain(analyze=True)` runs the query on the cluster and prints the
stages with runtime metrics:

```python
df.explain()
df.explain(analyze=True)
```

## DataFrame API

You can create a DataFrame from in-memory data using PyArrow record batches:

```python
from ballista import BallistaSessionContext, setup_test_cluster
from datafusion import col
import pyarrow

host, port = setup_test_cluster()
ctx = BallistaSessionContext(f"df://{host}:{port}")

batch = pyarrow.RecordBatch.from_arrays(
    [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
    names=["a", "b"],
)
df = ctx.create_dataframe([[batch]])

df = df.select(
    col("a") + col("b"),
    col("a") - col("b"),
)

result = df.collect()[0]

assert result.column(0) == pyarrow.array([5, 7, 9])
assert result.column(1) == pyarrow.array([-3, -3, -3])
```

## User Defined Functions

Python scalar, aggregate and window UDFs are serialized with the plan using
[cloudpickle](https://github.com/cloudpipe/cloudpickle), so they do not have to be
registered on the cluster:

```python
import pyarrow as pa
from datafusion import col, udf


def is_null(array: pa.Array) -> pa.Array:
    return array.is_null()


is_null_udf = udf(is_null, [pa.int64()], pa.bool_(), "stable", name="is_null_udf")

ctx.register_parquet("t", "./testdata/test.parquet")

df = ctx.table("t")
df.select(col("bigint_col"), is_null_udf(col("bigint_col")).alias("n")).show()

# or with SQL
ctx.register_udf(is_null_udf)
ctx.sql("SELECT bigint_col, is_null_udf(bigint_col) AS n FROM t").show()
```

UDFs are run by Python on the executors, so the cluster has to meet these requirements:

- The scheduler and executors are started from Python, with `BallistaScheduler`,
  `BallistaExecutor` or `setup_test_cluster`, see below. The Rust
  `ballista-scheduler` and `ballista-executor` binaries and the Docker images can not
  run Python UDFs.
- They run the same Python minor version as the client.
- Everything the UDF imports is installed on them.

### Trust Model

A cluster started from Python decodes UDFs with `cloudpickle.loads`, which can run
arbitrary Python code. Anyone who can reach the scheduler or executor ports can therefore
run arbitrary code on the cluster, as the user the processes run as. This is the same trust
model as PySpark.

Only expose such a cluster to trusted clients, for example by binding to `localhost` or a
private network, or restricting access with a firewall.

## Running a Cluster from Python

The scheduler and executors can be started from Python. This is needed for Python
UDFs, and is convenient for development.

```python
from ballista import BallistaScheduler

scheduler = BallistaScheduler(bind_host="127.0.0.1", bind_port=50050)
scheduler.start()
scheduler.wait_for_termination()
```

```python
from ballista import BallistaExecutor

executor = BallistaExecutor(
    bind_host="127.0.0.1",
    bind_port=50051,
    scheduler_host="localhost",
    scheduler_port=50050,
    vcores=8,
    work_dir="/tmp/ballista-executor",
)
executor.start()
executor.wait_for_termination()
```

All arguments are optional. `start` runs the process in the background, `wait_for_termination` blocks until it
stops, and `close` stops it. `work_dir` holds the executor's shuffle and spill files; when omitted, the executor
uses a temporary directory.
