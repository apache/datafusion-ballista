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

## Configuration

### Target Partitions

Set `datafusion.execution.target_partitions` to match your cluster capacity
(`executors × concurrent_tasks_per_executor`) by passing `cluster_config` to the
constructor. The default inherits from DataFusion and is based on the client's CPU
count, which is far too low for distributed execution:

```python
from ballista import BallistaSessionContext

executors = 4
concurrent_tasks = 8
target_partitions = executors * concurrent_tasks

ctx = BallistaSessionContext(
    "df://localhost:50050",
    cluster_config={
        "datafusion.execution.target_partitions": str(target_partitions),
    },
)
```

`cluster_config` is propagated to the scheduler-side session for distributed planning
and execution, and is also applied to the local context so settings consulted during
table registration (e.g. `datafusion.execution.listing_table_factory_infer_partitions`)
take effect before the plan is shipped.

This controls how many parallel tasks the scheduler creates per stage. Setting it too
low leaves executor capacity idle; setting it too high creates unnecessary scheduling
overhead.

## SQL

### Registering Tables

Before running SQL queries, register tables with the context using a `register_*`
method or a `CREATE EXTERNAL TABLE` statement:

```python
ctx.register_parquet("trips", "/mnt/bigdata/nyctaxi")
```

```python
ctx.sql("CREATE EXTERNAL TABLE trips STORED AS PARQUET LOCATION '/mnt/bigdata/nyctaxi'")
```

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

`explain` shows the logical and physical plans for a query:

```python
df.explain()
+---------------+-------------------------------------------------------------+
| plan_type     | plan                                                        |
+---------------+-------------------------------------------------------------+
| logical_plan  | Projection: #COUNT(UInt8(1))                                |
|               |   Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]         |
|               |     TableScan: trips projection=[VendorID]                  |
| physical_plan | ProjectionExec: expr=[COUNT(UInt8(1))@0 as COUNT(UInt8(1))] |
|               |   ProjectionExec: expr=[9071244 as COUNT(UInt8(1))]         |
|               |     EmptyExec: produce_one_row=true                         |
|               |                                                             |
+---------------+-------------------------------------------------------------+
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

The underlying DataFusion query engine supports Python UDFs but this has not yet been
implemented in Ballista. It is planned for a future release. See
[#173](https://github.com/apache/datafusion-ballista/issues/173) for status.
