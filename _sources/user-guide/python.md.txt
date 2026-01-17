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

Ballista provides Python bindings, allowing SQL and DataFrame queries to be executed from the Python shell.

Like PySpark, it allows you to build a plan through SQL or a DataFrame API against Parquet, CSV, JSON, and other
popular file formats files, run it in a distributed environment, and obtain the result back in Python.

## Connecting to a Cluster

The following code demonstrates how to create a Ballista context and connect to a scheduler.

If you are running a standalone cluster (runs locally), all you need to do is call the stand alone cluster method `standalone()` or your BallistaContext. If you are running a cluster in remote mode, you need to provide the URL `Ballista.remote("http://my-remote-ip:50050")`.

```text
>>> from ballista import BallistaBuilder
>>> # for a standalone instance
>>> # Ballista will initiate with an empty config
>>> # set config variables with `config()`
>>> ballista = BallistaBuilder()\
>>>    .config("ballista.job.name", "example ballista")
>>>
>>> ctx = ballista.standalone()
>>>
>>> # for a remote instance provide the URL
>>> ctx = ballista.remote("df://url-path-to-scheduler:50050")
```

## SQL

The Python bindings support executing SQL queries as well.

### Registering Tables

Before SQL queries can be executed, tables need to be registered with the context.

Tables can be registered against the context by calling one of the `register` methods, or by executing SQL.

```text
>>> ctx.register_parquet("trips", "/mnt/bigdata/nyctaxi")
```

```text
>>> ctx.sql("CREATE EXTERNAL TABLE trips STORED AS PARQUET LOCATION '/mnt/bigdata/nyctaxi'")
```

### Executing Queries

The `sql` method creates a `DataFrame`. The query is executed when an action such as `show` or `collect` is executed.

### Showing Query Results

```text
>>> df = ctx.sql("SELECT count(*) FROM trips")
>>> df.show()
+-----------------+
| COUNT(UInt8(1)) |
+-----------------+
| 9071244         |
+-----------------+
```

### Collecting Query Results

The `collect` method executes the query and returns the results in
[PyArrow](https://arrow.apache.org/docs/python/index.html) record batches.

```text
>>> df = ctx.sql("SELECT count(*) FROM trips")
>>> df.collect()
[pyarrow.RecordBatch
COUNT(UInt8(1)): int64]
```

### Viewing Query Plans

The `explain` method can be used to show the logical and physical query plans for a query.

```text
>>> df.explain()
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

## DataFrame

The following example demonstrates creating arrays with PyArrow and then creating a Ballista DataFrame.

```python
from ballista import BallistaBuilder
import pyarrow

# create a context
ctx = BallistaBuilder().standalone()

# create a RecordBatch and a new DataFrame from it
batch = pyarrow.RecordBatch.from_arrays(
    [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
    names=["a", "b"],
)
df = ctx.create_dataframe([[batch]])

# create a new statement
from datafusion import col
df = df.select(
    col("a") + col("b"),
    col("a") - col("b"),
)

# execute and collect the first (and only) batch
result = df.collect()[0]

assert result.column(0) == pyarrow.array([5, 7, 9])
assert result.column(1) == pyarrow.array([-3, -3, -3])
```

## User Defined Functions

The underlying DataFusion query engine supports Python UDFs but this functionality has not yet been implemented in
Ballista. It is planned for a future release. The tracking issue is [#173](https://github.com/apache/datafusion-ballista/issues/173).
