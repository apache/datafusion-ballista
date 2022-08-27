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

## Connecting to a Cluster

The following code demonstrates how to create a Ballista context and connect to a scheduler.

```text
>>> import ballista
>>> ctx = ballista.BallistaContext("localhost", 50050)
```

## Registering Tables

Tables can be registered against the context by calling one of the `register` methods, or by executing SQL.

```text
>>> ctx.register_parquet("trips", "/mnt/bigdata/nyctaxi")
```

```text
>>> ctx.sql("CREATE EXTERNAL TABLE trips STORED AS PARQUET LOCATION '/mnt/bigdata/nyctaxi'")
```

## Executing Queries

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

The `collect` method executres the query and returns the results in
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
