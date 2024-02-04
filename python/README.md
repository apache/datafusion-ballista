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

# PyBallista

Minimal Python client for Ballista.

The goal of this project is to provide a way to run SQL against a Ballista cluster from Python and collect results.

The goal is not to provide the full DataFrame API. This could be added later if there is sufficient interest
from maintainers, and should just be a thin wrapper around the DataFusion Python bindings.

This project is versioned and released independently from the main Ballista project and is intentionally not
part of the default Cargo workspace so that it doesn't cause overhead for maintainers of the main Ballista codebase.

## Example Usage

```python
from pyballista import SessionContext
>>> ctx = SessionContext("localhost", 50050)
>>> ctx.sql("create external table t stored as parquet location '/mnt/bigdata/tpch/sf10-parquet/lineitem.parquet'")
>>> df = ctx.sql("select * from t limit 5")
>>> df.collect()
```

Output:

```python
[pyarrow.RecordBatch
 l_orderkey: int64 not null
l_partkey: int64 not null
l_suppkey: int64 not null
l_linenumber: int32 not null
l_quantity: decimal128(11, 2) not null
l_extendedprice: decimal128(11, 2) not null
l_discount: decimal128(11, 2) not null
l_tax: decimal128(11, 2) not null
l_returnflag: string not null
l_linestatus: string not null
l_shipdate: date32[day] not null
l_commitdate: date32[day] not null
l_receiptdate: date32[day] not null
l_shipinstruct: string not null
l_shipmode: string not null
l_comment: string not null
_ignore: string not null
                    ----
l_orderkey: [17500001,17500001,17500001,17500001,17500001]
l_partkey: [1661786,1635206,907114,1849041,820472]
l_suppkey: [36835,60223,32124,24096,20473]
l_linenumber: [2,3,4,5,6]
l_quantity: [50.00,26.00,25.00,14.00,33.00]
l_extendedprice: [87385.00,29669.12,28026.75,13859.30,45950.19]
l_discount: [0.09,0.09,0.08,0.08,0.06]
l_tax: [0.00,0.04,0.04,0.04,0.07]
l_returnflag: ["N","N","N","N","N"]
l_linestatus: ["O","O","O","O","O"]
...]
```

## Building

```shell
python3 -m venv venv
source venv/bin/activate
maturin develop
```
