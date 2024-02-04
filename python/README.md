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

The goal of this project is to provide a way to run SQL against a Ballista cluster from Python and collect
results as PyArrow record batches.

Note that this client currently only provides a SQL API and not a DataFrame API. A future release will support
using the DataFrame API from DataFusion's Python bindings to create a logical plan and then execute that logical plan
from the Ballista context ([tracking issue](https://github.com/apache/arrow-ballista/issues/971)).

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

## Creating Virtual Environment

```shell
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

## Building

```shell
maturin develop
```

Note that you can also run `maturin develop --release` to get a release build locally.

## Testing

```shell
python3 -m pytest
```
