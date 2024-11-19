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

Python client for Ballista.

This project is versioned and released independently from the main Ballista project and is intentionally not
part of the default Cargo workspace so that it doesn't cause overhead for maintainers of the main Ballista codebase.

## Creating a SessionContext

Creates a new context and connects to a Ballista scheduler process.

```python
from ballista import BallistaBuilder
>>> ctx = BallistaBuilder().standalone()
```

## Example SQL Usage

```python
>>> ctx.sql("create external table t stored as parquet location '/mnt/bigdata/tpch/sf10-parquet/lineitem.parquet'")
>>> df = ctx.sql("select * from t limit 5")
>>> pyarrow_batches = df.collect()
```

## Example DataFrame Usage

```python
>>> df = ctx.read_parquet('/mnt/bigdata/tpch/sf10-parquet/lineitem.parquet').limit(5)
>>> pyarrow_batches = df.collect()
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
