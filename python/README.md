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

> [!IMPORTANT]
> Current approach is to support datafusion python API, there are know limitations of current approach,
> with some cases producing errors.
> We trying to come up with the best approach to support datafusion python interface.
> More details could be found at [#1142](https://github.com/apache/datafusion-ballista/issues/1142)

Creates a new context and connects to a Ballista scheduler process.

```python
from ballista import BallistaBuilder
>>> ctx = BallistaBuilder().standalone()
```

### Example SQL Usage

```python
>>> ctx.sql("create external table t stored as parquet location './testdata/test.parquet'")
>>> df = ctx.sql("select * from t limit 5")
>>> pyarrow_batches = df.collect()
```

### Example DataFrame Usage

```python
>>> df = ctx.read_parquet('./testdata/test.parquet').limit(5)
>>> pyarrow_batches = df.collect()
```

## Scheduler and Executor

Scheduler and executors can be configured and started from python code.

To start scheduler:

```python
from ballista import BallistaScheduler

scheduler = BallistaScheduler()

scheduler.start()
scheduler.wait_for_termination()
```

For executor:

```python
from ballista import BallistaExecutor

executor = BallistaExecutor()

executor.start()
executor.wait_for_termination()
```

## Development Process

### Creating Virtual Environment

```shell
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

### Building

```shell
maturin develop
```

Note that you can also run `maturin develop --release` to get a release build locally.

### Testing

```shell
python3 -m pytest
```
