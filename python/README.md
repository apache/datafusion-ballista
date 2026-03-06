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

# Ballista

Ballista support for datafusion python.

This project is versioned and released independently from the main Ballista project and is intentionally not
part of the default Cargo workspace so that it doesn't cause overhead for maintainers of the main Ballista codebase.

## Creating a SessionContext

> [!IMPORTANT]
> Current approach is to support datafusion python API, there are know limitations of current approach,
> with some cases producing errors.
>
> We are trying to come up with the best approach to support ballista python interface.
>
> More details could be found at [#1142](https://github.com/apache/datafusion-ballista/issues/1142)

Creates a new context which connects to a Ballista scheduler process.

```python
from datafusion import col, lit
from datafusion import DataFrame
# we do not need datafusion context
# it will be replaced by BallistaSessionContext
# from datafusion import SessionContext
from ballista import BallistaSessionContext

# Change from:
#
# ctx = SessionContext()
#
# to: 

ctx = BallistaSessionContext("df://localhost:50050")

# all other functions and functions are from
# datafusion module
ctx.sql("create external table t stored as parquet location './testdata/test.parquet'")
df : DataFrame = ctx.sql("select * from t limit 5")

df.show()
```

Known limitations and inefficiencies of the current approach:

- The client's `SessionConfig` is not propagated to Ballista.
- Ballista-specific configuration cannot be set.
- Anything requiring custom `datafusion_proto::logical_plan::LogicalExtensionCodec`.
- No support for `UDF` as DataFusion Python does not serialise them.
- A Ballista connection will be created for each request.

### Example DataFrame Usage

```python
ctx = BallistaSessionContext("df://localhost:50050")
df = ctx.read_parquet('./testdata/test.parquet').filter(col(id) > lit(4)).limit(5)

pyarrow_batches = df.collect()
```

Check [DataFusion python](https://datafusion.apache.org/python/) provides more examples and manuals.

## Jupyter Notebook Support

PyBallista provides first-class Jupyter notebook support with SQL magic commands and rich HTML rendering.

### HTML Table Rendering

DataFrames automatically render as styled HTML tables in Jupyter notebooks:

```python
from ballista import BallistaSessionContext

ctx = BallistaSessionContext("df://localhost:50050")
df = ctx.sql("SELECT * FROM my_table LIMIT 10")
df  # Renders as HTML table via _repr_html_()
```

### SQL Magic Commands

For a more interactive SQL experience, load the Ballista Jupyter extension:

```python
# Load the extension
%load_ext ballista.jupyter

# Connect to a Ballista cluster
%ballista connect df://localhost:50050

# Check connection status
%ballista status

# List registered tables
%ballista tables

# Show table schema
%ballista schema my_table

# Execute a simple query (line magic)
%sql SELECT COUNT(*) FROM orders

# Execute a complex query (cell magic)
%%sql
SELECT
    customer_id,
    SUM(amount) as total
FROM orders
GROUP BY customer_id
ORDER BY total DESC
LIMIT 10
```

You can also store results in a variable:

```python
%%sql my_result
SELECT * FROM orders WHERE status = 'pending'
```

### Execution Plan Visualization

Visualize query execution plans directly in notebooks:

```python
df = ctx.sql("SELECT * FROM orders WHERE amount > 100")
df.explain_visual()  # Displays SVG visualization

# With runtime statistics
df.explain_visual(analyze=True)
```

> **Note:** Full SVG visualization requires graphviz to be installed (`brew install graphviz` on macOS).

### Progress Indicators

For long-running queries, use `collect_with_progress()` to see execution status:

```python
df = ctx.sql("SELECT * FROM large_table")
batches = df.collect_with_progress()
```

### Example Notebooks

See the `examples/` directory for Jupyter notebooks demonstrating various features:

- `getting_started.ipynb` - Basic connection and queries
- `dataframe_api.ipynb` - DataFrame transformations
- `distributed_queries.ipynb` - Multi-stage distributed query examples

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

Detailed development process explanation can be found in [datafusion python documentation](https://datafusion.apache.org/python/contributor-guide/introduction.html#).
[Improving build speed section](https://datafusion.apache.org/python/contributor-guide/introduction.html#improving-build-speed) can be relevant.

### Creating Virtual Environment

#### pip

```shell
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

#### uv

```shell
uv sync --dev --no-install-package ballista
```

### Developing & Building

#### pip

```shell
maturin develop
```

Note that you can also run `maturin develop --release` to get a release build locally.

#### uv

```shell
uv run --no-project maturin develop --uv
```

Or `uv run --no-project maturin build --release --strip` to get a release build.

### Testing

#### pip

```shell
python3 -m pytest
```

#### uv

```shell
uv run --no-project pytest
```
