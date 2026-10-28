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

This project is tracked under its own `Cargo.toml` and is intentionally not part of the default Cargo workspace so
that it doesn't cause overhead for maintainers of the main Ballista codebase. It is released separately from the Rust
crates, built against the `ballista` crates published on crates.io, once datafusion-python has published a matching
release. See the
[Python client release process](https://github.com/apache/datafusion-ballista/blob/main/python/dev/release/README.md).

## Creating a SessionContext

> [!IMPORTANT]
>
> Ballista supports the DataFusion Python API natively: `BallistaSessionContext`
> returns a standard `datafusion.SessionContext` and queries created from it
> are executed on the Ballista cluster.
>
> Some features are still being worked on, such as support for Python UDFs
> and custom extension codecs. See the known limitations below.
>
> Progress has been tracked at [#2618](https://github.com/apache/datafusion-ballista/issues/2618)
>

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

Session and Ballista settings are set with the `config` argument, either as a dictionary
or with `BallistaSessionConfig`. They are applied to the local session and propagated
to the cluster:

```python
from ballista import BallistaSessionConfig

ctx = BallistaSessionContext(
    "df://localhost:50050",
    config={"datafusion.execution.target_partitions": 256},
)

ctx = BallistaSessionContext(
    "df://localhost:50050",
    config=BallistaSessionConfig()
    .with_job_name("daily report")
    .with_target_partitions(256),
)
```

Known limitations and inefficiencies of the current approach:

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

### Install Jupyter extras first

```bash
pip install "ballista[jupyter]"
```

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

# Register .parquet table
%register parquet public.test_data_v1 ../testdata/test.parquet

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
from ballista.jupyter import explain_visual

df = ctx.sql("SELECT * FROM orders WHERE amount > 100")
explain_visual(df)  # Displays SVG visualization

# With runtime statistics
explain_visual(df, analyze=True)
```

> **Note:** Full SVG visualization requires graphviz to be installed (`brew install graphviz` on macOS).

### Progress Indicators

For long-running queries, use `collect_with_progress()` to see execution status.
In Jupyter it displays elapsed time while the query runs:

```python
from ballista.jupyter import collect_with_progress

df = ctx.sql("SELECT * FROM large_table")
batches = collect_with_progress(df)
```

Outside Jupyter, or to handle progress yourself, pass a callback:

```python
def my_callback(status: str, progress: float):
    # progress is -1.0 while running, 1.0 when completed
    print(f"{status}: {progress}")

batches = collect_with_progress(df, callback=my_callback, poll_interval=1.0)
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
