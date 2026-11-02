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

Ballista support for DataFusion Python: run DataFusion Python queries on a
distributed Ballista cluster.

```bash
pip install ballista
```

User documentation is in the
[Python client user guide](https://datafusion.apache.org/ballista/user-guide/python/index.html).

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
> Some features are still being worked on, such as support for custom
> extension codecs. See the known limitations below.
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

# all other classes and functions are from
# the datafusion module
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

Python UDFs (scalar, aggregate and window) are serialised with the plan using `cloudpickle`, so
they do not need to be registered on the cluster. They require a scheduler and executors started
from Python (see [Scheduler and Executor](#scheduler-and-executor)), running the same Python minor
version as the client, with everything the UDF imports installed.

Known limitations and inefficiencies of the current approach:

- Anything requiring custom `datafusion_proto::logical_plan::LogicalExtensionCodec`.
- A cluster started from Python has no S3 support, so Python UDFs can not be used on S3 data.
- A Ballista connection will be created for each request.
- The scheduler address is only used when a query runs, so connection errors are raised by the
  first query, not by `BallistaSessionContext`.

### Example DataFrame Usage

```python
ctx = BallistaSessionContext("df://localhost:50050")
df = ctx.read_parquet('./testdata/test.parquet').filter(col("id") > lit(4)).limit(5)

pyarrow_batches = df.collect()
```

See the [DataFusion Python documentation](https://datafusion.apache.org/python/) for more examples.

## Jupyter Notebook Support

PyBallista provides first-class Jupyter notebook support with SQL magic commands and rich HTML rendering.

Install the Jupyter extras:

```bash
pip install "ballista[jupyter]"
```

DataFrames render as HTML tables, and the `ballista.jupyter` extension adds SQL magic commands
(`%ballista connect`, `%register`, `%sql`, `%%sql`), plan visualization (`explain_visual`) and
progress reporting (`collect_with_progress`):

```python
%load_ext ballista.jupyter
%ballista connect df://localhost:50050
%register parquet test_data ./testdata/test.parquet
%sql SELECT COUNT(*) FROM test_data
```

See [Jupyter Notebooks](https://datafusion.apache.org/ballista/user-guide/python/jupyter.html)
in the user guide for details.

### Example Notebooks

See the `examples/` directory for Jupyter notebooks demonstrating various features:

- `getting_started.ipynb` - Basic connection and queries
- `dataframe_api.ipynb` - DataFrame transformations
- `distributed_queries.ipynb` - Multi-stage distributed query examples

## Scheduler and Executor

Scheduler and executors can be configured and started from python code. They use
Python aware codecs, so unlike the Rust binaries they can run Python UDFs.

To start scheduler:

```python
from ballista import BallistaScheduler

scheduler = BallistaScheduler(bind_host="0.0.0.0", bind_port=50050)

scheduler.start()
scheduler.wait_for_termination()
```

For executor:

```python
from ballista import BallistaExecutor

executor = BallistaExecutor(
    bind_host="0.0.0.0",
    bind_port=50051,
    scheduler_host="localhost",
    scheduler_port=50050,
    vcores=8,
)

executor.start()
executor.wait_for_termination()
```

All constructor arguments are optional. `close()` stops a started process.

For tests and notebooks, `setup_test_cluster()` starts a scheduler and an executor in
the current process and returns the scheduler `(host, port)`.

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
