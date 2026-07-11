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

# Python Client Design

This page describes how the Ballista Python wheel (the `ballista` package)
is implemented and how it relates to
[`datafusion-python`](https://github.com/apache/datafusion-python).
It is intended for contributors working on the bindings; users should refer
to the [Python user guide](../user-guide/python.md).

## Goals and constraints

The Python client is intentionally not a full reimplementation of
DataFusion's Python API. It depends on `datafusion-python` for everything
related to building plans (`SessionContext`, `DataFrame`, expressions,
catalog access, I/O options) and only adds the pieces required to ship
those plans to a Ballista cluster for execution.

Two consequences of this design are worth highlighting up front:

- The Python crate (`pyballista`) is **versioned and released
  independently** from the rest of Ballista and is **excluded from the
  root Cargo workspace** (see the `exclude = [..., "python"]` line in
  the top-level `Cargo.toml`). It must pin to versions of `datafusion`,
  `datafusion-python`, and the published `ballista` crate that are
  mutually compatible — this is why the python crate's dependency
  versions can lag the main workspace.
- The wheel intercepts the standard `datafusion-python` API rather than
  defining its own. Users keep using `datafusion.DataFrame`, expressions,
  and write options — only the `SessionContext` is replaced.

## Crate and package layout

The relevant code lives under `python/`:

| Path | Role |
| ---- | ---- |
| `python/Cargo.toml` | Standalone Cargo project producing the `pyballista` cdylib. Depends on `ballista`, `ballista-core`, `ballista-executor`, `ballista-scheduler`, `datafusion`, `datafusion-proto`, and `datafusion-python` from crates.io. |
| `python/pyproject.toml` | Maturin build config. The Rust extension is built as `ballista._internal_ballista`; the pure-Python layer lives under `python/python/ballista/`. Runtime dependency on `datafusion` from PyPI. |
| `python/src/lib.rs` | PyO3 module entry point. Re-exports types from `datafusion-python` and exposes the bridge function. |
| `python/src/cluster.rs` | `BallistaScheduler` / `BallistaExecutor` PyO3 classes and the `setup_test_cluster` helper. |
| `python/src/utils.rs` | Shared Tokio runtime, `wait_for_future`, `spawn_feature`, error conversion. |
| `python/python/ballista/__init__.py` | Public surface — re-exports the Rust classes plus `BallistaSessionContext`, `DistributedDataFrame`, `ExecutionPlanVisualization`. |
| `python/python/ballista/extension.py` | The interception layer (metaclass + subclasses). |
| `python/python/ballista/jupyter.py` | IPython magics for `%ballista`, `%sql`, etc. |

## How execution is routed to Ballista

The wheel uses three mechanisms in combination: a metaclass that rewrites
methods on `SessionContext`, a subclass of `DataFrame` that holds the
remote address, and a single Rust bridge function that constructs a
Ballista-backed context on demand.

### 1. `BallistaSessionContext`

`BallistaSessionContext` (in `python/python/ballista/extension.py`)
subclasses `datafusion.SessionContext` and uses the
`RedefiningSessionContextMeta` metaclass:

```python
class BallistaSessionContext(SessionContext, metaclass=RedefiningSessionContextMeta):
    def __init__(self, address: str, config=None, runtime=None):
        super().__init__(config, runtime)
        self.address = address
        self.session_id_internal = super().session_id()
```

At class-creation time, the metaclass walks every method on
`SessionContext` whose return annotation is `"DataFrame"` and replaces
it with a wrapper that captures the result in a `DistributedDataFrame`:

```python
def __wrap_dataframe_result(func):
    def method_wrapper(*args, **kwargs):
        address = args[0].address
        session_id = args[0].session_id
        df = func(*args, **kwargs)
        return DistributedDataFrame(df, session_id, address)
    return method_wrapper
```

So `ctx.sql(...)`, `ctx.read_parquet(...)`, etc. all produce a
`DistributedDataFrame` while still using the regular `datafusion-python`
plan-construction code path underneath.

### 2. `DistributedDataFrame`

`DistributedDataFrame` extends `datafusion.DataFrame` and stores
`(address, session_id)` so that any execute call can later reach the
remote scheduler. Plan construction (`.filter`, `.select`, projections,
joins, …) continues to run locally inside `datafusion-python`; nothing
talks to Ballista yet.

The point of departure is `_to_internal_df`, called from every overridden
execution method (`write_csv`, `write_parquet[_with_options]`, etc.):

```python
def _to_internal_df(self):
    blob_plan = self.logical_plan().to_proto()
    df = create_ballista_data_frame(blob_plan, self.address, self._session_id)
    return df
```

It serializes the locally-built logical plan to protobuf via
`datafusion-python`'s `to_proto()` and hands the bytes to the Rust
bridge.

### 3. The Rust bridge

`create_ballista_data_frame` (in `python/src/lib.rs`) is the only place
where Ballista actually enters the picture:

```rust
let state = SessionStateBuilder::new_with_default_features()
    .with_session_id(session_id.to_string())
    .build();
let ctx = wait_for_future(py, SessionContext::remote_with_state(url, state))?;
let plan = logical_plan_from_bytes(plan_blob, &ctx.task_ctx())?;
Ok(datafusion_python::dataframe::PyDataFrame::new(
    DataFrame::new(ctx.state(), plan),
))
```

`SessionContext::remote_with_state` is the ballista-client extension
(`ballista/client/src/extension.rs`) that installs `BallistaQueryPlanner`.
The deserialized plan is wrapped in a `datafusion_python::PyDataFrame`
and returned to Python. When `.collect()` is called on it, execution
flows through `DistributedQueryExec` and out to the scheduler over
gRPC — see [Architecture](architecture.md) for the rest of the path.

## Cluster lifecycle from Python

`python/src/cluster.rs` provides `BallistaScheduler` and
`BallistaExecutor`. These are PyO3 classes that wrap the same process
entry points used by the standalone binaries:

- `BallistaScheduler.start` calls
  `ballista_scheduler::scheduler_process::start_server` after building
  a `BallistaCluster` from `SchedulerConfig`.
- `BallistaExecutor.start` calls
  `ballista_executor::executor_process::start_executor_process` with
  an `ExecutorProcessConfig`.

`setup_test_cluster` is a convenience used by the python test suite
(`python/python/tests/`). It spins up an in-process scheduler with
`new_standalone_scheduler` plus an executor via
`new_standalone_executor`, and returns the `(host, port)` of the
scheduler so tests can connect.

All async work shares a single Tokio runtime obtained from
`utils::get_tokio_runtime`, and `wait_for_future` releases the GIL
(`py.detach`) while blocking on it.

## Why the documented limitations exist

The
[python README](https://github.com/apache/datafusion-ballista/blob/main/python/README.md)
lists known limitations of the current approach. They are direct
consequences of the design above:

| Limitation | Cause |
| ---------- | ----- |
| Client `SessionConfig` is not propagated to Ballista | `create_ballista_data_frame` builds a fresh `SessionStateBuilder::new_with_default_features()` server-side and only carries `session_id` across. |
| Ballista-specific configuration cannot be set | There is no path for the Python `SessionConfig` to influence the Ballista-side `SessionState`. |
| No support for custom `LogicalExtensionCodec` | The bridge uses the default codec implicitly via `logical_plan_from_bytes`. |
| No support for Python UDFs | The bridge ships only the serialized logical plan; `datafusion-python` does not serialize Python UDFs into the proto representation. |
| A Ballista connection is created per request | `_to_internal_df` calls `SessionContext::remote_with_state(url, …)` on every terminal operation. |

A redesign is being tracked in
[#1142](https://github.com/apache/datafusion-ballista/issues/1142). See
also [#173](https://github.com/apache/datafusion-ballista/issues/173)
for the Python UDF tracking issue.

## Building and testing locally

The python crate is not part of the workspace, so it must be built from
its own directory using `maturin`:

```sh
cd python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
maturin develop                   # debug build into the active venv
python3 -m pytest                 # run the python test suite
```

`uv` is also supported — see the
[python README](https://github.com/apache/datafusion-ballista/blob/main/python/README.md#development-process)
for the equivalent commands.
