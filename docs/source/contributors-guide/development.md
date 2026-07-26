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

# Ballista Development

We welcome participation from everyone and encourage you to join us, ask
questions, and get involved.

All participation in the Apache DataFusion Ballista project is governed by the
Apache Software Foundation's [code of
conduct](https://www.apache.org/foundation/policies/conduct.html).

## Development Environment

The easiest way to get started if you are using VSCode or IntelliJ IDEA is to open the provided [Dev Container](https://containers.dev/overview)
which will install all the required dependencies including Rust, Docker, Node.js and Yarn. A Dev Container is a
development environment that runs in a Docker container. It is configured with all the required dependencies to
build and test the project. It also includes VS Code and the Rust and Node.js extensions. Other supporting tools
that use Dev Containers can be seen [here](https://containers.dev/supporting)

To use the Dev Container, open the project in VS Code and then click the "Reopen in Container" button in the
bottom right corner of the IDE.

If you are not using the Dev Container or VScode, you will need to install these dependencies yourself.

- [Rust](https://www.rust-lang.org/tools/install)
- [Protobuf Compiler](https://protobuf.dev/downloads/) is required to build the project.
- [Node.js](https://nodejs.org/en/download/) is required to build the project.
- [Yarn](https://classic.yarnpkg.com/en/docs/install) is required to build the UI.
- [Docker](https://docs.docker.com/get-docker/) is required to run the integration tests.

## Build the project

From the root of the project, build release binaries.

```shell
cargo build --release
```

## Testing the project

```shell
cargo test
```

### TPC-H plan-stability golden files

The `tpch_plan_stability` test suite in the `ballista-scheduler` crate guards the shape of the
distributed physical plans that Ballista produces for the 22 TPC-H queries. For each query it plans
the SQL and compares the staged plan text — the stage boundaries (`=== Stage N ===` banners),
`ShuffleWriterExec` / `UnresolvedShuffleExec` nodes, and any exchange reuse — against an approved
"golden" file. The queries are planned against a synthetic statistics-only table, so the suite is
deterministic and runs without TPC-H data or a running cluster.

The golden files live in
[`ballista/scheduler/tests/tpch_plan_stability/approved/`](https://github.com/apache/datafusion-ballista/tree/main/ballista/scheduler/tests/tpch_plan_stability/approved)
(`q1.txt` through `q22.txt`). When a change alters distributed planning, the suite fails with a
plan-drift diff. Review the diff: if the change is unintended, fix the regression; if the new plans
are correct, regenerate the golden files:

```shell
./dev/update-tpch-plan-stability.sh
```

This runs the suite in generate mode
(`BALLISTA_GENERATE_GOLDEN=1 cargo test -p ballista-scheduler --test tpch_plan_stability`) and
rewrites the approved files. Review the resulting changes under
`ballista/scheduler/tests/tpch_plan_stability/approved/` and commit them together with the code
change that caused them.

## Running the examples

```shell
cd examples
cargo run --example standalone_sql --features=ballista/standalone
```

## Building the Python Client from Source

The Python client (`ballista` on PyPI) lives in the `python/` directory and is versioned and released
independently from the main Ballista project, so it is intentionally not part of the default Cargo
workspace. Building it from source uses [maturin](https://www.maturin.rs/) to compile the Rust
extension module and install it into a Python virtual environment.

### Prerequisites

- Python 3.10 or newer
- The Rust toolchain (see [Development Environment](#development-environment))
- [`protoc`](https://protobuf.dev/downloads/) on your `PATH`

All commands in this section are run from the `python/` directory:

```shell
cd python
```

### Create a virtual environment

Using `pip`:

```shell
python3 -m venv .venv
source .venv/bin/activate
pip3 install --upgrade pip
pip3 install -r requirements.txt
```

The `pip` upgrade is required because `maturin develop` invokes `pip install --group`, which
needs pip 25.1 or newer. The pip shipped with `python3 -m venv` is often older than that.

Using [`uv`](https://docs.astral.sh/uv/):

```shell
uv sync --dev --no-install-package ballista
```

### Build and install

`maturin develop` compiles the Rust extension and installs it into the active virtual environment
as an editable package, so subsequent Python imports pick up your local changes.

Using `pip`:

```shell
maturin develop            # debug build
maturin develop --release  # release build (slower to compile, faster at runtime)
```

Using `uv`:

```shell
uv run --no-project maturin develop --uv
```

To produce a wheel without installing it (for example, to share or publish):

```shell
uv run --no-project maturin build --release --strip
```

### Run the tests

Using `pip`:

```shell
python3 -m pytest
```

Using `uv`:

```shell
uv run --no-project pytest
```

For more detail on the underlying workflow, including tips for improving build speed, see the
[DataFusion Python contributor guide](https://datafusion.apache.org/python/contributor-guide/introduction.html).

## Benchmarking

For performance testing and benchmarking with TPC-H and other datasets, see the [benchmarks README](https://github.com/apache/datafusion-ballista/blob/main/benchmarks/README.md).

This includes instructions for:

- Generating TPC-H test data
- Running benchmarks against DataFusion and Ballista
- Comparing performance with Apache Spark
- Running load tests

## Upgrade notes

When a change breaks a public API, renames or removes a configuration key or CLI
flag, changes a default that alters query plans or results, or changes the
wire/serialization format, add an entry describing the change and the required
action to the current unreleased page under
[`docs/source/upgrading/`](../upgrading/index.rst). This keeps the upgrade guide
complete for the next release.
