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

# Introduction

We welcome and encourage contributions of all kinds, such as:

1. Tickets with issue reports of feature requests
2. Documentation improvements
3. Code (PR or PR Review)

In addition to submitting new PRs, we have a healthy tradition of community members helping review each other's PRs.
Doing so is a great way to help the community as well as get more familiar with Rust and the relevant codebases.

You can find a curated
[good-first-issue](https://github.com/apache/datafusion-ballista/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
list to help you get started.

# Developer's Guide

This section describes how you can get started with Ballista development.

## Bootstrap Environment

Ballista contains components implemented in the following programming languages:

- Rust (Scheduler and Executor processes, Client library)

### Rust Environment

We use the standard Rust development tools.

- `cargo build`
- `cargo fmt` to format the code
- `cargo test` to test
- etc.

Testing setup:

- `rustup update stable` DataFusion uses the latest stable release of rust

Formatting instructions:

- [ci/scripts/rust_fmt.sh](ci/scripts/rust_fmt.sh)
- [ci/scripts/rust_clippy.sh](ci/scripts/rust_clippy.sh)
- [ci/scripts/rust_toml_fmt.sh](ci/scripts/rust_toml_fmt.sh)

or run them all at once:

- [dev/rust_lint.sh](dev/rust_lint.sh)

### Python Environment

Refer to the instructions in the Python Bindings [README](./python/README.md)

## Integration Tests

The integration tests can be executed by running the following command from the root of the repository.

```bash
./dev/integration-tests.sh
```

## How to format `.md` document

We are using `prettier` to format `.md` files.

You can either use `npm i -g prettier` to install it globally or use `npx` to run it as a standalone binary. Using `npx` required a working node environment. Upgrading to the latest prettier is recommended (by adding `--upgrade` to the `npm` command).

```bash
$ prettier --version
2.3.0
```

After you've confirmed your prettier version, you can format all the `.md` files:

```bash
prettier -w README.md {ballista,ballista-cli,benchmarks,dev,docs,examples,python}/**/*.md
```
