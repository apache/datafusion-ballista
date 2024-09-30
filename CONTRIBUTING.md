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
[good-first-issue](https://github.com/apache/arrow-kapot/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
list to help you get started.

# Developer's Guide

This section describes how you can get started with kapot development.

## Bootstrap Environment

kapot contains components implemented in the following programming languages:

- Rust (Scheduler and Executor processes, Client library)
- Python (Python bindings)
- Javascript (Scheduler Web UI)

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

### Rust Process Configuration

The scheduler and executor processes can be configured using toml files, environment variables and command-line
arguments. The specification for config options can be found here:

- [kapot/scheduler/scheduler_config_spec.toml](kapot/scheduler/scheduler_config_spec.toml)
- [kapot/executor/executor_config_spec.toml](kapot/executor/executor_config_spec.toml)

Those files fully define kapot's configuration. If there is a discrepancy between this documentation and the
files, assume those files are correct.

To get a list of command-line arguments, run the binary with `--help`

There is an example config file at [kapot/executor/examples/example_executor_config.toml](kapot/executor/examples/example_executor_config.toml)

The order of precedence for arguments is: default config file < environment variables < specified config file < command line arguments.

The executor and scheduler will look for the default config file at `/etc/kapot/[executor|scheduler].toml` To
specify a config file use the `--config-file` argument.

Environment variables are prefixed by `kapot_EXECUTOR` or `kapot_SCHEDULER` for the executor and scheduler
respectively. Hyphens in command line arguments become underscores. For example, the `--scheduler-host` argument
for the executor becomes `kapot_EXECUTOR_SCHEDULER_HOST`

### Python Environment

Refer to the instructions in the Python Bindings [README](./python/README.md)

### Javascript Environment

Refer to the instructions in the Scheduler Web UI [README](./kapot/scheduler/ui/README.md)

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
prettier -w README.md {kapot,kapot-cli,benchmarks,dev,docs,examples,python}/**/*.md
```
