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

# Versioning and Compatibility

Ballista version numbers have three parts, `X.Y.Z`. `X` is the major version of
[DataFusion](https://datafusion.apache.org/) that the release is built on, so Ballista `55.1.0` uses
DataFusion 55.

A new DataFusion major version is a breaking change for Ballista too. The Rust client hands you
DataFusion types such as `SessionContext`, and plans travel between the client, the scheduler and the
executors in DataFusion's serialization format, which can change between major versions. That is why
Ballista's major version follows DataFusion's.

## Releases

| Release | Example  | Contents                                                 |
| ------- | -------- | -------------------------------------------------------- |
| Major   | `56.0.0` | A new DataFusion major version, and any breaking changes |
| Minor   | `55.1.0` | Backward-compatible features and bug fixes               |
| Patch   | `55.1.1` | Bug fixes only                                           |

A new major version follows each DataFusion major release. Minor and patch releases are made from the
release branch of the current major version, such as `branch-55`, so features and fixes reach users
without waiting for the next DataFusion release. Only the newest release branch is maintained. There is
no fixed schedule: a minor release is proposed on the dev mailing list when the release branch has
enough changes to ship. The
[release process](https://github.com/apache/datafusion-ballista/blob/main/dev/release/README.md)
describes the steps.

Development happens on `main`. A build from `main` carries no compatibility guarantees, not even with
the release whose version number it reports.

## Compatibility within a major version

A minor or patch release is a drop-in replacement for the release before it on the same major version.
Upgrading is not expected to need changes to your code or configuration, apart from the two exceptions at
the end of this page, so these releases have no upgrade guide. The [changelog](../changelog/index.md)
lists what changed.

| Components             | Within a major version                            | Across major versions                |
| ---------------------- | ------------------------------------------------- | ------------------------------------ |
| Client and scheduler   | Any `X.x` client works with any `X.y` scheduler   | Not supported, the job is rejected   |
| Scheduler and executor | Any `X.x` scheduler works with any `X.y` executor | Not supported, upgrade both together |

You can therefore upgrade the schedulers, executors and clients of a cluster separately. A feature that
spans components takes effect once all of them are upgraded. These guarantees start with `55.0.0`;
earlier releases don't send the versions that the checks need.

The Python client, the `ballista` package on PyPI, follows the client rule: it needs the same major
version as the cluster. It is released separately because it depends on datafusion-python, which
publishes a new major version some weeks after DataFusion. Its version is the version of the Ballista
crates that it was built against, and it is released when it has changes to ship, so there is not a
Python release for every minor release. A `55.0.0` Python client works with a `55.1.0` cluster.

## What goes into a minor release

A change goes into a minor or patch release only if upgrading to it needs no action from users. In
practice, that means it would not need an entry in an [upgrade guide](../upgrading/index.rst).

These can be released in a minor release:

- Bug fixes
- Features that leave existing behavior alone, such as new configuration keys whose defaults keep
  today's behavior, new crates, binaries and endpoints, and new protocol fields that older components
  can safely ignore
- Performance improvements that do not change query results
- Documentation
- A newer DataFusion release of the same major version

These wait for the next major version:

- Breaking changes to the public Rust API, as defined by the
  [Cargo SemVer compatibility guide](https://doc.rust-lang.org/cargo/reference/semver.html)
- Removing or renaming a configuration key or command-line flag
- Changing a default in a way that alters query plans or results
- Changing the wire or serialization format in a way that an older component would misread, which
  includes any change to `BALLISTA_PROTOCOL_VERSION`
- Raising the minimum supported Rust version
- Moving to a new major version of a dependency that appears in the public API, such as `arrow` or
  `tonic`

Two exceptions apply:

- Adding a field to a configuration struct that implements `Default`, such as `SchedulerConfig` or
  `ExecutorProcessConfig`, is not treated as a breaking change. Build these structs with
  `..Default::default()`, or with their `with_*` methods where they have them, rather than with a full
  struct literal, so that new fields do not break your code.
- A fix for a default that produces wrong results can change that default in a minor release. `54.1.0`
  did this when it disabled the broadcast conversion of sort-merge joins by default. The changelog and
  the next major version's upgrade guide call out such a change.
