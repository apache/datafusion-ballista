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

# User Personas

Ballista serves several distinct audiences, each of which comes to the project
for a different reason and depends on a different set of capabilities. This page
names those audiences as **personas** and, for each one, records the guarantees
that audience relies on.

## Why this page exists

This is a review contract, not marketing copy. It exists so that changes to
Ballista stay **additive**:

- A pull request may **add** a new persona, or **extend** an existing one with
  new capabilities.
- A pull request must **not remove or silently regress** a capability that an
  existing persona depends on.

Reviewers evaluate every non-trivial pull request against this list. Each
persona lists concrete "red flags" — the kinds of change that would quietly take
functionality away from that audience. A red flag does not mean a change is
forbidden, but it does mean the change needs explicit discussion and maintainer
sign-off, and usually a migration path (for example an entry in the
[upgrade guide](../upgrading/index)), rather than landing silently.

These personas are deliberately in tension with one another. One audience wants
distributed execution to be a transparent implementation detail; another wants
the execution model exposed and tunable; another wants Ballista's internals to
be a stable, composable library. The point of keeping them all written down is
to make sure a change that delights one audience does not quietly break another.

## Persona 1: The DataFusion user going multi-node

- **Who**: A data engineer or analyst already using
  [DataFusion](https://datafusion.apache.org/) single-process (often through the
  Python bindings) who has outgrown a single machine.
- **Coming from**: DataFusion, single process.
- **Why Ballista**: Run the same SQL and DataFrame workloads across a cluster
  with minimal code change and _identical results_.
- **Depends on** (must not regress):
  - Query results and semantics match single-process DataFusion for the same
    input.
  - A client surface that mirrors DataFusion's `SessionContext` / DataFrame / SQL
    API.
  - `datafusion.*` session configuration overrides are honored end-to-end, so
    users tune DataFusion the way they already do.
  - Standard object stores and file formats work and produce correct results
    across partitions.
  - Distributed execution is _transparent_: getting a correct answer never
    requires the user to reason about stages, tasks, or partitioning.
- **Red flags in a pull request**:
  - Results diverge from single-process DataFusion for the same input.
  - `datafusion.*` overrides are dropped or ignored during scheduling/planning.
  - A DataFusion feature that is correct single-node becomes silently wrong when
    the plan is split across stages (for example dynamic filter pushdown,
    uncorrelated scalar subqueries, or a scan reading more than its assigned
    partition).
  - Client API changes that break the DataFusion-mirroring surface.

## Persona 2: The Spark user wanting the same execution model

- **Who**: A data engineer or platform owner running Spark SQL / batch jobs who
  wants a lighter, Rust-native alternative without relearning a new execution
  paradigm.
- **Coming from**: Apache Spark.
- **Why Ballista**: Keep the Spark mental model — plans split into **stages** at
  shuffle boundaries, one **task** per partition, shuffle files, executors with
  vcores, and adaptive query execution (**AQE**) for runtime adaptivity — on
  top of DataFusion and Arrow.
- **Depends on** (must not regress):
  - The stage/task execution model at shuffle boundaries.
  - The AQE / adaptive planner path (partition coalescing, dynamic join
    selection, runtime broadcast), available and gated behind configuration.
  - Broadcast joins for small build sides, to avoid shuffles and skew (the
    equivalent of Spark's broadcast hash join).
  - The operational surface Spark users expect: a scheduler plus executors,
    vcore concurrency, a tunable `target_partitions`, and stage/task
    observability (task timings, a history server / UI) for debugging skew.
  - Behavior driven by configuration and `SET`, not hard-coded planner choices.
- **Red flags in a pull request**:
  - Removing or short-circuiting the stage/task boundary model, or collapsing
    toward single-process execution.
  - Regressing or removing the AQE / adaptive planner path — or making it the
    only path in a way that breaks Persona 1's transparent static path.
  - Hard-coding a join or planner strategy instead of gating it behind
    configuration.
  - Removing the stage/task observability that Spark users rely on to diagnose
    skew.

## Persona 3: The library user building a specialized engine

- **Who**: An engineer using Ballista as a _framework_ — the foundation for a
  specialized or bespoke distributed query engine — rather than as a turnkey
  cluster.
- **Coming from**: Building on DataFusion as a library, now needing distribution
  (or replacing a hand-rolled distributed layer).
- **Why Ballista**: Reusable scheduler, executor, and plan-serialization
  building blocks with extension points, instead of writing distributed
  execution from scratch.
- **Depends on** (must not regress):
  - Stable, composable public APIs across the `scheduler`, `executor`, `client`,
    and `core` crates (the execution graph and plan serialization included).
  - Extension points that do not require forking: custom object stores,
    physical/logical extension codecs, custom operators and functions, a
    pluggable runtime producer, custom session configuration, and a custom query
    planner.
  - Behavior that stays _configurable rather than hard-coded_, so an embedder can
    swap the planner, join strategy, partitioning, or runtime environment.
  - Backward-compatible public APIs, with breaking changes signaled in the
    [upgrade guide](../upgrading/index).
  - No assumptions baked into the core that only fit the built-in clients or the
    TPC-H benchmark.
- **Red flags in a pull request**:
  - Breaking a public API signature without an upgrade-guide entry.
  - Hard-coding behavior that used to be pluggable, or removing an extension
    point (an extension-codec argument, the runtime-producer override,
    session-config injection).
  - Coupling core logic to a specific client, planner, or benchmark instead of
    keeping it behind a trait or configuration.
  - Wire-format or serialization changes that break externally-produced plans
    with no compatibility path.

## Adding a persona

This registry is append-only. As Ballista attracts new kinds of users, add a
persona for each one via a pull request, using the same schema (who they are,
what they are coming from, why Ballista, what they depend on, and the red flags
that would regress them). Do not remove existing personas: an audience that
relied on Ballista does not stop existing because our priorities shifted, and
the whole value of this page is that the guarantees only accumulate.
