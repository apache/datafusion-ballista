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

# TPC-H plan-stability suite

Freezes each TPC-H query's **distributed staged plan** (static planner, SF100
table statistics, `target_partitions=16`) as an approved text file under
`approved/`. The suite fails if a code change alters plan shape — join strategy,
shuffle/stage boundaries, or broadcast decisions.

- Run: `cargo test -p ballista-scheduler --test tpch_plan_stability`
- Regenerate after an intended change: `dev/update-tpch-plan-stability.sh`
  (or `BALLISTA_GENERATE_GOLDEN=1 cargo test -p ballista-scheduler --test tpch_plan_stability`),
  then review the diff under `approved/`.

Scope: TPC-H only, static planner, Ballista default config (SortMergeJoin).
Tables are dataless providers with injected SF100 cardinalities (`fixtures.rs`).

Query SQL under `queries/` is copied verbatim from `benchmarks/queries/` so the
suite is self-contained (the scheduler crate does not depend on the benchmark
binary). The copies are a frozen snapshot; if `benchmarks/queries/` changes, update
these to match and regenerate the approved plans.

## CI coverage

This suite is registered as a `[[test]]` target in `ballista/scheduler/Cargo.toml`
(`tpch_plan_stability`), so it already runs wherever CI exercises the workspace's
default cargo tests — no dedicated job was added:

- `.github/workflows/rust.yml` → `linux-test` (`cargo test --profile ci
  --features=testcontainers`) and `macos-test` (`cargo test --profile ci
  --locked`) both run from the workspace root without `-p`/`--workspace`
  scoping. Since the root `Cargo.toml` sets no `default-members`, this tests
  every workspace member, including `ballista-scheduler`, which picks up this
  target automatically.
- `.github/workflows/rust.yml` → `clippy` already runs `cargo clippy
  --all-targets --package ballista-scheduler --all-features -- -D warnings`,
  which lints this test target too.
- `.github/workflows/rust.yml` → `lint` runs `cargo fmt --all -- --check`,
  which covers these files as well.

The generated `approved/*.txt` golden plans carry no license header (the test
compares their exact bytes), so they are excluded from the Apache RAT license
check in `.github/workflows/dev.yml` via
`ballista/scheduler/tests/tpch_plan_stability/approved/*` in
`dev/release/rat_exclude_files.txt` — mirroring the existing
`ballista/scheduler/testdata/*` exclusion. The `queries/*.sql` copies are covered
by the pre-existing `**/*.sql` RAT exclusion.
