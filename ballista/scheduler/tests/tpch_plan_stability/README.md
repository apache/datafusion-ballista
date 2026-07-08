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
Query SQL is copied under `queries/`; tables are dataless providers with injected
SF100 cardinalities (`fixtures.rs`).

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
