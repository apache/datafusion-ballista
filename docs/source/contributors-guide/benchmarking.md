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

# Benchmarking

This page is a guide for contributors who want to measure the performance of a change, hunt down a regression, or compare Ballista against another engine. It covers what each benchmark in the repo is for, how to run it, how to read the output, and how to profile when something is unexpectedly slow.

The comprehensive setup notes for end-to-end TPC-H runs (data generation, docker-compose, Spark comparison) live in the [benchmarks README](https://github.com/apache/datafusion-ballista/blob/main/benchmarks/README.md). This page focuses on the contributor workflow.

## Choosing the right benchmark

| Goal | Benchmark | Where it lives |
|------|-----------|----------------|
| End-to-end query latency on TPC-H | `tpch` binary | `benchmarks/src/bin/tpch.rs` |
| End-to-end on a real-world dataset | `nyctaxi` binary | `benchmarks/src/bin/nyctaxi.rs` |
| Single-component shuffle write timing on Parquet input | `shuffle_bench` binary | `benchmarks/src/bin/shuffle_bench.rs` |
| Repeatable micro-benchmark for sort-shuffle (Criterion) | `sort_shuffle` Criterion bench | `benchmarks/benches/sort_shuffle.rs` |

A rough decision guide:

- **End-to-end SQL workloads.** If you are evaluating planner, scheduler, or whole-query changes, run the TPC-H binary. Pick a representative query rather than the full suite while iterating.
- **Hunting a perf regression in a single operator.** Use the closest binary or Criterion bench. The standalone shuffle binary is the right tool for shuffle-write changes because it isolates that stage from query planning, scheduling, and Flight transport.
- **Verifying a fix landed without regressing baseline.** Run the Criterion bench before and after with `--save-baseline` / `--baseline`. Criterion produces statistical confidence intervals, which the binaries do not.

## Generating TPC-H input

Both `tpch` and `shuffle_bench` need TPC-H Parquet files. Generate them with [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs):

```shell
cargo install tpchgen-cli
tpchgen-cli -s 10 --format parquet --output-dir /tmp/tpch-sf10
```

Increase `-s` for larger scale factors (each unit is ~1 GB of raw input). Use SF=1 for fast iteration, SF=10 or SF=100 to see meaningful throughput numbers.

## Running TPC-H

The full TPC-H runner lives in `benchmarks/src/bin/tpch.rs`. To run against an in-process DataFusion (no scheduler, no executor), use the `datafusion` subcommand:

```shell
cargo run --release --bin tpch -- benchmark datafusion \
    --query 1 \
    --path /tmp/tpch-sf10 \
    --format parquet \
    --iterations 3
```

To run against a real Ballista cluster, use `benchmark ballista` and pass `--host`/`--port` for the scheduler. See the [benchmarks README](https://github.com/apache/datafusion-ballista/blob/main/benchmarks/README.md) for the full distributed setup, including docker-compose and Spark comparison.

## Standalone shuffle benchmark

`shuffle_bench` drives the shuffle writer end-to-end on a Parquet input without going through the scheduler, which makes it the fastest way to iterate on shuffle-side changes and to capture flame graphs.

```shell
cargo run --release --bin shuffle_bench -- \
    --input /tmp/tpch-sf10/lineitem \
    --writer sort \
    --partitions 200 \
    --hash-columns 0
```

Useful flags:

- `--writer hash|sort` selects the hash-based or sort-based shuffle writer.
- `--memory-limit <bytes>` sets the runtime memory pool. With the sort writer this controls when spilling kicks in. Leave unset for an in-memory run.
- `--limit <rows>` caps rows read from Parquet, which is handy when iterating without waiting for full SF=10/100.
- `--iterations N --warmup M` runs `M` warmup iterations followed by `N` timed ones and prints min/avg/max.

The binary calls `env_logger::init()` at startup with a default `info` filter, so the sort-shuffle writer's per-spill log lines appear inline. Override with `RUST_LOG=debug` or `RUST_LOG=ballista_core=trace` for more detail.

## Sort-shuffle Criterion bench

`benchmarks/benches/sort_shuffle.rs` is a synthetic micro-benchmark with a fixed 100-column schema and an in-memory input. It measures sort-shuffle write throughput without I/O variability from Parquet. Run it with:

```shell
cargo bench --bench sort_shuffle
```

Use Criterion's baseline support to compare before and after a change:

```shell
git checkout main
cargo bench --bench sort_shuffle -- --save-baseline before

git checkout my-feature-branch
cargo bench --bench sort_shuffle -- --baseline before
```

The HTML report lands in `target/criterion/`.

## NYC taxi

`benchmarks/src/bin/nyctaxi.rs` runs a small set of analytical queries on the NYC taxi dataset. It is useful for reproducing user-reported issues that surface only on real-world data shapes. See the [benchmarks README](https://github.com/apache/datafusion-ballista/blob/main/benchmarks/README.md#nyc-taxi-benchmark) for download instructions.

## Reading metrics

Both binaries print DataFusion `ExecutionPlan` metrics for the last timed iteration. The shuffle binary's output for a sort-writer run looks like:

```
Shuffle metrics (last iteration):
  input_rows: 1000000
  output_rows: 1000000
  repart_time: 0.007s (0.1%)
  write_time: 0.323s (66.2%)
  spill_time: 0.000s (0.0%)
  spill_count: 0
  spill_bytes: 0
```

What to look at:

- **`input_rows` / `output_rows`** must match. A mismatch indicates rows were dropped or duplicated.
- **`write_time` percentage** is the share of wall-clock spent in IPC encoding plus disk write. A value near 100% on an unspilled run usually means the materialized batches are larger than they should be (e.g. uncompacted view arrays).
- **`spill_count`** counts spill events (one per memory-pressure flush), not per-partition batches. `spill_bytes` is the in-memory size of what was spilled, before compression.
- **`repart_time`** is the per-row hash partitioning cost. If this dominates, the regression is in `compute_partition_indices`, not the writer.

For end-to-end TPC-H runs, the per-stage metrics are accessible via the scheduler's REST API and via the explain plan. The [Metrics user guide](../user-guide/metrics.md) lists what each metric means.

## Profiling

When the binaries surface a slow path, capture a CPU profile to find the hot function. Both `cargo flamegraph` and `samply` work well for native Ballista code.

### cargo flamegraph

```shell
cargo install flamegraph

cargo flamegraph --release --bin shuffle_bench -- \
    --input /tmp/tpch-sf10/lineitem \
    --writer sort --partitions 200 --hash-columns 0
```

The SVG opens in a browser. Click into stack frames to drill down. On Linux, `flamegraph` requires `perf` and may need `kernel.perf_event_paranoid` lowered. On macOS it uses `dtrace`, which needs `sudo`.

### samply

[samply](https://github.com/mstange/samply) is a cross-platform alternative that uses the Firefox profiler UI. Build with debuginfo, then:

```shell
cargo install samply

cargo build --release --bin shuffle_bench
samply record ./target/release/shuffle_bench \
    --input /tmp/tpch-sf10/lineitem \
    --writer sort --partitions 200 --hash-columns 0
```

samply opens a browser with the loaded profile. The flame graph view is similar to `cargo flamegraph`, with the addition of a timeline that's helpful for spotting tail-latency events like spills.

### Tips for useful profiles

- Build with `--release` so the profile reflects production performance. Debug builds inline differently and produce misleading hot spots.
- Add `[profile.release] debug = "line-tables-only"` (or `debug = true`) to `Cargo.toml` if symbols are missing in the flame graph.
- Start with a short input (e.g. `--limit 1000000`) so the profile finishes quickly. Hot paths show up just as clearly with 1M rows as with 100M.
- If a single iteration is too short to sample reliably, use `--iterations 5 --warmup 1`.

## Writing new benchmarks

When adding a new benchmark, prefer a Criterion bench in `benchmarks/benches/` for anything that can be exercised on synthetic in-memory input. Reserve a binary in `benchmarks/src/bin/` for cases that need real Parquet files, end-to-end orchestration, or external configuration. Keep the synthetic data generator deterministic (seeded RNG) so results are reproducible across runs.
