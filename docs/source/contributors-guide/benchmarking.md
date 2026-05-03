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

This page is a guide for contributors who want to measure the performance of a change against TPC-H, hunt down a regression, or compare Ballista against another engine. It covers how to generate input data, how to run the TPC-H benchmark binary against in-process DataFusion or against a Ballista cluster, how to read the metrics, and how to profile when something is unexpectedly slow.

The comprehensive setup notes for end-to-end TPC-H runs (docker-compose, Spark comparison, load testing) live in the [benchmarks README](https://github.com/apache/datafusion-ballista/blob/main/benchmarks/README.md). This page focuses on the contributor workflow.

## Generating TPC-H input

Generate TPC-H Parquet files with [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs):

```shell
cargo install tpchgen-cli
tpchgen-cli -s 10 --format parquet --output-dir /tmp/tpch-sf10
```

Increase `-s` for larger scale factors (each unit is ~1 GB of raw input). Use SF=1 for fast iteration, SF=10 or SF=100 to see meaningful throughput numbers.

## Running TPC-H

The TPC-H runner lives in `benchmarks/src/bin/tpch.rs`. To run against an in-process DataFusion (no scheduler, no executor), use the `datafusion` subcommand:

```shell
cargo run --release --bin tpch -- benchmark datafusion \
    --query 1 \
    --path /tmp/tpch-sf10 \
    --format parquet \
    --iterations 3
```

To run against a real Ballista cluster, use `benchmark ballista` and pass `--host`/`--port` for the scheduler. See the [benchmarks README](https://github.com/apache/datafusion-ballista/blob/main/benchmarks/README.md) for the full distributed setup, including docker-compose and Spark comparison.

While iterating, pick a representative single query rather than running the full suite. Common picks: `--query 1` (aggregation-heavy), `--query 5` (joins), `--query 22` (correlated subquery). Use `--iterations 3` so you can look at min/avg/max and ignore the first cold run.

## Setting session configs

Both subcommands accept repeatable `-c key=value` (or `--config key=value`) flags to override DataFusion or Ballista session config keys. Each occurrence of `-c` overrides one key:

```shell
cargo run --release --bin tpch -- benchmark datafusion \
    --query 1 --path /tmp/tpch-sf10 --format parquet --iterations 3 \
    -c datafusion.execution.target_partitions=16 \
    -c datafusion.execution.batch_size=16384
```

Common keys worth tuning when benchmarking:

| Key | Effect |
|-----|--------|
| `datafusion.execution.target_partitions` | Number of partitions DataFusion uses for parallel execution |
| `datafusion.execution.batch_size` | Rows per batch flowing through operators |
| `datafusion.execution.collect_statistics` | Whether to collect file-level statistics during planning |
| `ballista.shuffle.sort_based.enabled` | Use the sort-based shuffle writer (default: true) |
| `ballista.shuffle.sort_based.batch_size` | Rows per batch when materializing buffered shuffle output |
| `ballista.shuffle.max_concurrent_read_requests` | Concurrent shuffle-read requests per executor |

`benchmark ballista` propagates these to the executors via the session config, so an override set on the client takes effect on the workers. `benchmark datafusion` applies them to the in-process `SessionContext` directly.

Unknown keys are skipped with a `Warning: could not set config '...'` message rather than failing the run, so check stdout if a config you set seems to have no effect.

## Reading metrics

The TPC-H binary prints per-iteration timings and per-query results. For a deeper look at where time is going inside a stage, ask DataFusion for the executed physical plan with metrics. The scheduler's REST API exposes per-stage metrics for distributed runs, and an explain plan with `analyze` shows them for the in-process runs. The [Metrics user guide](../user-guide/metrics.md) lists what each metric name means.

What to look at first when chasing a regression:

- **`input_rows` / `output_rows`** at each stage. A mismatch usually means a planner change altered semantics, not just performance.
- **`elapsed_compute`** vs **wall time** at the operator level. A wide gap implies the operator is waiting on input (upstream is the bottleneck), not doing work itself.
- **Shuffle stage timings** (`write_time`, `spill_count`, `spill_bytes`) when shuffle stages dominate. Spilling has a large effect on tail latency.

## Profiling

When the TPC-H binary surfaces a slow query, capture a CPU profile to find the hot function. Both `cargo flamegraph` and `samply` work well for native Ballista code.

### cargo flamegraph

```shell
cargo install flamegraph

cargo flamegraph --release --bin tpch -- benchmark datafusion \
    --query 1 \
    --path /tmp/tpch-sf10 \
    --format parquet \
    --iterations 3
```

The SVG opens in a browser. Click into stack frames to drill down. On Linux, `flamegraph` requires `perf` and may need `kernel.perf_event_paranoid` lowered. On macOS it uses `dtrace`, which needs `sudo`.

### samply

[samply](https://github.com/mstange/samply) is a cross-platform alternative that uses the Firefox profiler UI.

```shell
cargo install samply

cargo build --release --bin tpch
samply record ./target/release/tpch benchmark datafusion \
    --query 1 \
    --path /tmp/tpch-sf10 \
    --format parquet \
    --iterations 3
```

samply opens a browser with the loaded profile. The flame graph view is similar to `cargo flamegraph`, with the addition of a timeline that's helpful for spotting tail-latency events like spills.

### Tips for useful profiles

- Build with `--release` so the profile reflects production performance. Debug builds inline differently and produce misleading hot spots.
- Add `[profile.release] debug = "line-tables-only"` (or `debug = true`) to `Cargo.toml` if symbols are missing in the flame graph.
- Start small (SF=1 or SF=10) so the profile finishes quickly. Hot paths show up just as clearly at small scale.
- If a single iteration is too short to sample reliably, raise `--iterations`.
