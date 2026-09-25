# TPC-H SF1000 Query Plans (Ballista 54.0.0)

> **Draft — not intended to merge.** These plans are checked in as a
> discussion surface for community review. They are a point-in-time
> snapshot, not a maintained reference.

This directory contains the logical, pre-execution physical, and executed
stage plans (with runtime metrics) for all 22 TPC-H queries at scale factor
1000, captured from a Ballista scheduler event log.

## How the plans were produced

- **Ballista version:** 54.0.0
- **Data:** TPC-H SF1000 in Parquet, zstd-compressed, ~512 MiB row groups,
  Iceberg-style partitioned by the leading date column of each fact table
- **Cluster:** several 32-core Linux executors (specifics scrubbed)
- **Scheduler event log:** enabled via the new `--event-log-dir` flag from
  [#2264][pr2264]; plans extracted directly from `JobStart` / `JobEnd`
  records

Executor IPs and host-kernel strings were stripped before checking in.

## Notable planner / execution config

Non-default settings that shape these plans:

| Setting                                                | Value    |
| ------------------------------------------------------ | -------- |
| `ballista.planner.adaptive.enabled`                    | true     |
| `ballista.planner.adaptive_join.enabled`               | true     |
| `ballista.planner.coalesce.enabled`                    | false    |
| `ballista.optimizer.broadcast_join_threshold_bytes`    | 128 MiB  |
| `ballista.optimizer.hash_join_max_build_partition_bytes` | 64 MiB |
| `ballista.shuffle.compression.codec`                   | lz4      |
| `ballista.shuffle.sort_based.memory_limit_per_task_bytes` | 256 MiB |

Full config is included in each event log (not reproduced per query).

## Wall-clock summary

Single run, single job at a time, no warm-up:

| Query | Elapsed | Stages | | Query | Elapsed | Stages |
| ----- | ------: | -----: | - | ----- | ------: | -----: |
| q1    |   9.12s |      3 | | q12   |  11.30s |      5 |
| q2    |  39.46s |     10 | | q13   |  12.16s |      5 |
| q3    |  25.51s |      6 | | q14   |   5.14s |      4 |
| q4    |   8.98s |      5 | | q15   |  10.82s |      7 |
| q5    |  35.48s |      9 | | q16   |  16.28s |      8 |
| q6    |   5.21s |      2 | | q17   |  25.79s |      5 |
| q7    |  39.63s |     10 | | q18   |  50.63s |      7 |
| q8    | 164.21s |     12 | | q19   |  16.00s |      4 |
| q9    |  91.72s |     11 | | q20   |  26.24s |      9 |
| q10   |  39.57s |      7 | | q21   |  59.62s |     11 |
| q11   |  15.18s |      8 | | q22   |  10.01s |      6 |

## What's in each `qNN.md`

1. **Logical Plan** — as produced by the DataFusion optimizer
2. **Physical Plan (pre-execution)** — the plan Ballista sent to the
   scheduler, with `AdaptiveDatafusionExec` / `ExchangeExec` boundaries
   still unresolved
3. **Executed Stages** — one section per shuffle stage, with input/output
   rows, elapsed compute, task-duration and task-input percentiles, and the
   stage's physical plan

[pr2264]: https://github.com/apache/datafusion-ballista/pull/2264
