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

# Pipelined Execution (experimental)

By default Ballista runs a job one stage at a time. Each stage writes its output
to shuffle files, and the stages that read it start once it has finished. This
is the model Apache Spark uses, and it lets a job survive failures: a lost task
is simply run again from the shuffle files it reads.

Pipelined execution, enabled with `ballista.execution.mode = pipelined`, runs a
job the way Trino does instead. The scheduler places every task of the job at
once, and stages stream record batches to each other over Arrow Flight as they
produce them. No intermediate data is written to disk, and results start
reaching the client while the job is still running. The cost is that a failed
task fails the whole job.

Pipelined execution is experimental. Its behavior and settings may change.

## When to use it

Pipelined execution suits short, latency-sensitive queries, where the time
spent scheduling stages and writing and reading shuffle files is a large part
of the total. It needs the cluster to have room for the whole job at once.

Stay with blocking execution for:

- long-running jobs, which a single task or executor failure would fail;
- jobs that need far more vcores than the cluster has free;
- clusters shared by many concurrent jobs, where a pipelined job may wait a
  long time for its vcores to be free together.

## Enabling it

Pipelined jobs need push-staged scheduling, where the scheduler pushes tasks to
executors. This is the default for the `ballista-scheduler` and
`ballista-executor` binaries (`--scheduler-policy push-staged` and
`--task-scheduling-policy push-staged`). A pull-staged scheduler rejects
pipelined jobs.

Choose the mode per session, from SQL:

```sql
SET ballista.execution.mode = 'pipelined';
```

or when building the session:

```rust
let config = SessionConfig::new_with_ballista()
    .set_str("ballista.execution.mode", "pipelined");
let state = SessionStateBuilder::new()
    .with_config(config)
    .with_default_features()
    .build();
let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;
```

A standalone cluster, from `SessionContext::standalone_with_state`, runs
push-staged when the session state it is created from asks for pipelined
execution, whatever `ballista.standalone.task_scheduling_policy` says. One from
`SessionContext::standalone()` runs pull-staged, so setting the mode on it later
does not work.

## How a pipelined job runs

- **Placement.** The scheduler places all of the job's tasks at once, or waits
  until it can. On each executor it reserves as many vcores as the job's widest
  stage uses there, rather than the sum over stages, since stages waiting on
  each other use little CPU. The reservation is held until the job ends.
- **Wide stages.** A stage with more partitions than the cluster has vcores,
  such as a `UNION ALL` of several scans, waits until every vcore is free, and
  then runs several partitions per vcore.
- **Exchanges.** Each output partition of a stage is a bounded queue of record
  batches, so a slow consumer slows its producer down rather than letting data
  pile up. A consumer on the same executor as its producer reads the queue
  directly; others read it over Arrow Flight. The build side of a broadcast
  join is kept in memory until the job ends, so that every task that reads it
  can.
- **Results.** The client reads every partition of the final stage at once, as
  they are produced. `LIMIT` queries stop the stages feeding them once they
  have enough rows.
- **Joins.** Without adaptive query execution there are no measured sizes to
  choose join strategies from, so the scheduler chooses them from planning-time
  statistics. A join whose build side is estimated to fit under
  `ballista.optimizer.broadcast_join_threshold_bytes` is a broadcast hash
  join, one whose build side fits
  `ballista.optimizer.hash_join_max_build_partition_bytes` in each partition
  is a partitioned hash join, and any other join, including one whose build
  size is unknown, is a sort-merge join. A hash table cannot spill and every
  hash table of a pipelined job is held at once, so a join is only made a
  hash join when its build side is known to be small.
  `datafusion.optimizer.prefer_hash_join` does not apply, as under adaptive
  query execution.
- **Failures.** A failed task, or a lost executor that ran any of the job's
  tasks, fails the job. No task is retried. The one thing that is retried is
  opening a stream to another executor whose connection failed before that
  executor answered, `ballista.client.io_retries_times` times with
  `ballista.client.io_retry_wait_time_ms` between attempts; a stream that has
  started cannot be resumed.
- **Connections.** An executor keeps one connection to each other executor,
  and the streams of all its tasks share it, so a cluster of `e` executors
  holds `e - 1` connections per executor however wide the job. Sharing is safe
  because of the flow-control windows: a stream nobody is reading, such as a
  join's probe side while its build side loads, holds at most
  `ballista.pipelined.stream_window_bytes` of unread data, and the connection
  window is the largest HTTP/2 allows, so stalled streams cannot starve the
  streams something is waiting on until a connection carries more than
  `2147483647 / stream_window_bytes` of them, over 4000 at the default. A job
  wider than that should lower the stream window. The executor also listens
  with an accept queue of 4096, capped by `net.core.somaxconn`, for the burst
  of connections the first job after a restart opens.
- **Memory.** With `--memory-pool-size` set, all of a job's tasks on an
  executor share one memory pool, sized by the vcores the job reserved there.
  Every stage of the job holds its state at the same time, such as the hash
  tables of all its joins and the buffers of all its sorts, so a pipelined job
  can need more memory than the same query run in blocking mode. Sort-heavy
  plans need the most. For example, TPC-H query 8 at scale factor 10, planned
  with sort-merge joins over 16 partitions, runs in blocking mode on a 4-vcore
  executor with a 2 GB pool, but fails in pipelined mode with that pool. Give
  executors more memory, use fewer partitions, or run such queries in blocking
  mode.

## Settings

| Setting                                           | Default    | Meaning                                                                                                                                               |
| ------------------------------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ballista.execution.mode`                         | `blocking` | `blocking` or `pipelined`.                                                                                                                            |
| `ballista.pipelined.queue_capacity`               | `2`        | Batches each output partition holds before its producer waits for the consumer. Larger values smooth out uneven consumers at the cost of memory.      |
| `ballista.pipelined.registration_timeout_seconds` | `60`       | How long a consumer waits for its producer task to start, and a producer whose queue is full waits for its consumer to connect, before the job fails. |
| `ballista.pipelined.stream_window_bytes`          | `524288`   | HTTP/2 window of each stream to another executor. Bounds what a stalled stream holds; see Connections above.                                          |

`ballista.scheduler.max_partitions_per_task` caps how many partitions each task
of a pipelined job processes, as it does in blocking mode. Adaptive query
execution (`ballista.planner.adaptive.enabled`) does not apply to pipelined
jobs. `ballista.optimizer.hash_join_max_build_partition_bytes` (64 MiB by
default; 0 turns the check off) bounds the build side of each partitioned hash
join, as described under Joins above.

See the [configuration reference](configs.md) for every setting.

## Arrow Flight SQL

The [Flight SQL frontend](flightsql.md) runs pipelined jobs when the session
asks for them. It answers a pipelined query with a single endpoint, through
which the scheduler streams every partition of the result.

## Limitations

- Pipelined jobs are not retried, in whole or in part; only a stream open whose
  connection failed is.
- Pipelined jobs need push-staged scheduling.
- A pipelined job waiting for its vcores has no priority over other jobs, so a
  steady stream of smaller jobs can keep it waiting.
- Adaptive query execution does not apply.
