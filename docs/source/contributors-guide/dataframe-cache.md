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

# DataFrame Cache Design (`DataFrame::cache()`)

## Goal

`df.cache()` marks a DataFrame to be **computed once and reused** by later
actions instead of recomputed. In Ballista a cached dataset is **shuffle output
that is kept (pinned) past normal job cleanup, plus a scheduler-side record of
where that output lives**. It reuses the native [blocking shuffle](shuffle.md)
as its storage and transport — no separate cache store, no new wire format, and
the read-back is an ordinary `ShuffleReaderExec`.

The cache is a **hint, not a correctness dependency**: a cached node keeps its
input subplan (lineage), so lost cache data is simply recomputed — never a wrong
or missing answer.

## The model

In the **logical plan**, a cached dataset is a `BallistaCacheNode` wrapping the
subplan to cache. Materialized, it has two pieces:

1. **Pinned shuffle output** — the cached subplan runs as its own job; its final
   stage's shuffle files (`{work_dir}/{job_id}/{stage_id}/…`) survive job cleanup.
2. **A cross-job registry** — scheduler state (the `CacheState` trait, in-memory
   `InMemoryCacheState` over `CacheRegistry`) mapping a cache key to the schema
   and `PartitionLocation`s of that output, so a _different_ job can read it.

A registry is needed because Ballista materializes the cache in a separate
distributed **job**: the mapping from a cache to its shuffle output must outlive
that job and be visible to other jobs, so the scheduler owns it explicitly.

**Keying:** `(session_id, cache_id)`, where `cache_id` is a per-`cache()`-call
UUID in the `BallistaCacheNode`. Reuse happens whenever a submitted plan still
contains that node (same `cache_id`) — the cached DataFrame reused directly or
built upon. Dedup across _different_ plans with equal content is deferred.

## End-to-end flow

Three roles: the **client** holds the plan, the **scheduler** owns planning and
the registry, and the **executors** run tasks and hold the shuffle files.

`df.cache()` is lazy: it returns a DataFrame whose **logical plan** is wrapped in
a `BallistaCacheNode` with a fresh `cache_id` — nothing executes yet. Reuse is
keyed on that `cache_id`: any plan later submitted that still contains the node —
the same cached DataFrame, or a query built on top of it — resolves to the same
**cache entry** (the registry's record of the materialized output). Calling
`df.cache()` again mints a new `cache_id`.

Miss and hit are not separate stories — they **converge**: a miss materializes
and then reads the pinned output, while a hit does the same read and skips the
materialize.

```text
                     df.cache()                 client; lazy — wraps the logical
                         │                       plan in BallistaCacheNode
                         ▼                       {cache_id, session_id, input}
            submit a plan carrying the node      client ──► scheduler
                         │
                         ▼
             lookup((session_id, cache_id))      scheduler
                         │
            ┌────────────┴─────────────┐
          MISS                        HIT
            │                          │
            ▼                          ▼
   begin_materialization        validate locations
   run `input` as its own       against live executors
   job (two-job split, #1993)   (any dead ⇒ re-MISS)
   → executors write the                │
     pinned shuffle files               │
            │                           │
            ▼                           │
   complete_materialization             │
   → Materialized (fresh locs)          │
            │                           │
            └────────────┬──────────────┘
                         ▼
        lower BallistaCacheNode ──► physical ShuffleReaderExec,
        reading the pinned output served by the executors
                         │
                         ▼
                   results ──► client
```

**Cold path — first action (miss → materialize):**

1. The client submits a plan containing `BallistaCacheNode{cache_id, session_id, input}`.
2. The scheduler forms `key = (session_id, cache_id)` and calls `lookup(key)` → miss.
3. `begin_materialization(key, job)` claims the key; the scheduler runs `input`
   as its **own job** (a logical-plan rewrite that splits the query into two
   jobs, see #1993), writing pinned shuffle output on the executors.
4. On that job's `JobFinished`, `complete_materialization(key, job, schema, locations)` records the output → the cache entry becomes `Materialized`.
5. The outer query reads that output and returns results — so the first action
   both materializes and reads back.

**Warm path — later action (hit → read-back):**

1. The client submits a plan that still carries the same `cache_id`.
2. `lookup(key)` → hit. Its locations are validated against the live-executor
   set; a dead location downgrades the hit to a miss (see [Invalidation](#invalidation)).
3. The scheduler lowers the `BallistaCacheNode` to a **physical**
   `ShuffleReaderExec` over the recorded locations — the same mechanism stages
   use to read each other — so `input` is never planned or executed.
4. Results return with no materialization job.

**Ordering & concurrency.** Within one action the read is sequenced after the
create: the read's `ShuffleReaderExec` needs the recorded locations, so the
scheduler resolves it only on the create job's `JobFinished` — event-driven, no
polling. A _concurrent_ action that finds the key
`Pending` (`begin_materialization` → `AlreadyPending`) must either wait for that
materialization or fall back to computing the subplan inline; the exact policy —
wait-and-notify vs. poll vs. inline, and releasing waiters if the job aborts — is
an open question.

## Registry lifecycle

```text
  (absent) --begin--> Pending{job} --complete(job)--> Materialized
                      Pending{job} --abort(job)-----> (absent)
                      Materialized --invalidate------> (absent)
```

The contract is safe against the out-of-order events a distributed scheduler
produces (a completion callback racing invalidation, teardown, or a re-claim):

- `begin_materialization` atomically gives one job the claim.
- `complete_materialization` promotes **only** a `Pending` entry owned by that
  job (duplicate completions are idempotent); anything else is `Stale`, so a
  late completion cannot resurrect a removed entry or overwrite a newer claim.
- `abort_materialization` clears a failed job's pending claim.
- `invalidate` / `invalidate_executor` / `remove_session` remove entries and
  **return them** so the caller can unpin the shuffle files (Ballista deletes
  explicitly — there is no reclaimer).
- `pinned_stages()` lists the `(job_id, stage_id)` stages to protect from cleanup.

## Pinning

Shuffle output is normally reclaimed after a job finishes. A cached stage must
be exempted, so job-data cleanup consults `pinned_stages()` and skips those
stage dirs. Pins are per-`(job_id, stage_id)` because shuffle data is stored and
deleted per stage, and a cached stage is often an _intermediate_ stage (reaped
immediately on job success) that a job-level pin could not protect.

## Invalidation

An entry is removed when its data is no longer trustworthy or needed:

- **Executor loss** (`invalidate_executor`) — any materialized entry with a
  partition on the lost executor is dropped.
- **Session teardown** (`remove_session`) — all of a session's entries are
  dropped.
- **Explicit / eviction** (`invalidate`) — a single entry is dropped (used by
  the future eviction sweeper).

Invalidation is **whole-entry**: the entire cache is dropped and re-materialized
in full on the next access — we never serve a cache with missing partitions
(per-partition rebuild is a future optimization). Because a cached node keeps its
lineage, invalidation always degrades to a recompute, never a wrong answer.

Each removal **returns the removed entries** so the caller can unpin and delete
their shuffle files — Ballista has no background reclaimer, so cleanup is
explicit. A removed entry's next `lookup` is a miss, which re-triggers
materialization.

> **Executor loss during materialization (executor-loss follow-up).** If an
> executor is lost after the cached stage finishes but before its job completes,
> `complete_materialization` can record a now-dead location (the in-graph
> recovery only rebuilds output an unfinished stage still reads). The fix is
> **validate-on-read**: on a hit the planner checks locations against the
> live-executor set and, if any is dead, treats it as a miss and `invalidate`s
> it — enforcing "never read from a dead executor" under all timings.

## Durability & reconciliation

`CacheState` is a pluggable backend; the in-memory one loses entries on scheduler
restart (caches become orphaned files, reclaimed by cleanup), and a durable
backend can persist and rehydrate them — an optional efficiency extension. A
stale or missing location is caught by validate-on-read (executor gone) or the
`ShuffleReader` fetch-failure path (file gone) and degrades to re-materialization.

## Future work

- **Eviction:** entries today leave only via invalidation/executor-loss/session
  teardown, so a long-running scheduler accumulates cache data. Planned: a TTL /
  size-cap sweeper (via `invalidate`) and/or idle-session expiry (via
  `remove_session`), using the removal-returns-entries contract above.
- **Locality:** place read-back tasks on the executor holding each partition
  (`TaskDistributionPolicy::Custom`), Flight fetch as fallback (see [#2319]).

## Implementation plan

1. **Registry** (done) — `CacheRegistry` + `CacheState`/`InMemoryCacheState`.
2. **Materialize-on-miss + read-back** — wire `lookup`/`begin`/`complete`/`abort`
   into planning and the job handlers; builds on the two-job rewrite in [#1993].
3. **Pinning** — teach job-data cleanup to honor `pinned_stages()`.
4. **Locality** — place read-back tasks where partitions live ([#2319]).
5. **Executor-loss invalidation + session teardown** — wire
   `invalidate_executor` / `remove_session` and the unpin path, add
   validate-on-read on the hit path, and enable an end-to-end no-recompute test.
6. **Eviction** — TTL / size cap / idle-session expiry.

[#1993]: https://github.com/apache/datafusion-ballista/pull/1993
[#2319]: https://github.com/apache/datafusion-ballista/issues/2319
