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
actions. In Ballista a cached dataset is **shuffle output kept (pinned) past job
cleanup, plus a scheduler-side record of where it lives**. It reuses what
Ballista already has — shuffle files, the shuffle reader, and stage recovery —
rather than adding a cache store or a recovery path of its own.

The cache is a **hint, not a correctness dependency**: a cached node keeps its
subplan (lineage), so lost cache data is recomputed, never a wrong or partial
answer. In this PR a query that loses it mid-run fails with a clear error and
the retry rebuilds the cache; rebuild-and-resume is next
([Recovery](#recovery)).

## End-to-end flow

`df.cache()` is lazy: it wraps the DataFrame's logical plan in a
`BallistaCacheNode` with a fresh `cache_id` (a per-call UUID). Any plan later
submitted that still contains the node — the cached DataFrame itself, or a query
built on it — uses the same cache, keyed by `(session_id, cache_id)`.

```text
 df.cache() → plan wrapped in BallistaCacheNode{cache_id, input}    client, lazy
      │ action, e.g. collect()
      ▼
 lookup (session_id, cache_id); locations on live executors?         scheduler
      │
      ├─ MISS (cold) → build job runs `input` → pinned shuffle output
      │                 query job held (Queued; its id returned at once)
      │                 build done → record locations, pin the stage
      │                                       │
      └─ HIT (warm) ──────────────────────────┤
                                              ▼
        cache node → scan of its shuffle output → ShuffleReaderExec,
        a complete input of the stage reading it
                                              ▼
                     query job runs → results to the client
```

- **Cold path.** The first action claims the cache and runs its subplan as its
  own **build job**, writing pinned shuffle output. The query becomes a second
  job, held without blocking until the build finishes; then it reads the output.
  The first action both builds and reads.
- **Warm path.** A later action finds the cache built on live executors, so the
  cache node becomes a read and the subplan is never planned or run. A location
  on a dead executor turns the hit into a miss.
- **Both** paths end the same way: the read is an ordinary `ShuffleReaderExec`,
  registered as an input of the stage reading it (see
  [The read job](#the-read-job-and-external-inputs)), so Ballista's own rollback
  restores it like any other input. Losing the cache's data mid-query fails the
  query cleanly for now; rebuild-and-resume is the next step (see
  [Recovery](#recovery)).

A query waits on every build it needs, so independent caches build **in
parallel**, a cache built on another builds **after** it, and a query that finds
a cache already being built waits on that build. A build that fails or is
cancelled fails the queries waiting on it. A cache that hits hides its subplan:
caches nested inside it are not looked up. Before the held query runs, its caches
are checked again, so one that went stale while it waited is rebuilt first.

## The read job and external inputs

Before the query job is planned, each cache node becomes a read with an
**external id**:

```text
logical    Projection [c0 AS t.id, …]               back to the cached names
             TableScan ballista_cache_read_N [c0, …] scan of the cache's output
physical   ShuffleReaderExec(stage_id = id, …)      an ordinary shuffle read
graph      stage.inputs[id] = locations, complete   the read is a stage input
```

The scan is an ordinary `TableScan` whose provider returns the
`ShuffleReaderExec` (with a projection to the scan's names), so every query
planner plans it and a session needs nothing installed to read caches. Its
columns are positional (`c0`, `c1`, …) because a table has one qualifier and
unique names, which a cached frame (a join, say) need not have; the projection
above restores the cached qualifiers and names, typed as the build job wrote
them.

The id comes from a reserved stage-id range (`EXTERNAL_STAGE_ID_BASE`, 2³⁰),
because a stage's inputs are keyed by stage id: the read must not collide with
the job's own stages, and two reads of one cache are two inputs. Ids are per job;
executors find data from the locations, not the id.

The entry is complete from the start, so it never delays the stage; it is there
so a rollback (for example, after the job loses one of its own shuffle outputs)
restores the reader. For each running job the scheduler also records which
builds it read, to drop them if the job loses their data.

## Recovery

**In this PR: fail cleanly, rebuild on retry.** When a fetch of cache data
fails, or the scheduler loses an executor holding cache data that an unfinished
stage reads, the job fails with "lost DataFrame::cache() data; retry the query",
under both planners. A failed fetch is cache data when it names one of the
stage's registered cache locations (executor, build stage, partition), so a
failed fetch of the job's own output from the same executor is recovered as
usual, and the check holds even when the cache's build stage shares an id with
one of the job's own stages.
The builds the job read are dropped, so the retry rebuilds them (validate-on-read
already does that for caches on dead executors).

**Next: rebuild and resume** (follow-up PR). The graph reports the lost input
instead of failing, and the scheduler rebuilds the cache and resumes the stage:

```text
 query job running; a stage reads cache c1
      │ fetch failure, or an executor holding c1's data is lost
      ▼
 stage rolls back (Ballista's own rollback — the read is a stage input)
      ▼
 ExternalInputLost(job, id)        the graph cannot re-run another job's output
      ▼
 read record (job, id) → c1, subplan, lost build
 drop the lost build; rebuild c1 (or wait on a rebuild in flight)
      ▼
 rebuild done → ExternalInputReady(job, id, new locations)
      ▼
 stage resolves and re-runs → results to the client
```

- A failed fetch is reported under the reader's stage id — the external id for a
  cache read — so the graph knows exactly which input was lost.
- Readers that lose the same cache share one rebuild. A rebuild may have a
  different partition count; the reader declares unknown partitioning, so the
  new partitions are folded into the old count.
- Retries count towards `stage_max_failures`, like any stage's. If the rebuild
  fails, the job fails with the reason.
- The cache is rebuilt in full and the reading stage re-runs; the job keeps its
  other stages. See [Per-partition recovery](#per-partition-recovery).
- The adaptive planner cannot yet re-resolve a stage it rolls back (for any
  job), so under it a job that loses cache data still fails cleanly.

## Registry and pinning

```text
  (absent) --begin--> Pending{job} --complete(job)--> Materialized{job, schema, locations}
                      Pending{job} --abort(job)-----> (absent)
                      Materialized --invalidate------> (absent)
```

The registry is safe against the out-of-order events a distributed scheduler
produces:

- `begin_materialization` gives one job the claim; a concurrent miss gets
  `AlreadyPending` and waits on that job (`pending_job`).
- `complete_materialization` promotes only the claim owned by that job;
  anything else is `Stale`, so a late completion cannot resurrect a removed
  entry or overwrite a newer claim.
- `abort_materialization` clears a failed job's claim.
- `invalidate_if(key, job)` removes an entry only while it is still `job`'s, so
  acting on an entry seen earlier never drops a newer claim.
- Removals return the removed entries, so a caller can unpin their files.

The build job's `JobFinished` handler records the entry **before** cleanup runs,
and cleanup skips the stages in `pinned_stages()`, so cleanup cannot race the
pin.

## Invalidation

**Validate-on-read** enforces "never read from a dead executor": whenever a
cache is resolved into a read (when a query is submitted, and again before a held query runs), a location on an
executor that is not live turns the hit into a miss, and the dead entry is
dropped. A dropped entry's build data is deleted after the usual delay for a
finished job's data (`finished_job_data_clean_up_interval_seconds`), which lets
a query still reading it finish. Invalidation is **whole-entry** — a cache is rebuilt in full, never
served with missing partitions (see
[Per-partition recovery](#per-partition-recovery)).

`invalidate_executor` (on executor loss) and `remove_session` (on session
teardown) exist in the registry but are not called yet; until then a dead entry
is dropped when it is next read, or when a query loses its data.

## Planning & configuration

- **Planner.** Nothing to install: a cache read is a table scan, which any
  session's query planner plans, including a custom `override_session_builder`
  and a scheduler created from a user's state.
- **Storage.** Pinned shuffle output; for other media see
  [Extension points](#extension-points).
- **Turning caching off.** `ballista.cache.noop` (a session setting, on by
  default) makes `cache()` a no-op. An operator can turn caching off for the
  cluster with `--disable-dataframe-cache` (`SchedulerConfig::disable_dataframe_cache`):
  cache nodes then run inline.
- **`EXPLAIN`** never materializes: a built cache is shown as its read, any
  other as its subplan. `EXPLAIN ANALYZE` runs the query, so it materializes.

## Durability & limits

- **Scheduler restart.** The in-memory registry loses its entries; caches are
  rebuilt on next use, and their old files stay until executor cleanup. A
  durable `CacheState` backend could persist the entries.
- **Ordering.** A cache read declares no output ordering, so operators above a
  sorted cache may reorder it — as with DataFusion's own `DataFrame::cache()`.
- **Executor work-dir cleanup.** If enabled (`job_data_clean_up_interval_seconds`),
  an executor deletes job dirs untouched for `job_data_ttl_seconds`, pinned
  caches included; the next read fails and drops the cache, and a retry
  rebuilds it.
- **Mid-query loss.** Losing cache data mid-query fails the job (see
  [Recovery](#recovery)).

## Extension points

- **Other storage media** (e.g. memory with disk spill): the scheduler side
  deals only in `PartitionLocation`s, so a medium plugs in on the executors:
  `override_execution_engine` writes the build stage into the store and
  `override_arrow_flight_service` serves it, while `ShuffleReaderExec` reads it
  back by location. Evicted data surfaces as a failed fetch, handled like lost
  cache data. Still needed then: a marker identifying the build stage,
  memory release on cleanup, and read locality ([#2319]) for zero-copy reads.
- **`CacheState`**: the registry backend; a durable or shared one would keep
  entries across scheduler restarts.
- **External stage inputs**: a stage input produced by another job is generic
  in the execution graph; a cache is the first producer, reusing another job's
  shuffle output could be another.

## Future work

- **Eviction:** entries leave today only when found dead, so a long-running
  scheduler accumulates cache data. Planned: a TTL / size-cap sweeper and
  idle-session expiry, using the removal-returns-entries contract.
- **Locality:** place read tasks on the executor holding each partition
  (`TaskDistributionPolicy::Custom`), Flight fetch as fallback (see [#2319]).

### Per-partition recovery

Ballista re-runs a lost map output per partition: a successful producer stage
re-runs only the tasks whose output was on the lost executor
(`SuccessfulStage::reset_tasks`, `rerun_successful_stage`), while a consumer
stage re-runs in full. The cache can follow the same split — the build side
rebuilds only the lost partitions, the reading stage still re-runs:

```text
 Materialized → Partial{missing} → partial build of missing partitions
              → merge locations → ExternalInputReady
```

- **Narrow subplans** (no shuffle in the build's final stage): restrict the
  subplan to the missing partitions (`restrict_plan_to_partitions`).
- **Subplans with a shuffle:** the cleaned-up upstream stages re-run in full,
  unless intermediate outputs are pinned too.
- **Determinism:** only plans that give partition `j` the same rows on every
  run qualify; anything else falls back to the whole rebuild.

## Implementation plan

1. **Registry** (done) — `CacheRegistry` + `CacheState`/`InMemoryCacheState`.
2. **Materialize + read-back** (done) — two jobs linked without blocking;
   builds on the two-job rewrite in [#1993].
3. **Pinning** (done) — job-data cleanup honours `pinned_stages()`.
4. **Recovery, part 1** (done) — validate-on-read, re-checking a held query's
   caches, and failing cleanly when a running query loses cache data.
5. **Recovery, part 2** — rebuild-and-resume for a running job
   ([Recovery](#recovery)).
6. **Locality** — place read tasks where partitions live ([#2319]).
7. **Session teardown** — wire `remove_session` and sweep entries on executor
   loss (`invalidate_executor`), deleting their build data as invalidation
   does.
8. **Eviction** — TTL / size cap / idle-session expiry.
9. **Per-partition recovery** — rebuild only lost partitions of narrow,
   deterministic subplans; later, pinned intermediates for subplans with a
   shuffle.

[#1993]: https://github.com/apache/datafusion-ballista/pull/1993
[#2319]: https://github.com/apache/datafusion-ballista/issues/2319
