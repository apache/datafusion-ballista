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

# History Server

The scheduler forgets a job shortly after it finishes. Completed jobs are cleaned
up after `finished_job_state_clean_up_interval_seconds`, and everything is gone
when the scheduler restarts, so by the time you want to look at a slow query it
is usually too late.

The history server is Ballista's equivalent of the Spark History Server. When
event logging is enabled the scheduler writes a durable record of each job as it
runs, and the history server replays those records and serves the same `/api/*`
responses the live scheduler does. The existing TUI can point at it and browse
completed jobs with no scheduler running at all.

## Enabling event logging

Event logging is off by default. Start the scheduler with a directory to write
to:

```shell
ballista-scheduler --event-log-dir /var/lib/ballista/history
```

The scheduler writes one file per job, in
[JSON Lines](https://jsonlines.org/) format. While the job runs, records are
appended to `<job_id>.eventlog.running`; when it reaches a terminal state
(succeeds, fails, or is cancelled) the scheduler flushes, closes, and renames
the file to `<job_id>.eventlog`. Only `.eventlog` files are ever indexed by
the history server, so the `.running` suffix alone marks a job as not yet
ready to read.

Writes happen on a background task, so the scheduler's event loop never waits on
disk. If the queue backs up, progress records are dropped rather than allowed to
stall scheduling. The terminal record is the exception: it waits for queue
capacity, because a job missing it is invisible to the history server.

## Running the history server

Point it at the same directory:

```shell
ballista-history-server \
  --event-log-dir /var/lib/ballista/history \
  --bind-host 0.0.0.0 \
  --bind-port 50060
```

It indexes every completed log in the directory and serves them over the same
paths as the live scheduler:

| Endpoint                       | Serves                              |
| ------------------------------ | ----------------------------------- |
| `GET /api/jobs`                | every completed job, newest first   |
| `GET /api/job/{job_id}`        | one job's summary and plans         |
| `GET /api/job/{job_id}/stages` | per-stage and per-task detail       |
| `GET /api/job/{job_id}/config` | the session config the job ran with |
| `GET /api/job/{job_id}/dot`    | the stage DAG in DOT format         |

Because the TUI talks to that same API, you can browse history with:

```shell
BALLISTA__SCHEDULER__URL=http://localhost:50060 ballista-cli --tui
```

The TUI reads its URL from configuration rather than from the `--host` and
`--port` flags, which set the gRPC scheduler address used for running queries.
Pointing the TUI somewhere else means setting `BALLISTA__SCHEDULER__URL`, or
`scheduler.url` in the TUI's config file. Passing `--port 50060` alone leaves
the TUI on its default of `http://localhost:50050`, where it either finds your
live scheduler or reports that the scheduler is down.

## Picking up new jobs

The scheduler keeps writing to the directory while the history server is up, so
the directory is rescanned every `--update-interval-seconds` (10 by default) and
jobs that finished since the last pass are added to the list. This works whether
the scheduler writing the logs is the same process, a different one, or several
at once, and it does not matter whether the history server or the scheduler
started first — an event-log directory that does not exist yet is simply empty
until it appears.

A rescan only opens logs whose size or modification time has changed, so the
cost of a pass over a directory that has not changed is one `stat` per file.
Lowering the interval makes new jobs show up sooner at the cost of more of those
passes; `--update-interval-seconds 0` turns rescanning off entirely and pins the
list to what was there at startup.

Logs that have been deleted are dropped from the list on the next pass, so
pruning the directory does not leave behind entries that fail when opened.

Only each job's summary is held in memory, which is what `GET /api/jobs` is
built from. Everything else is read back out of the job's log when you ask for
it. A job with many tasks stores megabytes of plan and per-task detail, and
keeping all of that resident for every job in the directory would put the
server's memory use at the mercy of how long you retain logs.

## What is recorded

Each log holds an ordered timeline: the job's submission, each stage starting
and ending, and each task finishing with its row counts and compute time. The
final record carries the finished API responses themselves.

That last point is what makes replayed output trustworthy. The history server
does not rebuild a response from stored state; it re-serves the exact response
the scheduler built while the job was alive. There is no second implementation
that could drift from the live one.

Only the final record is served today. The per-task timeline is recorded so a
future UI can show a job progressing rather than only its end state.

## Operational notes

- **Disk is not reclaimed automatically.** Logs accumulate until you remove
  them. Size them against your job volume and prune with whatever you already
  use for log rotation.
- **A crash mid-job leaves a `.running` file, never listed.** If the scheduler
  dies (or is killed) before a job finishes, the file stays named
  `<job_id>.eventlog.running` and the history server excludes it by name alone
  without ever opening it. A restarted scheduler has no persisted memory of
  the job and will never write to, rename, or otherwise touch that file again,
  so a leftover `.running` file is permanently orphaned — safe (and necessary,
  since nothing does it automatically) to remove during routine log pruning,
  like any other stale log. This is also a directly visible signal, via `ls`,
  that a job never finished cleanly, with no need to open and inspect it.
  Damage confined to a job's stored responses in an already-`.eventlog`-named
  file is a separate, orthogonal case: it is only found when that job is
  opened, and shows up as a failed request for that one job.
- **Deleting a log takes up to one rescan to show.** Until the next pass the
  job is still listed, and opening it fails.
- **`GET /api/jobs` returns every job in one response.** There is no paging
  yet, so a directory holding a very large number of jobs produces a large
  response. Prune accordingly until paging exists.
  ([#2270](https://github.com/apache/datafusion-ballista/issues/2270))
- **Plans are rendered once, when the job ends.** The `?plan_format=` query
  parameter therefore has no effect against a history server; it returns the
  format captured at write time.
- **The history server has no cluster behind it.** `GET /api/executors` returns
  an empty list and `GET /api/state` returns a static payload, so that TUI
  screens expecting them still load.
