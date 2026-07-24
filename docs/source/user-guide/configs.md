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

# Configuration

**The configuration tables on this page are generated** from the `ConfigEntry`
definitions in `ballista/core/src/config.rs`. Anything between a pair of
generated config reference markers is overwritten the next time the docs are
regenerated, so do not hand-edit it. To change a setting's description, type, or
default, edit its `ConfigEntry` in `ballista/core/src/config.rs`, then run
`./dev/update_config_docs.sh` and commit the regenerated file. Prose outside the
generated sections is hand-written and safe to edit normally.

## Ballista Configuration Settings

Configuring Ballista is quite similar to configuring DataFusion. Most settings are identical, with only a few configurations specific to Ballista.

_Example: Specifying configuration options when creating a context_

```rust
use ballista::extension::{SessionConfigExt, SessionContextExt};

let session_config = SessionConfig::new_with_ballista()
    .with_information_schema(true)
    .with_ballista_job_name("Super Cool Ballista App");

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_config(session_config)
    .build();

let ctx: SessionContext = SessionContext::remote_with_state(&url,state).await?;
```

`SessionConfig::new_with_ballista()` will setup `SessionConfig` for use with ballista. This is not required, `SessionConfig::new` could be used, but it's advised as it will set up some sensible configuration defaults .

`SessionConfigExt` expose set of `SessionConfigExt::with_ballista_` and `SessionConfigExt::ballista_` methods which can tune retrieve ballista specific options.

Notable `SessionConfigExt` configuration methods would be:

```rust
/// Overrides ballista's [LogicalExtensionCodec]
fn with_ballista_logical_extension_codec(
    self,
    codec: Arc<dyn LogicalExtensionCodec>,
) -> SessionConfig;

/// Overrides ballista's [PhysicalExtensionCodec]
fn with_ballista_physical_extension_codec(
    self,
    codec: Arc<dyn PhysicalExtensionCodec>,
) -> SessionConfig;

/// Overrides ballista's [QueryPlanner]
fn with_ballista_query_planner(
    self,
    planner: Arc<dyn QueryPlanner + Send + Sync + 'static>,
) -> SessionConfig;
```

which could be used to change default ballista behavior.

If information schema is enabled all configuration parameters could be retrieved or set using SQL;

```rust
let ctx: SessionContext = SessionContext::remote_with_state(&url, state).await?;

let result = ctx
    .sql("select name, value from information_schema.df_settings where name like 'ballista'")
    .await?
    .collect()
    .await?;

let expected = [
    "+-------------------+-------------------------+",
    "| name              | value                   |",
    "+-------------------+-------------------------+",
    "| ballista.job.name | Super Cool Ballista App |",
    "+-------------------+-------------------------+",
];
```

### Session Settings

The following keys can be set on a `SessionConfig`. Ballista also accepts all
standard DataFusion settings.

<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.job.,ballista.standalone.,ballista.cache.,ballista.client.,ballista.optimizer.,ballista.planner.,ballista.scheduler. -->

<!-- prettier-ignore -->
| key                                                    | type    | default                       | description                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------------ | ------- | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ballista.cache.noop                                    | Boolean | true                          | Disable default cache node extension                                                                                                                                                                                                                                                                                                                                                                                                               |
| ballista.client.grpc_connect_timeout_seconds           | UInt64  | 20                            | Connection timeout for gRPC client in seconds                                                                                                                                                                                                                                                                                                                                                                                                      |
| ballista.client.grpc_http2_keepalive_interval_seconds  | UInt64  | 300                           | HTTP/2 keep-alive interval for gRPC client in seconds                                                                                                                                                                                                                                                                                                                                                                                              |
| ballista.client.grpc_max_message_size                  | UInt64  | 16777216                      | Configuration for max message size in gRPC clients                                                                                                                                                                                                                                                                                                                                                                                                 |
| ballista.client.grpc_tcp_keepalive_seconds             | UInt64  | 3600                          | TCP keep-alive interval for gRPC client in seconds                                                                                                                                                                                                                                                                                                                                                                                                 |
| ballista.client.grpc_timeout_seconds                   | UInt64  | 20                            | Request timeout for gRPC client in seconds                                                                                                                                                                                                                                                                                                                                                                                                         |
| ballista.client.initial_connection_window_size         | UInt64  | 67108864                      | HTTP/2 initial connection-level flow-control window for gRPC data-plane clients, in bytes. Should be >= the shuffle governor byte budget so the governor, not the transport window, is the binding backpressure. 0 leaves the tonic default.                                                                                                                                                                                                       |
| ballista.client.initial_stream_window_size             | UInt64  | 16777216                      | HTTP/2 initial stream-level flow-control window for gRPC data-plane clients, in bytes. 0 leaves the tonic default.                                                                                                                                                                                                                                                                                                                                 |
| ballista.client.io_retries_times                       | UInt16  | 3                             | Number of retries for IO operations in the Ballista client.                                                                                                                                                                                                                                                                                                                                                                                        |
| ballista.client.io_retry_wait_time_ms                  | UInt64  | 3000                          | Wait time in milliseconds between IO retries in the Ballista client.                                                                                                                                                                                                                                                                                                                                                                               |
| ballista.client.pull                                   | Boolean | false                         | Should client employ pull or push job tracking. In pull mode client will make a request to server in the loop, until job finishes. Pull mode is kept for legacy clients.                                                                                                                                                                                                                                                                           |
| ballista.client.use_tls                                | Boolean | false                         | Should connection between client, scheduler, and executors use TLS.                                                                                                                                                                                                                                                                                                                                                                                |
| ballista.job.client_side_cleanup                       | Boolean | true                          | When enabled, the client sends a best-effort CleanJobData request to the scheduler as soon as it finishes (or abandons) consuming a job's results, reclaiming the job's on-disk result data on executors immediately instead of waiting for the scheduler's timed cleanup. The scheduler's timed cleanup remains as a safety net.                                                                                                                  |
| ballista.job.name                                      | Utf8    | (none)                        | Sets the job name that will appear in the web user interface for any submitted jobs                                                                                                                                                                                                                                                                                                                                                                |
| ballista.optimizer.broadcast_join_threshold_bytes      | UInt64  | 10485760                      | Byte-size threshold below which a hash join's smaller side is promoted to CollectLeft and lowered via the broadcast pattern. Governs broadcast selection under both the static distributed planner and adaptive query planning (AQE). Set to 0 to disable promotion.                                                                                                                                                                               |
| ballista.optimizer.broadcast_join_threshold_rows       | UInt64  | 1000000                       | Row-count threshold below which a hash join's smaller side is promoted to CollectLeft and lowered via the broadcast pattern, used as a fallback when byte-size statistics are unavailable. Applies to adaptive query planning (AQE). Set to 0 to disable promotion via the row-count path.                                                                                                                                                         |
| ballista.optimizer.hash_join_max_build_partition_bytes | UInt64  | 67108864                      | Maximum per-partition hash-join build-side bytes for a Partitioned hash join under AQE. A build partition larger than this falls back to SortMergeJoin (spillable). Defaults to 64 MiB; 0 disables the check, which makes AQE use a hash join regardless of build size.                                                                                                                                                                            |
| ballista.planner.adaptive.enabled                      | Boolean | false                         | Enables Adaptive Query Planning (EXPERIMENTAL)                                                                                                                                                                                                                                                                                                                                                                                                     |
| ballista.planner.adaptive_join.enabled                 | Boolean | true                          | Enables the AQE dynamic join-selection rule (SelectJoinRule). When true (default), DynamicJoinSelectionExec nodes are resolved to concrete HashJoin or CollectLeft join implementations at runtime. Disable only for debugging.                                                                                                                                                                                                                    |
| ballista.planner.coalesce.enabled                      | Boolean | false                         | Enables the AQE coalesce-shuffle-partitions rule. Disabled by default — opt in when fewer/larger downstream tasks matter more than parallelism.                                                                                                                                                                                                                                                                                                    |
| ballista.planner.coalesce.merged_partition_factor      | Float64 | 1.2                           | Two adjacent partitions are merged when their combined size is below target_partition_bytes times this factor. Mirrors Spark's legacy coalesce semantics.                                                                                                                                                                                                                                                                                          |
| ballista.planner.coalesce.small_partition_factor       | Float64 | 0.2                           | A coalesced partition smaller than target_partition_bytes times this factor counts as small and is merged into its neighbour. Mirrors Spark's legacy coalesce semantics.                                                                                                                                                                                                                                                                           |
| ballista.planner.coalesce.target_partition_bytes       | UInt64  | 67108864                      | Target post-coalesce partition size in bytes. Mirrors Spark's advisoryPartitionSizeInBytes.                                                                                                                                                                                                                                                                                                                                                        |
| ballista.planner.propagate_empty.enabled               | Boolean | true                          | Enables the AQE propagate-empty-relation rule. Injects EmptyExec into the plan where an input is known to be empty, such as one side of a join, allowing downstream work to be skipped.                                                                                                                                                                                                                                                            |
| ballista.scheduler.max_partitions_per_task             | UInt64  | 1                             | Upper bound on the number of input partitions packed into a single task's `partition_slice`. `1` (default) means one task per input partition. Raise to enable multi-partition tasks (fewer tasks, parallel-sort / parallel-join wins); `0` means unbounded — the scheduler fills each task up to the executor's free vcore count. Does not apply to collapse stages, which must pack their full pending queue into a single task for correctness. |
| ballista.standalone.parallelism                        | UInt16  | number of available CPU cores | Number of concurrent tasks a standalone in-process executor will run.                                                                                                                                                                                                                                                                                                                                                                              |

<!-- END GENERATED CONFIG REFERENCE -->

## Shuffle Settings

The following session-level keys control Ballista's shuffle behavior. See
the [tuning guide](tuning-guide.md#shuffle-implementation) for an
explanation of the sort-based shuffle writer.

<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.shuffle. -->

<!-- prettier-ignore -->
| key                                                      | type    | default   | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------------------------------- | ------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ballista.shuffle.compression.codec                       | Utf8    | lz4       | Compression codec specification used in the shuffle process. Possible values: none, lz4, zstd. Defaults to lz4 to preserve current behaviour                                                                                                                                                                                                                                                                                                                                                                                  |
| ballista.shuffle.force_remote_read                       | Boolean | false     | Forces the shuffle reader to always read partitions via the Arrow Flight client, even when partitions are local to the node.                                                                                                                                                                                                                                                                                                                                                                                                  |
| ballista.shuffle.max_concurrent_read_requests            | UInt64  | 64        | Maximum concurrent requests shuffle reader can process                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ballista.shuffle.reader.default_block_size_bytes         | UInt64  | 1048576   | Assumed per-partition byte size charged to the shuffle governor when partition stats carry no byte count.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ballista.shuffle.reader.max_blocks_in_flight_per_address | UInt64  | 128       | Reduce-side shuffle governor: maximum concurrent in-flight partition fetches to a single executor address.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ballista.shuffle.reader.max_bytes_in_flight              | UInt64  | 50331648  | Reduce-side shuffle governor: maximum total in-flight bytes across concurrent remote partition fetches. Mirrors Spark's spark.reducer.maxSizeInFlight. Values above 4 GiB are clamped to 4 GiB (u32 semaphore limit).                                                                                                                                                                                                                                                                                                         |
| ballista.shuffle.remote_read_prefer_flight               | Boolean | false     | Forces the shuffle reader to use flight reader instead of block reader for remote read. Block reader usually has better performance and resource utilization                                                                                                                                                                                                                                                                                                                                                                  |
| ballista.shuffle.sort_based.batch_size                   | UInt64  | 8192      | Target batch size in rows for coalescing small batches in sort shuffle                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ballista.shuffle.sort_based.memory_limit_per_task_bytes  | UInt64  | 268435456 | Per-task buffered-bytes budget at which the sort shuffle writer spills its in-memory batches to disk. Counted independently of the runtime memory pool, so spilling kicks in even when the pool is unbounded. Total worst-case sort shuffle memory per executor is approximately vcores * this value. Set to 0 to disable the per-task budget and rely solely on runtime memory-pool pressure to trigger spilling; this is safe only with a bounded memory pool, otherwise the writer never spills and may run out of memory. |
| ballista.shuffle.writer_channel_capacity                 | UInt32  | 8         | Bounded channel capacity for async-to-blocking I/O bridge in shuffle writer                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

<!-- END GENERATED CONFIG REFERENCE -->

## Testing and Fault Injection

These settings drive chaos-monkey fault injection and exist for robustness
testing. They are not intended for production use.

<!-- BEGIN GENERATED CONFIG REFERENCE prefix=ballista.testing. -->

<!-- prettier-ignore -->
| key                                          | type    | default   | description                                                                                                                                                                                                                          |
| -------------------------------------------- | ------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| ballista.testing.chaos_execution.enabled     | Boolean | false     | Enables chaos-monkey execution injection for robustness testing. When true, ChaosExec is inserted at a random point in the plan once per optimize call.                                                                              |
| ballista.testing.chaos_execution.fault_type  | Utf8    | transient | Fault type injected by chaos-monkey execution. "transient": IoError (retryable); "fatal": Execution error (non-retryable); "panic": panics the task thread; "delay" or "delay:N": sleeps N ms per batch with no error (default N=1). |
| ballista.testing.chaos_execution.probability | Float64 | 0.25      | Failure probability (0.0–1.0) passed to ChaosExec when chaos execution is enabled.                                                                                                                                                   |
| ballista.testing.chaos_execution.seed        | Utf8    | (empty)   | Optional u64 seed for the chaos RNG. Empty string (default) means non-deterministic; set to a numeric value to get reproducible fault injection across runs.                                                                         |

<!-- END GENERATED CONFIG REFERENCE -->

## Ballista Scheduler Configuration Settings

Besides the BallistaContext configuration settings, a few configuration settings for the Ballista scheduler to better
manage the whole cluster are also needed to be taken care of.

_Example: Specifying configuration options when starting the scheduler_

```shell
./ballista-scheduler --scheduler-policy push-staged --event-loop-buffer-size 1000000 --task-distribution round-robin
```

| key                                          | type   | default     | description                                                                                                                |
| -------------------------------------------- | ------ | ----------- | -------------------------------------------------------------------------------------------------------------------------- |
| scheduler-policy                             | Utf8   | pull-staged | Sets the task scheduling policy for the scheduler, possible values: pull-staged, push-staged.                              |
| event-loop-buffer-size                       | UInt32 | 10000       | Sets the event loop buffer size. for a system of high throughput, a larger value like 1000000 is recommended.              |
| task-distribution                            | Utf8   | bias        | Sets the task distribution policy for the scheduler, possible values: bias, round-robin                                    |
| finished-job-data-clean-up-interval-seconds  | UInt64 | 300         | Sets the delayed interval for cleaning up finished job data, mainly the shuffle data, 0 means the cleaning up is disabled. |
| finished-job-state-clean-up-interval-seconds | UInt64 | 3600        | Sets the delayed interval for cleaning up finished job state stored in the backend, 0 means the cleaning up is disabled.   |
