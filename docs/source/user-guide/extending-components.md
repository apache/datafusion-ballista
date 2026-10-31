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

# Extending Ballista Scheduler And Executors

Ballista scheduler and executor provide a set of configuration options
which can be used to extend their basic functionality. They allow registering
new configuration extensions, object stores, logical and physical codecs ...

- `function registry` - provides possibility to override set of build in functions.
- `config producer` - function which creates new `SessionConfig`, which can hold extended configuration options
- `runtime producer` - function which creates new `RuntimeEnv` based on provided `SessionConfig`.
- `session builder` - function which creates new `SessionState` for each user session
- `logical codec` - overrides `LogicalCodec`
- `physical codec` - overrides `PhysicalCodec`
- `partition sliceable resolver` - lets the scheduler restrict `ExecutionPlan` nodes of your own to the partitions of a task, see [Custom Physical Plan Nodes](#custom-physical-plan-nodes)

Ballista executor can be configured using `ExecutorProcessConfig` which supports overriding `function registry`,`runtime producer`, `config producer`, `logical codec`, `physical codec`.

Ballista scheduler can be tuned using `SchedulerConfig` which supports overriding `config producer`, `session builder`, `logical codec`, `physical codec` and setting a `partition sliceable resolver`.

## Example: Custom Object Store Integration

Extending basic building blocks will be demonstrated by integrating S3 object store. For this, new `ObjectStoreRegistry` and `S3Options` will be provided. `ObjectStoreRegistry` creates `ObjectStore` instances configured using `S3Options`, and caches them so that scans of the same bucket with the same options share one store.

For this specific task `config producer`, `runtime producer` and `session builder` have to be provided, and client, scheduler and executor need to be configured.

These three functions ship in `ballista_core::object_store`, so the snippets below are the
shipped implementations rather than something you have to write from scratch. They are included
from the source file at build time, so they cannot drift from it.

```{literalinclude} ../../../ballista/core/src/object_store.rs
:language: rust
:start-at: /// Custom [SessionConfig] constructor method
:end-before: /// Custom [RuntimeEnv] constructor method
```

```{literalinclude} ../../../ballista/core/src/object_store.rs
:language: rust
:start-at: /// Custom [RuntimeEnv] constructor method
:end-before: /// Custom [SessionState] with S3 support enabled
```

```{literalinclude} ../../../ballista/core/src/object_store.rs
:language: rust
:start-at: /// Custom [SessionState] with S3 support enabled
:end-before: /// Custom [SessionState] with S3 support.
```

`S3Options` & `CustomObjectStoreRegistry` are implemented in `ballista_core::object_store`. The
scheduler, executor, and client below are the runnable `custom-scheduler.rs`, `custom-executor.rs`,
and `custom-client.rs` from
[`examples/examples/`](https://github.com/apache/datafusion-ballista/tree/main/examples/examples),
also included from source.

### Configuring Scheduler

```{literalinclude} ../../../examples/examples/custom-scheduler.rs
:language: rust
:start-after: under the License.
```

To keep the scheduler's own command-line interface, parse `ballista_scheduler::config::Config`
with clap and convert it, then apply the overrides:

```rust
let opt = Config::parse();
let mut config: SchedulerConfig = opt.try_into()?;
config.override_config_producer = Some(Arc::new(session_config_with_s3_support));
config.override_session_builder = Some(Arc::new(session_state_with_s3_support));
```

### Configuring Executor

```{literalinclude} ../../../examples/examples/custom-executor.rs
:language: rust
:start-after: under the License.
```

As with the scheduler, to keep the executor's own command line, parse
`ballista_executor::config::Config` with clap, convert it to `ExecutorProcessConfig` with
`try_into()`, and then apply the overrides.

### Configuring Client

```{literalinclude} ../../../examples/examples/custom-client.rs
:language: rust
:start-after: under the License.
```

## Custom Physical Plan Nodes

A plan with `ExecutionPlan` nodes of your own can run on a Ballista cluster once the scheduler and the executors have a `physical codec` that serializes them. A node that holds data per partition needs one thing more.

When a stage is split between tasks, each task gets a copy of the plan restricted to its own partitions, and operators see only task-local partition indices: a task that runs partitions 4 and 5 executes partitions 0 and 1 of every node. The scheduler restricts the leaves it knows, such as shuffle readers and file and in-memory scans. A node it doesn't know would read its per-partition data, such as one list of files for each partition, from the wrong index, and the stage would return some partitions twice and others not at all. The scheduler fails a job whose split stage holds such a leaf. It cannot tell when an interior node holds per-partition data, so register those too.

To make your node restrictable, implement `PartitionSliceable` for it and have the scheduler find it with a `partition sliceable resolver`:

```rust
use ballista_core::execution_plans::plan_algebra::{PartitionSliceable, slice_by_global_partition};

impl PartitionSliceable for MyScanExec {
    fn restrict_to_partitions(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
        partitions: &[usize],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // `partitions` are indices of this node's own partitions, in the order
        // the task should number them.
        let files = slice_by_global_partition(&self.files, partitions, "MyScanExec", "file lists")?;
        Ok(Arc::new(MyScanExec::new(files, children)))
    }
}

let config = SchedulerConfig::default().with_partition_sliceable_resolver(Arc::new(|plan| {
    plan.downcast_ref::<MyScanExec>()
        .map(|node| node as &dyn PartitionSliceable)
}));
```

- `children` are the node's inputs, none for a scan, restricted to `partitions`. An input the node declares `SinglePartition` arrives whole, as a join's build side does. Partition `i` of the node must read partition `i` of every other input.
- The restricted node must have exactly `partitions.len()` partitions. The scheduler rejects any other count.
- `partitions` is empty for a `UnionExec` branch that belongs to other tasks. Return a node without partitions.
- The restricted node is serialized with the scheduler's `physical codec`, so what it carries per partition has to survive encoding and decoding.
- The resolver is asked after Ballista's own list. A node that is read whole, such as one below a `CoalescePartitionsExec` or on a join's build side, needs none of this.
- The standalone scheduler (`SessionContext::standalone`) builds its own `SchedulerConfig`, so it cannot be given a resolver.
