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

Ballista executor can be configured using `ExecutorProcessConfig` which supports overriding `function registry`,`runtime producer`, `config producer`, `logical codec`, `physical codec`.

Ballista scheduler can be tuned using `SchedulerConfig` which supports overriding `config producer`, `session builder`, `logical codec`, `physical codec`

## Example: Custom Object Store Integration

Extending basic building blocks will be demonstrated by integrating S3 object store. For this, new `ObjectStoreRegistry` and `S3Options` will be provided. `ObjectStoreRegistry` creates new `ObjectStore` instances configured using `S3Options`.

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
