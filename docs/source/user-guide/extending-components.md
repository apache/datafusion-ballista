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

`S3Options` & `CustomObjectStoreRegistry` are implemented in `ballista_core::object_store`. Runnable
versions of the scheduler, executor, and client wiring below are in
[`examples/examples/`](https://github.com/apache/datafusion-ballista/tree/main/examples/examples)
as `custom-scheduler.rs`, `custom-executor.rs`, and `custom-client.rs`.

### Configuring Scheduler

```rust
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let config: SchedulerConfig = SchedulerConfig {
        // overriding default config producer with custom producer
        // which has required S3 configuration options
        override_config_producer: Some(Arc::new(session_config_with_s3_support)),
        // overriding default session builder, which has custom session configuration
        // runtime environment and session state.
        override_session_builder: Some(Arc::new(session_state_with_s3_support)),
        ..Default::default()
    };

    let addr = format!("{}:{}", config.bind_host, config.bind_port);
    let addr = addr
        .parse()
        .map_err(|e: AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
```

To keep the scheduler's own command-line interface, parse `ballista_scheduler::config::Config`
with clap and convert it, then apply the overrides:

```rust
let opt = Config::parse();
let mut config: SchedulerConfig = opt.try_into()?;
config.override_config_producer = Some(Arc::new(session_config_with_s3_support));
```

### Configuring Executor

```rust
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let config: ExecutorProcessConfig = ExecutorProcessConfig {
        // overriding default config producer with custom producer
        // which has required S3 configuration options
        override_config_producer: Some(Arc::new(session_config_with_s3_support)),
        // overriding default runtime producer with custom producer
        // which knows how to create S3 connections
        override_runtime_producer: Some(Arc::new(runtime_env_with_s3_support)),
        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
```

As with the scheduler, `ballista_executor::executor_process::ExecutorProcessConfig` can be
built from the executor's own clap-parsed options with `opt.try_into()` when you want to keep
the standard command line.

### Configuring Client

```rust
let test_data = ballista_examples::test_util::examples_test_data();

// new session state with required custom session configuration and runtime environment
// `state_with_s3_support()` is the shorthand for the two calls below
let state = session_state_with_s3_support(session_config_with_s3_support())?;

let ctx: SessionContext =
    SessionContext::remote_with_state("df://localhost:50050", state).await?;

// once we have it all setup we can configure object store
//
// as session config has relevant S3 options registered and exposed,
// S3 configuration options can be changed using SQL `SET` statement.

ctx.sql("SET s3.allow_http = true").await?.show().await?;

ctx.sql(&format!("SET s3.access_key_id = '{}'", S3_ACCESS_KEY_ID))
    .await?
    .show()
    .await?;

ctx.sql(&format!("SET s3.secret_access_key = '{}'", S3_SECRET_KEY))
    .await?
    .show()
    .await?;

ctx.sql("SET s3.endpoint = 'http://localhost:9000'")
    .await?
    .show()
    .await?;

ctx.register_parquet(
    "test",
    &format!("{test_data}/alltypes_plain.parquet"),
    Default::default(),
)
.await?;

let write_dir_path = &format!("s3://{}/write_test.parquet", S3_BUCKET);

ctx.sql("select * from test")
    .await?
    .write_parquet(write_dir_path, Default::default(), Default::default())
    .await?;

ctx.register_parquet("written_table", write_dir_path, Default::default())
    .await?;

let result = ctx
    .sql("select id, string_col, timestamp_col from written_table where id > 4")
    .await?
    .collect()
    .await?;

let expected = [
    "+----+------------+---------------------+",
    "| id | string_col | timestamp_col       |",
    "+----+------------+---------------------+",
    "| 5  | 31         | 2009-03-01T00:01:00 |",
    "| 6  | 30         | 2009-04-01T00:00:00 |",
    "| 7  | 31         | 2009-04-01T00:01:00 |",
    "+----+------------+---------------------+",
];

assert_batches_eq!(expected, &result);
```
