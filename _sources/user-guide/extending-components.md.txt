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

Ballista scheduler can be tunned using `SchedulerConfig` which supports overriding `config producer`, `session builder`, `logical codec`, `physical codec`

## Example: Custom Object Store Integration

Extending basic building blocks will be demonstrated by integrating S3 object store. For this, new `ObjectStoreRegistry` and `S3Options` will be provided. `ObjectStoreRegistry` creates new `ObjectStore` instances configured using `S3Options`.

For this specific task `config producer`, `runtime producer` and `session builder` have to be provided, and client, scheduler and executor need to be configured.

```rust
/// Custom [SessionConfig] constructor method
///
/// This method registers config extension [S3Options]
/// which is used to configure [ObjectStore] with ACCESS and
/// SECRET key
pub fn custom_session_config_with_s3_options() -> SessionConfig {
    SessionConfig::new_with_ballista()
        .with_information_schema(true)
        .with_option_extension(S3Options::default())
}
```

```rust
/// Custom [RuntimeEnv] constructor method
///
/// It will register [CustomObjectStoreRegistry] which will
/// use configuration extension [S3Options] to configure
/// and created [ObjectStore]s
pub fn custom_runtime_env_with_s3_support(
    session_config: &SessionConfig,
) -> Result<Arc<RuntimeEnv>> {
    let s3options = session_config
        .options()
        .extensions
        .get::<S3Options>()
        .ok_or(DataFusionError::Configuration(
            "S3 Options not set".to_string(),
        ))?;

    let config = RuntimeConfig::new().with_object_store_registry(Arc::new(
        CustomObjectStoreRegistry::new(s3options.clone()),
    ));

    Ok(Arc::new(RuntimeEnv::try_new(config)?))
}
```

```rust
/// Custom [SessionState] constructor method
///
/// It will configure [SessionState] with provided [SessionConfig],
/// and [RuntimeEnv].
pub fn custom_session_state_with_s3_support(
    session_config: SessionConfig,
) -> SessionState {
    let runtime_env = custom_runtime_env_with_s3_support(&session_config).unwrap();

    SessionStateBuilder::new()
        .with_runtime_env(runtime_env)
        .with_config(session_config)
        .build()
}
```

`S3Options` & `CustomObjectStoreRegistry` implementation can be found in examples sub-project.

### Configuring Scheduler

```rust
#[tokio::main]
async fn main() -> Result<()> {
  // parse CLI options (default options which Ballista scheduler exposes)
  let (opt, _remaining_args) =
      Config::including_optional_config_files(&["/etc/ballista/scheduler.toml"])
          .unwrap_or_exit();

  let addr = format!("{}:{}", opt.bind_host, opt.bind_port);
  let addr = addr.parse()?;

  // converting CLI options to SchedulerConfig
  let mut config: SchedulerConfig = opt.try_into()?;

  // overriding default runtime producer with custom producer
  // which knows how to create S3 connections
  config.override_config_producer =
      Some(Arc::new(custom_session_config_with_s3_options));

  // overriding default session builder, which has custom session configuration
  // runtime environment and session state.
  config.override_session_builder = Some(Arc::new(|session_config: SessionConfig| {
      custom_session_state_with_s3_support(session_config)
  }));
  let cluster = BallistaCluster::new_from_config(&config).await?;
  start_server(cluster, addr, Arc::new(config)).await?;
  Ok(())
}
```

### Configuring Executor

```rust
#[tokio::main]
async fn main() -> Result<()> {
  // parse CLI options (default options which Ballista executor exposes)
  let (opt, _remaining_args) =
      Config::including_optional_config_files(&["/etc/ballista/executor.toml"])
          .unwrap_or_exit();

  // Converting CLI options to executor configuration
  let mut config: ExecutorProcessConfig = opt.try_into().unwrap();

  // overriding default config producer with custom producer
  // which has required S3 configuration options
  config.override_config_producer =
      Some(Arc::new(custom_session_config_with_s3_options));

  // overriding default runtime producer with custom producer
  // which knows how to create S3 connections
  config.override_runtime_producer =
      Some(Arc::new(|session_config: &SessionConfig| {
          custom_runtime_env_with_s3_support(session_config)
      }));

  start_executor_process(Arc::new(config)).await
  Ok(())
}

```

### Configuring Client

```rust
let test_data = ballista_examples::test_util::examples_test_data();

// new sessions state with required custom session configuration and runtime environment
let state =
    custom_session_state_with_s3_support(custom_session_config_with_s3_options());

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
ctx.sql("SET s3.allow_http = true").await?.show().await?;

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

## Example: Client Side Logical/Physical Codec

Default physical and logical codecs can be replaced if needed. For scheduler and executor procedure is similar to previous example. At the client side procedure is slightly different, `ballista::prelude::SessionConfigExt` provides methods to be used to override physical and logical codecs on client side.

```rust
let session_config = SessionConfig::new_with_ballista()
    .with_information_schema(true)
    .with_ballista_physical_extension_codec(Arc::new(BetterPhysicalCodec::default()))
    .with_ballista_logical_extension_codec(Arc::new(BetterLogicalCodec::default()));

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_config(session_config)
    .build();

let ctx: SessionContext = SessionContext::standalone_with_state(state).await?;
```
