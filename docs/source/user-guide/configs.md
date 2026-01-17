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
| task-distribution                            | Utf8   | bias        | Sets the task distribution policy for the scheduler, possible values: bias, round-robin, consistent-hash.                  |
| finished-job-data-clean-up-interval-seconds  | UInt64 | 300         | Sets the delayed interval for cleaning up finished job data, mainly the shuffle data, 0 means the cleaning up is disabled. |
| finished-job-state-clean-up-interval-seconds | UInt64 | 3600        | Sets the delayed interval for cleaning up finished job state stored in the backend, 0 means the cleaning up is disabled.   |
