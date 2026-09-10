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

# Distributing DataFusion with Ballista

To connect to a Ballista cluster from Rust, first start by creating a `SessionContext` connected to remote scheduler server.

```rust
use ballista::prelude::*;
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};

let config = SessionConfig::new_with_ballista()
    .with_target_partitions(4)
    .with_ballista_job_name("Remote SQL Example");

let state = SessionStateBuilder::new()
    .with_config(config)
    .with_default_features()
    .build();

let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;
```

For testing purposes, standalone, in process cluster could be started with:

```rust
use ballista::prelude::*;
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
let config = SessionConfig::new_with_ballista()
    .with_target_partitions(1)
    .with_ballista_standalone_parallelism(2);

let state = SessionStateBuilder::new()
    .with_config(config)
    .with_default_features()
    .build();

let ctx = SessionContext::standalone_with_state(state).await?;

```

The following examples require running remote scheduler and executor nodes.

Full example using the DataFrame API, from
[`examples/examples/remote-dataframe.rs`](https://github.com/apache/datafusion-ballista/blob/main/examples/examples/remote-dataframe.rs):

```{literalinclude} ../../../examples/examples/remote-dataframe.rs
:language: rust
:lines: 18-
```

A full example demonstrating SQL usage with a user-specified `SessionConfig`, from
[`examples/examples/remote-sql.rs`](https://github.com/apache/datafusion-ballista/blob/main/examples/examples/remote-sql.rs):

```{literalinclude} ../../../examples/examples/remote-sql.rs
:language: rust
:lines: 18-
```
