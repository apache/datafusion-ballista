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

# Apache Iceberg

The `ballista-iceberg` crate (`integrations/iceberg`) lets Ballista scan
[Apache Iceberg](https://iceberg.apache.org/) tables. On top of
[iceberg-datafusion](https://github.com/apache/datafusion-iceberg), it adds the
codecs that send Iceberg tables and scans between the client, the scheduler and
the executors.

The crate is not published yet. It depends on unreleased revisions of
iceberg-datafusion and iceberg-rust, and needs Rust 1.95.

## Usage

Ready the session with `register_iceberg`, then mount an Iceberg catalog:

```rust
use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ballista_iceberg::register_iceberg;
use iceberg_datafusion::IcebergCatalogProvider;

let state = SessionStateBuilder::new()
    .with_config(SessionConfig::new_with_ballista())
    .with_default_features()
    .build();
let ctx = SessionContext::standalone_with_state(register_iceberg(state)).await?;

ctx.register_catalog("rest", Arc::new(IcebergCatalogProvider::try_new(catalog).await?));
ctx.sql("SELECT * FROM rest.ns.t").await?.show().await?;
```

A single table registered as an `IcebergCatalogTableProvider` works the same
way. Either kind reads the table's latest version in each query.

For time travel, register an `IcebergStaticTableProvider`, which keeps one
version: `IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)`.

The example registers both on a table in a temporary local warehouse, and shows
a commit reaching the live table but not the static one:

```shell
cargo run -p ballista-iceberg --example standalone-iceberg-scan
```

## SQL clients

Flight SQL clients (JDBC, ADBC, BI tools) send SQL to the scheduler, which plans
it, so they hold no catalog. To let them query Iceberg tables, mount catalogs
on the scheduler with `iceberg_session_builder`, and give the scheduler and
executors the Iceberg codecs:

```rust
use ballista_iceberg::{IcebergLogicalCodec, IcebergPhysicalCodec, iceberg_session_builder};

let rest = IcebergCatalogProvider::try_new(catalog).await?;
let mut config = SchedulerConfig::default()
    .with_flight_sql(true)
    .with_override_session_builder(Arc::new(iceberg_session_builder([(
        "rest".to_string(),
        Arc::new(rest) as _,
    )])));
config.override_logical_codec = Some(Arc::new(IcebergLogicalCodec::default()));
config.override_physical_codec = Some(Arc::new(IcebergPhysicalCodec::default()));
```

Executors set the same two codecs on their `ExecutorProcessConfig`. A client can
then run `SELECT * FROM rest.ns.t`. Tables are pinned the same way as on the
client, while the scheduler plans the query.

## How it works

Ballista sends the client's plan to the scheduler, and each stage's plan to
the executors, as bytes. The codecs replace each live Iceberg table with one
version of it: the location of its metadata file and its serialized `FileIO`.
Metadata files never change, so every node rebuilds exactly that version.

- **A catalog-backed table** is pinned to the version its catalog serves when
  the client plans the query. DataFusion looks up each table a query names
  once, so every scan of it, such as each side of a self-join, reads that
  version, and each new query reads the latest one.
- **A static table** is sent as the version it holds.
- **A scan** is sent as the version the scheduler planned it against.

Tables sent from the client also carry the metadata itself, so the scheduler
needn't read the file. Scans sent to executors carry only the location, which
keeps each task's plan small.

Only the node that plans a query talks to a catalog: the client, or the
scheduler for SQL clients. Executors never need catalog configuration.

## Limitations

- **Reads only.** `INSERT INTO` an Iceberg table fails with "isn't supported
  on Ballista yet".
- **No metadata tables.** Tables such as `t$snapshots` can't be sent to the
  scheduler yet.
- **One task per scan.** Iceberg scans do not split into partitions yet, so one
  executor reads the whole table. Operators above the scan still run in
  parallel.
- **Storage credentials travel in plain text.** Each table's serialized
  `FileIO` includes them, so the links between client, scheduler and executors
  must be trusted.
- **A view keeps the version from when it was created.** DataFusion stores a
  view as a planned query, and tables are pinned when a query is planned. The
  same goes for a `DataFrame` or a Flight SQL prepared statement run more than
  once: each run reads the version from when it was planned. To read newer
  commits, recreate the view or plan the query again.
- **A separately deployed scheduler and executors** must link
  `ballista-iceberg` and set the Iceberg codecs as their
  `override_logical_codec` and `override_physical_codec` (see
  [Extending Components](extending-components.md)). The tests cover standalone
  mode and an in-process scheduler serving Flight SQL.
- **A mounted catalog lists its tables once.** `IcebergCatalogProvider` reads
  the table names when it is built, so a table created afterwards isn't
  visible until the catalog is mounted again.
