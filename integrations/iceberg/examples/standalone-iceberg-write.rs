// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! # Standalone Iceberg write example
//!
//! Demonstrates distributed reads and writes against an Apache Iceberg table
//! from a standalone Ballista cluster.
//!
//! The example starts its own Iceberg REST catalog and MinIO with
//! testcontainers, so all it needs is a running docker daemon:
//!
//! ```bash
//! cargo run -p iceberg-ballista --example standalone-iceberg-write
//! ```
//!
//! The containers are removed when the example exits.

// Shared with the integration tests rather than duplicated.
#[path = "../tests/fixture/mod.rs"]
mod fixture;

use ballista::datafusion::common::Result;
use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use iceberg_ballista::{
    IcebergCatalogConfig, register_iceberg_codecs, register_iceberg_table,
};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    // Iceberg REST catalog + MinIO in docker, removed when this drops.
    println!("== starting Iceberg REST catalog and MinIO ==");
    let catalog_fixture = fixture::IcebergFixture::start().await;
    let props = catalog_fixture.props();

    // Create the target table in the fresh catalog.
    let (namespace, table) = fixture::create_demo_table(&props).await;

    // Build a Ballista session config with the Iceberg codecs installed, so the
    // standalone scheduler and executor can serialize the Iceberg plan nodes.
    let config = register_iceberg_codecs(
        SessionConfig::new_with_ballista()
            .with_target_partitions(2)
            .with_ballista_standalone_parallelism(2),
    );
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::standalone_with_state(state).await?;

    // Register the catalog-backed Iceberg table for distributed reads and writes.
    let catalog_config = IcebergCatalogConfig::new("rest", "rest", props);
    register_iceberg_table(&ctx, "events", catalog_config, namespace, table).await?;

    // Distributed INSERT: IcebergWriteExec runs across the cluster and
    // IcebergCommitExec atomically appends the data files to the table.
    println!("== INSERT ==");
    ctx.sql("INSERT INTO events VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await?
        .show()
        .await?;

    // Read it back through the distributed scan.
    println!("== SELECT ==");
    ctx.sql("SELECT id, name FROM events ORDER BY id")
        .await?
        .show()
        .await?;

    Ok(())
}
