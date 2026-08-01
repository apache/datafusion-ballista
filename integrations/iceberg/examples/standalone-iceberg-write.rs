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
//! This example requires a running Iceberg REST catalog and MinIO. Bring up the
//! docker fixture from the workspace root:
//!
//! ```bash
//! docker compose -f integrations/iceberg/dev/docker-compose.yaml up -d --wait
//! cargo run -p iceberg-ballista --example standalone-iceberg-write
//! ```
//!
//! Endpoints can be overridden with the `ICEBERG_REST_URI` and
//! `ICEBERG_S3_ENDPOINT` environment variables.

use std::collections::HashMap;
use std::sync::Arc;

use ballista::datafusion::common::Result;
use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_ballista::{
    IcebergCatalogConfig, register_iceberg_codecs, register_iceberg_table,
};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

/// Catalog + storage properties for the REST catalog and its MinIO storage.
fn catalog_props() -> HashMap<String, String> {
    let rest_uri = std::env::var("ICEBERG_REST_URI")
        .unwrap_or_else(|_| "http://localhost:8181".to_string());
    let s3_endpoint = std::env::var("ICEBERG_S3_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    HashMap::from([
        ("uri".to_string(), rest_uri),
        ("s3.endpoint".to_string(), s3_endpoint),
        ("s3.access-key-id".to_string(), "admin".to_string()),
        ("s3.secret-access-key".to_string(), "password".to_string()),
        ("s3.region".to_string(), "us-east-1".to_string()),
        ("s3.path-style-access".to_string(), "true".to_string()),
    ])
}

/// Creates the demo namespace and table in the catalog if they do not exist.
async fn ensure_table(
    props: &HashMap<String, String>,
) -> Result<(NamespaceIdent, String)> {
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", props.clone())
        .await
        .expect("build rest catalog");

    let namespace = NamespaceIdent::new("ballista_demo".to_string());
    if !catalog
        .namespace_exists(&namespace)
        .await
        .expect("ns exists")
    {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
    }

    let table_ident = TableIdent::new(namespace.clone(), "events".to_string());
    if !catalog
        .table_exists(&table_ident)
        .await
        .expect("table exists")
    {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .build()
            .expect("build schema");
        let creation = TableCreation::builder()
            .name("events".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog
            .create_table(&namespace, creation)
            .await
            .expect("create table");
    }

    Ok((namespace, "events".to_string()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let props = catalog_props();

    // Make sure the target table exists in the catalog.
    let (namespace, table) = ensure_table(&props).await?;

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
