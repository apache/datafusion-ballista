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

//! Helpers shared by the integration tests.

#![allow(dead_code)]

#[cfg(feature = "testcontainers")]
pub mod rustfs;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_iceberg::register_iceberg_codecs;
use datafusion::assert_batches_eq;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::{
    IcebergCatalogProvider, IcebergCatalogTableProvider, IcebergStaticTableProvider,
};

/// A memory catalog with its warehouse in `dir`, holding an empty [`create_table`].
pub async fn memory_catalog(dir: &Path) -> Arc<dyn Catalog> {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                dir.display().to_string(),
            )]),
        )
        .await
        .unwrap();
    create_table(&catalog).await;
    Arc::new(catalog)
}

/// Creates the empty table `ns.t (id bigint, name string)`.
pub async fn create_table(catalog: &dyn Catalog) {
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))
                .into(),
        ])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .build();
    catalog.create_table(&namespace, creation).await.unwrap();
}

/// `ns.t`, read through `catalog` on every query.
pub async fn catalog_table(catalog: &Arc<dyn Catalog>) -> IcebergCatalogTableProvider {
    let namespace = NamespaceIdent::new("ns".to_string());
    IcebergCatalogTableProvider::try_new(catalog.clone(), namespace, "t")
        .await
        .unwrap()
}

/// Commits `values` to `ns.t` with plain DataFusion, and returns the table as
/// of that commit.
pub async fn insert(catalog: &Arc<dyn Catalog>, values: &str) -> Table {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(catalog_table(catalog).await))
        .unwrap();
    ctx.sql(&format!("INSERT INTO t VALUES {values}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    catalog
        .load_table(&TableIdent::from_strs(["ns", "t"]).unwrap())
        .await
        .unwrap()
}

/// Commits to an empty `ns.t` twice, and checks that standalone Ballista reads
/// `catalog` mounted at the latest commit and a static table at the first.
pub async fn assert_standalone_scans(catalog: Arc<dyn Catalog>) -> Result<()> {
    let first = insert(&catalog, "(1, 'a'), (2, NULL)").await;

    let state = SessionStateBuilder::new()
        .with_config(register_iceberg_codecs(SessionConfig::new_with_ballista()))
        .with_default_features()
        .build();
    let ctx = SessionContext::standalone_with_state(state).await?;
    let mount = IcebergCatalogProvider::try_new(catalog.clone()).await?;
    ctx.register_catalog("iceberg", Arc::new(mount));
    let v1 = IcebergStaticTableProvider::try_new_from_table(first).await?;
    ctx.register_table("t_v1", Arc::new(v1))?;

    insert(&catalog, "(3, 'c')").await;

    let result = ctx
        .sql("SELECT * FROM iceberg.ns.t ORDER BY id")
        .await?
        .collect()
        .await?;
    let expected = [
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 1  | a    |",
        "| 2  |      |",
        "| 3  | c    |",
        "+----+------+",
    ];
    assert_batches_eq!(expected, &result);

    let result = ctx
        .sql("SELECT * FROM t_v1 ORDER BY id")
        .await?
        .collect()
        .await?;
    let expected = [
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 1  | a    |",
        "| 2  |      |",
        "+----+------+",
    ];
    assert_batches_eq!(expected, &result);

    Ok(())
}
