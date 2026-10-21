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
//! Scans an Iceberg table through a standalone Ballista cluster: live, at its
//! latest version, and static, at an older one.
//!
//! The table is created in a temporary local warehouse, so the example needs
//! nothing running:
//!
//! ```text
//! cargo run -p iceberg-ballista --example standalone-iceberg-scan
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_ballista::register_iceberg_codecs;
use iceberg_datafusion::{
    IcebergCatalogTableProvider, IcebergStaticTableProvider, to_datafusion_error,
};

#[tokio::main]
async fn main() -> Result<()> {
    let warehouse = tempfile::tempdir()?;
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.path().display().to_string(),
                )]),
            )
            .await
            .map_err(to_datafusion_error)?,
    );

    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .map_err(to_datafusion_error)?;
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "city", Type::Primitive(PrimitiveType::String))
                .into(),
        ])
        .build()
        .map_err(to_datafusion_error)?;
    let creation = TableCreation::builder()
        .name("trips".to_string())
        .schema(schema)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .map_err(to_datafusion_error)?;

    // A catalog-backed table reads the table's latest version in every query,
    // so it sees commits made after it was registered: a live read. Ballista
    // can't write to Iceberg yet, so plain DataFusion commits through it.
    let trips = Arc::new(
        IcebergCatalogTableProvider::try_new(catalog, namespace, "trips").await?,
    );
    let writer = SessionContext::new();
    writer.register_table("trips", trips.clone())?;
    let insert = async |values: &str| -> Result<()> {
        let sql = format!("INSERT INTO trips VALUES {values}");
        writer.sql(&sql).await?.collect().await?;
        Ok(())
    };
    insert("(1, 'Paris'), (2, 'Oslo')").await?;

    // Start standalone Ballista with the Iceberg codecs.
    let state = SessionStateBuilder::new()
        .with_config(register_iceberg_codecs(SessionConfig::new_with_ballista()))
        .with_default_features()
        .build();
    let ctx = SessionContext::standalone_with_state(state).await?;
    ctx.register_table("trips", trips.clone())?;

    // A static table always reads the version it was made from: time travel.
    let table = trips
        .catalog()
        .load_table(trips.table_ident())
        .await
        .map_err(to_datafusion_error)?;
    let v1 = IcebergStaticTableProvider::try_new_from_table(table).await?;
    ctx.register_table("trips_v1", Arc::new(v1))?;

    ctx.sql("SELECT * FROM trips ORDER BY id")
        .await?
        .show()
        .await?;
    insert("(3, 'Lima')").await?;
    // Now includes Lima.
    ctx.sql("SELECT * FROM trips ORDER BY id")
        .await?
        .show()
        .await?;
    // Still only Paris and Oslo.
    ctx.sql("SELECT * FROM trips_v1 ORDER BY id")
        .await?
        .show()
        .await?;
    Ok(())
}
