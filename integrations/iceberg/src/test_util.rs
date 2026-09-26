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

//! Fixtures for the codec unit tests.

use std::collections::HashMap;
use std::path::Path;

use datafusion_iceberg::IcebergCatalogConfig;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::spec::{
    FormatVersion, MAIN_BRANCH, NestedField, Operation, PartitionSpec, PrimitiveType,
    Schema, Snapshot, SortOrder, Summary, TableMetadata, TableMetadataBuilder, Type,
};
use iceberg::table::Table;
use iceberg::test_utils::test_runtime;

pub(crate) fn catalog_config() -> IcebergCatalogConfig {
    IcebergCatalogConfig::new(
        "rest",
        "rest",
        HashMap::from([("uri".to_string(), "http://localhost:8181".to_string())]),
    )
}

/// Metadata of table `ns.tbl` with columns `{id, name}` and the given
/// snapshots, committed in order to the main branch, so the last one is current.
/// No data files exist: only the metadata is real.
fn metadata(snapshot_ids: &[i64]) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))
                .into(),
        ])
        .build()
        .unwrap();
    let mut metadata = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "/test/tbl".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;

    let mut parent = None;
    for (sequence, &id) in (1..).zip(snapshot_ids) {
        let snapshot = Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence)
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list(format!("/test/tbl/snap-{id}.avro"))
            .with_schema_id(metadata.current_schema_id())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        metadata = TableMetadataBuilder::new_from_metadata(metadata, None)
            .set_branch_snapshot(snapshot, MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        parent = Some(id);
    }
    metadata
}

fn table_at(metadata: TableMetadata, file_io: FileIO, location: &str) -> Table {
    Table::builder()
        .metadata(metadata)
        .identifier(TableIdent::from_strs(["ns", "tbl"]).unwrap())
        .file_io(file_io)
        .metadata_location(location)
        .runtime(test_runtime())
        .build()
        .unwrap()
}

/// [`metadata`] as a table whose metadata file, `/test/tbl/metadata.json`, does
/// not exist: enough to encode nodes, not to decode them.
pub(crate) fn table(snapshot_ids: &[i64]) -> Table {
    table_with_file_io(snapshot_ids, FileIO::new_with_fs())
}

/// [`table`] with a given `FileIO`.
pub(crate) fn table_with_file_io(snapshot_ids: &[i64], file_io: FileIO) -> Table {
    table_at(metadata(snapshot_ids), file_io, "/test/tbl/metadata.json")
}

/// [`metadata`] written to a metadata file under `dir`, so the table can be
/// rebuilt from it.
pub(crate) fn stored_table(dir: &Path, snapshot_ids: &[i64]) -> Table {
    let location = dir.join("metadata.json");
    let metadata = metadata(snapshot_ids);
    std::fs::write(&location, serde_json::to_vec(&metadata).unwrap()).unwrap();
    table_at(metadata, FileIO::new_with_fs(), location.to_str().unwrap())
}

/// A local-filesystem storage backend that records which thread reads each
/// file, so tests can check where a table's work runs.
pub(crate) mod recording_storage {
    use std::ops::Range;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use iceberg::Result;
    use iceberg::io::{
        FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile,
        Storage, StorageConfig, StorageFactory,
    };
    use serde::{Deserialize, Serialize};

    /// `(path, reading thread's name)` of every read, across all tests;
    /// [`take_reads`] picks out one test's by directory.
    static READS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

    fn record(path: &str) {
        let thread = std::thread::current().name().unwrap_or("").to_string();
        READS.lock().unwrap().push((path.to_string(), thread));
    }

    /// Removes and returns the reads of files under `dir`.
    pub(crate) fn take_reads(dir: &Path) -> Vec<(String, String)> {
        let dir = dir.to_str().unwrap();
        let mut reads = READS.lock().unwrap();
        let (taken, kept) = reads.drain(..).partition(|(path, _)| path.contains(dir));
        *reads = kept;
        taken
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) struct RecordingStorageFactory;

    #[typetag::serde]
    impl StorageFactory for RecordingStorageFactory {
        fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
            Ok(Arc::new(RecordingStorage))
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct RecordingStorage;

    /// Records each read when it happens, not when the reader is opened.
    struct RecordingRead(Box<dyn FileRead>, String);

    #[async_trait]
    impl FileRead for RecordingRead {
        async fn read(&self, range: Range<u64>) -> Result<Bytes> {
            record(&self.1);
            self.0.read(range).await
        }
    }

    #[async_trait]
    #[typetag::serde]
    impl Storage for RecordingStorage {
        async fn exists(&self, path: &str) -> Result<bool> {
            LocalFsStorage.exists(path).await
        }
        async fn metadata(&self, path: &str) -> Result<FileMetadata> {
            LocalFsStorage.metadata(path).await
        }
        async fn read(&self, path: &str) -> Result<Bytes> {
            record(path);
            LocalFsStorage.read(path).await
        }
        async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
            let reader = LocalFsStorage.reader(path).await?;
            Ok(Box::new(RecordingRead(reader, path.to_string())))
        }
        async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
            LocalFsStorage.write(path, bs).await
        }
        async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
            LocalFsStorage.writer(path).await
        }
        async fn delete(&self, path: &str) -> Result<()> {
            LocalFsStorage.delete(path).await
        }
        async fn delete_prefix(&self, path: &str) -> Result<()> {
            LocalFsStorage.delete_prefix(path).await
        }
        async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
            LocalFsStorage.delete_stream(paths).await
        }
        fn new_input(&self, path: &str) -> Result<InputFile> {
            Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
        }
        fn new_output(&self, path: &str) -> Result<OutputFile> {
            Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
        }
    }
}

/// Table `ns.t` with an `id` column under `dir`, stored through
/// [`recording_storage`], after `commits` single-row INSERTs, so a scan has
/// real data files and one manifest per commit to read.
pub(crate) async fn table_with_rows(dir: &Path, commits: usize) -> Table {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};

    let warehouse = dir.to_str().unwrap().to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(recording_storage::RecordingStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.clone(),
                )]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name("t".to_string())
        .location(format!("{warehouse}/t"))
        .schema(schema)
        .build();
    catalog.create_table(&namespace, creation).await.unwrap();

    let provider = datafusion_iceberg::IcebergTableProvider::try_new(
        catalog.clone(),
        namespace.clone(),
        "t",
    )
    .await
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    for id in 0..commits {
        ctx.sql(&format!("INSERT INTO t VALUES ({id})"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }
    catalog
        .load_table(&TableIdent::new(namespace, "t".to_string()))
        .await
        .unwrap()
}
