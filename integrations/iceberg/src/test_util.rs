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
