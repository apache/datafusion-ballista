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

//! Explicit storage layouts over immutable Parquet files.
//!
//! A provider guarantees that each file belongs to its declared Iceberg bucket
//! (integer/long encoding, Murmur3 seed 0, positive-mask modulo), or the distinct
//! null group. Files must be immutable for the lifetime of a query and retries.
//! The manifest is a read contract, not an Iceberg catalog or table reader.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, Statistics, plan_err, stats::Precision};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{GroupedPartitioning, expressions::Column};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use serde::{Deserialize, Serialize};

use crate::config::BallistaConfig;

/// Wire tag for the explicitly registered manifest provider.
pub(crate) const MANIFEST_TAG: &[u8] = b"ballista.bucketed-parquet.v1\0";

/// One immutable file and its source-declared bucket. `None` denotes null keys.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketFile {
    /// Object path within the registered store; immutable during query execution.
    pub path: String,
    /// Exact object length in bytes.
    pub size: u64,
    /// Iceberg bucket value, or the distinct null group.
    pub bucket: Option<u32>,
    /// Optional exact row count supplied by the source manifest.
    #[serde(default)]
    pub num_rows: Option<usize>,
}

/// Immutable file inventory with a documented Iceberg integer bucket transform.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketedParquetManifest {
    /// Registered object-store URL, for example `file://` or `s3://bucket`.
    pub object_store_url: String,
    /// Name of the integer bucketing column.
    pub key: String,
    /// Number of non-null buckets in the source transform.
    pub buckets: u32,
    /// Complete selected immutable file inventory.
    pub files: Vec<BucketFile>,
}

/// A file-backed provider that exposes storage groups through standard scans.
#[derive(Debug)]
pub struct BucketedParquetTable {
    schema: SchemaRef,
    manifest: BucketedParquetManifest,
}

impl BucketedParquetTable {
    /// Validate the explicit inventory; row-to-bucket membership is the source's
    /// guarantee. The reference adapter supports one Int32 or Int64 bucket key.
    pub fn try_new(schema: SchemaRef, manifest: BucketedParquetManifest) -> Result<Self> {
        if manifest.buckets == 0 || manifest.buckets > i32::MAX as u32 {
            return plan_err!("Bucket count must be between 1 and i32::MAX");
        }
        ObjectStoreUrl::parse(&manifest.object_store_url)?;
        let index = schema.index_of(&manifest.key)?;
        if !matches!(
            schema.field(index).data_type(),
            DataType::Int32 | DataType::Int64
        ) {
            return plan_err!("Bucketed Parquet supports Int32 and Int64 keys");
        }
        let mut paths = HashSet::new();
        for file in &manifest.files {
            if file.path.is_empty()
                || !paths.insert(
                    PartitionedFile::new(file.path.clone(), file.size)
                        .object_meta
                        .location,
                )
            {
                return plan_err!(
                    "Bucket manifest contains an empty or repeated file path"
                );
            }
            if file.bucket.is_some_and(|bucket| bucket >= manifest.buckets) {
                return plan_err!("File bucket is outside the manifest bucket domain");
            }
        }
        Ok(Self { schema, manifest })
    }

    /// Explicit source layout used to reconstruct this provider remotely.
    pub fn manifest(&self) -> &BucketedParquetManifest {
        &self.manifest
    }
}

/// A complete semantic domain: the key type remains explicit even though
/// Iceberg hashes promoted Int32 and Int64 identically.
pub fn iceberg_bucket_scheme(data_type: &DataType, buckets: u32) -> String {
    format!(
        "iceberg.bucket.v1:murmur3-x86-32:seed=0:le-i64:mask=7fffffff:null=separate:type={data_type:?}:buckets={buckets}"
    )
}

/// Recognize this adapter's domain without accepting arbitrary matching labels.
pub fn iceberg_bucket_count(scheme: &str) -> Option<u32> {
    let (_, count) = scheme.rsplit_once(":buckets=")?;
    let count = count.parse::<u32>().ok()?;
    if count == 0 {
        return None;
    }
    [DataType::Int32, DataType::Int64]
        .iter()
        .any(|ty| iceberg_bucket_scheme(ty, count) == scheme)
        .then_some(count)
}

/// Canonical label for a bucket or for the separate null group.
pub fn bucket_group_id(bucket: Option<u32>) -> Vec<u8> {
    bucket.map(|b| b.to_be_bytes().to_vec()).unwrap_or_default()
}

#[async_trait]
impl TableProvider for BucketedParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Keep all bucket positions, including empty ones, so independent scan
        // pruning cannot shift the meaning of an execution partition.
        // The final group is reserved for Iceberg's null transform value.
        let mut groups: Vec<FileGroup> = (0..=self.manifest.buckets)
            .map(|_| FileGroup::default())
            .collect();
        for file in &self.manifest.files {
            let index = file.bucket.unwrap_or(self.manifest.buckets) as usize;
            let mut file_stats = Statistics::new_unknown(&self.schema);
            file_stats.num_rows = file
                .num_rows
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent);
            file_stats.calculate_total_byte_size(&self.schema);
            groups[index].push(
                PartitionedFile::new(file.path.clone(), file.size)
                    .with_statistics(Arc::new(file_stats)),
            );
        }
        let statistics =
            statistics_for_files(groups.iter().flat_map(FileGroup::iter), &self.schema);
        let groups = groups
            .into_iter()
            .map(|group| with_group_statistics(group, &self.schema))
            .collect();
        let enabled = state
            .config_options()
            .extensions
            .get::<BallistaConfig>()
            .is_some_and(|config| config.storage_partition_join_enabled());
        let partitioning = if enabled {
            let key = Column::new_with_schema(&self.manifest.key, &self.schema)?;
            let ty = self.schema.field(key.index()).data_type();
            let mut ids: Vec<_> = (0..self.manifest.buckets)
                .map(|b| bucket_group_id(Some(b)))
                .collect();
            ids.push(bucket_group_id(None));
            Some(Partitioning::Grouped(GroupedPartitioning::try_new(
                vec![Arc::new(key)],
                iceberg_bucket_scheme(ty, self.manifest.buckets),
                ids,
            )?))
        } else {
            None
        };
        let source = Arc::new(ParquetSource::new(Arc::clone(&self.schema)));
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse(&self.manifest.object_store_url)?,
            source,
        )
        .with_file_groups(groups)
        .with_statistics(statistics)
        .with_projection_indices(projection.cloned())?
        .with_limit(limit)
        .with_output_partitioning(partitioning)
        .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

/// Restrict a declared grouping in exactly the same order as its physical inputs.
pub fn select_groups(
    grouped: &GroupedPartitioning,
    indices: &[usize],
) -> Result<Partitioning> {
    if indices.is_empty() {
        return Ok(Partitioning::UnknownPartitioning(0));
    }
    let ids = indices
        .iter()
        .map(|&i| {
            grouped.group_ids().get(i).cloned().ok_or_else(|| {
                datafusion::common::internal_datafusion_err!(
                    "Group index {i} is outside the scan layout"
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Partitioning::Grouped(GroupedPartitioning::try_new(
        grouped.keys().to_vec(),
        grouped.scheme().to_owned(),
        ids,
    )?))
}

/// Recompute a group's row/byte estimates after combining its immutable files.
pub fn with_group_statistics(group: FileGroup, schema: &SchemaRef) -> FileGroup {
    let statistics = statistics_for_files(group.iter(), schema);
    group.with_statistics(Arc::new(statistics))
}

fn statistics_for_files<'a>(
    files: impl Iterator<Item = &'a PartitionedFile>,
    schema: &SchemaRef,
) -> Statistics {
    let rows = files.fold(Precision::Exact(0), |sum, file| {
        let rows = file
            .statistics
            .as_ref()
            .map(|s| s.num_rows)
            .unwrap_or(Precision::Absent);
        sum.add(&rows)
    });
    let mut statistics = Statistics::new_unknown(schema);
    statistics.num_rows = rows;
    statistics.calculate_total_byte_size(schema);
    statistics
}
