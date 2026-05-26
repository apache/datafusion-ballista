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

//! Per-task `FileScanConfig` narrowing.
//!
//! DataFusion 54 added a shared work queue (`SharedWorkSource`) to
//! `FileScanConfig`: when any partition opens its stream, it pulls files from
//! a queue populated with every file in the scan. That model assumes every
//! partition runs together inside the same DataSourceExec instance so they
//! cooperatively drain the queue exactly once. Ballista breaks that
//! assumption — each task deserialises its own copy of the plan and runs a
//! single partition — so the partition that does run drains the whole queue
//! and ends up scanning every file. Concretely, a 6-file scan executed by 6
//! tasks reads 36 files and returns six copies of the data.
//!
//! [`restrict_file_scan_to_partition`] rewrites the plan tree just before
//! execution so that every `FileScanConfig` only contains files for the
//! partition being executed. The file group count (and therefore the
//! advertised partitioning) is preserved by replacing the other slots with
//! empty groups, so partition routing through the rest of the plan is
//! unaffected.
//!
//! See `ballista/client/tests/multi_file_scan.rs` for the end-to-end
//! regression that motivated this helper.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;

/// Narrow every `FileScanConfig` in `plan` so that only `partition`'s file
/// group has files; all other slots become empty.
///
/// This keeps `file_groups.len()` (and therefore the advertised partition
/// count) unchanged, which means the rest of the plan can still route
/// `execute(partition)` calls through unmodified operators. The
/// `SharedWorkSource` that the file scan builds from `file_groups` ends up
/// containing only the relevant partition's files, so the active partition
/// reads exactly its assigned slice instead of draining the full set.
///
/// If the leaf is something other than a `FileScanConfig`-backed
/// `DataSourceExec`, the node is left alone — there's nothing for the shared
/// queue to mishandle.
pub fn restrict_file_scan_to_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_down(|node| {
        let Some(data_source_exec) = node.downcast_ref::<DataSourceExec>() else {
            return Ok(Transformed::no(node));
        };
        let Some(file_scan) = data_source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
        else {
            return Ok(Transformed::no(node));
        };

        // Nothing to do for single-partition scans: the shared queue still
        // matches what `execute(0)` would consume, so this is already
        // correct.
        if file_scan.file_groups.len() <= 1 {
            return Ok(Transformed::no(node));
        }

        let mut new_groups: Vec<FileGroup> =
            Vec::with_capacity(file_scan.file_groups.len());
        for (idx, group) in file_scan.file_groups.iter().enumerate() {
            if idx == partition {
                new_groups.push(group.clone());
            } else {
                new_groups.push(FileGroup::new(Vec::new()));
            }
        }

        let new_config = FileScanConfigBuilder::from(file_scan.clone())
            .with_file_groups(new_groups)
            .build();
        let new_exec =
            DataSourceExec::from_data_source(new_config) as Arc<dyn ExecutionPlan>;
        Ok(Transformed::yes(new_exec))
    })
    .map(|t| t.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use std::sync::Arc;

    fn dummy_file(name: &str) -> PartitionedFile {
        PartitionedFile::new(name.to_string(), 0)
    }

    fn build_plan(num_groups: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let groups: Vec<FileGroup> = (0..num_groups)
            .map(|i| FileGroup::new(vec![dummy_file(&format!("f{i}.parquet"))]))
            .collect();
        let file_source = Arc::new(ParquetSource::new(schema.clone()));
        let config =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
                .with_file_groups(groups)
                .build();
        DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>
    }

    fn file_groups_of(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<String>> {
        let exec = plan
            .downcast_ref::<DataSourceExec>()
            .expect("DataSourceExec");
        let scan = exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("FileScanConfig");
        scan.file_groups
            .iter()
            .map(|g| g.iter().map(|f| f.path().to_string()).collect())
            .collect()
    }

    #[test]
    fn keeps_only_target_partition_files() {
        let plan = build_plan(4);
        let restricted = restrict_file_scan_to_partition(plan, 2).unwrap();
        let groups = file_groups_of(&restricted);
        assert_eq!(
            groups,
            vec![
                vec![] as Vec<String>,
                vec![],
                vec!["f2.parquet".to_string()],
                vec![],
            ],
            "only file_groups[2] should keep its file; the others must be empty so \
             the SharedWorkSource contains only this task's slice"
        );
    }

    #[test]
    fn preserves_partition_count() {
        let plan = build_plan(3);
        let restricted = restrict_file_scan_to_partition(plan, 1).unwrap();
        let groups = file_groups_of(&restricted);
        assert_eq!(
            groups.len(),
            3,
            "file_groups length must be preserved so DataSourceExec keeps its \
             advertised partition count"
        );
    }

    #[test]
    fn single_partition_scan_is_left_alone() {
        let plan = build_plan(1);
        let restricted = restrict_file_scan_to_partition(Arc::clone(&plan), 0).unwrap();
        // The transform should detect there's nothing to narrow and return
        // the original Arc untouched (it's a no-op in that case).
        assert!(Arc::ptr_eq(&plan, &restricted));
    }
}
