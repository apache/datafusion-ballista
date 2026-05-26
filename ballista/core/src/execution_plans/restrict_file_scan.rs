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

//! Disable DataFusion 54's cross-partition file work stealing on every
//! `FileScanConfig` in a plan tree.
//!
//! DataFusion 54 added a `SharedWorkSource` to `FileScanConfig`: when any
//! partition opens its stream, it pulls files from a queue populated with
//! every file in the scan. That model assumes every partition of the same
//! `DataSourceExec` instance runs together and cooperatively drains the
//! queue exactly once. Ballista breaks the assumption — each task
//! deserialises its own copy of the plan and runs a single partition — so
//! the partition that does run drains the whole queue and ends up scanning
//! every file. A 6-file scan executed by 6 tasks reads 36 files and returns
//! six copies of the data.
//!
//! The fix is to pin every `FileScanConfig` to `preserve_order = true`
//! before execution. DataFusion's `FileScanConfig::create_sibling_state`
//! short-circuits to `None` when that flag is set, so no shared queue is
//! ever installed. Each partition then falls back to its own
//! `WorkSource::Local(file_groups[partition])` and scans exactly the files
//! the planner assigned to it.
//!
//! Notes:
//! * We can't just narrow `file_groups` per task, because broadcast hash
//!   joins call `execute(0..K)` on the build-side `DataSourceExec` from
//!   inside the join, so every partition slot must keep its files. TPC-H
//!   Q11 hangs if you empty out the build-side slots — see
//!   `ballista/client/tests/multi_file_scan.rs` for the simpler regression.
//! * `preserve_order = true` only disables file reordering at scan time;
//!   it's already implicitly true whenever the config has an output
//!   ordering, so the runtime path is well-exercised upstream.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;

/// Rewrite every `FileScanConfig` in `plan` so its sibling work source is
/// suppressed, forcing each partition to scan only its own file group.
///
/// The `partition` argument is the index of the partition this task will
/// execute. It is currently unused — pinning `preserve_order = true` is
/// enough to disable work stealing for any partition — but kept in the
/// signature so callers can stay symmetric across writer types and so a
/// future per-task narrowing scheme can drop in without touching them.
///
/// If the leaf is something other than a `FileScanConfig`-backed
/// `DataSourceExec`, or the config is single-partition (and so already has
/// nothing to share), the node is returned unchanged.
pub fn restrict_file_scan_to_partition(
    plan: Arc<dyn ExecutionPlan>,
    _partition: usize,
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

        // Single-partition scans don't trigger the work-stealing bug
        // (there's nothing to steal from), and the flag is already set if
        // the user opted into ordering preservation.
        if file_scan.file_groups.len() <= 1 || file_scan.preserve_order {
            return Ok(Transformed::no(node));
        }

        let new_config = FileScanConfigBuilder::from(file_scan.clone())
            .with_preserve_order(true)
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
    use datafusion::datasource::physical_plan::{FileGroup, ParquetSource};
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

    fn file_scan(plan: &Arc<dyn ExecutionPlan>) -> FileScanConfig {
        let exec = plan
            .downcast_ref::<DataSourceExec>()
            .expect("DataSourceExec");
        exec.data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("FileScanConfig")
            .clone()
    }

    #[test]
    fn sets_preserve_order_to_disable_work_stealing() {
        let plan = build_plan(4);
        assert!(
            !file_scan(&plan).preserve_order,
            "test fixture should start with default preserve_order=false"
        );
        let restricted = restrict_file_scan_to_partition(plan, 2).unwrap();
        let scan = file_scan(&restricted);
        assert!(
            scan.preserve_order,
            "preserve_order must be set so create_sibling_state returns None and \
             the SharedWorkSource is never installed"
        );
    }

    #[test]
    fn keeps_all_files_in_their_original_groups() {
        let plan = build_plan(3);
        let restricted = restrict_file_scan_to_partition(plan, 1).unwrap();
        let scan = file_scan(&restricted);
        let groups: Vec<Vec<String>> = scan
            .file_groups
            .iter()
            .map(|g| g.iter().map(|f| f.path().to_string()).collect())
            .collect();
        assert_eq!(
            groups,
            vec![
                vec!["f0.parquet".to_string()],
                vec!["f1.parquet".to_string()],
                vec!["f2.parquet".to_string()],
            ],
            "every file_groups slot must keep its files so broadcast hash joins \
             can still iterate the full set on the build side"
        );
    }

    #[test]
    fn single_partition_scan_is_left_alone() {
        let plan = build_plan(1);
        let restricted = restrict_file_scan_to_partition(Arc::clone(&plan), 0).unwrap();
        // Single-partition scans have nothing to steal; the transform skips
        // them and returns the original Arc untouched.
        assert!(Arc::ptr_eq(&plan, &restricted));
    }

    #[test]
    fn preserves_partition_count() {
        let plan = build_plan(3);
        let restricted = restrict_file_scan_to_partition(plan, 1).unwrap();
        let scan = file_scan(&restricted);
        assert_eq!(
            scan.file_groups.len(),
            3,
            "file_groups length must be preserved so DataSourceExec keeps its \
             advertised partition count"
        );
    }
}
