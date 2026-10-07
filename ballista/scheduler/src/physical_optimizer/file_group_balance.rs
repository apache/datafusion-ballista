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

//! Prunes scan files by statistics at planning time and spreads the survivors
//! evenly over the scan's file groups.
//!
//! DataFusion builds a file scan's file groups before any predicate is applied:
//! the files are sorted by path and cut into contiguous runs. Statistics-based
//! pruning only happens later, inside each task, as it opens its files. When
//! the files are clustered by the filtered column (for example Hive-style
//! `l_shipdate=...` directories filtered by a date range), every file that
//! survives sits in a few neighbouring groups. Only the tasks that own those
//! groups have rows to read, the rest open and skip their files, and the stage
//! takes as long as the busiest few tasks.
//!
//! [`BalanceFileGroups`] evaluates the scan's predicate against each file's
//! statistics while planning, drops the files that cannot match, and deals the
//! remaining files round-robin over the groups so the surviving work is spread
//! across tasks. It leaves a scan untouched when no file can be pruned.

use ballista_core::config::BallistaConfig;
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::pruning::PrunableStatistics;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;
use std::sync::Arc;

/// Physical optimizer rule that prunes file scans by file statistics and
/// rebalances the surviving files across the scan's file groups.
///
/// See the [module documentation](self) for the problem it solves.
///
/// A scan is left untouched when any of the following holds, because the
/// layout of its groups carries meaning that must not be disturbed:
///
/// * it declares an output partitioning (files are grouped by partition value),
/// * it declares an output ordering, or must preserve file order,
/// * it has a single file group,
/// * it has no predicate, or the predicate cannot prune anything,
/// * no file can be pruned from the statistics collected during planning.
///
/// Files without statistics are always kept. The number of file groups never
/// grows, and shrinks only when fewer files survive than there were groups.
#[derive(Debug, Default)]
pub struct BalanceFileGroups;

impl BalanceFileGroups {
    /// Creates a new `BalanceFileGroups` rule.
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for BalanceFileGroups {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // On unless a Ballista session turns it off.
        let enabled = config
            .extensions
            .get::<BallistaConfig>()
            .is_none_or(|c| c.balance_scan_file_groups_enabled());
        if !enabled {
            return Ok(plan);
        }

        plan.transform_up(|plan| {
            let Some(exec) = plan.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(file_groups) = balance_file_groups(config) else {
                return Ok(Transformed::no(plan));
            };
            let config = FileScanConfigBuilder::from(config.clone())
                .with_file_groups(file_groups)
                .build();
            Ok(Transformed::yes(DataSourceExec::from_data_source(config)))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "BalanceFileGroups"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns the rebalanced file groups for `config`, or `None` when the scan
/// should keep its current groups.
fn balance_file_groups(config: &FileScanConfig) -> Option<Vec<FileGroup>> {
    let group_count = config.file_groups.len();
    if group_count <= 1
        || config.output_partitioning.is_some()
        || config.preserve_order
        || !config.output_ordering.is_empty()
    {
        return None;
    }

    let predicate = config.file_source().filter()?;
    // `build` yields `None` when the predicate cannot prune anything.
    let pruning_predicate = PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(config.file_schema()))
        .build(predicate)?;

    // Each file is judged on its own, because one file with missing or inexact
    // statistics would otherwise stop the whole batch from pruning anything.
    // A file that cannot be judged is kept.
    let keep = |file: &PartitionedFile| -> bool {
        let Some(stats) = &file.statistics else {
            return true;
        };
        let prunable = PrunableStatistics::new(
            vec![Arc::clone(stats)],
            Arc::clone(config.file_schema()),
        );
        pruning_predicate
            .prune(&prunable)
            .ok()
            .is_none_or(|keep| keep[0])
    };

    let total: usize = config.file_groups.iter().map(|group| group.len()).sum();
    let survivors: Vec<&PartitionedFile> = config
        .file_groups
        .iter()
        .flat_map(|group| group.iter())
        .filter(|file| keep(file))
        .collect();
    if survivors.len() == total {
        return None;
    }
    debug!(
        "Pruned {} scan files by statistics, {} remain over up to {group_count} file groups",
        total - survivors.len(),
        survivors.len()
    );

    // Never more groups than files, so no task is handed an empty group, and
    // never fewer than one, so the scan keeps a partition to run.
    let new_group_count = survivors.len().clamp(1, group_count);
    let mut groups = vec![FileGroup::default(); new_group_count];
    for (idx, file) in survivors.into_iter().enumerate() {
        groups[idx % new_group_count].push(file.clone());
    }
    Some(groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::config::BALLISTA_BALANCE_SCAN_FILE_GROUPS;
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::stats::Precision;
    use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::Partitioning;
    use datafusion::prelude::SessionConfig;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("d", DataType::Int64, false)]))
    }

    /// A file whose column `d` spans exactly `min..=max`.
    fn file(name: &str, min: i64, max: i64) -> PartitionedFile {
        let stats = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                min_value: Precision::Exact(ScalarValue::Int64(Some(min))),
                max_value: Precision::Exact(ScalarValue::Int64(Some(max))),
                ..ColumnStatistics::new_unknown()
            }],
        };
        let mut file = PartitionedFile::new(name.to_string(), 100);
        file.statistics = Some(Arc::new(stats));
        file
    }

    /// `d >= lo AND d < hi`
    fn range_predicate(lo: i64, hi: i64) -> Arc<dyn PhysicalExpr> {
        let col = || Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>;
        let lit = |v: i64| {
            Arc::new(Literal::new(ScalarValue::Int64(Some(v)))) as Arc<dyn PhysicalExpr>
        };
        Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(col(), Operator::GtEq, lit(lo))),
            Operator::And,
            Arc::new(BinaryExpr::new(col(), Operator::Lt, lit(hi))),
        ))
    }

    /// A scan over `groups`, filtered by `predicate` when given.
    fn scan(
        groups: Vec<Vec<PartitionedFile>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> FileScanConfigBuilder {
        let mut source = ParquetSource::new(schema());
        if let Some(predicate) = predicate {
            source = source.with_predicate(predicate);
        }
        let mut builder = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source),
        );
        for group in groups {
            builder = builder.with_file_group(FileGroup::new(group));
        }
        builder
    }

    fn optimize(builder: FileScanConfigBuilder) -> Arc<dyn ExecutionPlan> {
        let plan = DataSourceExec::from_data_source(builder.build());
        BalanceFileGroups::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    fn scan_config(plan: &Arc<dyn ExecutionPlan>) -> &FileScanConfig {
        let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
        exec.data_source().downcast_ref::<FileScanConfig>().unwrap()
    }

    fn group_names(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<String>> {
        scan_config(plan)
            .file_groups
            .iter()
            .map(|g| g.iter().map(|f| f.path().to_string()).collect())
            .collect()
    }

    /// Asserts the scan still has the four contiguous groups of
    /// `clustered_groups`.
    fn assert_unchanged(plan: &Arc<dyn ExecutionPlan>) {
        assert_eq!(
            group_names(plan),
            vec![
                vec!["f0", "f1"],
                vec!["f2", "f3"],
                vec!["f4", "f5"],
                vec!["f6", "f7"]
            ]
        );
    }

    /// Four contiguous groups of two files each; only the files of group 1
    /// and the first file of group 2 are in range.
    fn clustered_groups() -> Vec<Vec<PartitionedFile>> {
        (0..4)
            .map(|g| {
                (0..2)
                    .map(|i| {
                        let n = g * 2 + i;
                        file(&format!("f{n}"), n * 10, n * 10 + 9)
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn spreads_surviving_files_across_groups() {
        // Files f2..f5 (min 20..50) survive d in [20, 60).
        let plan = optimize(scan(clustered_groups(), Some(range_predicate(20, 60))));
        assert_eq!(
            group_names(&plan),
            vec![vec!["f2"], vec!["f3"], vec!["f4"], vec!["f5"]]
        );
        assert_eq!(plan.properties().output_partitioning().partition_count(), 4);
    }

    #[test]
    fn shrinks_to_the_number_of_surviving_files() {
        let plan = optimize(scan(clustered_groups(), Some(range_predicate(20, 40))));
        assert_eq!(group_names(&plan), vec![vec!["f2"], vec!["f3"]]);
        assert_eq!(plan.properties().output_partitioning().partition_count(), 2);
    }

    #[test]
    fn keeps_one_group_when_every_file_is_pruned() {
        let plan = optimize(scan(clustered_groups(), Some(range_predicate(500, 600))));
        assert_eq!(group_names(&plan), vec![Vec::<String>::new()]);
    }

    #[test]
    fn deals_surplus_files_round_robin() {
        // 6 survivors (f2..f7) over 4 groups.
        let plan = optimize(scan(clustered_groups(), Some(range_predicate(20, 80))));
        assert_eq!(
            group_names(&plan),
            vec![vec!["f2", "f6"], vec!["f3", "f7"], vec!["f4"], vec!["f5"]]
        );
    }

    #[test]
    fn leaves_scan_alone_when_nothing_is_pruned() {
        let plan = optimize(scan(clustered_groups(), Some(range_predicate(0, 1000))));
        assert_unchanged(&plan);
    }

    #[test]
    fn leaves_scan_alone_without_a_predicate() {
        assert_unchanged(&optimize(scan(clustered_groups(), None)));
    }

    #[test]
    fn prunes_around_files_without_usable_statistics() {
        // `nostats` has no statistics at all, `partial` only an inexact
        // minimum. Neither can be judged, so both are kept.
        let mut groups = clustered_groups();
        groups[0][0] = PartitionedFile::new("nostats".to_string(), 100);
        let mut partial = file("partial", 0, 9);
        let mut stats = Statistics::clone(partial.statistics.as_ref().unwrap());
        stats.column_statistics[0].min_value =
            Precision::Inexact(ScalarValue::Int64(Some(0)));
        stats.column_statistics[0].max_value = Precision::Absent;
        partial.statistics = Some(Arc::new(stats));
        groups[0][1] = partial;

        let plan = optimize(scan(groups, Some(range_predicate(20, 40))));
        assert_eq!(
            group_names(&plan),
            vec![vec!["nostats"], vec!["partial"], vec!["f2"], vec!["f3"]]
        );
    }

    #[test]
    fn leaves_scan_alone_with_declared_output_partitioning() {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("d", 0));
        let builder = scan(clustered_groups(), Some(range_predicate(20, 60)))
            .with_output_partitioning(Some(Partitioning::Hash(vec![key], 4)));
        assert_unchanged(&optimize(builder));
    }

    #[test]
    fn leaves_scan_alone_when_order_must_be_preserved() {
        let builder = scan(clustered_groups(), Some(range_predicate(20, 60)))
            .with_preserve_order(true);
        assert_unchanged(&optimize(builder));
    }

    #[test]
    fn leaves_scan_alone_when_disabled() -> Result<()> {
        let config = SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_BALANCE_SCAN_FILE_GROUPS, false);
        let plan = DataSourceExec::from_data_source(
            scan(clustered_groups(), Some(range_predicate(20, 60))).build(),
        );
        let plan = BalanceFileGroups::new().optimize(plan, config.options())?;
        assert_unchanged(&plan);
        Ok(())
    }

    /// Plans a real `ListingTable` over 16 Parquet files clustered by `d`, so
    /// the rule is exercised against the statistics and pushed-down predicate
    /// DataFusion actually produces.
    #[tokio::test]
    async fn balances_a_planned_listing_table() -> Result<()> {
        use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::prelude::{ParquetReadOptions, SessionContext};

        let dir = tempfile::tempdir()?;
        for n in 0..16i64 {
            let values: ArrayRef =
                Arc::new(Int64Array::from_iter_values((n * 10)..(n * 10 + 10)));
            let batch = RecordBatch::try_new(schema(), vec![values])?;
            let path = dir.path().join(format!("f{n:02}.parquet"));
            let mut writer =
                ArrowWriter::try_new(std::fs::File::create(path)?, schema(), None)?;
            writer.write(&batch)?;
            writer.close()?;
        }

        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(4),
        );
        ctx.register_parquet(
            "t",
            dir.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
        // Files f04..f07 hold d in [40, 80).
        let plan = ctx
            .sql("SELECT d FROM t WHERE d >= 40 AND d < 80")
            .await?
            .create_physical_plan()
            .await?;

        let scan_groups = |plan: &Arc<dyn ExecutionPlan>| -> Vec<usize> {
            let mut leaf = plan;
            while let Some(child) = leaf.children().first() {
                leaf = child;
            }
            scan_config(leaf)
                .file_groups
                .iter()
                .map(|g| g.len())
                .collect()
        };

        // DataFusion cuts 16 files into 4 contiguous runs of 4.
        assert_eq!(scan_groups(&plan), vec![4, 4, 4, 4]);

        let balanced =
            BalanceFileGroups::new().optimize(plan, &ConfigOptions::default())?;
        // Only 4 files survive, one per group.
        assert_eq!(scan_groups(&balanced), vec![1, 1, 1, 1]);
        Ok(())
    }
}
