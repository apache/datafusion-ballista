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

//! Utilities for producing dot diagrams from execution graphs

use crate::state::execution_graph::{ExecutionGraph, ExecutionStage};
use ballista_core::execution_plans::{
    ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::file_format::{
    AvroExec, CsvExec, FileScanConfig, NdJsonExec, ParquetExec,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;
use object_store::path::Path;
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::sync::Arc;

/// Utility for producing dot diagrams from execution graphs
pub struct ExecutionGraphDot {
    graph: Arc<ExecutionGraph>,
}

impl ExecutionGraphDot {
    /// Create a DOT graph from the provided ExecutionGraph
    pub fn generate(graph: Arc<ExecutionGraph>) -> Result<String, fmt::Error> {
        let mut dot = Self { graph };
        dot._generate()
    }

    fn _generate(&mut self) -> Result<String, fmt::Error> {
        // sort the stages by key for deterministic output for tests
        let stages = self.graph.stages();
        let mut stage_ids: Vec<usize> = stages.keys().cloned().collect();
        stage_ids.sort();

        let mut dot = String::new();

        writeln!(&mut dot, "digraph G {{")?;

        let mut cluster = 0;
        let mut stage_meta = vec![];

        #[allow(clippy::explicit_counter_loop)]
        for id in &stage_ids {
            let stage = stages.get(id).unwrap(); // safe unwrap
            let stage_name = format!("stage_{}", id);
            writeln!(&mut dot, "\tsubgraph cluster{} {{", cluster)?;
            match stage {
                ExecutionStage::UnResolved(stage) => {
                    writeln!(&mut dot, "\t\tlabel = \"Stage {} [UnResolved]\";", id)?;
                    stage_meta.push(write_stage_plan(
                        &mut dot,
                        &stage_name,
                        &stage.plan,
                        0,
                    )?);
                }
                ExecutionStage::Resolved(stage) => {
                    writeln!(&mut dot, "\t\tlabel = \"Stage {} [Resolved]\";", id)?;
                    stage_meta.push(write_stage_plan(
                        &mut dot,
                        &stage_name,
                        &stage.plan,
                        0,
                    )?);
                }
                ExecutionStage::Running(stage) => {
                    writeln!(&mut dot, "\t\tlabel = \"Stage {} [Running]\";", id)?;
                    stage_meta.push(write_stage_plan(
                        &mut dot,
                        &stage_name,
                        &stage.plan,
                        0,
                    )?);
                }
                ExecutionStage::Completed(stage) => {
                    writeln!(&mut dot, "\t\tlabel = \"Stage {} [Completed]\";", id)?;
                    stage_meta.push(write_stage_plan(
                        &mut dot,
                        &stage_name,
                        &stage.plan,
                        0,
                    )?);
                }
                ExecutionStage::Failed(stage) => {
                    writeln!(&mut dot, "\t\tlabel = \"Stage {} [FAILED]\";", id)?;
                    stage_meta.push(write_stage_plan(
                        &mut dot,
                        &stage_name,
                        &stage.plan,
                        0,
                    )?);
                }
            }
            cluster += 1;
            writeln!(&mut dot, "\t}}")?; // end of subgraph
        }

        // write links between stages
        for meta in &stage_meta {
            for (reader_node, parent_stage_id) in &meta.readers {
                // shuffle write node is always node zero
                let parent_shuffle_write_node = format!("stage_{}_0", parent_stage_id);
                writeln!(
                    &mut dot,
                    "\t{} -> {}",
                    parent_shuffle_write_node, reader_node,
                )?;
            }
        }

        writeln!(&mut dot, "}}")?; // end of digraph

        Ok(dot)
    }
}

/// Write the query tree for a single stage and build metadata needed to later draw
/// the links between the stages
fn write_stage_plan(
    f: &mut String,
    prefix: &str,
    plan: &Arc<dyn ExecutionPlan>,
    i: usize,
) -> Result<StagePlanState, fmt::Error> {
    let mut state = StagePlanState {
        readers: HashMap::new(),
    };
    write_stage_plan2(f, prefix, plan, i, &mut state)?;
    Ok(state)
}

fn write_stage_plan2(
    f: &mut String,
    prefix: &str,
    plan: &Arc<dyn ExecutionPlan>,
    i: usize,
    state: &mut StagePlanState,
) -> Result<(), fmt::Error> {
    let node_name = format!("{}_{}", prefix, i);
    let operator_name = get_operator_name(plan);
    let display_name = sanitize(&operator_name);

    if let Some(reader) = plan.as_any().downcast_ref::<ShuffleReaderExec>() {
        for part in &reader.partition {
            for loc in part {
                state
                    .readers
                    .insert(node_name.clone(), loc.partition_id.stage_id);
            }
        }
    } else if let Some(reader) = plan.as_any().downcast_ref::<UnresolvedShuffleExec>() {
        state.readers.insert(node_name.clone(), reader.stage_id);
    }

    let mut metrics_str = vec![];
    if let Some(metrics) = plan.metrics() {
        if let Some(x) = metrics.output_rows() {
            metrics_str.push(format!("output_rows={}", x))
        }
        if let Some(x) = metrics.elapsed_compute() {
            metrics_str.push(format!("elapsed_compute={}", x))
        }
    }
    if metrics_str.is_empty() {
        writeln!(
            f,
            "\t\t{} [shape=box, label=\"{}\"]",
            node_name, display_name
        )?;
    } else {
        writeln!(
            f,
            "\t\t{} [shape=box, label=\"{}\n{}\"]",
            node_name,
            display_name,
            metrics_str.join(", ")
        )?;
    }

    for (j, child) in plan.children().into_iter().enumerate() {
        write_stage_plan2(f, &node_name, &child, j, state)?;
        // write link from child to parent
        writeln!(f, "\t\t{}_{} -> {}", node_name, j, node_name)?;
    }

    Ok(())
}

#[derive(Debug)]
struct StagePlanState {
    /// map from reader node name to parent stage id
    readers: HashMap<String, usize>,
}

/// Make strings dot-friendly
fn sanitize(str: &str) -> String {
    // TODO
    str.to_string()
}

fn get_operator_name(plan: &Arc<dyn ExecutionPlan>) -> String {
    if let Some(exec) = plan.as_any().downcast_ref::<FilterExec>() {
        format!("Filter: {}", exec.predicate())
    } else if plan.as_any().downcast_ref::<ProjectionExec>().is_some() {
        "Projection".to_string()
    } else if let Some(exec) = plan.as_any().downcast_ref::<SortExec>() {
        let sort_expr = exec
            .expr()
            .iter()
            .map(|e| {
                let asc = if e.options.descending { " DESC" } else { "" };
                let nulls = if e.options.nulls_first {
                    " NULLS FIRST"
                } else {
                    ""
                };
                format!("{}{}{}", e.expr, asc, nulls)
            })
            .collect::<Vec<String>>()
            .join(", ");
        format!("Sort: {}", sanitize(&sort_expr))
    } else if let Some(exec) = plan.as_any().downcast_ref::<AggregateExec>() {
        let group_exprs_with_alias = exec.group_expr().expr();
        let group_expr = group_exprs_with_alias
            .iter()
            .map(|(e, _)| format!("{}", e))
            .collect::<Vec<String>>()
            .join(", ");
        let aggr_expr = exec
            .aggr_expr()
            .iter()
            .map(|e| e.name().to_owned())
            .collect::<Vec<String>>()
            .join(", ");
        format!(
            "Aggregate[groupBy={}, aggr={}]",
            sanitize(&group_expr),
            sanitize(&aggr_expr)
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        format!("CoalesceBatches[batchSize={}]", exec.target_batch_size())
    } else if let Some(exec) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        // TODO show the partition strategy
        format!(
            "CoalescePartitions [{} partitions]",
            exec.output_partitioning().partition_count()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let join_expr = exec
            .on()
            .iter()
            .map(|(l, r)| format!("{} = {}", l, r))
            .collect::<Vec<String>>()
            .join(" AND ");
        // TODO join filter
        //let filter_expr = exec.filter().map(|f| f.expression())
        format!("HashJoin: {}", sanitize(&join_expr))
    } else if let Some(exec) = plan.as_any().downcast_ref::<UnresolvedShuffleExec>() {
        format!("UnresolvedShuffleExec [stage_id={}]", exec.stage_id)
    } else if let Some(exec) = plan.as_any().downcast_ref::<ShuffleReaderExec>() {
        format!("ShuffleReader [{} partitions]", exec.partition.len())
    } else if let Some(exec) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
        format!(
            "ShuffleWriter [{} partitions]",
            exec.output_partitioning().partition_count()
        )
    } else if plan.as_any().downcast_ref::<MemoryExec>().is_some() {
        "MemoryExec".to_string()
    } else if let Some(exec) = plan.as_any().downcast_ref::<CsvExec>() {
        let parts = exec.output_partitioning().partition_count();
        format!(
            "CSV: {} [{} partitions]",
            get_file_scan(exec.base_config()),
            parts
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<NdJsonExec>() {
        let parts = exec.output_partitioning().partition_count();
        format!("JSON [{} partitions]", parts)
    } else if let Some(exec) = plan.as_any().downcast_ref::<AvroExec>() {
        let parts = exec.output_partitioning().partition_count();
        format!(
            "Avro: {} [{} partitions]",
            get_file_scan(exec.base_config()),
            parts
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<ParquetExec>() {
        let parts = exec.output_partitioning().partition_count();
        format!(
            "Parquet: {} [{} partitions]",
            get_file_scan(exec.base_config()),
            parts
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        format!(
            "GlobalLimit(skip={}, fetch={:?})",
            exec.skip(),
            exec.fetch()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        format!("LocalLimit({})", exec.fetch())
    } else if let Some(exec) = plan.as_any().downcast_ref::<RepartitionExec>() {
        format!(
            "RepartitionExec [{} partitions]",
            exec.output_partitioning().partition_count()
        )
    } else {
        debug!(
            "Unknown physical operator when producing DOT graph: {:?}",
            plan
        );
        "Unknown Operator".to_string()
    }
}

/// Get summary of file scan locations
fn get_file_scan(scan: &FileScanConfig) -> String {
    if !scan.file_groups.is_empty() {
        let partitioned_files: Vec<PartitionedFile> = scan
            .file_groups
            .iter()
            .flat_map(|part_file| part_file.clone())
            .collect();
        let paths: Vec<Path> = partitioned_files
            .iter()
            .map(|part_file| part_file.object_meta.location.clone())
            .collect();
        match paths.len() {
            0 => "No files found".to_owned(),
            1 => {
                // single file
                format!("{}", paths[0])
            }
            _ => {
                // multiple files so show parent directory
                let path = format!("{}", paths[0]);
                let path = if let Some(i) = path.rfind('/') {
                    &path[0..i]
                } else {
                    &path
                };
                format!("{} [{} files]", path, paths.len())
            }
        }
    } else {
        "".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::state::execution_graph::ExecutionGraph;
    use crate::state::execution_graph_dot::ExecutionGraphDot;
    use ballista_core::error::{BallistaError, Result};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    #[tokio::test]
    async fn dot() -> Result<()> {
        let ctx =
            SessionContext::with_config(SessionConfig::new().with_target_partitions(48));
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        let table = Arc::new(MemTable::try_new(schema.clone(), vec![])?);
        ctx.register_table("foo", table.clone())?;
        ctx.register_table("bar", table.clone())?;
        ctx.register_table("baz", table)?;
        let df = ctx
            .sql("SELECT * FROM foo JOIN bar ON foo.a = bar.a JOIN baz on bar.a = baz.a")
            .await?;
        let plan = df.to_logical_plan()?;
        let plan = ctx.create_physical_plan(&plan).await?;
        let graph = ExecutionGraph::new("scheduler_id", "job_id", "session_id", plan)?;
        let dot = ExecutionGraphDot::generate(Arc::new(graph))
            .map_err(|e| BallistaError::Internal(format!("{:?}", e)))?;

        let expected = r#"digraph G {
	subgraph cluster0 {
		label = "Stage 1 [Resolved]";
		stage_1_0 [shape=box, label="ShuffleWriter [0 partitions]"]
		stage_1_0_0 [shape=box, label="MemoryExec"]
		stage_1_0_0 -> stage_1_0
	}
	subgraph cluster1 {
		label = "Stage 2 [Resolved]";
		stage_2_0 [shape=box, label="ShuffleWriter [0 partitions]"]
		stage_2_0_0 [shape=box, label="MemoryExec"]
		stage_2_0_0 -> stage_2_0
	}
	subgraph cluster2 {
		label = "Stage 3 [UnResolved]";
		stage_3_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_3_0_0 [shape=box, label="CoalesceBatches[batchSize=4096]"]
		stage_3_0_0_0 [shape=box, label="HashJoin: a@0 = a@0"]
		stage_3_0_0_0_0 [shape=box, label="CoalesceBatches[batchSize=4096]"]
		stage_3_0_0_0_0_0 [shape=box, label="UnresolvedShuffleExec [stage_id=1]"]
		stage_3_0_0_0_0_0 -> stage_3_0_0_0_0
		stage_3_0_0_0_0 -> stage_3_0_0_0
		stage_3_0_0_0_1 [shape=box, label="CoalesceBatches[batchSize=4096]"]
		stage_3_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=2]"]
		stage_3_0_0_0_1_0 -> stage_3_0_0_0_1
		stage_3_0_0_0_1 -> stage_3_0_0_0
		stage_3_0_0_0 -> stage_3_0_0
		stage_3_0_0 -> stage_3_0
	}
	subgraph cluster3 {
		label = "Stage 4 [Resolved]";
		stage_4_0 [shape=box, label="ShuffleWriter [0 partitions]"]
		stage_4_0_0 [shape=box, label="MemoryExec"]
		stage_4_0_0 -> stage_4_0
	}
	subgraph cluster4 {
		label = "Stage 5 [UnResolved]";
		stage_5_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_5_0_0 [shape=box, label="Projection"]
		stage_5_0_0_0 [shape=box, label="CoalesceBatches[batchSize=4096]"]
		stage_5_0_0_0_0 [shape=box, label="HashJoin: a@1 = a@0"]
		stage_5_0_0_0_0_0 [shape=box, label="CoalesceBatches[batchSize=4096]"]
		stage_5_0_0_0_0_0_0 [shape=box, label="UnresolvedShuffleExec [stage_id=3]"]
		stage_5_0_0_0_0_0_0 -> stage_5_0_0_0_0_0
		stage_5_0_0_0_0_0 -> stage_5_0_0_0_0
		stage_5_0_0_0_0_1 [shape=box, label="CoalesceBatches[batchSize=4096]"]
		stage_5_0_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=4]"]
		stage_5_0_0_0_0_1_0 -> stage_5_0_0_0_0_1
		stage_5_0_0_0_0_1 -> stage_5_0_0_0_0
		stage_5_0_0_0_0 -> stage_5_0_0_0
		stage_5_0_0_0 -> stage_5_0_0
		stage_5_0_0 -> stage_5_0
	}
	stage_1_0 -> stage_3_0_0_0_0_0
	stage_2_0 -> stage_3_0_0_0_1_0
	stage_3_0 -> stage_5_0_0_0_0_0_0
	stage_4_0 -> stage_5_0_0_0_0_1_0
}
"#;
        assert_eq!(expected, &dot);
        Ok(())
    }
}
