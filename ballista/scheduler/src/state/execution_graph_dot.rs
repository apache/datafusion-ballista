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

use crate::state::execution_graph::ExecutionGraph;
use ballista_core::execution_plans::{
    ShuffleReaderExec, ShuffleWriterExec, SortShuffleWriterExec, UnresolvedShuffleExec,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::CrossJoinExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr};
use log::debug;
use object_store::path::Path;
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::sync::Arc;

/// Utility for producing dot diagrams from execution graphs
pub struct ExecutionGraphDot<'a> {
    graph: &'a ExecutionGraph,
}

impl<'a> ExecutionGraphDot<'a> {
    /// Create a DOT graph from the provided ExecutionGraph
    pub fn generate(graph: &'a ExecutionGraph) -> Result<String, fmt::Error> {
        let mut dot = Self { graph };
        dot._generate()
    }

    /// Create a DOT graph for one query stage from the provided ExecutionGraph
    pub fn generate_for_query_stage(
        graph: &ExecutionGraph,
        stage_id: usize,
    ) -> Result<String, fmt::Error> {
        if let Some(stage) = graph.stages().get(&stage_id) {
            let mut dot = String::new();
            writeln!(&mut dot, "digraph G {{")?;
            let stage_name = format!("stage_{stage_id}");
            write_stage_plan(&mut dot, &stage_name, stage.plan(), 0)?;
            writeln!(&mut dot, "}}")?;
            Ok(dot)
        } else {
            Err(fmt::Error)
        }
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
            let stage_name = format!("stage_{id}");
            writeln!(&mut dot, "\tsubgraph cluster{cluster} {{")?;
            writeln!(
                &mut dot,
                "\t\tlabel = \"Stage {} [{}]\";",
                id,
                stage.variant_name()
            )?;
            stage_meta.push(write_stage_plan(&mut dot, &stage_name, stage.plan(), 0)?);
            cluster += 1;
            writeln!(&mut dot, "\t}}")?; // end of subgraph
        }

        // write links between stages
        for meta in &stage_meta {
            let mut links = vec![];
            for (reader_node, parent_stage_id) in &meta.readers {
                // shuffle write node is always node zero
                let parent_shuffle_write_node = format!("stage_{parent_stage_id}_0");
                links.push(format!("{parent_shuffle_write_node} -> {reader_node}"));
            }
            // keep the order deterministic
            links.sort();
            for link in links {
                writeln!(&mut dot, "\t{link}")?;
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
    plan: &dyn ExecutionPlan,
    i: usize,
) -> Result<StagePlanState, fmt::Error> {
    let mut state = StagePlanState {
        readers: HashMap::new(),
    };
    write_plan_recursive(f, prefix, plan, i, &mut state)?;
    Ok(state)
}

fn write_plan_recursive(
    f: &mut String,
    prefix: &str,
    plan: &dyn ExecutionPlan,
    i: usize,
    state: &mut StagePlanState,
) -> Result<(), fmt::Error> {
    let node_name = format!("{prefix}_{i}");
    let display_name = get_operator_name(plan);

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
            metrics_str.push(format!("output_rows={x}"))
        }
        if let Some(x) = metrics.elapsed_compute() {
            metrics_str.push(format!("elapsed_compute={x}"))
        }
    }
    if metrics_str.is_empty() {
        writeln!(f, "\t\t{node_name} [shape=box, label=\"{display_name}\"]")?;
    } else {
        writeln!(
            f,
            "\t\t{} [shape=box, label=\"{}
{}\"]",
            node_name,
            display_name,
            metrics_str.join(", ")
        )?;
    }

    for (j, child) in plan.children().into_iter().enumerate() {
        write_plan_recursive(f, &node_name, child.as_ref(), j, state)?;
        // write link from child to parent
        writeln!(f, "\t\t{node_name}_{j} -> {node_name}")?;
    }

    Ok(())
}

#[derive(Debug)]
struct StagePlanState {
    /// map from reader node name to parent stage id
    readers: HashMap<String, usize>,
}

/// Make strings dot-friendly
fn sanitize_dot_label(str: &str) -> String {
    // TODO make max length configurable eventually
    sanitize(str, Some(100))
}

/// Make strings dot-friendly
fn sanitize(str: &str, max_len: Option<usize>) -> String {
    let mut sanitized = String::new();
    for ch in str.chars() {
        match ch {
            '"' => sanitized.push('`'),
            ' ' | '_' | '+' | '-' | '*' | '/' | '(' | ')' | '[' | ']' | '{' | '}'
            | '!' | '@' | '#' | '$' | '%' | '&' | '=' | ':' | ';' | '\\' | '\'' | '.'
            | ',' | '<' | '>' | '`' => sanitized.push(ch),
            _ if ch.is_ascii_alphanumeric() || ch.is_ascii_whitespace() => {
                sanitized.push(ch)
            }
            _ => sanitized.push('?'),
        }
    }
    // truncate after translation because we know we only have ASCII chars at this point
    // so the slice is safe (not splitting unicode character bytes)
    if let Some(limit) = max_len
        && sanitized.len() > limit
    {
        sanitized.truncate(limit);
        return sanitized + " ...";
    }
    sanitized
}

fn get_operator_name(plan: &dyn ExecutionPlan) -> String {
    if let Some(exec) = plan.as_any().downcast_ref::<FilterExec>() {
        format!("Filter: {}", exec.predicate())
    } else if let Some(exec) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let expr = exec
            .expr()
            .iter()
            // FIXME: not sure about this change
            .map(|e| format!("{e:?}"))
            .collect::<Vec<String>>()
            .join(", ");
        format!("Projection: {}", sanitize_dot_label(&expr))
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
        format!("Sort: {}", sanitize_dot_label(&sort_expr))
    } else if let Some(exec) = plan.as_any().downcast_ref::<AggregateExec>() {
        let group_exprs_with_alias = exec.group_expr().expr();
        let group_expr = group_exprs_with_alias
            .iter()
            .map(|(e, _)| format!("{e}"))
            .collect::<Vec<String>>()
            .join(", ");
        let aggr_expr = exec
            .aggr_expr()
            .iter()
            .map(|e| e.name().to_owned())
            .collect::<Vec<String>>()
            .join(", ");
        format!(
            "Aggregate
groupBy=[{}]
aggr=[{}]",
            sanitize_dot_label(&group_expr),
            sanitize_dot_label(&aggr_expr)
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        format!("CoalesceBatches [batchSize={}]", exec.target_batch_size())
    } else if let Some(exec) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        format!(
            "CoalescePartitions [{}]",
            format_partitioning(exec.properties().output_partitioning().clone())
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<RepartitionExec>() {
        format!(
            "RepartitionExec [{}]",
            format_partitioning(exec.properties().output_partitioning().clone())
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let join_expr = exec
            .on()
            .iter()
            .map(|(l, r)| format!("{l} = {r}"))
            .collect::<Vec<String>>()
            .join(" AND ");
        let filter_expr = if let Some(f) = exec.filter() {
            format!("{}", f.expression())
        } else {
            "".to_string()
        };
        format!(
            "HashJoin
join_expr={}
filter_expr={}",
            sanitize_dot_label(&join_expr),
            sanitize_dot_label(&filter_expr)
        )
    } else if plan.as_any().downcast_ref::<CrossJoinExec>().is_some() {
        "CrossJoin".to_string()
    } else if plan.as_any().downcast_ref::<UnionExec>().is_some() {
        "Union".to_string()
    } else if let Some(exec) = plan.as_any().downcast_ref::<UnresolvedShuffleExec>() {
        format!("UnresolvedShuffleExec [stage_id={}]", exec.stage_id)
    } else if let Some(exec) = plan.as_any().downcast_ref::<ShuffleReaderExec>() {
        format!("ShuffleReader [{} partitions]", exec.partition.len())
    } else if let Some(exec) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
        format!(
            "ShuffleWriter [{} partitions]",
            exec.input_partition_count()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<SortShuffleWriterExec>() {
        format!(
            "SortShuffleWriter [{} partitions]",
            exec.input_partition_count()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        let config = if let Some(config) =
            exec.data_source().as_any().downcast_ref::<FileScanConfig>()
        {
            get_file_scan(config)
        } else if let Some(_config) = exec
            .data_source()
            .as_any()
            .downcast_ref::<MemorySourceConfig>()
        {
            "Memory".to_string()
        } else {
            "Unknown".to_string()
        };

        let parts = exec.properties().output_partitioning().partition_count();

        format!("DataSourceExec: ({config}) [{parts} partitions]")
    } else if let Some(exec) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        format!(
            "GlobalLimit(skip={}, fetch={:?})",
            exec.skip(),
            exec.fetch()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        format!("LocalLimit({})", exec.fetch())
    } else {
        debug!("Unknown physical operator when producing DOT graph: {plan:?}");
        "Unknown Operator".to_string()
    }
}

fn format_partitioning(x: Partitioning) -> String {
    match x {
        Partitioning::UnknownPartitioning(n) | Partitioning::RoundRobinBatch(n) => {
            format!("{n} partitions")
        }
        Partitioning::Hash(expr, n) => {
            format!("{} partitions, expr={}", n, format_expr_list(&expr))
        }
    }
}

fn format_expr_list(exprs: &[Arc<dyn PhysicalExpr>]) -> String {
    let expr_strings: Vec<String> = exprs.iter().map(|e| format!("{e}")).collect();
    expr_strings.join(", ")
}

/// Get summary of file scan locations
fn get_file_scan(scan: &FileScanConfig) -> String {
    if !scan.file_groups.is_empty() {
        let partitioned_files: Vec<PartitionedFile> = scan
            .file_groups
            .iter()
            .flat_map(|part_file| part_file.clone().into_inner())
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
    use crate::planner::DefaultDistributedPlanner;
    use crate::state::execution_graph::ExecutionGraph;
    use crate::state::execution_graph_dot::ExecutionGraphDot;
    use ballista_core::error::{BallistaError, Result};
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    #[tokio::test]
    async fn dot() -> Result<()> {
        let graph = test_graph().await?;
        let dot = ExecutionGraphDot::generate(&graph)
            .map_err(|e| BallistaError::Internal(format!("{e:?}")))?;

        let expected = r#"digraph G {
	subgraph cluster0 {
		label = "Stage 1 [Resolved]";
		stage_1_0 [shape=box, label="ShuffleWriter [2 partitions]"]
		stage_1_0_0 [shape=box, label="DataSourceExec: (Memory) [2 partitions]"]
		stage_1_0_0 -> stage_1_0
	}
	subgraph cluster1 {
		label = "Stage 2 [Resolved]";
		stage_2_0 [shape=box, label="ShuffleWriter [2 partitions]"]
		stage_2_0_0 [shape=box, label="DataSourceExec: (Memory) [2 partitions]"]
		stage_2_0_0 -> stage_2_0
	}
	subgraph cluster2 {
		label = "Stage 3 [Unresolved]";
		stage_3_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_3_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_3_0_0_0 [shape=box, label="HashJoin
join_expr=a@0 = a@0
filter_expr="]
		stage_3_0_0_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_3_0_0_0_0_0 [shape=box, label="UnresolvedShuffleExec [stage_id=1]"]
		stage_3_0_0_0_0_0 -> stage_3_0_0_0_0
		stage_3_0_0_0_0 -> stage_3_0_0_0
		stage_3_0_0_0_1 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_3_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=2]"]
		stage_3_0_0_0_1_0 -> stage_3_0_0_0_1
		stage_3_0_0_0_1 -> stage_3_0_0_0
		stage_3_0_0_0 -> stage_3_0_0
		stage_3_0_0 -> stage_3_0
	}
	subgraph cluster3 {
		label = "Stage 4 [Resolved]";
		stage_4_0 [shape=box, label="ShuffleWriter [2 partitions]"]
		stage_4_0_0 [shape=box, label="DataSourceExec: (Memory) [2 partitions]"]
		stage_4_0_0 -> stage_4_0
	}
	subgraph cluster4 {
		label = "Stage 5 [Unresolved]";
		stage_5_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_5_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_5_0_0_0 [shape=box, label="HashJoin
join_expr=b@3 = b@1
filter_expr="]
		stage_5_0_0_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_5_0_0_0_0_0 [shape=box, label="UnresolvedShuffleExec [stage_id=3]"]
		stage_5_0_0_0_0_0 -> stage_5_0_0_0_0
		stage_5_0_0_0_0 -> stage_5_0_0_0
		stage_5_0_0_0_1 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_5_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=4]"]
		stage_5_0_0_0_1_0 -> stage_5_0_0_0_1
		stage_5_0_0_0_1 -> stage_5_0_0_0
		stage_5_0_0_0 -> stage_5_0_0
		stage_5_0_0 -> stage_5_0
	}
	stage_1_0 -> stage_3_0_0_0_0_0
	stage_2_0 -> stage_3_0_0_0_1_0
	stage_3_0 -> stage_5_0_0_0_0_0
	stage_4_0 -> stage_5_0_0_0_1_0
}
"#;
        assert_eq!(expected, &dot);
        Ok(())
    }

    #[tokio::test]
    async fn query_stage() -> Result<()> {
        let graph = test_graph().await?;
        let dot = ExecutionGraphDot::generate_for_query_stage(&graph, 3)
            .map_err(|e| BallistaError::Internal(format!("{e:?}")))?;

        let expected = r#"digraph G {
		stage_3_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_3_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_3_0_0_0 [shape=box, label="HashJoin
join_expr=a@0 = a@0
filter_expr="]
		stage_3_0_0_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_3_0_0_0_0_0 [shape=box, label="UnresolvedShuffleExec [stage_id=1]"]
		stage_3_0_0_0_0_0 -> stage_3_0_0_0_0
		stage_3_0_0_0_0 -> stage_3_0_0_0
		stage_3_0_0_0_1 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_3_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=2]"]
		stage_3_0_0_0_1_0 -> stage_3_0_0_0_1
		stage_3_0_0_0_1 -> stage_3_0_0_0
		stage_3_0_0_0 -> stage_3_0_0
		stage_3_0_0 -> stage_3_0
}
"#;
        assert_eq!(expected, &dot);
        Ok(())
    }

    #[tokio::test]
    async fn dot_optimized() -> Result<()> {
        let graph = test_graph_optimized().await?;
        let dot = ExecutionGraphDot::generate(&graph)
            .map_err(|e| BallistaError::Internal(format!("{e:?}")))?;

        let expected = r#"digraph G {
	subgraph cluster0 {
		label = "Stage 1 [Resolved]";
		stage_1_0 [shape=box, label="ShuffleWriter [2 partitions]"]
		stage_1_0_0 [shape=box, label="DataSourceExec: (Memory) [2 partitions]"]
		stage_1_0_0 -> stage_1_0
	}
	subgraph cluster1 {
		label = "Stage 2 [Resolved]";
		stage_2_0 [shape=box, label="ShuffleWriter [2 partitions]"]
		stage_2_0_0 [shape=box, label="DataSourceExec: (Memory) [2 partitions]"]
		stage_2_0_0 -> stage_2_0
	}
	subgraph cluster2 {
		label = "Stage 3 [Resolved]";
		stage_3_0 [shape=box, label="ShuffleWriter [2 partitions]"]
		stage_3_0_0 [shape=box, label="DataSourceExec: (Memory) [2 partitions]"]
		stage_3_0_0 -> stage_3_0
	}
	subgraph cluster3 {
		label = "Stage 4 [Unresolved]";
		stage_4_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_4_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0 [shape=box, label="HashJoin
join_expr=a@1 = a@0
filter_expr="]
		stage_4_0_0_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0_0_0 [shape=box, label="HashJoin
join_expr=a@0 = a@0
filter_expr="]
		stage_4_0_0_0_0_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0_0_0_0_0 [shape=box, label="UnresolvedShuffleExec [stage_id=1]"]
		stage_4_0_0_0_0_0_0_0 -> stage_4_0_0_0_0_0_0
		stage_4_0_0_0_0_0_0 -> stage_4_0_0_0_0_0
		stage_4_0_0_0_0_0_1 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=2]"]
		stage_4_0_0_0_0_0_1_0 -> stage_4_0_0_0_0_0_1
		stage_4_0_0_0_0_0_1 -> stage_4_0_0_0_0_0
		stage_4_0_0_0_0_0 -> stage_4_0_0_0_0
		stage_4_0_0_0_0 -> stage_4_0_0_0
		stage_4_0_0_0_1 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=3]"]
		stage_4_0_0_0_1_0 -> stage_4_0_0_0_1
		stage_4_0_0_0_1 -> stage_4_0_0_0
		stage_4_0_0_0 -> stage_4_0_0
		stage_4_0_0 -> stage_4_0
	}
	stage_1_0 -> stage_4_0_0_0_0_0_0_0
	stage_2_0 -> stage_4_0_0_0_0_0_1_0
	stage_3_0 -> stage_4_0_0_0_1_0
}
"#;
        assert_eq!(expected, &dot);
        Ok(())
    }

    #[tokio::test]
    async fn query_stage_optimized() -> Result<()> {
        let graph = test_graph_optimized().await?;
        let dot = ExecutionGraphDot::generate_for_query_stage(&graph, 4)
            .map_err(|e| BallistaError::Internal(format!("{e:?}")))?;

        let expected = r#"digraph G {
		stage_4_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_4_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0 [shape=box, label="HashJoin
join_expr=a@1 = a@0
filter_expr="]
		stage_4_0_0_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0_0_0 [shape=box, label="HashJoin
join_expr=a@0 = a@0
filter_expr="]
		stage_4_0_0_0_0_0_0 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0_0_0_0_0 [shape=box, label="UnresolvedShuffleExec [stage_id=1]"]
		stage_4_0_0_0_0_0_0_0 -> stage_4_0_0_0_0_0_0
		stage_4_0_0_0_0_0_0 -> stage_4_0_0_0_0_0
		stage_4_0_0_0_0_0_1 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=2]"]
		stage_4_0_0_0_0_0_1_0 -> stage_4_0_0_0_0_0_1
		stage_4_0_0_0_0_0_1 -> stage_4_0_0_0_0_0
		stage_4_0_0_0_0_0 -> stage_4_0_0_0_0
		stage_4_0_0_0_0 -> stage_4_0_0_0
		stage_4_0_0_0_1 [shape=box, label="CoalesceBatches [batchSize=4096]"]
		stage_4_0_0_0_1_0 [shape=box, label="UnresolvedShuffleExec [stage_id=3]"]
		stage_4_0_0_0_1_0 -> stage_4_0_0_0_1
		stage_4_0_0_0_1 -> stage_4_0_0_0
		stage_4_0_0_0 -> stage_4_0_0
		stage_4_0_0 -> stage_4_0
}
"#;
        assert_eq!(expected, &dot);
        Ok(())
    }

    async fn test_graph() -> Result<ExecutionGraph> {
        let mut config = SessionConfig::new()
            .with_target_partitions(48)
            .with_batch_size(4096);
        config
            .options_mut()
            .optimizer
            .enable_round_robin_repartition = false;
        let ctx = SessionContext::new_with_config(config);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
        ]));
        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![], vec![]])?);
        ctx.register_table("foo", table.clone())?;
        ctx.register_table("bar", table.clone())?;
        ctx.register_table("baz", table)?;
        let df = ctx
            .sql("SELECT * FROM foo JOIN bar ON foo.a = bar.a JOIN baz on bar.b = baz.b")
            .await?;
        let plan = df.into_optimized_plan()?;
        let plan = ctx.state().create_physical_plan(&plan).await?;
        let mut planner = DefaultDistributedPlanner::new();
        ExecutionGraph::new(
            "scheduler_id",
            "job_id",
            "job_name",
            "session_id",
            plan,
            0,
            Arc::new(SessionConfig::new_with_ballista()),
            &mut planner,
        )
    }

    // With the improvement of https://github.com/apache/arrow-datafusion/pull/4122,
    // Redundant RepartitionExec can be removed so that the stage number will be reduced
    async fn test_graph_optimized() -> Result<ExecutionGraph> {
        let mut config = SessionConfig::new()
            .with_target_partitions(48)
            .with_batch_size(4096);
        config
            .options_mut()
            .optimizer
            .enable_round_robin_repartition = false;
        let ctx = SessionContext::new_with_config(config);
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        // we specify the input partitions to be > 1 because of https://github.com/apache/datafusion/issues/12611
        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![], vec![]])?);
        ctx.register_table("foo", table.clone())?;
        ctx.register_table("bar", table.clone())?;
        ctx.register_table("baz", table)?;
        let df = ctx
            .sql("SELECT * FROM foo JOIN bar ON foo.a = bar.a JOIN baz on bar.a = baz.a")
            .await?;
        let plan = df.into_optimized_plan()?;
        let plan = ctx.state().create_physical_plan(&plan).await?;
        let mut planner = DefaultDistributedPlanner::new();
        ExecutionGraph::new(
            "scheduler_id",
            "job_id",
            "job_name",
            "session_id",
            plan,
            0,
            Arc::new(SessionConfig::new_with_ballista()),
            &mut planner,
        )
    }
}
