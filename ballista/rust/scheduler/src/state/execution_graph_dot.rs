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
use ballista_core::execution_plans::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::file_format::ParquetExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Utility for producing dot diagrams from execution graphs
pub struct ExecutionGraphDot {
    graph: Arc<ExecutionGraph>,
}

impl ExecutionGraphDot {
    pub fn new(graph: Arc<ExecutionGraph>) -> Self {
        Self { graph }
    }
}

impl Display for ExecutionGraphDot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut cluster = 0;
        writeln!(f, "digraph G {{")?;

        let stages = self.graph.stages();

        // sort the stages by key for deterministic output for tests
        let mut keys: Vec<usize> = stages.keys().cloned().collect();
        keys.sort();

        for id in &keys {
            let stage = stages.get(id).unwrap(); // safe unwrap
            let stage_name = format!("stage_{}", id);
            writeln!(f, "	subgraph cluster{} {{", cluster)?;
            match stage {
                ExecutionStage::UnResolved(stage) => {
                    writeln!(f, "		label = \"Stage {} [UnResolved]\";", id)?;
                    write_operator(f, &stage_name, &stage.plan, 0)?;
                }
                ExecutionStage::Resolved(stage) => {
                    writeln!(f, "		label = \"Stage {} [Resolved]\";", id)?;
                    write_operator(f, &stage_name, &stage.plan, 0)?;
                }
                ExecutionStage::Running(stage) => {
                    writeln!(f, "		label = \"Stage {} [Running]\";", id)?;
                    write_operator(f, &stage_name, &stage.plan, 0)?;
                }
                ExecutionStage::Completed(stage) => {
                    writeln!(f, "		label = \"Stage {} [Completed]\";", id)?;
                    write_operator(f, &stage_name, &stage.plan, 0)?;
                }
                ExecutionStage::Failed(stage) => {
                    writeln!(f, "		label = \"Stage {} [FAILED]\";", id)?;
                    write_operator(f, &stage_name, &stage.plan, 0)?;
                }
            }
            cluster += 1;
            writeln!(f, "	}}")?; // end of subgraph
        }

        // links
        for id in &keys {
            let stage = stages.get(id).unwrap(); // safe unwrap
            let stage_name = format!("stage_{}", id);
            let last_node = get_last_node_name(&stage_name, 0);
            for parent_stage_id in stage.output_links() {
                let parent_stage_name = format!("stage_{}", parent_stage_id);
                let parent_stage = stages.get(parent_stage_id).unwrap();
                let first_node =
                    get_first_node_name(&parent_stage_name, parent_stage.plan(), 0);
                writeln!(f, "	{} -> {}", last_node, first_node)?;
            }
        }

        writeln!(f, "}}") // end of digraph
    }
}

fn write_operator(
    f: &mut Formatter<'_>,
    prefix: &str,
    plan: &Arc<dyn ExecutionPlan>,
    i: usize,
) -> std::fmt::Result {
    let node_name = format!("{}_{}", prefix, i);
    let operator_name = get_operator_name(plan);
    let display_name = sanitize(&operator_name);

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
        writeln!(f, "		{} [shape=box, label=\"{}\"]", node_name, display_name)?;
    } else {
        writeln!(
            f,
            "		{} [shape=box, label=\"{}
{}\"]",
            node_name,
            display_name,
            metrics_str.join(", ")
        )?;
    }

    let mut j = 0;
    for child in plan.children() {
        write_operator(f, &node_name, &child, j)?;
        // write link from child to parent
        writeln!(f, "		{}_{} -> {}", node_name, j, node_name)?;
        j += 1;
    }

    Ok(())
}

fn get_last_node_name(prefix: &str, i: usize) -> String {
    format!("{}_{}", prefix, i)
}

fn get_first_node_name(prefix: &str, plan: &dyn ExecutionPlan, i: usize) -> String {
    let node_name = format!("{}_{}", prefix, i);
    let mut j = 0;
    let mut last_name = node_name.clone();
    for child in plan.children() {
        last_name = get_first_node_name(&node_name, child.as_ref(), j);
        j += 1;
    }
    last_name
}

/// Make strings dot-friendly
fn sanitize(str: &str) -> String {
    let x = str.to_string().replace('"', "\"");
    x.trim().to_string()
}

fn get_operator_name(plan: &Arc<dyn ExecutionPlan>) -> String {
    if let Some(exec) = plan.as_any().downcast_ref::<FilterExec>() {
        format!("Filter: {}", exec.predicate())
    } else if let Some(_) = plan.as_any().downcast_ref::<ProjectionExec>() {
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
    } else if let Some(_) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        "CoalesceBatches".to_string()
    } else if let Some(_) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        "CoalescePartitions".to_string()
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
    } else if let Some(exec) = plan.as_any().downcast_ref::<ShuffleReaderExec>() {
        format!("ShuffleReader [{} partitions]", exec.partition.len())
    } else if let Some(exec) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
        format!(
            "ShuffleWriter [{} partitions]",
            exec.output_partitioning().partition_count()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<ParquetExec>() {
        let parts = exec.output_partitioning().partition_count();
        // TODO make robust
        let filename = &exec.base_config().file_groups[0][0].object_meta.location;
        format!("Parquet: {} [{} partitions]", filename, parts)
    } else if let Some(exec) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        format!(
            "GlobalLimit(skip={}, fetch={:?})",
            exec.skip(),
            exec.fetch()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        format!("LocalLimit({})", exec.fetch())
    } else {
        "Unknown Operator".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::state::execution_graph::ExecutionGraph;
    use crate::state::execution_graph_dot::ExecutionGraphDot;
    use ballista_core::error::Result;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn dot() -> Result<()> {
        let ctx = SessionContext::new();
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
        let dot = ExecutionGraphDot::new(Arc::new(graph));
        let dot = format!("{}", dot);
        let expected = r#"digraph G {
	subgraph cluster0 {
		label = "Stage 1 [Resolved]";
		stage_1_0 [shape=box, label="ShuffleWriter [0 partitions]"]
		stage_1_0_0 [shape=box, label="Unknown Operator"]
		stage_1_0_0 -> stage_1_0
	}
	subgraph cluster1 {
		label = "Stage 2 [Resolved]";
		stage_2_0 [shape=box, label="ShuffleWriter [0 partitions]"]
		stage_2_0_0 [shape=box, label="Unknown Operator"]
		stage_2_0_0 -> stage_2_0
	}
	subgraph cluster2 {
		label = "Stage 3 [UnResolved]";
		stage_3_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_3_0_0 [shape=box, label="CoalesceBatches"]
		stage_3_0_0_0 [shape=box, label="HashJoin: a@0 = a@0"]
		stage_3_0_0_0_0 [shape=box, label="CoalesceBatches"]
		stage_3_0_0_0_0_0 [shape=box, label="Unknown Operator"]
		stage_3_0_0_0_0_0 -> stage_3_0_0_0_0
		stage_3_0_0_0_0 -> stage_3_0_0_0
		stage_3_0_0_0_1 [shape=box, label="CoalesceBatches"]
		stage_3_0_0_0_1_0 [shape=box, label="Unknown Operator"]
		stage_3_0_0_0_1_0 -> stage_3_0_0_0_1
		stage_3_0_0_0_1 -> stage_3_0_0_0
		stage_3_0_0_0 -> stage_3_0_0
		stage_3_0_0 -> stage_3_0
	}
	subgraph cluster3 {
		label = "Stage 4 [Resolved]";
		stage_4_0 [shape=box, label="ShuffleWriter [0 partitions]"]
		stage_4_0_0 [shape=box, label="Unknown Operator"]
		stage_4_0_0 -> stage_4_0
	}
	subgraph cluster4 {
		label = "Stage 5 [UnResolved]";
		stage_5_0 [shape=box, label="ShuffleWriter [48 partitions]"]
		stage_5_0_0 [shape=box, label="Projection"]
		stage_5_0_0_0 [shape=box, label="CoalesceBatches"]
		stage_5_0_0_0_0 [shape=box, label="HashJoin: a@1 = a@0"]
		stage_5_0_0_0_0_0 [shape=box, label="CoalesceBatches"]
		stage_5_0_0_0_0_0_0 [shape=box, label="Unknown Operator"]
		stage_5_0_0_0_0_0_0 -> stage_5_0_0_0_0_0
		stage_5_0_0_0_0_0 -> stage_5_0_0_0_0
		stage_5_0_0_0_0_1 [shape=box, label="CoalesceBatches"]
		stage_5_0_0_0_0_1_0 [shape=box, label="Unknown Operator"]
		stage_5_0_0_0_0_1_0 -> stage_5_0_0_0_0_1
		stage_5_0_0_0_0_1 -> stage_5_0_0_0_0
		stage_5_0_0_0_0 -> stage_5_0_0_0
		stage_5_0_0_0 -> stage_5_0_0
		stage_5_0_0 -> stage_5_0
	}
	stage_1_0 -> stage_3_0_0_0_1_0
	stage_2_0 -> stage_3_0_0_0_1_0
	stage_3_0 -> stage_5_0_0_0_0_1_0
	stage_4_0 -> stage_5_0_0_0_0_1_0
}
"#;
        assert_eq!(expected, &dot);
        Ok(())
    }
}
