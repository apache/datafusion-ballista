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

use crate::tui::domain::jobs::{GraphNode, GraphStage, StagesGraph};
use dotparser::{GraphEvent, dot};
use std::collections::BTreeMap;

pub fn parse_dot(job_id: &str, content: &str) -> StagesGraph {
    let events = dot::parse(content);

    let unknown_key = u32::MAX;
    let mut stages: BTreeMap<u32, Vec<GraphNode>> = BTreeMap::new();
    let mut edges: Vec<(String, String)> = Vec::new();

    for event in events {
        match event {
            GraphEvent::AddNode { id, label, .. } => {
                let stage_num = if id.starts_with("stage_") {
                    id.split('_')
                        .nth(1)
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(unknown_key)
                } else {
                    unknown_key
                };
                let node = GraphNode {
                    label: label.unwrap_or_else(|| id.clone()),
                    id,
                };
                stages.entry(stage_num).or_default().push(node);
            }
            GraphEvent::AddEdge { from, to, .. } => {
                edges.push((from, to));
            }
            _ => {}
        }
    }

    let stages = stages
        .into_iter()
        .filter_map(|(k, nodes)| {
            if k == unknown_key {
                None
            } else {
                Some(GraphStage {
                    label: format!("Stage {k}"),
                    nodes,
                })
            }
        })
        .collect();

    StagesGraph {
        job_id: job_id.to_string(),
        stages,
        edges,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_content_produces_empty_graph() {
        let graph = parse_dot("job1", "");
        assert_eq!(graph.job_id, "job1");
        assert!(graph.stages.is_empty());
        assert!(graph.edges.is_empty());
    }

    #[test]
    fn node_without_stage_prefix_is_filtered_out() {
        let dot = r#"digraph { root [label="RootNode"] }"#;
        let graph = parse_dot("job1", dot);
        assert!(graph.stages.is_empty());
    }

    #[test]
    fn single_stage_node_no_label_uses_id_as_label() {
        let dot = r#"digraph {
        stage_0 [label="stage_0"]
        }"#;
        let graph = parse_dot("job1", dot);
        assert_eq!(graph.stages.len(), 1);
        assert_eq!(graph.stages[0].label, "Stage 0");
        assert_eq!(graph.stages[0].nodes[0].label, "stage_0");
        assert_eq!(graph.stages[0].nodes[0].id, "stage_0");
    }

    #[test]
    fn single_stage_node_with_label_uses_label() {
        let dot = r#"digraph {
        stage_0 [label="HashAggregate"]
        }"#;
        let graph = parse_dot("job1", dot);
        assert_eq!(graph.stages.len(), 1);
        assert_eq!(graph.stages[0].nodes[0].label, "HashAggregate");
        assert_eq!(graph.stages[0].nodes[0].id, "stage_0");
    }

    #[test]
    fn nodes_in_different_stages_are_ordered() {
        let dot = r#"digraph {
        stage_1 [label="SortExec"]
        stage_0 [label="HashAgg"]
        }"#;
        let graph = parse_dot("job1", dot);
        assert_eq!(graph.stages.len(), 2);
        // BTreeMap ensures stage 0 comes before stage 1
        assert_eq!(graph.stages[0].label, "Stage 0");
        assert_eq!(graph.stages[1].label, "Stage 1");
    }

    #[test]
    fn edge_between_nodes_is_captured() {
        let dot = r#"digraph {
        stage_0 [label="A"]
        stage_1 [label="B"]
        stage_0 -> stage_1
        }"#;
        let graph = parse_dot("job1", dot);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(
            graph.edges[0],
            ("stage_0".to_string(), "stage_1".to_string())
        );
    }

    #[test]
    fn realistic_multi_stage_dot() {
        let dot = r#"digraph {
            stage_0 [label="HashAggregate"]
            stage_1 [label="SortExec"]
            stage_2 [label="ProjectionExec"]
            stage_0 -> stage_1
            stage_1 -> stage_2
        }"#;
        let graph = parse_dot("realistic_job", dot);
        assert_eq!(graph.job_id, "realistic_job");
        assert_eq!(graph.stages.len(), 3);
        assert_eq!(graph.stages[0].nodes[0].label, "HashAggregate");
        assert_eq!(graph.stages[1].nodes[0].label, "SortExec");
        assert_eq!(graph.stages[2].nodes[0].label, "ProjectionExec");
        assert_eq!(graph.edges.len(), 2);
    }

    #[test]
    fn non_numeric_stage_suffix_goes_to_unknown_and_is_filtered() {
        // stage_abc cannot be parsed as u32, so it's filtered out
        let dot = r#"digraph {
        stage_abc [label="Mystery"]
        }"#;
        let graph = parse_dot("job1", dot);
        assert!(graph.stages.is_empty());
    }
}
