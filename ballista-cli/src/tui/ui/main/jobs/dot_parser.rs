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

use crate::tui::domain::{GraphNode, GraphStage, StagesGraph};
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
