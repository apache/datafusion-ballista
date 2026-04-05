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

use crate::tui::app::App;
use crate::tui::domain::jobs::GraphNode;
use ratatui::Frame;
use ratatui::prelude::{Color, Line, Modifier, Span, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use std::collections::{HashMap, HashSet};

pub(crate) fn render_job_dot_popup(f: &mut Frame, app: &App) {
    let Some(graph) = &app.job_dot_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(60, 60, f.area());
    f.render_widget(Clear, area);

    let block = Block::default()
        .title(format!(
            " Job Stages: {} (↑↓ scroll, any other key to close) ",
            graph.job_id
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    // Width of the area inside the popup block borders.
    let inner_width = area.width.saturating_sub(2) as usize;
    // Node box line structure: ' ┌' + inner + '┐ '  →  inner_width = node_inner_width + 4
    let node_inner_width = inner_width.saturating_sub(4);
    // Position of ┬/┴ junction within the inner content (0-indexed).
    let junction_col = node_inner_width / 2;
    // Column of the ↓ arrow within the full inner_width line.
    // 1 (indent space) + 1 (┌/└) + junction_col
    let arrow_col = 2 + junction_col;

    let border_style = Style::default().fg(Color::Cyan);
    let label_style = Style::default().fg(Color::White);
    let stage_style = Style::default()
        .fg(Color::Yellow)
        .add_modifier(Modifier::BOLD);
    let arrow_style = Style::default().fg(Color::Green);

    let mut lines: Vec<Line> = Vec::new();

    for stage in graph.stages.iter() {
        lines.push(Line::from(""));

        lines.push(Line::from(Span::styled(
            format!(" {}:", stage.label),
            stage_style,
        )));
        lines.push(Line::from(""));

        if stage.nodes.is_empty() {
            continue;
        }

        let stage_node_ids: HashSet<&str> =
            stage.nodes.iter().map(|n| n.id.as_str()).collect();

        // Intra-stage edges only.
        let intra_edges: Vec<(&str, &str)> = graph
            .edges
            .iter()
            .filter(|(from, to)| {
                stage_node_ids.contains(from.as_str())
                    && stage_node_ids.contains(to.as_str())
            })
            .map(|(from, to)| (from.as_str(), to.as_str()))
            .collect();

        // Build successor map and in-degree map for Kahn's topological sort.
        let mut successors: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        for node in &stage.nodes {
            successors.entry(node.id.as_str()).or_default();
            in_degree.entry(node.id.as_str()).or_insert(0);
        }
        for &(from, to) in &intra_edges {
            successors.entry(from).or_default().push(to);
            *in_degree.entry(to).or_insert(0) += 1;
        }

        // Kahn's algorithm — initialise queue in original node order.
        let mut queue: Vec<&str> = stage
            .nodes
            .iter()
            .filter(|n| in_degree[n.id.as_str()] == 0)
            .map(|n| n.id.as_str())
            .collect();
        let mut qi = 0;
        let mut ordered_ids: Vec<&str> = Vec::new();
        while qi < queue.len() {
            let id = queue[qi];
            qi += 1;
            ordered_ids.push(id);
            if let Some(succs) = successors.get(id) {
                for &succ in succs {
                    let deg = in_degree.get_mut(succ).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push(succ);
                    }
                }
            }
        }
        // Append nodes unreachable via the sort (disconnected or cycle).
        let ordered_set: HashSet<&str> = ordered_ids.iter().copied().collect();
        for node in &stage.nodes {
            if !ordered_set.contains(node.id.as_str()) {
                ordered_ids.push(node.id.as_str());
            }
        }

        let node_map: HashMap<&str, &GraphNode> =
            stage.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
        let edge_set: HashSet<(&str, &str)> = intra_edges.into_iter().collect();

        for (i, &node_id) in ordered_ids.iter().enumerate() {
            let Some(node) = node_map.get(node_id) else {
                continue;
            };

            let prev_id = if i > 0 {
                Some(ordered_ids[i - 1])
            } else {
                None
            };
            let next_id = ordered_ids.get(i + 1).copied();

            let connect_top =
                prev_id.is_some_and(|prev| edge_set.contains(&(prev, node_id)));
            let connect_bottom =
                next_id.is_some_and(|next| edge_set.contains(&(node_id, next)));

            lines.push(node_top_border(
                node_inner_width,
                junction_col,
                connect_top,
                border_style,
            ));
            lines.push(node_text_row(
                node_inner_width,
                &node.label,
                label_style,
                border_style,
            ));
            lines.push(node_bottom_border(
                node_inner_width,
                junction_col,
                connect_bottom,
                border_style,
            ));

            if connect_bottom {
                lines.push(arrow_connector(inner_width, arrow_col, arrow_style));
            }
        }
    }

    let paragraph = Paragraph::new(lines)
        .block(block)
        .scroll((app.job_dot_scroll, 0));

    f.render_widget(paragraph, area);
}

fn truncate_to_width(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        s.to_string()
    } else if max > 1 {
        let truncated: String = s.chars().take(max - 1).collect();
        format!("{}…", truncated)
    } else {
        s.chars().take(max).collect()
    }
}

/// Top border of a node box: ` ┌─…─┐ ` or ` ┌─…─┴─…─┐ ` when connected from above.
fn node_top_border(
    inner_w: usize,
    junction_col: usize,
    connect_top: bool,
    style: Style,
) -> Line<'static> {
    let border = if connect_top && inner_w > 0 {
        let j = junction_col.min(inner_w.saturating_sub(1));
        format!(" ┌{}┴{}┐ ", "─".repeat(j), "─".repeat(inner_w - j - 1))
    } else {
        format!(" ┌{}┐ ", "─".repeat(inner_w))
    };
    Line::from(Span::styled(border, style))
}

/// Bottom border of a node box: ` └─…─┘ ` or ` └─…─┬─…─┘ ` when connected below.
fn node_bottom_border(
    inner_w: usize,
    junction_col: usize,
    connect_bottom: bool,
    style: Style,
) -> Line<'static> {
    let border = if connect_bottom && inner_w > 0 {
        let j = junction_col.min(inner_w.saturating_sub(1));
        format!(" └{}┬{}┘ ", "─".repeat(j), "─".repeat(inner_w - j - 1))
    } else {
        format!(" └{}┘ ", "─".repeat(inner_w))
    };
    Line::from(Span::styled(border, style))
}

/// A content row inside a node box: ` │ <text> │ `.
fn node_text_row(
    inner_w: usize,
    text: &str,
    text_style: Style,
    border_style: Style,
) -> Line<'static> {
    // Available chars for text = inner_w - 1 (leading space already included).
    let available = inner_w.saturating_sub(1);
    let truncated = truncate_to_width(text, available);
    let content = format!(" {:<width$}", truncated, width = available);
    Line::from(vec![
        Span::styled(" │".to_string(), border_style),
        Span::styled(content, text_style),
        Span::styled("│ ".to_string(), border_style),
    ])
}

/// A single ↓ arrow line centred at `arrow_col`.
fn arrow_connector(total_width: usize, arrow_col: usize, style: Style) -> Line<'static> {
    let before = arrow_col.min(total_width);
    let after = total_width.saturating_sub(before + 1);
    Line::from(vec![
        Span::raw(" ".repeat(before)),
        Span::styled("↓".to_string(), style),
        Span::raw(" ".repeat(after)),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- truncate_to_width ---

    #[test]
    fn truncate_shorter_than_max_unchanged() {
        assert_eq!(truncate_to_width("hello", 10), "hello");
    }

    #[test]
    fn truncate_exact_length_unchanged() {
        assert_eq!(truncate_to_width("hello", 5), "hello");
    }

    #[test]
    fn truncate_longer_adds_ellipsis() {
        assert_eq!(truncate_to_width("hello world", 6), "hello…");
    }

    #[test]
    fn truncate_max_zero_is_empty() {
        assert_eq!(truncate_to_width("abc", 0), "");
    }

    #[test]
    fn truncate_max_one_is_first_char_no_ellipsis() {
        assert_eq!(truncate_to_width("abc", 1), "a");
    }

    #[test]
    fn truncate_max_two_is_one_char_plus_ellipsis() {
        assert_eq!(truncate_to_width("abc", 2), "a…");
    }

    // --- node_top_border ---

    #[test]
    fn top_border_no_connect_has_only_dashes() {
        let style = Style::default();
        let line = node_top_border(4, 2, false, style);
        let content = line.spans[0].content.as_ref();
        assert_eq!(content, " ┌────┐ ");
        assert!(!content.contains('┴'));
    }

    #[test]
    fn top_border_with_connect_has_junction() {
        let style = Style::default();
        let line = node_top_border(4, 2, true, style);
        let content = line.spans[0].content.as_ref();
        assert!(content.contains('┴'), "expected ┴ in {content:?}");
        assert_eq!(content, " ┌──┴─┐ ");
    }

    #[test]
    fn top_border_zero_width_no_panic() {
        let style = Style::default();
        let line = node_top_border(0, 0, true, style);
        // connect_top branch requires inner_w > 0, so falls back to plain border
        assert_eq!(line.spans[0].content.as_ref(), " ┌┐ ");
    }

    // --- node_bottom_border ---

    #[test]
    fn bottom_border_no_connect_has_only_dashes() {
        let style = Style::default();
        let line = node_bottom_border(4, 2, false, style);
        let content = line.spans[0].content.as_ref();
        assert_eq!(content, " └────┘ ");
        assert!(!content.contains('┬'));
    }

    #[test]
    fn bottom_border_with_connect_has_junction() {
        let style = Style::default();
        let line = node_bottom_border(4, 2, true, style);
        let content = line.spans[0].content.as_ref();
        assert!(content.contains('┬'), "expected ┬ in {content:?}");
        assert_eq!(content, " └──┬─┘ ");
    }

    #[test]
    fn bottom_border_zero_width_no_panic() {
        let style = Style::default();
        let line = node_bottom_border(0, 0, true, style);
        assert_eq!(line.spans[0].content.as_ref(), " └┘ ");
    }

    // --- node_text_row ---

    #[test]
    fn text_row_short_text_is_padded() {
        let style = Style::default();
        let line = node_text_row(10, "Hi", style, style);
        // available = inner_w - 1 = 9; content = " Hi       " (1 leading + 8 trailing spaces)
        let content = line.spans[1].content.as_ref();
        assert_eq!(content.len(), 10); // 1 space + 9 chars padded
        assert!(content.contains("Hi"));
    }

    #[test]
    fn text_row_long_text_is_truncated() {
        let style = Style::default();
        let line = node_text_row(6, "Hello World", style, style);
        // available = 5; truncated = "Hell…"; content = " Hell…"
        let content = line.spans[1].content.as_ref();
        assert!(content.contains('…'), "expected ellipsis in {content:?}");
    }

    // --- arrow_connector ---

    #[test]
    fn arrow_connector_middle_span_is_down_arrow() {
        let style = Style::default();
        let line = arrow_connector(10, 5, style);
        assert_eq!(line.spans[1].content.as_ref(), "↓");
    }

    #[test]
    fn arrow_connector_positions_arrow_correctly() {
        let style = Style::default();
        let line = arrow_connector(10, 3, style);
        // before = 3 spaces, arrow, after = 10 - 3 - 1 = 6 spaces
        assert_eq!(line.spans[0].content.as_ref(), "   ");
        assert_eq!(line.spans[2].content.as_ref(), "      ");
    }
}
