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
use ratatui::Frame;
use ratatui::prelude::{Color, Line, Span, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

pub(crate) fn render_scheduler_info(f: &mut Frame, app: &App) {
    if let Some(scheduler_state) = app.dashboard_data.scheduler_state.as_ref() {
        let area = crate::tui::ui::centered_rect(25, 35, f.area());
        f.render_widget(Clear, area);

        let mut enabled_features = Vec::new();
        enabled_features.push("rest-api".to_string());
        let mut disabled_features = Vec::new();

        if scheduler_state.prometheus_support {
            enabled_features.push("prometheus-metrics".to_string());
        } else {
            disabled_features.push("prometheus-metrics".to_string());
        }

        if scheduler_state.keda_support {
            enabled_features.push("keda-scaler".to_string());
        } else {
            disabled_features.push("keda-scaler".to_string());
        }

        if scheduler_state.spark_support {
            enabled_features.push("spark-compat".to_string());
        } else {
            disabled_features.push("spark-compat".to_string());
        }

        if scheduler_state.substrait_support {
            enabled_features.push("substrait".to_string());
        } else {
            disabled_features.push("substrait".to_string());
        }

        if scheduler_state.graphviz_support {
            enabled_features.push("graphviz-support".to_string());
        } else {
            disabled_features.push("graphviz-support".to_string());
        }

        let mut info_text = vec![
            Line::from(""),
            Line::from(format!(" Policy: {}", scheduler_state.scheduling_policy)),
        ];

        if !enabled_features.is_empty() {
            info_text.push(Line::from(""));
            info_text.push(Line::from(vec![Span::styled(
                " Enabled features",
                Style::default().fg(Color::Green),
            )]));
            enabled_features.sort();
            for feature in enabled_features {
                info_text.push(Line::from(vec![Span::styled(
                    format!("  - {feature}"),
                    Style::default().fg(Color::Green),
                )]));
            }
        }

        if !disabled_features.is_empty() {
            info_text.push(Line::from(""));
            info_text.push(Line::from(vec![Span::styled(
                " Disabled features",
                Style::default().fg(Color::Red),
            )]));
            disabled_features.sort();
            for feature in disabled_features {
                info_text.push(Line::from(vec![Span::styled(
                    format!("  - {feature}"),
                    Style::default().fg(Color::Red),
                )]));
            }
        }

        info_text.push(Line::from(""));

        let block = Block::default()
            .title(" Scheduler Information (Press any key to close) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        let para = Paragraph::new(info_text)
            .block(block)
            .wrap(Wrap { trim: false });

        f.render_widget(para, area);
    }
}
