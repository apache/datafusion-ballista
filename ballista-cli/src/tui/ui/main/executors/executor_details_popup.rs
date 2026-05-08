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
use crate::tui::domain::executors::{ExecutorDetails, ExecutorDetailsPopup};
use ratatui::Frame;
use ratatui::prelude::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph};

pub(crate) fn render_executor_details_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.executor_details_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(35, 55, f.area());
    f.render_widget(Clear, area);

    let executor = &popup.executor;
    let title = " Executor details ";

    let lines = build_lines(app, popup, executor);

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::LightCyan))
        .border_type(BorderType::Thick);

    let paragraph = Paragraph::new(lines)
        .block(block)
        .scroll((popup.scroll_position, 0));

    f.render_widget(paragraph, area);
}

fn build_lines<'a>(
    app: &'a App,
    _popup: &'a ExecutorDetailsPopup,
    executor_details: &'a ExecutorDetails,
) -> Vec<Line<'a>> {
    let executor = &executor_details.executor_info;
    let label_style = Style::default().fg(Color::Yellow);

    let mut lines = vec![
        Line::from(vec![
            Span::styled("  Address     ", label_style),
            Span::raw(format!("{}:{}", executor.host, executor.port)),
        ]),
        Line::from(vec![
            Span::styled("  ID          ", label_style),
            Span::raw(executor.id.clone()),
        ]),
        Line::from(vec![
            Span::styled("  Last Seen   ", label_style),
            Span::raw(app.format_datetime(executor.last_seen)),
        ]),
        Line::from(vec![
            Span::styled("  Task Slots  ", label_style),
            Span::raw(executor.specification.task_slots.to_string()),
        ]),
    ];

    if !executor.metrics.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled("  Metrics", label_style)]));
        for metric in &executor.metrics {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("    {:<20} ", metric_name(&metric.typ)),
                    label_style,
                ),
                Span::raw(app.format_size(metric.value as usize)),
            ]));
        }
    }

    let os = &executor_details.os_info;
    lines.push(Line::from(""));
    lines.push(Line::from(vec![Span::styled("  OS Info", label_style)]));
    lines.push(Line::from(vec![
        Span::styled("    System Name      ", label_style),
        Span::raw(os.system_name.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    OS Version       ", label_style),
        Span::raw(os.os_ver.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    OS Version Long  ", label_style),
        Span::raw(os.os_ver_long.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Kernel Version   ", label_style),
        Span::raw(os.kernel_ver.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Physical Cores   ", label_style),
        Span::raw(os.physical_cores.to_string()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Num Disks        ", label_style),
        Span::raw(os.num_disks.to_string()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Total Disk       ", label_style),
        Span::raw(app.format_size(os.total_disk_space as usize)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Available Disk   ", label_style),
        Span::raw(app.format_size(os.total_available_disk_space as usize)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Open Files Limit ", label_style),
        Span::raw(os.open_files_limit.to_string()),
    ]));

    lines
}

fn metric_name(typ: &str) -> String {
    match typ {
        "proc_physical_memory" => "Physical Memory".to_string(),
        "proc_virtual_memory" => "Virtual Memory".to_string(),
        "peak_physical_memory" => "Peak Physical Memory".to_string(),
        "peak_virtual_memory" => "Peak Virtual Memory".to_string(),
        _ => typ.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::metric_name;

    #[test]
    fn metric_name_proc_physical_memory() {
        assert_eq!(metric_name("proc_physical_memory"), "Physical Memory");
    }

    #[test]
    fn metric_name_proc_virtual_memory() {
        assert_eq!(metric_name("proc_virtual_memory"), "Virtual Memory");
    }

    #[test]
    fn metric_name_peak_physical_memory() {
        assert_eq!(metric_name("peak_physical_memory"), "Peak Physical Memory");
    }

    #[test]
    fn metric_name_peak_virtual_memory() {
        assert_eq!(metric_name("peak_virtual_memory"), "Peak Virtual Memory");
    }

    #[test]
    fn metric_name_unknown_type_returns_as_is() {
        assert_eq!(metric_name("cpu_usage"), "cpu_usage");
    }

    #[test]
    fn metric_name_empty_string_returns_empty() {
        assert_eq!(metric_name(""), "");
    }
}
