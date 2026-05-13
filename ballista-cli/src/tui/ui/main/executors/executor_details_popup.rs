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
use crate::tui::domain::executors::ExecutorDetails;
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

    let lines = build_lines(app, executor);

    let block = Block::default()
        .title(title)
        .title_style(Style::default().bold())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::LightCyan))
        .border_type(BorderType::Thick);

    let paragraph = Paragraph::new(lines)
        .block(block)
        .scroll((popup.scroll_position, 0));

    f.render_widget(paragraph, area);
}

fn build_lines<'a>(app: &'a App, executor_details: &'a ExecutorDetails) -> Vec<Line<'a>> {
    let executor = &executor_details.executor_info;
    let label_style = Style::default().fg(Color::Yellow);

    let mut lines = vec![
        Line::from(vec![
            Span::styled("  Address     ", label_style),
            Span::raw(format!("{}:{}", executor.host, executor.port)),
        ]),
        Line::from(vec![
            Span::styled("  ID          ", label_style),
            Span::raw(&executor.id),
        ]),
        Line::from(vec![
            Span::styled("  Last Seen   ", label_style),
            Span::raw(super::format_last_seen(executor, app)),
        ]),
    ];

    let os = &executor_details.os_info;
    lines.push(Line::from(""));
    lines.push(Line::from(vec![Span::styled(
        "  OS Info",
        label_style.bold(),
    )]));
    lines.push(Line::from(vec![
        Span::styled("    System Name      ", label_style),
        Span::raw(&os.system_name),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    OS Version       ", label_style),
        Span::raw(&os.os_ver),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    OS Version Long  ", label_style),
        Span::raw(&os.os_ver_long),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Kernel Version   ", label_style),
        Span::raw(&os.kernel_ver),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Physical Cores   ", label_style),
        Span::raw(app.format_count(os.physical_cores as usize)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("    Num Disks        ", label_style),
        Span::raw(app.format_count(os.num_disks as usize)),
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
