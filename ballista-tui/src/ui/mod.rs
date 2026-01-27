mod header;
mod main;

use crate::app::{App, Views};
use main::{render_dashboard, render_jobs, render_metrics};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

use header::render_header;

pub(crate) fn render(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(0)])
        .split(f.area());

    render_header(f, chunks[0], &app);
    render_content(f, app, chunks[1]);

    // Overlay help if active
    if app.show_help {
        render_help_overlay(f);
    }
}

fn render_content(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(100)])
        .split(area);

    // render_sidebar(f, app, chunks[0]);
    render_main_view(f, app, chunks[0]);
}

fn render_main_view(f: &mut Frame, app: &App, area: Rect) {
    if app.current_view == Views::Dashboard {
        render_dashboard(f, area, app);
    } else if app.current_view == Views::Jobs {
        render_jobs(f, area, app);
    } else if app.current_view == Views::Metrics {
        render_metrics(f, area, app);
    }

    // if app.is_loading {
    //       let block = Block::default()
    //           .borders(Borders::ALL)
    //           .border_style(Style::default().fg(Color::DarkGray));
    //       let p = Paragraph::new("⟳ Fetching Market Data...")
    //           .block(block)
    //           .alignment(Alignment::Center);
    //       f.render_widget(p, area);
    //       return;
    //   }

    //   render_no_data(f, area);
}

fn render_no_data(f: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let p = Paragraph::new("No Data Available - Press 'r' to refresh")
        .block(block)
        .alignment(Alignment::Center);
    f.render_widget(p, area);
}

fn render_help_overlay(f: &mut Frame) {
    let area = centered_rect(60, 70, f.area());

    f.render_widget(Clear, area);

    let help_text = vec![
        Line::from(""),
        Line::from(Span::styled(
            "KEYBOARD SHORTCUTS",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Navigation",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from("  ↑/k       Move up"),
        Line::from("  ↓/j       Move down"),
        Line::from("  /         Search stocks"),
        Line::from("  Esc       Exit search / Close help"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Time Ranges",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from("  1         1 Day (intraday)"),
        Line::from("  2         5 Days"),
        Line::from("  3         1 Month"),
        Line::from("  4         3 Months"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Features",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from("  w         Toggle watchlist"),
        Line::from("  W         Show watchlist only"),
        Line::from("  p         Toggle portfolio view"),
        Line::from("  r         Refresh data"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  General",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from("  ?/h       Show this help"),
        Line::from("  q/Esc     Quit"),
        Line::from(""),
        Line::from(Span::styled(
            "Press any key to close",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let block = Block::default()
        .title(" Help ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let para = Paragraph::new(help_text)
        .block(block)
        .wrap(Wrap { trim: false });

    f.render_widget(para, area);
}

// Helper functions

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn _render_sidebar(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    // Search box
    let search_text = if app.search_mode {
        format!("/{}_", app.search_query)
    } else if !app.search_query.is_empty() {
        format!("Filter: {}", app.search_query)
    } else {
        "Press / to search...".to_string()
    };

    let search_style = if app.search_mode {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let search_block = Block::default()
        .title(" Search ")
        .borders(Borders::ALL)
        .border_style(if app.search_mode {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        });

    let search_para = Paragraph::new(search_text)
        .style(search_style)
        .block(search_block);

    f.render_widget(search_para, chunks[0]);
}
