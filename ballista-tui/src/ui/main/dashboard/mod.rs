use chrono::{DateTime, Utc};
use color_eyre::eyre::Result;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::Style,
    widgets::{Block, Borders, Clear, Paragraph},
};
use serde::Deserialize;

use crate::{
    app::App,
    event::{Event, UiData},
};

#[derive(Deserialize)]
pub struct SchedulerState {
    started: i64,
    version: String,
}

pub fn render_dashboard(f: &mut Frame, area: Rect, app: &App) {
    let (started, version) = parse_scheduler_state(&app.dashboard_data);

    f.render_widget(Clear, area);

    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(40),
            Constraint::Percentage(40),
            Constraint::Percentage(20),
            Constraint::Min(0),
        ])
        .split(vertical_chunks[0]);

    let scheduler_url_block = Block::default()
        .borders(Borders::ALL)
        .title("Scheduler URL");
    let scheduler_url_paragraph = Paragraph::new(app.scheduler_url.as_str())
        .block(scheduler_url_block)
        .alignment(Alignment::Left);
    f.render_widget(scheduler_url_paragraph, chunks[0]);

    let started_block = Block::default().borders(Borders::ALL).title("Started");
    let started_text = started.format("%Y-%m-%d %H:%M:%S UTC").to_string();
    let started_paragraph = Paragraph::new(started_text)
        .block(started_block)
        .alignment(Alignment::Left);
    f.render_widget(started_paragraph, chunks[1]);

    let version_block = Block::default().borders(Borders::ALL).title("Version");
    let version_paragraph = Paragraph::new(version)
        .block(version_block)
        .alignment(Alignment::Left);
    f.render_widget(version_paragraph, chunks[2]);
}

pub async fn load_data(app: &App) -> Result<()> {
    let response = reqwest::get(&app.scheduler_url).await?;
    let data = response.text().await?;

    if let Some(event_tx) = &app.event_tx {
        event_tx.send(Event::DataLoaded {
            data: UiData::SchedulerState(data),
        })?;
    }
    Ok(())
}

fn parse_scheduler_state(data: &str) -> (DateTime<Utc>, String) {
    if data.is_empty() {
        (Utc::now(), "unknown".to_string())
    } else {
        match serde_json::from_str::<SchedulerState>(data) {
            Ok(SchedulerState { started, version }) => {
                let datetime =
                    DateTime::from_timestamp_millis(started).unwrap_or_else(Utc::now);
                (datetime, version)
            }
            Err(err) => {
                eprintln!("Failed to parse scheduler state: {err}");
                (Utc::now(), "unknown".to_string())
            }
        }
    }
}
