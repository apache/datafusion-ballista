use chrono::{DateTime, Utc};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    widgets::{Block, Borders, Clear, Paragraph},
};

use crate::{
    TuiResult,
    app::App,
    error::TuiError,
    event::{Event, UiData},
};

pub fn render_dashboard(f: &mut Frame, area: Rect, app: &App) {
    let (started, version) = match &app.dashboard_data.scheduler_state {
        Some(state) => {
            let datetime =
                DateTime::from_timestamp_millis(state.started).unwrap_or_else(Utc::now);
            (datetime, state.version.clone())
        }
        None => (Utc::now(), "unknown".to_string()),
    };

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
    let scheduler_url_paragraph = Paragraph::new(app.http_client.scheduler_url())
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

pub async fn load_data(app: &App) -> TuiResult<()> {
    let scheduler_state = app.http_client.get_scheduler_state().await?;
    if let Some(event_tx) = &app.event_tx {
        event_tx
            .send(Event::DataLoaded {
                data: UiData::SchedulerState(scheduler_state),
            })
            .map_err(TuiError::SendError)?;
    }
    Ok(())
}
