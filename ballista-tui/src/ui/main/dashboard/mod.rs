use color_eyre::eyre::Result;
use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::Style,
    widgets::{Block, Borders, Clear, Paragraph},
};

use crate::{app::App, event::Event};

pub fn render_dashboard(f: &mut Frame, area: Rect, app: &App) {
    f.render_widget(Clear, area);
    let block = Block::default().borders(Borders::all());
    let paragraph = Paragraph::new(app.dashboard_data.clone())
        .style(Style::default().bold())
        .centered()
        .block(block)
        .alignment(Alignment::Left);
    f.render_widget(paragraph, area);
}

pub async fn load_data(app: &mut App) -> Result<()> {
    let response = reqwest::get("https://jsonplaceholder.typicode.com/todos/1").await?;
    let data = response.text().await?;

    if let Some(event_tx) = &app.event_tx {
        event_tx.send(Event::DataLoaded { data })?;
    }
    Ok(())
}
