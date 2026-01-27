use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::Style,
    widgets::{Block, Borders, Clear, Paragraph},
};

use crate::app::App;

pub fn render_jobs(f: &mut Frame, area: Rect, _app: &App) {
    f.render_widget(Clear, area);
    let block = Block::default().borders(Borders::all());
    let paragraph = Paragraph::new("Jobs")
        .style(Style::default().bold())
        .centered()
        .block(block)
        .alignment(Alignment::Left);
    f.render_widget(paragraph, area);
}
