use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::Style,
    widgets::{Block, Borders, Paragraph},
};

use crate::app::App;

pub fn render_metrics(f: &mut Frame, area: Rect, _app: &App) {
    let block = Block::default().borders(Borders::all());
    let paragraph = Paragraph::new("Metrics")
        .style(Style::default().bold())
        .centered()
        .block(block)
        .alignment(Alignment::Left);
    f.render_widget(paragraph, area);
}
