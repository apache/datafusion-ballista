use chrono::{DateTime, Utc};
use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

use crate::app::App;

pub fn render_executors(f: &mut Frame, area: Rect, app: &App) {
    fn no_live_executors(block: Block<'_>) -> Paragraph<'_> {
        Paragraph::new("No live executors")
            .block(block)
            .alignment(Alignment::Center)
    }

    let block = Block::default().borders(Borders::ALL).title("Executors");

    match &app.dashboard_data.executors_data {
        Some(executors) => {
            if executors.is_empty() {
                // let paragraph = Paragraph::new("No live executors")
                //     .block(block)
                //     .alignment(Alignment::Center);
                f.render_widget(no_live_executors(block), area);
                return;
            }

            let items: Vec<ListItem> = executors
                .iter()
                .map(|ex| {
                    let datetime = DateTime::from_timestamp_millis(ex.last_seen)
                        .unwrap_or_else(Utc::now);
                    let last_seen = datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string();

                    let line = format!(
                        "{}:{} ({}) — Last seen: {}",
                        ex.host, ex.port, ex.id, last_seen
                    );
                    ListItem::new(line)
                })
                .collect();

            let list = List::new(items).block(block);
            f.render_widget(list, area);
        }
        None => {
            // let paragraph = Paragraph::new("No live executors")
            //     .block(block)
            //     .alignment(Alignment::Center);
            // f.render_widget(paragraph, area);
            f.render_widget(no_live_executors(block), area);
        }
    };
}
