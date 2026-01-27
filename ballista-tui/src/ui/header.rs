use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::Style,
    widgets::{Block, Borders, Paragraph},
};

use crate::app::{App, Views};

const MENU_ITEMS: [&str; 3] = ["Dashboard", "Jobs", "Metrics"];
const PERCENTAGE: u16 = 100 / MENU_ITEMS.len() as u16;
const MENU_CONSTRAINTS: [Constraint; MENU_ITEMS.len()] =
    [Constraint::Percentage(PERCENTAGE); MENU_ITEMS.len()];
const BANNER: &'static str = r#"
 _               _                    _
| \  _. _|_  _. |_     _ o  _  ._    |_)  _. | | o  _ _|_  _.
|_/ (_|  |_ (_| | |_| _> | (_) | |   |_) (_| | | | _>  |_ (_|
"#;

pub(super) fn render_header(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Length(3)])
        .split(area);

    render_block(f, chunks[0]);
    render_menu(f, chunks[1], &app);
}

fn render_block(f: &mut Frame, area: Rect) {
    let block = Block::default().borders(Borders::empty());
    let paragraph = Paragraph::new(BANNER)
        .style(Style::default().bold())
        .centered()
        .block(block)
        .alignment(Alignment::Left);
    f.render_widget(paragraph, area);
}

fn render_menu(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(MENU_CONSTRAINTS)
        .split(area);

    for (index, menu_item) in MENU_ITEMS.iter().enumerate() {
        let mut block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().dark_gray());
        let mut paragraph = Paragraph::new(*menu_item)
            .style(Style::default().dark_gray())
            .block(block.clone())
            .alignment(Alignment::Center);

        if app.current_view == Views::Dashboard && *menu_item == "Dashboard" {
            block = block.border_style(Style::default().white());
            paragraph = paragraph.style(Style::default().white()).block(block);
        } else if app.current_view == Views::Jobs && *menu_item == "Jobs" {
            block = block.border_style(Style::default().white());
            paragraph = paragraph.style(Style::default().white()).block(block);
        } else if app.current_view == Views::Metrics && *menu_item == "Metrics" {
            block = block.border_style(Style::default().white());
            paragraph = paragraph.style(Style::default().white()).block(block);
        }

        f.render_widget(paragraph, chunks[index]);
    }
}
