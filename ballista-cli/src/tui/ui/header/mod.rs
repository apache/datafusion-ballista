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
use crate::tui::ui::header::scheduler_state::render_scheduler_state;
use ratatui::style::{Style, Stylize};
use ratatui::widgets::BorderType;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    text::{Line, Text},
    widgets::{Block, Borders, Paragraph},
};

pub mod scheduler_state;

const MENU_ITEMS: [&str; 3] = ["Jobs", "Executors", "Metrics"];
const PERCENTAGE: u16 = 100 / MENU_ITEMS.len() as u16;
const MENU_CONSTRAINTS: [Constraint; MENU_ITEMS.len()] =
    [Constraint::Percentage(PERCENTAGE); MENU_ITEMS.len()];

pub(super) fn render_header(f: &mut Frame, area: Rect, app: &App) {
    let banner_percentage = match area.width {
        0..200 => 30,
        220..230 => 40,
        _ => 45,
    };
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(banner_percentage), // banner
            Constraint::Percentage(100 - banner_percentage), // scheduler info and menu
        ])
        .split(area);

    render_banner(f, chunks[0], app.theme.banner);
    render_navbar(f, chunks[1], app);
}

#[cfg(not(feature = "web"))]
fn render_banner(f: &mut Frame, area: Rect, banner_style: Style) {
    use tui_big_text::{BigText, PixelSize};

    // Quadrant is the densest PixelSize drawn purely from the Block Elements range
    // (U+2580..U+259F), which every monospace font carries. The denser sizes
    // (ThirdHeight/Sextant/QuarterHeight/Octant) need Symbols for Legacy Computing,
    // which most fonts lack and render as tofu. Quadrant costs 4 rows per line, so the
    // header is sized to fit two of them.
    let big_text = BigText::builder()
        .pixel_size(PixelSize::Quadrant)
        .style(banner_style)
        .lines(vec![" DataFusion".into(), " Ballista".into()])
        .build();
    f.render_widget(big_text, area);
}

// Web: logo is displayed as a DOM <img> element overlaid on the Ratzilla canvas.
// Halfblock characters are not reliably present in Ratzilla's WebGL2 glyph atlas,
// so we leave this area blank and rely on setup_logo_dom() in wasm.rs.
#[cfg(feature = "web")]
fn render_banner(f: &mut Frame, area: Rect, _banner_style: Style) {
    f.render_widget(Block::default(), area);
}

#[cfg(all(test, not(feature = "web")))]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    /// The height of the header chunk in `ui::render`, which is what the banner is given.
    const HEADER_HEIGHT: u16 = 8;
    /// The banner area as laid out by `render_header` on a wide terminal: 30% of the width.
    const BANNER_WIDTH: u16 = 60;

    fn draw_banner(height: u16) -> ratatui::buffer::Buffer {
        let backend = TestBackend::new(BANNER_WIDTH, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| render_banner(f, f.area(), Style::default()))
            .unwrap();
        terminal.backend().buffer().clone()
    }

    /// The banner must only use Block Elements (U+2580..=U+259F), which every monospace
    /// font carries. The denser `tui-big-text` pixel sizes emit Symbols for Legacy
    /// Computing (sextants at U+1FB00.., octants at U+1CD00..), which most fonts lack and
    /// render as tofu.
    #[test]
    fn banner_uses_only_widely_supported_block_element_glyphs() {
        let buffer = draw_banner(HEADER_HEIGHT);

        let offenders: Vec<char> = buffer
            .content()
            .iter()
            .flat_map(|cell| cell.symbol().chars())
            .filter(|c| !c.is_whitespace() && !matches!(c, '\u{2580}'..='\u{259F}'))
            .collect();

        assert!(
            offenders.is_empty(),
            "banner emitted glyphs outside Block Elements: {offenders:?}"
        );
    }

    /// The banner's natural height is set by its pixel size (4 rows per line for Quadrant,
    /// two lines), and `ratatui` clips rather than complains when a widget outgrows its
    /// area. Render into a deliberately over-tall buffer so the banner can draw its full
    /// height, then pin that height to the header's: too tall and " Ballista" would be cut
    /// off in the real layout, too short and the header wastes rows.
    #[test]
    fn banner_natural_height_matches_the_header_height() {
        let buffer = draw_banner(HEADER_HEIGHT * 2);

        let drawn_height = (0..HEADER_HEIGHT * 2)
            .filter(|&row| {
                (0..BANNER_WIDTH)
                    .any(|col| !buffer[(col, row)].symbol().trim().is_empty())
            })
            .map(|row| row + 1)
            .max()
            .unwrap_or(0);

        assert_eq!(
            drawn_height, HEADER_HEIGHT,
            "banner draws {drawn_height} rows but the header reserves {HEADER_HEIGHT}; \
             adjust the banner's PixelSize or the header's Constraint::Length so they agree"
        );
    }
}

fn render_navbar(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0), // Scheduler info
            Constraint::Min(0), // navigation
        ])
        .split(area);

    render_scheduler_state(f, chunks[0], app);
    render_menu(f, chunks[1], app);
}

fn render_menu(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(MENU_CONSTRAINTS)
        .split(area);

    for (index, menu_item) in MENU_ITEMS.iter().enumerate() {
        let mut block = Block::default()
            .borders(Borders::ALL)
            .border_style(app.theme.nav_inactive);

        let first_char = menu_item.chars().next().unwrap().underlined();
        let rest_chars = menu_item.chars().skip(1).collect::<String>();
        let line = Line::from(vec![first_char, rest_chars.into()]);
        let text = Text::from(line);

        let is_active = (app.is_executors_view() && *menu_item == "Executors")
            || (app.is_jobs_view() && *menu_item == "Jobs")
            || (app.is_metrics_view() && *menu_item == "Metrics");

        let style = if is_active && app.is_scheduler_up() {
            block = block
                .border_style(app.theme.nav_active)
                .border_type(BorderType::Thick);
            app.theme.nav_active
        } else {
            app.theme.nav_inactive
        };

        let paragraph = Paragraph::new(text)
            .style(style)
            .block(block.clone())
            .alignment(Alignment::Center);

        f.render_widget(paragraph, chunks[index]);
    }
}
