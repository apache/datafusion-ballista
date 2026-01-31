mod executors;
mod scheduler_state;

pub use executors::render_executors;
pub use scheduler_state::render_scheduler_state;

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    widgets::Clear,
};

use crate::tui::{
    TuiResult,
    app::App,
    error::TuiError,
    event::{Event, UiData},
};

pub fn render_dashboard(f: &mut Frame, area: Rect, app: &App) {
    f.render_widget(Clear, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    render_scheduler_state(f, chunks[0], app);

    render_executors(f, chunks[1], app);
}

pub async fn load_dashboard_data(app: &App) -> TuiResult<()> {
    let scheduler_state = app.http_client.get_scheduler_state().await?;
    let executors_data = app.http_client.get_executors().await?;
    if let Some(event_tx) = &app.event_tx {
        event_tx
            .send(Event::DataLoaded {
                data: UiData::Dashboard(scheduler_state, executors_data),
            })
            .map_err(TuiError::SendError)?;
    }

    Ok(())
}
