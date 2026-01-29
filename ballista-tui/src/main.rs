pub mod app;
pub mod domain;
pub mod error;
pub mod event;
pub mod http_client;
pub mod logging;
pub mod tui;
pub mod ui;

use app::App;
use color_eyre::Result;
use event::{Event, EventHandler};
use std::time::Duration;
use tui::TuiWrapper;

use crate::{error::TuiError, event::UiData};

pub type TuiResult<OK> = Result<OK, TuiError>;

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_file_logger("ballista-tui", "info")?;
    tracing::info!("Starting the Ballista TUI application");

    color_eyre::install()?;

    let mut tui_wrapper = TuiWrapper::new()?;
    let mut app = App::new();
    let mut events = EventHandler::new(Duration::from_millis(250));

    let (app_tx, mut app_rx) = tokio::sync::mpsc::unbounded_channel();
    app.set_event_tx(app_tx);
    let _ = crate::ui::load_data(&app).await;

    loop {
        tui_wrapper.terminal.draw(|f| ui::render(f, &app))?;

        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Event::Key(key)) => app.on_key(key).await?,
                    Some(Event::Tick) => app.on_tick(),
                    Some(Event::Resize(_, _)) => {},
                    Some(Event::DataLoaded { .. }) => {},
                    None => break,
                }
            }
            Some(app_event) = app_rx.recv() => {
                if let Event::DataLoaded { data } = app_event {
                  match data {
                    UiData::SchedulerState(state) => {
                      app.dashboard_data = app.dashboard_data.with_scheduler_state(Some(state));
                    }
                  }
                }
            }
        }

        tokio::task::yield_now().await;

        if app.should_quit {
            tracing::info!("Stopping the Ballista TUI application!");
            break;
        }
    }

    Ok(())
}
