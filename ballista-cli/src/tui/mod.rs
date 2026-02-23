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

mod app;
mod domain;
mod error;
mod event;
mod http_client;
mod infrastructure;
mod terminal;
mod ui;

use app::App;
use color_eyre::Result;
use event::{Event, EventHandler};
use std::time::Duration;
use terminal::TuiWrapper;

use crate::tui::{error::TuiError, event::UiData, infrastructure::Settings};

pub type TuiResult<OK> = Result<OK, TuiError>;

pub async fn tui_main() -> Result<()> {
    infrastructure::init_file_logger("ballista", "info")?;
    tracing::info!("Starting the Ballista TUI application");

    color_eyre::install()?;
    let config = Settings::new()?;

    let mut tui_wrapper = TuiWrapper::new()?;
    let mut app = App::new(config)?;
    let mut events = EventHandler::new(Duration::from_millis(2000));

    let (app_tx, mut app_rx) = tokio::sync::mpsc::unbounded_channel();
    app.set_event_tx(app_tx);
    let _ = crate::tui::ui::load_dashboard_data(&app).await;

    loop {
        tui_wrapper.terminal.draw(|f| ui::render(f, &app))?;

        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Event::Key(key)) => app.on_key(key).await?,
                    Some(Event::Tick) => app.on_tick().await,
                    Some(Event::DataLoaded { .. }) => {},
                    None => break,
                }
            }
            Some(app_event) = app_rx.recv() => {
                if let Event::DataLoaded { data } = app_event {
                  match data {
                    UiData::Dashboard(state, executors_data) => {
                      app.dashboard_data = app.dashboard_data.with_scheduler_state(state).with_executors_data(Some(executors_data));
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
