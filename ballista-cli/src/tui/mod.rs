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
#[cfg(not(feature = "web"))]
use event::{Event, EventHandler};
#[cfg(not(feature = "web"))]
use std::sync::Arc;
#[cfg(not(feature = "web"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(feature = "web"))]
use std::time::Duration;
use terminal::TuiWrapper;

use crate::tui::event::Event;
use crate::tui::{error::TuiError, infrastructure::Settings};

pub type TuiResult<OK> = Result<OK, TuiError>;

#[cfg(not(feature = "web"))]
pub async fn tui_main(tui_mode: Arc<AtomicBool>) -> TuiResult<()> {
    tui_mode.store(true, Ordering::Release);
    tracing::info!("Starting the Ballista TUI application");

    let config = Settings::new()?;
    tracing::debug!("TUI configuration: {:?}", config);

    let mut tui_wrapper = TuiWrapper::new()?;
    let mut events = EventHandler::new(Duration::from_millis(config.tick_interval_ms));
    let mut app = App::new(config)?;

    let (app_tx, mut app_rx) = tokio::sync::mpsc::channel(16);
    app.set_event_tx(app_tx);
    let _ = ui::load_executors_data(&app).await;

    loop {
        tui_wrapper.terminal.draw(|f| ui::render(f, &app))?;

        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Event::Key(key)) => app.on_key(key).await?,
                    Some(Event::Tick) => app.on_tick().await,
                    Some(evt) => tracing::warn!("Unexpected event: {evt:?}"),
                    None => break,
                }
            }
            Some(app_event) = app_rx.recv() => {
                if let Event::DataLoaded { data } = app_event {
                    app.apply_ui_data(data);
                }
            }
        }

        tokio::task::yield_now().await;

        if app.should_quit() {
            tracing::info!("Stopping the Ballista TUI application!");
            break;
        }
    }

    Ok(())
}

/// Entry point for the web browser TUI (WASM target). Sets up Ratzilla callbacks and starts
/// the Ratatui render loop via `draw_web`.
#[cfg(feature = "web")]
pub async fn tui_web_main() -> TuiResult<()> {
    use crate::tui::event::{EventHandler, Sender};
    use ratzilla::WebRenderer;
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen_futures::spawn_local;

    let config = Settings::new()?;
    let tick_ms = config.tick_interval_ms as u32;

    let wrapper = TuiWrapper::new()?;
    let app = Rc::new(RefCell::new(App::new(config)?));

    let sender = Sender::new(Rc::clone(&app));
    app.borrow_mut().set_event_tx(sender);

    let mut event_handler = EventHandler::new(tick_ms, app.clone(), &wrapper.terminal);

    // tracing::info!("Listening on key event");
    // let key_event_app = Rc::clone(&app);
    // wrapper.terminal.on_key_event(move |key_event| {
    //     let key_event_app = Rc::clone(&key_event_app);
    //     spawn_local(async move {
    //         match key_event_app.borrow_mut().on_key(key_event.clone()).await {
    //             Ok(_) => {
    //                 tracing::info!("==== Handled {key_event:?}");
    //             }
    //             Err(e) => {
    //                 tracing::error!("An error while handling a key event: {e}")
    //             }
    //         }
    //     });
    // });

    // ── Render loop (driven by requestAnimationFrame) ─────────────────
    let app_render = Rc::clone(&app);
    wrapper.terminal.draw_web(move |f| {
        while let Some(event) = event_handler.next() {
            match event {
                Event::DataLoaded { data } => app.borrow_mut().apply_ui_data(data),
                Event::Tick => {
                    tracing::info!("==== Received Tick Event");
                }
                Event::Key(key_event) => {
                    tracing::info!("==== Received KeyEvent: {key_event:?}");
                    let app_key_event = Rc::clone(&app_render);
                    spawn_local(async move {
                        match app_key_event.borrow_mut().on_key(key_event).await {
                            Ok(_) => tracing::info!("==== Handled key event"),
                            Err(err) => tracing::error!(
                                "An error while handling a key event: {err}"
                            ),
                        }
                    });
                }
            }
        }

        let app = app_render.borrow();
        ui::render(f, &*app);
    });

    Ok(())
}
