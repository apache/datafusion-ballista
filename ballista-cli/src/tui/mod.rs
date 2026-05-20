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
                    Some(evt) => tracing::debug!("Unexpected event: {evt:?}"),
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
pub fn tui_web_main() -> TuiResult<()> {
    use app::WebKeyAsyncAction;
    use ratzilla::WebRenderer;
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen_futures::spawn_local;

    let config = Settings::new()?;
    let tick_ms = config.tick_interval_ms as u32;

    let wrapper = TuiWrapper::new()?;
    let app = Rc::new(RefCell::new(App::new(config)?));

    // ── Initial data load ─────────────────────────────────────────────
    {
        let app = Rc::clone(&app);
        spawn_local(async move {
            let data = app.borrow().load_tick_data().await;
            app.borrow_mut().apply_ui_data(data);
        });
    }

    // ── Tick timer: refresh data periodically ─────────────────────────
    let app_tick = Rc::clone(&app);
    let _tick_timer = gloo_timers::callback::Interval::new(tick_ms, move || {
        let app = Rc::clone(&app_tick);
        spawn_local(async move {
            let data = app.borrow().load_tick_data().await;
            app.borrow_mut().apply_ui_data(data);
        });
    });

    // ── Keyboard events ───────────────────────────────────────────────
    let app_key = Rc::clone(&app);
    wrapper.terminal.on_key_event(move |key_event| {
        let app = Rc::clone(&app_key);
        spawn_local(async move {
            // Synchronous state mutation (brief mutable borrow — released before any await)
            let async_action = {
                let mut a = app.borrow_mut();
                a.on_key_sync(&key_event)
            };

            // Async work (uses immutable borrow, or brief mutable borrow for updates)
            if let Some(action) = async_action {
                match action {
                    WebKeyAsyncAction::LoadJobStages(job_id) => {
                        let result = {
                            let a = app.borrow();
                            a.http_client.get_job_stages(&job_id).await
                        };
                        match result {
                            Ok(stages) => app.borrow_mut().apply_ui_data(
                                crate::tui::event::UiData::JobStagesData(job_id, stages),
                            ),
                            Err(e) => {
                                tracing::error!("Failed to load job stages: {e:?}")
                            }
                        }
                    }
                    WebKeyAsyncAction::LoadExecutorDetails(executor_id) => {
                        let result = {
                            let a = app.borrow();
                            a.http_client.get_executor(&executor_id).await
                        };
                        match result {
                            Ok(executor) => app.borrow_mut().apply_ui_data(
                                crate::tui::event::UiData::ExecutorDetails(executor),
                            ),
                            Err(e) => {
                                tracing::error!("Failed to load executor details: {e:?}")
                            }
                        }
                    }
                    WebKeyAsyncAction::LoadJobDot(job_id) => {
                        let result = {
                            let a = app.borrow();
                            a.http_client.get_job_dot(&job_id).await
                        };
                        match result {
                            Ok(dot_string) => {
                                let graph =
                                    ui::dot_parser::parse_dot(&job_id, &dot_string);
                                app.borrow_mut().apply_ui_data(
                                    crate::tui::event::UiData::JobStagesGraph(graph),
                                )
                            }
                            Err(e) => {
                                tracing::error!("Failed to load job dot: {e:?}")
                            }
                        }
                    }
                    WebKeyAsyncAction::CancelJob(job_id) => {
                        let result = {
                            let a = app.borrow();
                            a.http_client.cancel_job(&job_id).await
                        };
                        use crate::tui::domain::jobs::CancelJobResult;
                        let cancel_result = match result {
                            Ok(resp) if resp.canceled => {
                                CancelJobResult::Success { job_id }
                            }
                            Ok(_) => CancelJobResult::NotCanceled { job_id },
                            Err(e) => CancelJobResult::Failure {
                                job_id,
                                error: e.to_string(),
                            },
                        };
                        app.borrow_mut().cancel_job_result = Some(cancel_result);
                    }
                    WebKeyAsyncAction::UpdateJobDetails(job_id) => {
                        if let Some(id) = job_id {
                            let result = {
                                let a = app.borrow();
                                a.http_client.get_job_details(&id).await
                            };
                            match result {
                                Ok(details) => app.borrow_mut().apply_ui_data(
                                    crate::tui::event::UiData::JobDetails(details),
                                ),
                                Err(e) => {
                                    tracing::error!("Failed to load job details: {e:?}")
                                }
                            }
                        }
                    }
                    WebKeyAsyncAction::ReloadView => {
                        let data = app.borrow().load_tick_data().await;
                        app.borrow_mut().apply_ui_data(data);
                    }
                }
            }
        });
    });

    // ── Render loop (driven by requestAnimationFrame) ─────────────────
    let app_render = Rc::clone(&app);
    wrapper.terminal.draw_web(move |f| {
        let app = app_render.borrow();
        ui::render(f, &*app);
    });

    Ok(())
}
