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

use crate::tui::error::TuiError;

pub type TuiResult<OK> = Result<OK, TuiError>;

#[cfg(not(feature = "web"))]
pub(crate) use _tui::main;
#[cfg(feature = "web")]
pub(crate) use web::main;

#[cfg(not(feature = "web"))]
mod _tui {
    use crate::tui::{
        TuiResult,
        app::App,
        event::{Event, UiData, tui::EventHandler},
        infrastructure::Settings,
        terminal::TuiWrapper,
        ui,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;

    pub async fn main(tui_mode: Arc<AtomicBool>) -> TuiResult<()> {
        tui_mode.store(true, Ordering::Release);
        tracing::info!("Starting the Ballista TUI application");

        let config = Settings::new()?;
        tracing::debug!("TUI configuration: {:?}", config);

        let mut tui_wrapper = TuiWrapper::new()?;
        let mut events = EventHandler::new(
            Duration::from_millis(config.data_reload_interval_ms),
            Duration::from_millis(config.repaint_interval_ms),
        );
        let mut app = App::new(config)?;

        let (app_tx, mut app_rx) = tokio::sync::mpsc::channel(16);
        app.set_event_tx(app_tx);

        let data = match app.http_client.get_scheduler_state().await {
            Ok(state) => UiData::SchedulerState(Some(state)),
            Err(e) => {
                tracing::error!("Failed to load scheduler state: {e:?}");
                UiData::SchedulerState(None)
            }
        };
        app.send_event(Event::DataLoaded { data }).await.ok();

        loop {
            tui_wrapper.terminal.draw(|f| ui::render(f, &app))?;

            tokio::select! {
                maybe_event = events.next() => {
                    match maybe_event {
                        Some(Event::Key(key)) => app.on_key(key).await?,
                        Some(Event::Repaint) => {},
                        Some(Event::DataReload) => app.on_tick().await,
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
}

#[cfg(feature = "web")]
pub(crate) mod web {
    use crate::tui::{
        TuiResult,
        app::{App, WebKeyAsyncAction},
        domain::jobs::{CancelJobResult, JobConfigEntry, JobConfigPopup},
        event::{
            Event, UiData,
            web::{EventHandler, Sender},
        },
        http_client::HttpClient,
        infrastructure::Settings,
        terminal::TuiWrapper,
        ui,
    };

    /// Entry point for the web browser TUI (WASM target). Sets up Ratzilla callbacks and starts
    /// the Ratatui render loop via `draw_web`.
    pub async fn main() -> TuiResult<()> {
        use ratzilla::WebRenderer;
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;
        use wasm_bindgen_futures::spawn_local;

        let config = Settings::new()?;
        let data_reload_interval_ms =
            u32::try_from(config.data_reload_interval_ms).unwrap_or(u32::MAX);
        let repaint_interval_ms =
            u32::try_from(config.repaint_interval_ms).unwrap_or(u32::MAX);

        let wrapper = TuiWrapper::new()?;
        let app = Rc::new(RefCell::new(App::new(config)?));

        /// Reset the tick in flight flag on tick event handler drop.
        struct ResetOnDrop(Rc<Cell<bool>>);
        impl Drop for ResetOnDrop {
            fn drop(&mut self) {
                self.0.set(false);
            }
        }
        let tick_in_flight = Rc::new(Cell::new(false));

        let (tx, mut rx) = EventHandler::new(
            data_reload_interval_ms,
            repaint_interval_ms,
            &wrapper.terminal,
        );
        app.borrow_mut().set_event_tx(tx.clone());

        // Initial data load — http_client extracted with a brief borrow, no borrow held across await
        {
            tracing::info!("Initial data load");
            let http_client = app.borrow().http_client.clone();
            let tx = tx.clone();
            spawn_local(async move {
                let data = match http_client.get_scheduler_state().await {
                    Ok(state) => UiData::SchedulerState(Some(state)),
                    Err(e) => {
                        tracing::error!("Failed to load scheduler state: {e:?}");
                        UiData::SchedulerState(None)
                    }
                };
                tx.send(Event::DataLoaded { data }).await.ok();
                tx.send(Event::DataReload).await.ok();
            });
        }

        // ── Render loop (driven by requestAnimationFrame) ─────────────────
        let app_render = Rc::clone(&app);
        wrapper.terminal.draw_web(move |f| {
            while let Some(event) = rx.next() {
                match event {
                    Event::DataLoaded { data } => {
                        // Brief synchronous borrow — safe, no await held
                        app.borrow_mut().apply_ui_data(data);
                    }
                    Event::Repaint => {
                        // just repaint
                    }
                    Event::DataReload => {
                        if tick_in_flight.get() {
                            continue;
                        }
                        tick_in_flight.set(true);
                        // Extract state with brief borrows, then spawn async data load
                        let http_client = app.borrow().http_client.clone();
                        let is_executors = app.borrow().is_executors_view();
                        let is_jobs = app.borrow().is_jobs_view();
                        let tx = tx.clone();
                        let _tick_in_flight_done =
                            ResetOnDrop(Rc::clone(&tick_in_flight));
                        spawn_local(async move {
                            let data = load_tick_data_for_view(
                                &http_client,
                                is_executors,
                                is_jobs,
                            )
                            .await;
                            tx.send(Event::DataLoaded { data }).await.ok();
                        });
                    }
                    Event::Key(key_event) => {
                        // on_key_sync is purely synchronous — brief borrow_mut, no await
                        let action = app.borrow_mut().on_key_sync(&key_event);
                        if let Some(action) = action {
                            let http_client = app.borrow().http_client.clone();
                            let is_executors = app.borrow().is_executors_view();
                            let is_jobs = app.borrow().is_jobs_view();
                            let tx = tx.clone();
                            spawn_local(async move {
                                execute_web_async_action(
                                    http_client,
                                    tx,
                                    is_executors,
                                    is_jobs,
                                    action,
                                )
                                .await;
                            });
                        }
                    }
                }
            }

            let app = app_render.borrow();
            ui::render(f, &app);
        });

        Ok(())
    }

    /// Loads data for the currently active view without borrowing `App` during the await.
    async fn load_tick_data_for_view(
        http_client: &HttpClient,
        is_executors: bool,
        is_jobs: bool,
    ) -> UiData {
        if is_executors {
            let (scheduler_result, executors_result, jobs_result) = tokio::join!(
                http_client.get_scheduler_state(),
                http_client.get_executors(),
                http_client.get_jobs(),
            );
            UiData::Executors(
                scheduler_result
                    .map_err(|e| tracing::error!("Failed to load scheduler state: {e:?}"))
                    .ok(),
                executors_result.unwrap_or_else(|e| {
                    tracing::error!("Failed to load executors: {e:?}");
                    vec![]
                }),
                jobs_result.unwrap_or_else(|e| {
                    tracing::error!("Failed to load jobs: {e:?}");
                    vec![]
                }),
            )
        } else if is_jobs {
            UiData::Jobs(http_client.get_jobs().await.unwrap_or_else(|e| {
                tracing::error!("Failed to load jobs: {e:?}");
                vec![]
            }))
        } else {
            UiData::Metrics(http_client.get_metrics().await.unwrap_or_else(|e| {
                tracing::error!("Failed to load metrics: {e:?}");
                vec![]
            }))
        }
    }

    /// Executes an async action triggered by a key press, sending the result through `tx`.
    // Captures only `Arc<HttpClient>` and `Sender<Event>` — no `Rc<RefCell<App>>` borrow.
    async fn execute_web_async_action(
        http_client: std::sync::Arc<HttpClient>,
        tx: Sender<Event>,
        is_executors: bool,
        is_jobs: bool,
        action: WebKeyAsyncAction,
    ) {
        async fn send_data(data: UiData, tx: Sender<Event>) {
            tx.send(Event::DataLoaded { data }).await.ok();
        }

        match action {
            WebKeyAsyncAction::LoadJobStages(id) => {
                match http_client.get_job_stages(&id).await {
                    Ok(mut stages) => {
                        stages
                            .stages
                            .sort_by_key(|s| s.id.parse::<u64>().unwrap_or(u64::MAX));
                        send_data(UiData::JobStagesData(id, stages), tx).await;
                    }
                    Err(e) => {
                        tracing::error!("Failed to load stages for job '{id}': {e:?}")
                    }
                }
            }
            WebKeyAsyncAction::LoadExecutorDetails(id) => {
                match http_client.get_executor(&id).await {
                    Ok(executor) => {
                        send_data(UiData::ExecutorDetails(executor), tx).await;
                    }
                    Err(e) => tracing::error!("Failed to load executor '{id}': {e:?}"),
                }
            }
            WebKeyAsyncAction::LoadJobDot(id) => match http_client.get_job_dot(&id).await
            {
                Ok(dot_content) => {
                    let graph = ui::dot_parser::parse_dot(&id, &dot_content);
                    send_data(UiData::JobStagesGraph(graph), tx).await;
                }
                Err(e) => tracing::error!("Failed to load job dot for '{id}': {e:?}"),
            },
            WebKeyAsyncAction::LoadJobConfig(id) => {
                match http_client.get_job_config(&id).await {
                    Ok(config) => {
                        let entries = config
                            .into_iter()
                            .map(|(key, value)| JobConfigEntry { key, value })
                            .collect();
                        send_data(
                            UiData::JobConfig(JobConfigPopup::new(id, entries)),
                            tx,
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::error!("Failed to load job config for '{id}': {e:?}")
                    }
                }
            }
            WebKeyAsyncAction::CancelJob(id) => {
                let result = match http_client.cancel_job(&id).await {
                    Ok(resp) if resp.canceled => CancelJobResult::Success { job_id: id },
                    Ok(_) => CancelJobResult::NotCanceled { job_id: id },
                    Err(e) => CancelJobResult::Failure {
                        job_id: id,
                        error: e.to_string(),
                    },
                };
                send_data(UiData::CancelJobResult(result), tx).await;
            }
            WebKeyAsyncAction::UpdateJobDetails(job_id) => {
                if let Some(id) = job_id {
                    match http_client.get_job_details(&id).await {
                        Ok(details) => {
                            send_data(UiData::JobDetails(details), tx).await;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to load job details for '{id}': {e:?}"
                            )
                        }
                    }
                }
            }
            WebKeyAsyncAction::ReloadView => {
                let data =
                    load_tick_data_for_view(&http_client, is_executors, is_jobs).await;
                send_data(data, tx).await;
            }
        }
    }
}
