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
use event::{Event, EventHandler};
#[cfg(not(feature = "web"))]
use std::sync::Arc;
#[cfg(not(feature = "web"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(feature = "web"))]
use std::time::Duration;
use terminal::TuiWrapper;

use crate::tui::event::UiData;
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
    // let _ = ui::load_executors_data(&app).await;
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
    use crate::tui::event::UiData;
    use ratzilla::WebRenderer;
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen_futures::spawn_local;

    let config = Settings::new()?;
    let tick_ms = config.tick_interval_ms as u32;

    let wrapper = TuiWrapper::new()?;
    let app = Rc::new(RefCell::new(App::new(config)?));

    let (sender, mut event_handler) = EventHandler::new(tick_ms, &wrapper.terminal);
    app.borrow_mut().set_event_tx(sender.clone());

    // Initial data load — http_client extracted with a brief borrow, no borrow held across await
    {
        tracing::info!("Initial data load");
        let http_client = app.borrow().http_client.clone();
        let tx = sender.clone();
        spawn_local(async move {
            let data = match http_client.get_scheduler_state().await {
                Ok(state) => UiData::SchedulerState(Some(state)),
                Err(e) => {
                    tracing::error!("Failed to load scheduler state: {e:?}");
                    UiData::SchedulerState(None)
                }
            };
            tx.send(Event::DataLoaded { data }).await.ok();
        });
    }

    // ── Render loop (driven by requestAnimationFrame) ─────────────────
    let app_render = Rc::clone(&app);
    wrapper.terminal.draw_web(move |f| {
        while let Some(event) = event_handler.next() {
            match event {
                Event::DataLoaded { data } => {
                    // Brief synchronous borrow — safe, no await held
                    app.borrow_mut().apply_ui_data(data);
                }
                Event::Tick => {
                    // Extract state with brief borrows, then spawn async data load
                    let http_client = app.borrow().http_client.clone();
                    let is_executors = app.borrow().is_executors_view();
                    let is_jobs = app.borrow().is_jobs_view();
                    let tx = sender.clone();
                    spawn_local(async move {
                        let data =
                            load_tick_data_for_view(&http_client, is_executors, is_jobs)
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
                        let tx = sender.clone();
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
        ui::render(f, &*app);
    });

    Ok(())
}

/// Loads data for the currently active view without borrowing `App` during the await.
#[cfg(feature = "web")]
async fn load_tick_data_for_view(
    http_client: &crate::tui::http_client::HttpClient,
    is_executors: bool,
    is_jobs: bool,
) -> event::UiData {
    use crate::tui::event::UiData;
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
/// Captures only `Arc<HttpClient>` and `Sender<Event>` — no `Rc<RefCell<App>>` borrow.
#[cfg(feature = "web")]
async fn execute_web_async_action(
    http_client: std::sync::Arc<http_client::HttpClient>,
    tx: event::Sender<Event>,
    is_executors: bool,
    is_jobs: bool,
    action: app::WebKeyAsyncAction,
) {
    use crate::tui::domain::jobs::CancelJobResult;
    use app::WebKeyAsyncAction;
    use event::{Event, UiData};

    match action {
        WebKeyAsyncAction::LoadJobStages(id) => {
            match http_client.get_job_stages(&id).await {
                Ok(mut stages) => {
                    stages
                        .stages
                        .sort_by_key(|s| s.id.parse::<u64>().unwrap_or(u64::MAX));
                    tx.send(Event::DataLoaded {
                        data: UiData::JobStagesData(id, stages),
                    })
                    .await
                    .ok();
                }
                Err(e) => tracing::error!("Failed to load stages for job '{id}': {e:?}"),
            }
        }
        WebKeyAsyncAction::LoadExecutorDetails(id) => {
            match http_client.get_executor(&id).await {
                Ok(executor) => {
                    tx.send(Event::DataLoaded {
                        data: UiData::ExecutorDetails(executor),
                    })
                    .await
                    .ok();
                }
                Err(e) => tracing::error!("Failed to load executor '{id}': {e:?}"),
            }
        }
        WebKeyAsyncAction::LoadJobDot(id) => match http_client.get_job_dot(&id).await {
            Ok(dot_content) => {
                let graph = ui::dot_parser::parse_dot(&id, &dot_content);
                tx.send(Event::DataLoaded {
                    data: UiData::JobStagesGraph(graph),
                })
                .await
                .ok();
            }
            Err(e) => tracing::error!("Failed to load job dot for '{id}': {e:?}"),
        },
        WebKeyAsyncAction::CancelJob(id) => {
            let result = match http_client.cancel_job(&id).await {
                Ok(resp) if resp.canceled => CancelJobResult::Success { job_id: id },
                Ok(_) => CancelJobResult::NotCanceled { job_id: id },
                Err(e) => CancelJobResult::Failure {
                    job_id: id,
                    error: e.to_string(),
                },
            };
            tx.send(Event::DataLoaded {
                data: UiData::CancelJobResult(result),
            })
            .await
            .ok();
        }
        WebKeyAsyncAction::UpdateJobDetails(job_id) => {
            if let Some(id) = job_id {
                match http_client.get_job_details(&id).await {
                    Ok(details) => {
                        tx.send(Event::DataLoaded {
                            data: UiData::JobDetails(details),
                        })
                        .await
                        .ok();
                    }
                    Err(e) => {
                        tracing::error!("Failed to load job details for '{id}': {e:?}")
                    }
                }
            }
        }
        WebKeyAsyncAction::ReloadView => {
            let data = load_tick_data_for_view(&http_client, is_executors, is_jobs).await;
            tx.send(Event::DataLoaded { data }).await.ok();
        }
    }
}
