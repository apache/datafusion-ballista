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
use ratatui::widgets::ScrollbarState;
use std::time::Duration;
use terminal::TuiWrapper;

use crate::tui::domain::{
    executors::ExecutorsData,
    jobs::{JobsData, stages::JobStagesPopup},
    metrics::MetricsData,
};
use crate::tui::{error::TuiError, event::UiData, infrastructure::Settings};

pub type TuiResult<OK> = Result<OK, TuiError>;

pub async fn tui_main() -> TuiResult<()> {
    infrastructure::init_file_logger("ballista", "info")?;
    tracing::info!("Starting the Ballista TUI application");

    let config = Settings::new()?;

    let mut tui_wrapper = TuiWrapper::new()?;
    let mut app = App::new(config)?;
    let mut events = EventHandler::new(Duration::from_millis(2000));

    let (app_tx, mut app_rx) = tokio::sync::mpsc::channel(16);
    app.set_event_tx(app_tx);
    let _ = crate::tui::ui::load_executors_data(&app).await;

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
                  match data {
                    UiData::Executors(state, executors, jobs) => {
                            let old_scrollbar_position = app.executors_data.scrollbar_state.get_position();
                            let scrollbar_state = ScrollbarState::new(executors.len()).position(old_scrollbar_position);
                            app.executors_data = ExecutorsData {
                                executors,
                                scrollbar_state,
                                table_state: app.executors_data.table_state,
                                sort_column: app.executors_data.sort_column,
                                sort_order: app.executors_data.sort_order,
                                scheduler_state: state,
                                jobs,
                            };
                            app.executors_data.sort();
                    },
                    UiData::Metrics(metrics) => {
                            let old_scrollbar_position = app.metrics_data.scrollbar_state.get_position();
                            let scrollbar_state = ScrollbarState::new(metrics.len()).position(old_scrollbar_position);
                            app.metrics_data = MetricsData {
                                metrics,
                                scrollbar_state,
                                table_state: app.metrics_data.table_state,
                                sort_column: app.metrics_data.sort_column,
                                sort_order: app.metrics_data.sort_order
                            };
                            app.metrics_data.sort();
                    }
                    UiData::Jobs(jobs) => {
                            let old_scrollbar_position = app.jobs_data.scrollbar_state.get_position();
                            let scrollbar_state = ScrollbarState::new(jobs.len()).position(old_scrollbar_position);
                            app.jobs_data = JobsData {
                                jobs,
                                scrollbar_state,
                                table_state: app.jobs_data.table_state,
                                sort_column: app.jobs_data.sort_column,
                                sort_order: app.jobs_data.sort_order
                            };
                    }
                    UiData::JobDetails(details) => {
                        app.job_details = Some(details);
                    }
                    UiData::JobStagesGraph(graph) => {
                        app.job_dot_popup = Some(graph);
                    }
                    UiData::JobStagesData(job_id, stages) => {
                        app.job_stages_popup = Some(JobStagesPopup::new(job_id, stages));
                    }
                  }
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
