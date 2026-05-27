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

pub mod executor_details_popup;
mod executors_table;
mod jobs;

use jobs::render_jobs;

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    widgets::Clear,
};

use crate::tui::{
    TuiResult,
    app::App,
    domain::executors::Executor,
    event::{Event, UiData},
};

pub async fn load_executor_details_popup(app: &App, executor_id: &str) -> TuiResult<()> {
    let executor = app.http_client.get_executor(executor_id).await?;
    app.send_event(Event::DataLoaded {
        data: UiData::ExecutorDetails(executor),
    })
    .await
}

pub fn render_executors(f: &mut Frame, area: Rect, app: &App) {
    f.render_widget(Clear, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0), // Executors
            Constraint::Min(0), // Jobs
        ])
        .split(area);

    if app.is_scheduler_up() {
        executors_table::render_executors(f, chunks[0], app);
        render_jobs(f, chunks[1], app);
    }
}

pub async fn load_executors_data(app: &App) -> TuiResult<()> {
    let (scheduler_result, executors_result, jobs_result) = tokio::join!(
        app.http_client.get_scheduler_state(),
        app.http_client.get_executors(),
        app.http_client.get_jobs(),
    );

    let scheduler_state = scheduler_result
        .map_err(|e| tracing::error!("Failed to load the scheduler state: {e:?}"))
        .ok();
    let executors_data = executors_result.unwrap_or_else(|e| {
        tracing::error!("Failed to load the executors data: {e:?}");
        vec![]
    });
    let jobs_data = jobs_result.unwrap_or_else(|e| {
        tracing::error!("Failed to load the jobs data: {e:?}");
        vec![]
    });

    app.send_event(Event::DataLoaded {
        data: UiData::Executors(scheduler_state, executors_data, jobs_data),
    })
    .await
}

fn format_last_seen(executor: &Executor, app: &App) -> String {
    executor
        .last_seen
        .map(|d| match d.try_into() {
            Ok(d) => app.format_datetime(d),
            Err(_) => "Invalid timestamp".to_string(),
        })
        .unwrap_or_else(|| "N/A".to_string())
}
