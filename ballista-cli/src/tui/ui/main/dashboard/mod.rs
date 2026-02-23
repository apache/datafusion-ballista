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

    let is_scheduler_up = render_scheduler_state(f, chunks[0], app);

    if is_scheduler_up {
        render_executors(f, chunks[1], app);
    }
}

pub async fn load_dashboard_data(app: &App) -> TuiResult<()> {
    let scheduler_state = match app.http_client.get_scheduler_state().await {
        Ok(state) => Some(state),
        Err(e) => {
            tracing::error!("Failed to load the scheduler state: {:?}", e);
            None
        }
    };
    let executors_data = match app.http_client.get_executors().await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to load the executors data: {:?}", e);
            vec![]
        }
    };

    match &app.event_tx {
        Some(event_tx) => {
            event_tx
                .send(Event::DataLoaded {
                    data: UiData::Dashboard(scheduler_state, executors_data),
                })
                .map_err(TuiError::SendError)?;
        }
        None => {
            tracing::warn!("Dashboard data loaded but event_tx is not set");
        }
    }
    
    Ok(())
}
