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

use crate::tui::TuiResult;
use crate::tui::app::App;
use crate::tui::domain::{
    SchedulerState,
    executors::Executor,
    jobs::{
        Job, JobDetails,
        stages::{JobStagesResponse, StagesGraph},
    },
    metrics::Metric,
};
#[cfg(not(feature = "web"))]
use crossterm::event::{EventStream, KeyEvent};
#[cfg(not(feature = "web"))]
use futures::{FutureExt, StreamExt};
use ratzilla::WebRenderer;
#[cfg(feature = "web")]
use ratzilla::event::KeyEvent;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(feature = "web"))]
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub enum UiData {
    SchedulerState(Option<SchedulerState>),
    Executors(Option<SchedulerState>, Vec<Executor>, Vec<Job>),
    Metrics(Vec<Metric>),
    Jobs(Vec<Job>),
    JobDetails(JobDetails),
    JobStagesGraph(StagesGraph),
    JobStagesData(String, JobStagesResponse),
    ExecutorDetails(Executor),
}

#[derive(Clone, Debug)]
#[expect(clippy::large_enum_variant)]
pub enum Event {
    Key(KeyEvent),
    Tick,
    DataLoaded { data: UiData },
}

#[cfg(not(feature = "web"))]
#[derive(Debug)]
pub struct EventHandler {
    rx: mpsc::UnboundedReceiver<Event>,
}

#[cfg(not(feature = "web"))]
impl EventHandler {
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut reader = EventStream::new();
            let mut interval = tokio::time::interval(tick_rate);

            loop {
                let interval_delay = interval.tick();
                let crossterm_event = reader.next().fuse();

                tokio::select! {
                    _ = interval_delay => {
                        if tx.send(Event::Tick).is_err() {
                            break;
                        }
                    }
                    Some(Ok(evt)) = crossterm_event => {
                      if let crossterm::event::Event::Key(key) = evt
                          && key.kind == crossterm::event::KeyEventKind::Press
                              && tx.send(Event::Key(key)).is_err()
                          {
                              break;
                          }
                    }
                }
            }
        });

        Self { rx }
    }

    pub async fn next(&mut self) -> Option<Event> {
        self.rx.recv().await
    }
}

#[cfg(feature = "web")]
#[derive(Debug)]
pub struct EventHandler {
    events: Arc<RefCell<VecDeque<Event>>>,
}

#[cfg(feature = "web")]
impl EventHandler {
    pub fn new(
        tick_rate: u32,
        app_tick: Rc<RefCell<App>>,
        terminal: &ratatui::Terminal<ratzilla::DomBackend>,
    ) -> Self {
        use wasm_bindgen_futures::spawn_local;

        let events = Arc::new(RefCell::new(VecDeque::new()));

        // ── Initial data load ─────────────────────────────────────────────
        {
            tracing::info!("Initial data load");
            let app_initial = Rc::clone(&app_tick);
            let events_initial = Arc::clone(&events);
            spawn_local(async move {
                let data =
                    match app_initial.borrow().http_client.get_scheduler_state().await {
                        Ok(state) => UiData::SchedulerState(Some(state)),
                        Err(e) => {
                            tracing::error!("Failed to load scheduler state: {e:?}");
                            UiData::SchedulerState(None)
                        }
                    };
                events_initial
                    .borrow_mut()
                    .push_back(Event::DataLoaded { data });
            });
        }

        tracing::info!("Setting up tick timer: refresh data every {tick_rate}ms");
        let tick_events = Arc::clone(&events);
        gloo_timers::callback::Interval::new(tick_rate, move || {
            let tick_events = Arc::clone(&tick_events);
            let app = Rc::clone(&app_tick);
            spawn_local(async move {
                let data = app.borrow().load_tick_data().await;
                // tracing::info!("UIData: {:#?}", &data);
                tick_events
                    .borrow_mut()
                    .push_back(Event::DataLoaded { data });
            });
        })
        .forget();

        let key_events = Arc::clone(&events);
        terminal.on_key_event(move |key_event| {
            tracing::info!("on key event: {key_event:?}");
            key_events.borrow_mut().push_back(Event::Key(key_event));
        });

        Self { events }
    }

    pub fn next(&mut self) -> Option<Event> {
        self.events.borrow_mut().pop_front()
    }
}

#[cfg(feature = "web")]
pub struct Sender<E> {
    app: Rc<RefCell<App>>,
    phantom: std::marker::PhantomData<E>,
}

#[cfg(feature = "web")]
impl Debug for Sender<Event> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sender")
    }
}

#[cfg(feature = "web")]
impl<E> Sender<E>
where
    E: Debug + Send + 'static,
{
    pub fn new(app: Rc<RefCell<App>>) -> Self {
        Self {
            app,
            phantom: std::marker::PhantomData,
        }
    }

    pub async fn send(&self, event: Event) -> TuiResult<()> {
        tracing::info!("Sending {event:?}");
        match event {
            Event::DataLoaded { data } => {
                // self.app.borrow_mut().apply_ui_data(data);
            }
            _ => {
                tracing::warn!("Unhandled event: {event:?}");
            }
        }
        Ok(())
    }
}
