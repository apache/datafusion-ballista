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

#[cfg(feature = "web")]
use crate::tui::TuiResult;
use crate::tui::domain::{
    SchedulerState,
    executors::Executor,
    jobs::{
        CancelJobResult, Job, JobDetails,
        stages::{JobStagesResponse, StagesGraph},
    },
    metrics::Metric,
};
#[cfg(not(feature = "web"))]
use crossterm::event::{EventStream, KeyEvent};
#[cfg(not(feature = "web"))]
use futures::{FutureExt, StreamExt};
#[cfg(feature = "web")]
use ratzilla::WebRenderer;
#[cfg(feature = "web")]
use ratzilla::event::KeyEvent;
#[cfg(feature = "web")]
use std::cell::RefCell;
#[cfg(feature = "web")]
use std::collections::VecDeque;
#[cfg(feature = "web")]
use std::fmt::Debug;
#[cfg(feature = "web")]
use std::rc::Rc;
#[cfg(not(feature = "web"))]
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
    CancelJobResult(CancelJobResult),
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
    events: Rc<RefCell<VecDeque<Event>>>,
}

#[cfg(feature = "web")]
impl EventHandler {
    /// Returns a `(Sender, EventHandler)` pair. The `Sender` can be cloned and distributed
    /// to any code that needs to push events; the `EventHandler` drains the queue each frame.
    pub fn new(
        tick_rate: u32,
        terminal: &ratatui::Terminal<ratzilla::DomBackend>,
    ) -> (Sender<Event>, Self) {
        let queue = Rc::new(RefCell::new(VecDeque::new()));
        let sender = Sender {
            queue: Rc::clone(&queue),
        };

        tracing::info!("Setting up tick timer: refresh data every {tick_rate}ms");
        let tick_sender = sender.clone();
        gloo_timers::callback::Interval::new(tick_rate, move || {
            tick_sender.queue.borrow_mut().push_back(Event::Tick);
        })
        .forget();

        let key_sender = sender.clone();
        terminal.on_key_event(move |key_event| {
            tracing::info!("on key event: {key_event:?}");
            key_sender
                .queue
                .borrow_mut()
                .push_back(Event::Key(key_event));
        });

        (sender, Self { events: queue })
    }

    pub fn next(&mut self) -> Option<Event> {
        self.events.borrow_mut().pop_front()
    }
}

#[cfg(feature = "web")]
#[derive(Clone)]
pub struct Sender<E> {
    queue: Rc<RefCell<VecDeque<E>>>,
}

#[cfg(feature = "web")]
impl Debug for Sender<Event> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sender")
    }
}

#[cfg(feature = "web")]
impl Sender<Event> {
    pub async fn send(&self, event: Event) -> TuiResult<()> {
        self.queue.borrow_mut().push_back(event);
        Ok(())
    }
}
