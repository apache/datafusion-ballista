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

use crate::tui::domain::{
    SchedulerState,
    executors::Executor,
    jobs::{
        CancelJobResult, Job, JobConfigPopup, JobDetails,
        stages::{JobStagesResponse, StagePlanTab, StagesGraph},
    },
    metrics::Metric,
};
#[cfg(not(feature = "web"))]
use crossterm::event::KeyEvent;
#[cfg(feature = "web")]
use ratzilla::event::KeyEvent;

#[derive(Clone, Debug)]
pub enum UiData {
    SchedulerState(Option<SchedulerState>),
    Executors(Option<SchedulerState>, Vec<Executor>, Vec<Job>),
    Metrics(Vec<Metric>),
    Jobs(Vec<Job>),
    JobDetails(JobDetails),
    JobConfig(JobConfigPopup),
    JobStagesGraph(StagesGraph),
    JobStagesData(String, JobStagesResponse),
    JobStagesPlanData(StagePlanTab, JobStagesResponse),
    ExecutorDetails(Executor),
    CancelJobResult(CancelJobResult),
}

#[derive(Clone, Debug)]
#[expect(clippy::large_enum_variant)]
pub enum Event {
    Key(KeyEvent),
    /// Refresh the data from the scheduler at a given interval.
    /// Reloads the jobs/executors/metrics
    DataReload,
    /// Repaint the UI without refreshing the data.
    /// It is helpful for doing animations
    Repaint,
    /// An event sent after an interaction with the UI,
    /// for example, a Key event that led to an HTTP request.
    DataLoaded {
        data: UiData,
    },
}

#[cfg(not(feature = "web"))]
pub(crate) mod tui {
    use crate::tui::event::Event;
    use crossterm::event::EventStream;
    use futures::{FutureExt, StreamExt};
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    pub struct EventHandler {
        rx: mpsc::UnboundedReceiver<Event>,
    }

    impl EventHandler {
        pub fn new(data_reload_duration: Duration, repaint_duration: Duration) -> Self {
            let (tx, rx) = mpsc::unbounded_channel();

            tokio::spawn(async move {
                let mut reader = EventStream::new();
                let mut data_reload_interval =
                    tokio::time::interval(data_reload_duration);
                let mut repaint_interval = tokio::time::interval(repaint_duration);

                loop {
                    let data_reload_tick = data_reload_interval.tick();
                    let repaint_tick = repaint_interval.tick();
                    let crossterm_event = reader.next().fuse();

                    tokio::select! {
                        _ = data_reload_tick => {
                            if let Err(err) = tx.send(Event::DataReload) {
                                tracing::debug!("Failed to send DataReload event: {err:?}");
                                break;
                            }
                        }
                        _ = repaint_tick => {
                            if let Err(err) = tx.send(Event::Repaint) {
                                tracing::debug!("Failed to send Repaint event: {err:?}");
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
}

#[cfg(feature = "web")]
pub(crate) mod web {
    use crate::tui::TuiResult;
    use crate::tui::event::Event;
    use ratzilla::WebRenderer;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::fmt::Debug;
    use std::rc::Rc;

    #[derive(Debug)]
    pub struct EventHandler {
        events: Rc<RefCell<VecDeque<Event>>>,
    }

    impl EventHandler {
        /// Returns a `(Sender, EventHandler)` pair. The `Sender` can be cloned and distributed
        /// to any code that needs to push events; the `EventHandler` drains the queue each frame.
        pub fn new(
            data_reload_interval_ms: u32,
            repaint_interval_ms: u32,
            terminal: &mut ratatui::Terminal<ratzilla::WebGl2Backend>,
        ) -> (Sender<Event>, Self) {
            let queue = Rc::new(RefCell::new(VecDeque::new()));
            let sender = Sender {
                queue: Rc::clone(&queue),
            };

            tracing::info!(
                "Setting up tick timer: repaint UI every {repaint_interval_ms}ms"
            );
            let repaint_sender = sender.clone();
            gloo_timers::callback::Interval::new(repaint_interval_ms, move || {
                repaint_sender.queue.borrow_mut().push_back(Event::Repaint);
            })
            .forget();

            tracing::info!(
                "Setting up tick timer: refresh data every {data_reload_interval_ms}ms"
            );
            let data_reload_sender = sender.clone();
            gloo_timers::callback::Interval::new(data_reload_interval_ms, move || {
                data_reload_sender
                    .queue
                    .borrow_mut()
                    .push_back(Event::DataReload);
            })
            .forget();

            let key_sender = sender.clone();
            if let Err(err) = terminal.on_key_event(move |key_event| {
                tracing::debug!("on key event: {key_event:?}");
                key_sender
                    .queue
                    .borrow_mut()
                    .push_back(Event::Key(key_event));
            }) {
                tracing::error!("Failed to set up key event listener: {err:?}");
            };

            (sender, Self { events: queue })
        }

        pub fn next(&mut self) -> Option<Event> {
            self.events.borrow_mut().pop_front()
        }
    }

    #[derive(Clone)]
    pub struct Sender<E> {
        queue: Rc<RefCell<VecDeque<E>>>,
    }

    impl Debug for Sender<Event> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Sender")
        }
    }

    impl Sender<Event> {
        pub async fn send(&self, event: Event) -> TuiResult<()> {
            self.queue.borrow_mut().push_back(event);
            Ok(())
        }
    }
}
