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

use crossterm::event::{EventStream, KeyEvent};
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc;

use crate::tui::domain::{ExecutorsData, SchedulerState};

#[derive(Clone, Debug)]
pub enum UiData {
    Dashboard(SchedulerState, Vec<ExecutorsData>),
}

#[derive(Clone, Debug)]
pub enum Event {
    Key(KeyEvent),
    Tick,
    DataLoaded { data: UiData },
}

#[derive(Debug)]
pub struct EventHandler {
    rx: mpsc::UnboundedReceiver<Event>,
}

impl EventHandler {
    pub fn new(tick_rate: std::time::Duration) -> Self {
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
                        match evt {
                            crossterm::event::Event::Key(key) => {
                                if key.kind == crossterm::event::KeyEventKind::Press
                                    && tx.send(Event::Key(key)).is_err()
                                {
                                    break;
                                }
                            }
                            _ => {}
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
