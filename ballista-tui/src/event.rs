use crossterm::event::{EventStream, KeyEvent};
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc;

use crate::domain::SchedulerState;

#[derive(Clone, Debug)]
pub enum UiData {
    SchedulerState(SchedulerState),
}

#[derive(Clone, Debug)]
pub enum Event {
    Key(KeyEvent),
    Tick,
    Resize(u16, u16),
    DataLoaded { data: UiData },
}

#[derive(Debug)]
pub struct EventHandler {
    rx: mpsc::UnboundedReceiver<Event>,
}

impl EventHandler {
    pub fn new(tick_rate: std::time::Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let _tx = tx.clone();

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
                            crossterm::event::Event::Resize(x, y) => {
                                if tx.send(Event::Resize(x, y)).is_err() {
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
