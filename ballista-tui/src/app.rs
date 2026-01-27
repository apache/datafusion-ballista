use crate::event::Event;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;

#[derive(PartialEq)]
pub(crate) enum Views {
    Dashboard,
    Jobs,
    Metrics,
}

pub(crate) struct App {
    pub should_quit: bool,
    pub is_loading: bool,
    pub event_tx: Option<UnboundedSender<Event>>,
    pub last_refresh: Instant,
    pub current_view: Views,

    // Search
    pub search_mode: bool,
    pub search_query: String,

    // Help panel
    pub show_help: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            current_view: Views::Dashboard,
            should_quit: false,
            is_loading: false,
            event_tx: None,
            last_refresh: Instant::now(),
            search_mode: false,
            search_query: String::new(),
            show_help: false,
        }
    }

    fn config_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".graphite")
    }

    pub fn set_event_tx(&mut self, tx: UnboundedSender<Event>) {
        self.event_tx = Some(tx);
        // self.load_data();
    }

    pub fn on_tick(&mut self) {
        // Auto-refresh every 60 seconds (only for day view)
    }

    pub fn on_key(&mut self, key: KeyEvent) {
        // Help panel takes priority
        if self.show_help {
            self.show_help = false;
            return;
        }

        // Search mode input handling
        if self.search_mode {
            match key.code {
                KeyCode::Esc => {
                    self.search_mode = false;
                    self.search_query.clear();
                }
                KeyCode::Enter => {
                    self.search_mode = false;
                    // Keep the filter active
                }
                KeyCode::Backspace => {
                    self.search_query.pop();
                }
                KeyCode::Char(_c) => {
                    //
                }
                _ => {}
            }
            return;
        }

        // Normal mode
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.should_quit = true;
            }
            KeyCode::Char('?') | KeyCode::Char('h') => {
                self.show_help = true;
            }
            KeyCode::Char('/') => {
                self.search_mode = true;
                self.search_query.clear();
            }
            // KeyCode::Up | KeyCode::Char('k') => {
            //     self.previous();
            // }
            // KeyCode::Down | KeyCode::Char('j') => {
            //     self.next();
            // }
            KeyCode::Char('d') => {
                self.current_view = Views::Dashboard;
            }
            KeyCode::Char('j') => {
                self.current_view = Views::Jobs;
            }
            KeyCode::Char('m') => {
                self.current_view = Views::Metrics;
            }
            // Clear search filter
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.search_query.clear();
            }
            _ => {}
        }
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}
