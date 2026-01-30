use crate::{domain::DashboardData, event::Event, infrastructure::Settings};
use color_eyre::eyre::{Ok, Result};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::{sync::Arc, time::Instant};
use tokio::sync::mpsc::UnboundedSender;

use crate::http_client::HttpClient;

#[derive(PartialEq)]
pub enum Views {
    Dashboard,
    Jobs,
    Metrics,
}

pub struct App {
    pub should_quit: bool,
    pub event_tx: Option<UnboundedSender<Event>>,
    pub last_refresh: Instant,
    pub current_view: Views,

    pub dashboard_data: DashboardData,

    // Search
    pub search_mode: bool,
    pub search_query: String,

    // Help panel
    pub show_help: bool,

    pub http_client: Arc<HttpClient>,
}

impl App {
    pub fn new(config: Settings) -> Result<Self> {
        Ok(Self {
            current_view: Views::Dashboard,
            should_quit: false,
            event_tx: None,
            last_refresh: Instant::now(),
            search_mode: false,
            search_query: String::new(),
            show_help: false,
            dashboard_data: DashboardData::builder().build(),
            http_client: Arc::new(HttpClient::new(config)?),
        })
    }

    pub fn set_event_tx(&mut self, tx: UnboundedSender<Event>) {
        self.event_tx = Some(tx);
        // self.load_data();
    }

    pub async fn on_tick(&mut self) {
        let _ = crate::ui::load_dashboard_data(&self).await;
    }

    pub async fn on_key(&mut self, key: KeyEvent) -> Result<()> {
        // Help panel takes priority
        if self.show_help {
            self.show_help = false;
            return Ok(());
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
            return Ok(());
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
                let _ = crate::ui::load_dashboard_data(self).await;
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
        Ok(())
    }
}
