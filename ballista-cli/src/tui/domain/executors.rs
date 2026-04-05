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

use crate::tui::domain::{SortOrder, jobs::Job};
use ratatui::widgets::{ScrollbarState, TableState};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Executor {
    pub host: String,
    pub port: u16,
    pub id: String,
    pub last_seen: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortColumn {
    None,
    Host,
    Id,
    LastSeen,
}

#[derive(Clone, Debug)]
pub struct ExecutorsData {
    pub executors: Vec<Executor>,
    pub table_state: TableState,
    pub scrollbar_state: ScrollbarState,
    pub sort_column: SortColumn,
    pub sort_order: SortOrder,
    pub scheduler_state: Option<super::SchedulerState>,
    pub jobs: Vec<Job>,
}

impl ExecutorsData {
    pub fn new() -> Self {
        Self {
            executors: Vec::new(),
            table_state: TableState::default(),
            scrollbar_state: ScrollbarState::new(0).position(0),
            sort_column: SortColumn::None,
            sort_order: SortOrder::Ascending,
            scheduler_state: None,
            jobs: Vec::new(),
        }
    }

    pub fn sort(&mut self) {
        match self.sort_column {
            SortColumn::Host => self.executors.sort_by(|a, b| {
                let a_host = format!("{}:{}", a.host, a.port);
                let b_host = format!("{}:{}", b.host, b.port);
                let cmp = a_host.cmp(&b_host);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::Id => self.executors.sort_by(|a, b| {
                let cmp = a.id.cmp(&b.id);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::LastSeen => self.executors.sort_by(|a, b| {
                let cmp = a.last_seen.cmp(&b.last_seen);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::None => {}
        }
    }

    fn get_selected_executor_index(&self) -> Option<usize> {
        self.table_state.selected()
    }

    pub fn scroll_down(&mut self) {
        if self.executors.is_empty() {
            self.table_state.select(None);
            return;
        }

        if let Some(selected) = self.get_selected_executor_index() {
            if selected < self.executors.len() - 1 {
                self.table_state.select(Some(selected + 1));
            } else {
                self.table_state.select(None);
            }
        } else {
            self.table_state.select(Some(0));
        }
        self.scrollbar_state = self
            .scrollbar_state
            .position(self.get_selected_executor_index().unwrap_or(0));
    }

    pub fn scroll_up(&mut self) {
        if self.executors.is_empty() {
            self.table_state.select(None);
            return;
        }

        if let Some(selected) = self.get_selected_executor_index() {
            if selected == 0 {
                self.table_state.select(None);
            } else {
                self.table_state.select(Some(selected - 1));
            }
        } else {
            self.table_state.select(Some(self.executors.len() - 1));
        }
        self.scrollbar_state = self
            .scrollbar_state
            .position(self.get_selected_executor_index().unwrap_or(0));
    }
}
