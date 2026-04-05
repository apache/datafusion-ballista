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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_executor(host: &str, port: u16, id: &str, last_seen: i64) -> Executor {
        Executor {
            host: host.to_string(),
            port,
            id: id.to_string(),
            last_seen,
        }
    }

    fn make_executors_data(
        executors: Vec<Executor>,
        sort_column: SortColumn,
        sort_order: SortOrder,
    ) -> ExecutorsData {
        ExecutorsData {
            executors,
            sort_column,
            sort_order,
            ..ExecutorsData::new()
        }
    }

    // --- sort tests ---

    #[test]
    fn sort_by_none_preserves_order() {
        let mut data = make_executors_data(
            vec![
                make_executor("z-host", 8080, "id-c", 300),
                make_executor("a-host", 8080, "id-a", 100),
                make_executor("m-host", 8080, "id-b", 200),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].id, "id-c");
        assert_eq!(data.executors[1].id, "id-a");
        assert_eq!(data.executors[2].id, "id-b");
    }

    #[test]
    fn sort_by_host_ascending() {
        let mut data = make_executors_data(
            vec![
                make_executor("z-host", 8080, "id-c", 300),
                make_executor("a-host", 8080, "id-a", 100),
                make_executor("m-host", 8080, "id-b", 200),
            ],
            SortColumn::Host,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].host, "a-host");
        assert_eq!(data.executors[1].host, "m-host");
        assert_eq!(data.executors[2].host, "z-host");
    }

    #[test]
    fn sort_by_host_descending() {
        let mut data = make_executors_data(
            vec![
                make_executor("a-host", 8080, "id-a", 100),
                make_executor("z-host", 8080, "id-c", 300),
                make_executor("m-host", 8080, "id-b", 200),
            ],
            SortColumn::Host,
            SortOrder::Descending,
        );
        data.sort();
        assert_eq!(data.executors[0].host, "z-host");
        assert_eq!(data.executors[1].host, "m-host");
        assert_eq!(data.executors[2].host, "a-host");
    }

    #[test]
    fn sort_by_host_uses_port_in_comparison() {
        // Two executors with same host but different ports — sorted as "host:port"
        let mut data = make_executors_data(
            vec![
                make_executor("host", 9000, "id-b", 200),
                make_executor("host", 8080, "id-a", 100),
            ],
            SortColumn::Host,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].port, 8080);
        assert_eq!(data.executors[1].port, 9000);
    }

    #[test]
    fn sort_by_id_ascending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-c", 300),
                make_executor("host", 8080, "id-a", 100),
                make_executor("host", 8080, "id-b", 200),
            ],
            SortColumn::Id,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].id, "id-a");
        assert_eq!(data.executors[1].id, "id-b");
        assert_eq!(data.executors[2].id, "id-c");
    }

    #[test]
    fn sort_by_id_descending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 100),
                make_executor("host", 8080, "id-c", 300),
                make_executor("host", 8080, "id-b", 200),
            ],
            SortColumn::Id,
            SortOrder::Descending,
        );
        data.sort();
        assert_eq!(data.executors[0].id, "id-c");
        assert_eq!(data.executors[1].id, "id-b");
        assert_eq!(data.executors[2].id, "id-a");
    }

    #[test]
    fn sort_by_last_seen_ascending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-c", 300),
                make_executor("host", 8080, "id-a", 100),
                make_executor("host", 8080, "id-b", 200),
            ],
            SortColumn::LastSeen,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].last_seen, 100);
        assert_eq!(data.executors[1].last_seen, 200);
        assert_eq!(data.executors[2].last_seen, 300);
    }

    #[test]
    fn sort_by_last_seen_descending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 100),
                make_executor("host", 8080, "id-c", 300),
                make_executor("host", 8080, "id-b", 200),
            ],
            SortColumn::LastSeen,
            SortOrder::Descending,
        );
        data.sort();
        assert_eq!(data.executors[0].last_seen, 300);
        assert_eq!(data.executors[1].last_seen, 200);
        assert_eq!(data.executors[2].last_seen, 100);
    }

    // --- scroll_down tests ---

    #[test]
    fn scroll_down_empty_list_stays_none() {
        let mut data = ExecutorsData::new();
        data.scroll_down();
        assert_eq!(data.table_state.selected(), None);
    }

    #[test]
    fn scroll_down_with_no_selection_selects_first() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1),
                make_executor("host", 8081, "id-b", 2),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.scroll_down();
        assert_eq!(data.table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_down_advances_selection() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1),
                make_executor("host", 8081, "id-b", 2),
                make_executor("host", 8082, "id-c", 3),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.table_state.select(Some(0));
        data.scroll_down();
        assert_eq!(data.table_state.selected(), Some(1));
    }

    #[test]
    fn scroll_down_at_last_item_deselects() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1),
                make_executor("host", 8081, "id-b", 2),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.table_state.select(Some(1));
        data.scroll_down();
        assert_eq!(data.table_state.selected(), None);
    }

    // --- scroll_up tests ---

    #[test]
    fn scroll_up_empty_list_stays_none() {
        let mut data = ExecutorsData::new();
        data.scroll_up();
        assert_eq!(data.table_state.selected(), None);
    }

    #[test]
    fn scroll_up_with_no_selection_selects_last() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1),
                make_executor("host", 8081, "id-b", 2),
                make_executor("host", 8082, "id-c", 3),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.scroll_up();
        assert_eq!(data.table_state.selected(), Some(2));
    }

    #[test]
    fn scroll_up_moves_selection_back() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1),
                make_executor("host", 8081, "id-b", 2),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.table_state.select(Some(1));
        data.scroll_up();
        assert_eq!(data.table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_up_at_first_item_deselects() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1),
                make_executor("host", 8081, "id-b", 2),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.table_state.select(Some(0));
        data.scroll_up();
        assert_eq!(data.table_state.selected(), None);
    }
}
