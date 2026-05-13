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
    pub last_seen: Option<u64>,
    pub specification: Specification,
    pub metrics: Vec<Metric>,
}

impl Executor {
    pub fn task_slots(&self) -> u32 {
        self.specification.task_slots
    }

    pub fn proc_physical_memory_usage(&self) -> u64 {
        self.metrics
            .iter()
            .find(|m| m.typ == "proc_physical_memory")
            .map(|m| m.value)
            .unwrap_or(0)
    }

    pub fn peak_physical_memory_usage(&self) -> u64 {
        self.metrics
            .iter()
            .find(|m| m.typ == "peak_physical_memory")
            .map(|m| m.value)
            .unwrap_or(0)
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct ExecutorDetails {
    pub executor_info: Executor,
    pub os_info: OsInfo,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Metric {
    #[serde(rename = "type")]
    pub typ: String,
    pub value: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Specification {
    pub task_slots: u32,
}

#[derive(Deserialize, Clone, Debug)]
pub struct OsInfo {
    pub kernel_ver: String,
    pub num_disks: u32,
    pub open_files_limit: u64,
    pub os_ver: String,
    pub os_ver_long: String,
    pub physical_cores: u32,
    pub system_name: String,
    pub total_available_disk_space: u64,
    pub total_disk_space: u64,
}

pub struct ExecutorDetailsPopup {
    pub executor: ExecutorDetails,
    pub scroll_position: u16,
}

impl ExecutorDetailsPopup {
    pub fn new(executor: ExecutorDetails) -> Self {
        Self {
            executor,
            scroll_position: 0,
        }
    }

    pub fn scroll_up(&mut self) {
        self.scroll_position = self.scroll_position.saturating_sub(1);
    }

    pub fn scroll_down(&mut self) {
        const MAX_SCROLL_POSITION: u16 = 10;
        self.scroll_position = self
            .scroll_position
            .saturating_add(1)
            .min(MAX_SCROLL_POSITION);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortColumn {
    None,
    Host,
    Id,
    TaskSlots,
    ProcPhysicalMemoryUsage,
    PeakPhysicalMemoryUsage,
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
            SortColumn::TaskSlots => self.executors.sort_by(|a, b| {
                let cmp = a.task_slots().cmp(&b.task_slots());
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::ProcPhysicalMemoryUsage => self.executors.sort_by(|a, b| {
                let cmp = a
                    .proc_physical_memory_usage()
                    .cmp(&b.proc_physical_memory_usage());
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::PeakPhysicalMemoryUsage => self.executors.sort_by(|a, b| {
                let cmp = a
                    .peak_physical_memory_usage()
                    .cmp(&b.peak_physical_memory_usage());
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

    pub fn selected_executor(&self) -> Option<&Executor> {
        self.table_state
            .selected()
            .and_then(|i| self.executors.get(i))
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

    fn make_executor(
        host: &str,
        port: u16,
        id: &str,
        task_slots: u32,
        proc_physical_memory_usage: u64,
        peak_physical_memory_usage: u64,
        last_seen: Option<u64>,
    ) -> Executor {
        Executor {
            host: host.to_string(),
            port,
            id: id.to_string(),
            last_seen,
            specification: Specification { task_slots },
            metrics: vec![
                Metric {
                    typ: "proc_physical_memory".to_string(),
                    value: proc_physical_memory_usage,
                },
                Metric {
                    typ: "peak_physical_memory".to_string(),
                    value: peak_physical_memory_usage,
                },
            ],
        }
    }

    fn make_executor_details(id: &str) -> ExecutorDetails {
        ExecutorDetails {
            executor_info: make_executor("host", 8080, id, 1, 0, 0, Some(0)),
            os_info: OsInfo {
                kernel_ver: "5.15".to_string(),
                num_disks: 1,
                open_files_limit: 1024,
                os_ver: "Ubuntu 22.04".to_string(),
                os_ver_long: "Ubuntu 22.04.1 LTS".to_string(),
                physical_cores: 4,
                system_name: "Linux".to_string(),
                total_available_disk_space: 50_000_000_000,
                total_disk_space: 100_000_000_000,
            },
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
                make_executor("z-host", 8080, "id-c", 1, 0, 0, Some(300)),
                make_executor("a-host", 8080, "id-a", 1, 0, 0, Some(100)),
                make_executor("m-host", 8080, "id-b", 1, 0, 0, Some(200)),
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
                make_executor("z-host", 8080, "id-c", 1, 0, 0, Some(300)),
                make_executor("a-host", 8080, "id-a", 1, 0, 0, Some(100)),
                make_executor("m-host", 8080, "id-b", 1, 0, 0, Some(200)),
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
                make_executor("a-host", 8080, "id-a", 1, 0, 0, Some(100)),
                make_executor("z-host", 8080, "id-c", 1, 0, 0, Some(300)),
                make_executor("m-host", 8080, "id-b", 1, 0, 0, Some(200)),
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
                make_executor("host", 9000, "id-b", 1, 0, 0, Some(200)),
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(100)),
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
                make_executor("host", 8080, "id-c", 1, 0, 0, Some(300)),
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(100)),
                make_executor("host", 8080, "id-b", 1, 0, 0, Some(200)),
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
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(100)),
                make_executor("host", 8080, "id-c", 1, 0, 0, Some(300)),
                make_executor("host", 8080, "id-b", 1, 0, 0, Some(200)),
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
    fn sort_by_task_slots_ascending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 2, 0, 0, Some(100)),
                make_executor("host", 8080, "id-b", 3, 0, 0, Some(200)),
                make_executor("host", 8080, "id-c", 1, 0, 0, Some(300)),
            ],
            SortColumn::TaskSlots,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].id, "id-c");
        assert_eq!(data.executors[0].task_slots(), 1);
        assert_eq!(data.executors[1].id, "id-a");
        assert_eq!(data.executors[1].task_slots(), 2);
        assert_eq!(data.executors[2].id, "id-b");
        assert_eq!(data.executors[2].task_slots(), 3);
    }

    #[test]
    fn sort_by_task_slots_descending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(100)),
                make_executor("host", 8080, "id-c", 2, 0, 0, Some(300)),
                make_executor("host", 8080, "id-b", 3, 0, 0, Some(200)),
            ],
            SortColumn::TaskSlots,
            SortOrder::Descending,
        );
        data.sort();
        assert_eq!(data.executors[0].id, "id-b");
        assert_eq!(data.executors[0].task_slots(), 3);
        assert_eq!(data.executors[1].id, "id-c");
        assert_eq!(data.executors[1].task_slots(), 2);
        assert_eq!(data.executors[2].id, "id-a");
        assert_eq!(data.executors[2].task_slots(), 1);
    }

    #[test]
    fn sort_by_proc_physical_memory_ascending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-c", 1, 3, 0, Some(300)),
                make_executor("host", 8080, "id-a", 1, 1, 0, Some(100)),
                make_executor("host", 8080, "id-b", 1, 2, 0, Some(200)),
                make_executor("host", 8080, "id-d", 1, 0, 0, None),
            ],
            SortColumn::ProcPhysicalMemoryUsage,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].proc_physical_memory_usage(), 0);
        assert_eq!(data.executors[1].proc_physical_memory_usage(), 1);
        assert_eq!(data.executors[2].proc_physical_memory_usage(), 2);
        assert_eq!(data.executors[3].proc_physical_memory_usage(), 3);
    }

    #[test]
    fn sort_by_proc_physical_memory_descending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1, 1, 0, Some(100)),
                make_executor("host", 8080, "id-c", 1, 3, 0, Some(300)),
                make_executor("host", 8080, "id-b", 1, 2, 0, Some(200)),
                make_executor("host", 8080, "id-d", 1, 0, 0, None),
            ],
            SortColumn::ProcPhysicalMemoryUsage,
            SortOrder::Descending,
        );
        data.sort();
        assert_eq!(data.executors[0].proc_physical_memory_usage(), 3);
        assert_eq!(data.executors[1].proc_physical_memory_usage(), 2);
        assert_eq!(data.executors[2].proc_physical_memory_usage(), 1);
        assert_eq!(data.executors[3].proc_physical_memory_usage(), 0);
    }

    #[test]
    fn sort_by_peak_physical_memory_ascending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-c", 1, 0, 3, Some(300)),
                make_executor("host", 8080, "id-a", 1, 0, 1, Some(100)),
                make_executor("host", 8080, "id-b", 1, 0, 2, Some(200)),
                make_executor("host", 8080, "id-d", 1, 0, 0, None),
            ],
            SortColumn::PeakPhysicalMemoryUsage,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].peak_physical_memory_usage(), 0);
        assert_eq!(data.executors[1].peak_physical_memory_usage(), 1);
        assert_eq!(data.executors[2].peak_physical_memory_usage(), 2);
        assert_eq!(data.executors[3].peak_physical_memory_usage(), 3);
    }

    #[test]
    fn sort_by_peak_physical_memory_descending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1, 0, 1, Some(100)),
                make_executor("host", 8080, "id-c", 1, 0, 3, Some(300)),
                make_executor("host", 8080, "id-b", 1, 0, 2, Some(200)),
                make_executor("host", 8080, "id-d", 1, 0, 0, None),
            ],
            SortColumn::PeakPhysicalMemoryUsage,
            SortOrder::Descending,
        );
        data.sort();
        assert_eq!(data.executors[0].peak_physical_memory_usage(), 3);
        assert_eq!(data.executors[1].peak_physical_memory_usage(), 2);
        assert_eq!(data.executors[2].peak_physical_memory_usage(), 1);
        assert_eq!(data.executors[3].peak_physical_memory_usage(), 0);
    }

    #[test]
    fn sort_by_last_seen_ascending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-c", 1, 0, 0, Some(300)),
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(100)),
                make_executor("host", 8080, "id-b", 1, 0, 0, Some(200)),
                make_executor("host", 8080, "id-d", 1, 0, 0, None),
            ],
            SortColumn::LastSeen,
            SortOrder::Ascending,
        );
        data.sort();
        assert_eq!(data.executors[0].last_seen, None);
        assert_eq!(data.executors[1].last_seen, Some(100));
        assert_eq!(data.executors[2].last_seen, Some(200));
        assert_eq!(data.executors[3].last_seen, Some(300));
    }

    #[test]
    fn sort_by_last_seen_descending() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(100)),
                make_executor("host", 8080, "id-c", 1, 0, 0, Some(300)),
                make_executor("host", 8080, "id-b", 1, 0, 0, Some(200)),
                make_executor("host", 8080, "id-d", 1, 0, 0, None),
            ],
            SortColumn::LastSeen,
            SortOrder::Descending,
        );
        data.sort();
        assert_eq!(data.executors[0].last_seen, Some(300));
        assert_eq!(data.executors[1].last_seen, Some(200));
        assert_eq!(data.executors[2].last_seen, Some(100));
        assert_eq!(data.executors[3].last_seen, None);
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
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(1)),
                make_executor("host", 8081, "id-b", 1, 0, 0, Some(2)),
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
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(1)),
                make_executor("host", 8081, "id-b", 1, 0, 0, Some(2)),
                make_executor("host", 8082, "id-c", 1, 0, 0, Some(3)),
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
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(1)),
                make_executor("host", 8081, "id-b", 1, 0, 0, Some(2)),
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
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(1)),
                make_executor("host", 8081, "id-b", 1, 0, 0, Some(2)),
                make_executor("host", 8082, "id-c", 1, 0, 0, Some(3)),
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
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(1)),
                make_executor("host", 8081, "id-b", 1, 0, 0, Some(2)),
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
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(1)),
                make_executor("host", 8081, "id-b", 1, 0, 0, Some(2)),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.table_state.select(Some(0));
        data.scroll_up();
        assert_eq!(data.table_state.selected(), None);
    }

    // --- selected_executor tests ---

    #[test]
    fn selected_executor_none_when_no_selection() {
        let data = make_executors_data(
            vec![make_executor("host", 8080, "id-a", 1, 0, 0, Some(1))],
            SortColumn::None,
            SortOrder::Ascending,
        );
        assert!(data.selected_executor().is_none());
    }

    #[test]
    fn selected_executor_returns_correct_executor() {
        let mut data = make_executors_data(
            vec![
                make_executor("host", 8080, "id-a", 1, 0, 0, Some(1)),
                make_executor("host", 8081, "id-b", 1, 0, 0, Some(2)),
            ],
            SortColumn::None,
            SortOrder::Ascending,
        );
        data.table_state.select(Some(1));
        assert_eq!(data.selected_executor().unwrap().id, "id-b");
    }

    // --- ExecutorDetailsPopup tests ---

    #[test]
    fn executor_details_popup_new_scroll_position_is_zero() {
        let popup = ExecutorDetailsPopup::new(make_executor_details("id-1"));
        assert_eq!(popup.scroll_position, 0);
    }

    #[test]
    fn executor_details_popup_scroll_down_increments() {
        let mut popup = ExecutorDetailsPopup::new(make_executor_details("id-1"));
        popup.scroll_down();
        assert_eq!(popup.scroll_position, 1);
    }

    #[test]
    fn executor_details_popup_scroll_down_multiple_times() {
        let mut popup = ExecutorDetailsPopup::new(make_executor_details("id-1"));
        popup.scroll_down();
        popup.scroll_down();
        popup.scroll_down();
        assert_eq!(popup.scroll_position, 3);
    }

    #[test]
    fn executor_details_popup_scroll_up_decrements() {
        let mut popup = ExecutorDetailsPopup::new(make_executor_details("id-1"));
        popup.scroll_down();
        popup.scroll_down();
        popup.scroll_up();
        assert_eq!(popup.scroll_position, 1);
    }

    #[test]
    fn executor_details_popup_scroll_up_saturates_at_zero() {
        let mut popup = ExecutorDetailsPopup::new(make_executor_details("id-1"));
        popup.scroll_up();
        popup.scroll_up();
        assert_eq!(popup.scroll_position, 0);
    }
}
