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

pub mod executors;
pub mod jobs;
pub mod metrics;
#[cfg(test)]
pub(crate) mod test_util;

use ratatui::widgets::{ScrollbarState, TableState};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct SchedulerState {
    pub started: i64,
    pub version: String,
    pub datafusion_version: String,
    pub substrait_support: bool,
    pub keda_support: bool,
    pub prometheus_support: bool,
    pub graphviz_support: bool,
    pub scheduling_policy: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

pub(super) fn scroll_down(
    table_state: &mut TableState,
    scrollbar_state: &mut ScrollbarState,
    len: usize,
) {
    if len == 0 {
        table_state.select(None);
        return;
    }

    if let Some(selected) = table_state.selected() {
        if selected < len - 1 {
            table_state.select(Some(selected + 1));
        } else {
            table_state.select(None);
        }
    } else {
        table_state.select(Some(0));
    }

    *scrollbar_state = (*scrollbar_state).position(table_state.selected().unwrap_or(0));
}

pub(super) fn scroll_up(
    table_state: &mut TableState,
    scrollbar_state: &mut ScrollbarState,
    len: usize,
) {
    if len == 0 {
        table_state.select(None);
        return;
    }

    if let Some(selected) = table_state.selected() {
        if selected == 0 {
            table_state.select(None);
        } else {
            table_state.select(Some(selected - 1));
        }
    } else {
        table_state.select(Some(len - 1));
    }

    *scrollbar_state = (*scrollbar_state).position(table_state.selected().unwrap_or(0));
}

#[cfg(test)]
mod tests {
    use super::{ScrollbarState, TableState, scroll_down, scroll_up};

    // --- scroll_down tests ---
    #[test]
    fn scroll_down_empty_list_stays_none() {
        let row_count = 0;
        let mut table_state = TableState::default();
        let mut scrollbar_state = ScrollbarState::new(row_count);

        scroll_down(&mut table_state, &mut scrollbar_state, row_count);
        assert_eq!(table_state.selected(), None);
    }

    #[test]
    fn scroll_down_with_no_selection_selects_first() {
        let row_count = 2;
        let mut table_state = TableState::default();
        let mut scrollbar_state = ScrollbarState::new(row_count);

        scroll_down(&mut table_state, &mut scrollbar_state, row_count);
        assert_eq!(table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_down_advances_selection() {
        let row_count = 3;
        let mut table_state = TableState::default();
        let mut scrollbar_state = ScrollbarState::new(row_count);
        table_state.select(Some(0));

        scroll_down(&mut table_state, &mut scrollbar_state, row_count);

        assert_eq!(table_state.selected(), Some(1));
    }

    #[test]
    fn scroll_down_at_last_item_deselects() {
        let row_count = 2;
        let mut table_state = TableState::default();
        let mut scrollbar_state = ScrollbarState::new(row_count);
        table_state.select(Some(row_count - 1));

        scroll_down(&mut table_state, &mut scrollbar_state, row_count);

        assert_eq!(table_state.selected(), None);
    }

    // --- scroll_up tests ---
    #[test]
    fn scroll_up_empty_list_stays_none() {
        let row_count = 0;
        let mut table_state = TableState::default();
        let mut scrollbar_state = ScrollbarState::new(row_count);

        scroll_up(&mut table_state, &mut scrollbar_state, row_count);

        assert_eq!(table_state.selected(), None);
    }

    #[test]
    fn scroll_up_with_no_selection_selects_last() {
        let row_count = 3;
        let mut table_state = TableState::default();
        let mut scrollbar_state = ScrollbarState::new(row_count);

        scroll_up(&mut table_state, &mut scrollbar_state, row_count);

        assert_eq!(table_state.selected(), Some(row_count - 1));
    }

    #[test]
    fn scroll_up_moves_selection_back() {
        let row_count = 2;
        let mut table_state = TableState::default();
        let mut scrollbar_state = ScrollbarState::new(row_count);
        table_state.select(Some(1));

        scroll_up(&mut table_state, &mut scrollbar_state, row_count);

        assert_eq!(table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_up_at_first_item_deselects() {
        let row_count = 2;
        let mut table_state = TableState::default();
        let mut scrollbar_state = ScrollbarState::new(row_count);
        table_state.select(Some(0));

        scroll_up(&mut table_state, &mut scrollbar_state, row_count);

        assert_eq!(table_state.selected(), None);
    }
}
