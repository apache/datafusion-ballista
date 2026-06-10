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

use ratatui::prelude::Style;
use serde::Deserialize;

/// Selects a built-in colour preset.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ThemeName {
    #[default]
    Dark,
    Light,
}

// /// A colour specification usable in the user's config file.
// /// Supports named colours (`"Red"`, `"LightBlue"`, …), 256-colour indices
// /// (`108`), and 24-bit RGB (`{r: 180, g: 100, b: 0}`).
// #[derive(Debug, Deserialize, Clone)]
// #[serde(untagged)]
// pub enum ColorSpec {
//     Named(String),
//     Indexed(u8),
//     Rgb { r: u8, g: u8, b: u8 },
// }

/// Optional per-role fg-colour overrides applied on top of the chosen preset.
/// Each absent field keeps the preset's value.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ThemeOverride {
    pub status_running: Option<Style>,
    pub status_queued: Option<Style>,
    pub status_failed: Option<Style>,
    pub status_completed: Option<Style>,
    pub status_unknown: Option<Style>,
    pub table_header: Option<Style>,
    pub row_even: Option<Style>,
    pub row_odd: Option<Style>,
    pub row_selected: Option<Style>,
    pub popup_border: Option<Style>,
    pub popup_border_alt: Option<Style>,
    pub popup_border_jobs_stages: Option<Style>,
    pub nav_active: Option<Style>,
    pub nav_inactive: Option<Style>,
    pub search_active: Option<Style>,
    pub search_inactive: Option<Style>,
    pub search_cursor: Option<Style>,
    pub help_header: Option<Style>,
    pub help_section: Option<Style>,
    pub help_item: Option<Style>,
    pub help_item_dim: Option<Style>,
    pub detail_label: Option<Style>,
    pub graph_border: Option<Style>,
    pub graph_label: Option<Style>,
    pub graph_stage: Option<Style>,
    pub graph_arrow: Option<Style>,
    pub tile_running: Option<Style>,
    pub tile_queued: Option<Style>,
    pub tile_completed: Option<Style>,
    pub tile_failed: Option<Style>,
    pub scheduler_down: Option<Style>,
    pub cancel_success: Option<Style>,
    pub cancel_not_done: Option<Style>,
    pub cancel_failure: Option<Style>,
    pub banner: Option<Style>,
    pub feature_enabled: Option<Style>,
    pub feature_disabled: Option<Style>,
    pub text_error: Option<Style>,
}
