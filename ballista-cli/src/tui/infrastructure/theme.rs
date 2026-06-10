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

use crate::tui::infrastructure::ThemeSettings;
use ratatui::prelude::{Color, Style};
use serde::Deserialize;

/// Selects a built-in color preset.
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub(super) enum ThemeName {
    #[default]
    Dark,
    Light,
}

/// Optional per-role fg-color overrides applied on top of the chosen preset.
/// Each absent field keeps the preset's value.
#[derive(Debug, Deserialize, Default, Clone)]
pub(super) struct ThemeOverride {
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

/// All visual styles used throughout the TUI, keyed by semantic role.
/// Build one of the built-in presets with [`Theme::dark`] or [`Theme::light`],
/// or call [`Theme::from_settings`] to resolve user config (including overrides).
#[derive(Debug, Clone)]
pub struct Theme {
    // ── Job / stage / task status ─────────────────────────────────────────
    pub status_running: Style,
    pub status_queued: Style,
    pub status_failed: Style,
    pub status_completed: Style,
    pub status_unknown: Style,

    // ── Table chrome ──────────────────────────────────────────────────────
    pub table_header: Style,
    pub row_even: Style,
    pub row_odd: Style,
    pub row_selected: Style,

    // ── Popup borders ─────────────────────────────────────────────────────
    /// Default popup border (most dialogs).
    pub popup_border: Style,
    /// Alternate popup border — stage tasks popup.
    pub popup_border_alt: Style,
    /// Job-stages popup border.
    pub popup_border_jobs_stages: Style,

    // ── Navigation ────────────────────────────────────────────────────────
    pub nav_active: Style,
    pub nav_inactive: Style,

    // ── Search box ────────────────────────────────────────────────────────
    pub search_active: Style,
    pub search_inactive: Style,
    pub search_cursor: Style,

    // ── Help overlay ──────────────────────────────────────────────────────
    pub help_header: Style,
    pub help_section: Style,
    pub help_item: Style,
    pub help_item_dim: Style,

    // ── Executor details ──────────────────────────────────────────────────
    pub detail_label: Style,

    // ── DOT / stages graph ────────────────────────────────────────────────
    pub graph_border: Style,
    pub graph_label: Style,
    pub graph_stage: Style,
    pub graph_arrow: Style,

    // ── Executor-view job-count tiles ─────────────────────────────────────
    pub tile_running: Style,
    pub tile_queued: Style,
    pub tile_completed: Style,
    pub tile_failed: Style,

    // ── Miscellaneous indicators ──────────────────────────────────────────
    pub scheduler_down: Style,
    pub cancel_success: Style,
    pub cancel_not_done: Style,
    pub cancel_failure: Style,
    pub feature_enabled: Style,
    pub feature_disabled: Style,

    // ── Footer ────────────────────────────────────────────────────────────
    pub footer: Style,

    // ── Misc ──────────────────────────────────────────────────────────────
    pub banner: Style,
    pub text_error: Style,
    pub text_info: Style,
    pub app_background: Style,
}

impl Theme {
    /// Dark theme — matches the original hardcoded color palette.
    pub fn dark() -> Self {
        Self {
            status_running: Style::default().fg(Color::LightBlue).bold(),
            status_queued: Style::default().fg(Color::LightMagenta).bold(),
            status_failed: Style::default().fg(Color::LightRed).bold(),
            status_completed: Style::default().fg(Color::LightGreen).bold(),
            status_unknown: Style::default().fg(Color::Gray).bold(),

            table_header: Style::default().fg(Color::LightYellow).bold(),
            row_even: Style::default().bg(Color::DarkGray),
            row_odd: Style::default().bg(Color::Black),
            row_selected: Style::default().bg(Color::Indexed(29)),

            popup_border: Style::default().fg(Color::LightCyan).bold(),
            popup_border_alt: Style::default().fg(Color::Indexed(193)).bold(),
            popup_border_jobs_stages: Style::default().fg(Color::LightBlue).bold(),

            nav_active: Style::default().fg(Color::White).bold(),
            nav_inactive: Style::default().dim(),

            search_active: Style::default().fg(Color::Yellow),
            search_inactive: Style::default().dim(),
            search_cursor: Style::default().fg(Color::Yellow).bold(),

            help_header: Style::default().fg(Color::Cyan).bold(),
            help_section: Style::default().fg(Color::Yellow),
            help_item: Style::default(),
            help_item_dim: Style::default().dim(),

            detail_label: Style::default().fg(Color::Yellow),

            graph_border: Style::default().fg(Color::Cyan),
            graph_label: Style::default().fg(Color::White),
            graph_stage: Style::default().fg(Color::Yellow).bold(),
            graph_arrow: Style::default().fg(Color::Green),

            tile_running: Style::default().fg(Color::LightBlue),
            tile_queued: Style::default().fg(Color::Magenta),
            tile_completed: Style::default().fg(Color::Green),
            tile_failed: Style::default().fg(Color::Red),

            scheduler_down: Style::default().fg(Color::Red).bold(),

            cancel_success: Style::default().fg(Color::Green),
            cancel_not_done: Style::default().fg(Color::Yellow),
            cancel_failure: Style::default().fg(Color::Red),

            banner: Style::default().fg(Color::Yellow),

            feature_enabled: Style::default().fg(Color::Green),
            feature_disabled: Style::default().fg(Color::Red),

            text_info: Style::default().bold(),
            text_error: Style::default().fg(Color::Red).bold(),
            app_background: Style::default().bg(Color::Black),

            footer: Style::default().bold(),
        }
    }

    /// Light theme — designed for terminals with a light/white background.
    pub fn light() -> Self {
        Self {
            // Deeper saturated colors instead of "Light*" variants, which wash
            // out on white backgrounds.
            status_running: Style::default().fg(Color::Rgb(0, 0, 139)).bold(),
            status_queued: Style::default().fg(Color::Rgb(139, 0, 139)).bold(),
            status_failed: Style::default().fg(Color::Rgb(139, 0, 0)).bold(),
            status_completed: Style::default().fg(Color::Rgb(0, 139, 0)).bold(),
            status_unknown: Style::default().fg(Color::DarkGray).bold(),

            // Dark text on pale-yellow header; near-white and terminal-default row
            // backgrounds so the table looks at home on white terminals.
            table_header: Style::default().fg(Color::Black).bold(),
            row_even: Style::default().bg(Color::Indexed(255)), // #eeeeee near-white
            row_odd: Style::default().bg(Color::Reset),
            row_selected: Style::default().bg(Color::Indexed(108)), // muted green

            popup_border: Style::default().fg(Color::Indexed(30)), // dark teal
            popup_border_alt: Style::default()
                .fg(Color::Indexed(136)) // dark gold
                .bold(),
            popup_border_jobs_stages: Style::default().fg(Color::Blue).bold(),

            nav_active: Style::default().fg(Color::Black).bold(),
            nav_inactive: Style::default().fg(Color::Indexed(244)), // medium gray

            search_active: Style::default().fg(Color::Indexed(166)), // dark orange
            search_inactive: Style::default().dim(),
            search_cursor: Style::default().fg(Color::Indexed(166)).bold(),

            help_header: Style::default()
                .fg(Color::Indexed(24)) // dark blue
                .bold(),
            help_section: Style::default().fg(Color::Indexed(130)), // dark orange-brown
            help_item: Style::default().fg(Color::Black),
            help_item_dim: Style::default().fg(Color::Indexed(244)),

            detail_label: Style::default().fg(Color::Indexed(130)),

            graph_border: Style::default().fg(Color::Indexed(24)),
            graph_label: Style::default().fg(Color::Black),
            graph_stage: Style::default().fg(Color::Indexed(130)).bold(),
            graph_arrow: Style::default().fg(Color::Indexed(28)), // forest green

            tile_running: Style::default().fg(Color::Rgb(0, 0, 139)),
            tile_queued: Style::default().fg(Color::Rgb(139, 0, 139)), // dark magenta
            tile_completed: Style::default().fg(Color::Rgb(0, 139, 0)),
            tile_failed: Style::default().fg(Color::Rgb(139, 0, 0)), // dark red

            scheduler_down: Style::default().fg(Color::Indexed(124)),

            cancel_success: Style::default().fg(Color::Indexed(28)),
            cancel_not_done: Style::default().fg(Color::Indexed(130)),
            cancel_failure: Style::default().fg(Color::Indexed(124)),

            banner: Style::default().fg(Color::Indexed(136)), // dark gold

            feature_enabled: Style::default().fg(Color::Indexed(28)),
            feature_disabled: Style::default().fg(Color::Indexed(124)),

            text_error: Style::default().fg(Color::Indexed(124)),
            text_info: Style::default().fg(Color::Black).bold(),
            app_background: Style::default().bg(Color::Gray),

            footer: Style::default().bold(),
        }
    }

    /// Resolve a [`Theme`] from user [`ThemeSettings`]: choose the base preset
    /// then apply any per-field fg-color overrides.
    pub fn from_settings(settings: &ThemeSettings) -> Self {
        let base = match settings.name {
            ThemeName::Dark => Self::dark(),
            ThemeName::Light => Self::light(),
        };
        base.apply_overrides(&settings.overrides)
    }

    fn apply_overrides(mut self, overrides: &ThemeOverride) -> Self {
        macro_rules! patch {
            ($field:ident) => {
                if let Some(style) = overrides.$field {
                    self.$field = self.$field.patch(style);
                }
            };
        }
        patch!(status_running);
        patch!(status_queued);
        patch!(status_failed);
        patch!(status_completed);
        patch!(status_unknown);
        patch!(table_header);
        patch!(row_even);
        patch!(row_odd);
        patch!(row_selected);
        patch!(popup_border);
        patch!(popup_border_alt);
        patch!(popup_border_jobs_stages);
        patch!(nav_active);
        patch!(nav_inactive);
        patch!(search_active);
        patch!(search_inactive);
        patch!(search_cursor);
        patch!(help_header);
        patch!(help_section);
        patch!(help_item);
        patch!(help_item_dim);
        patch!(detail_label);
        patch!(graph_border);
        patch!(graph_label);
        patch!(graph_stage);
        patch!(graph_arrow);
        patch!(tile_running);
        patch!(tile_queued);
        patch!(tile_completed);
        patch!(tile_failed);
        patch!(scheduler_down);
        patch!(cancel_success);
        patch!(cancel_not_done);
        patch!(cancel_failure);
        patch!(banner);
        patch!(feature_enabled);
        patch!(feature_disabled);
        patch!(text_error);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::infrastructure::{ThemeName, ThemeOverride, ThemeSettings};
    use core::str::FromStr;
    use ratatui::prelude::Modifier;

    // ── helpers ────────────────────────────────────────────────────────────

    fn dark_settings() -> ThemeSettings {
        ThemeSettings {
            name: ThemeName::Dark,
            overrides: ThemeOverride::default(),
        }
    }

    fn light_settings() -> ThemeSettings {
        ThemeSettings {
            name: ThemeName::Light,
            overrides: ThemeOverride::default(),
        }
    }

    fn settings_with_banner_override(style: Style) -> ThemeSettings {
        ThemeSettings {
            name: ThemeName::Dark,
            overrides: ThemeOverride {
                banner: Some(style),
                ..Default::default()
            },
        }
    }

    // ── Theme::dark() ──────────────────────────────────────────────────────

    #[test]
    fn dark_status_running_is_light_blue_bold() {
        let theme = Theme::dark();
        assert_eq!(
            theme.status_running,
            Style::default().fg(Color::LightBlue).bold()
        );
    }

    #[test]
    fn dark_status_failed_is_light_red_bold() {
        let theme = Theme::dark();
        assert_eq!(
            theme.status_failed,
            Style::default().fg(Color::LightRed).bold()
        );
    }

    #[test]
    fn dark_table_header_is_light_yellow_and_bold() {
        let theme = Theme::dark();
        assert_eq!(theme.table_header.fg, Some(Color::LightYellow));
        assert!(
            theme.table_header.add_modifier.contains(Modifier::BOLD),
            "table_header should be bold"
        );
    }

    #[test]
    fn dark_row_even_background_is_dark_gray() {
        assert_eq!(Theme::dark().row_even.bg, Some(Color::DarkGray));
    }

    #[test]
    fn dark_row_odd_background_is_black() {
        assert_eq!(Theme::dark().row_odd.bg, Some(Color::Black));
    }

    #[test]
    fn dark_row_selected_background_is_indexed_29() {
        assert_eq!(Theme::dark().row_selected.bg, Some(Color::Indexed(29)));
    }

    #[test]
    fn dark_banner_is_yellow() {
        assert_eq!(Theme::dark().banner.fg, Some(Color::Yellow));
    }

    // ── Theme::light() ─────────────────────────────────────────────────────

    #[test]
    fn light_status_running_is_blue_bold() {
        let theme = Theme::light();
        assert_eq!(theme.status_running.fg, Some(Color::Rgb(0, 0, 139)));
        assert!(theme.status_running.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn light_table_header_fg_is_black() {
        let theme = Theme::light();
        assert_eq!(theme.table_header.fg, Some(Color::Black));
    }

    #[test]
    fn light_row_odd_background_is_reset() {
        assert_eq!(Theme::light().row_odd.bg, Some(Color::Reset));
    }

    #[test]
    fn light_row_even_background_is_indexed_255() {
        assert_eq!(Theme::light().row_even.bg, Some(Color::Indexed(255)));
    }

    #[test]
    fn light_row_selected_background_is_indexed_108() {
        assert_eq!(Theme::light().row_selected.bg, Some(Color::Indexed(108)));
    }

    // ── Theme::from_settings() ─────────────────────────────────────────────

    #[test]
    fn from_settings_dark_uses_dark_base() {
        let theme = Theme::from_settings(&dark_settings());
        assert_eq!(theme.status_running.fg, Some(Color::LightBlue));
        assert_eq!(theme.row_odd.bg, Some(Color::Black));
    }

    #[test]
    fn from_settings_light_uses_light_base() {
        let theme = Theme::from_settings(&light_settings());
        assert_eq!(theme.status_running.fg, Some(Color::Rgb(0, 0, 139)));
        assert_eq!(theme.row_odd.bg, Some(Color::Reset));
    }

    // ── Override: ColorSpec variants ───────────────────────────────────────

    #[test]
    fn override_named_color_changes_fg() {
        let theme = Theme::from_settings(&settings_with_banner_override(
            Style::default().fg(Color::Red),
        ));
        assert_eq!(theme.banner.fg, Some(Color::Red));
    }

    #[test]
    fn override_indexed_color_changes_fg() {
        let settings = ThemeSettings {
            name: ThemeName::Dark,
            overrides: ThemeOverride {
                row_selected: Some(Style::default().fg(Color::Indexed(108))),
                ..Default::default()
            },
        };
        let theme = Theme::from_settings(&settings);
        assert_eq!(theme.row_selected.fg, Some(Color::Indexed(108)));
    }

    #[test]
    fn override_rgb_color_changes_fg() {
        let theme = Theme::from_settings(&settings_with_banner_override(
            Style::default().fg(Color::Rgb(180, 100, 0)),
        ));
        assert_eq!(theme.banner.fg, Some(Color::Rgb(180, 100, 0)));
    }

    #[test]
    fn partial_override_only_affects_specified_field() {
        let settings = ThemeSettings {
            name: ThemeName::Dark,
            overrides: ThemeOverride {
                banner: Some(Style::default().fg(Color::Cyan)),
                ..Default::default()
            },
        };
        let theme = Theme::from_settings(&settings);
        assert_eq!(theme.banner.fg, Some(Color::Cyan));
        // Unspecified field keeps dark-theme default
        assert_eq!(theme.status_running.fg, Some(Color::LightBlue));
    }

    #[test]
    fn override_preserves_modifiers_of_base_theme() {
        // table_header in dark theme has bg=Black and BOLD modifier.
        // Overriding only the fg should leave bg and modifiers intact.
        let settings = ThemeSettings {
            name: ThemeName::Dark,
            overrides: ThemeOverride {
                table_header: Some(Style::default().fg(Color::Cyan)),
                ..Default::default()
            },
        };
        let theme = Theme::from_settings(&settings);
        assert_eq!(theme.table_header.fg, Some(Color::Cyan));
        assert!(theme.table_header.add_modifier.contains(Modifier::BOLD)); // bold preserved
    }

    // ── named_color: case-insensitivity and all variants ──────────────────

    #[test]
    fn named_color_is_case_insensitive() {
        for name in &["LIGHTBLUE", "LightBlue", "lightblue"] {
            let theme = Theme::from_settings(&settings_with_banner_override(
                Style::default().fg(Color::from_str(name).unwrap()),
            ));
            assert_eq!(
                theme.banner.fg,
                Some(Color::LightBlue),
                "case variant '{}' should resolve",
                name
            );
        }
    }

    #[test]
    fn named_color_grey_aliases_gray() {
        let theme = Theme::from_settings(&settings_with_banner_override(
            Style::default().fg(Color::from_str("grey").unwrap()),
        ));
        assert_eq!(theme.banner.fg, Some(Color::Gray));
    }

    #[test]
    fn named_color_darkgrey_aliases_darkgray() {
        let theme = Theme::from_settings(&settings_with_banner_override(
            Style::default().fg(Color::from_str("darkgrey").unwrap()),
        ));
        assert_eq!(theme.banner.fg, Some(Color::DarkGray));
    }

    #[test]
    fn all_named_colors_resolve_correctly() {
        let cases: &[(&str, Color)] = &[
            ("black", Color::Black),
            ("red", Color::Red),
            ("green", Color::Green),
            ("yellow", Color::Yellow),
            ("blue", Color::Blue),
            ("magenta", Color::Magenta),
            ("cyan", Color::Cyan),
            ("gray", Color::Gray),
            ("grey", Color::Gray),
            ("darkgray", Color::DarkGray),
            ("darkgrey", Color::DarkGray),
            ("lightred", Color::LightRed),
            ("lightgreen", Color::LightGreen),
            ("lightyellow", Color::LightYellow),
            ("lightblue", Color::LightBlue),
            ("lightmagenta", Color::LightMagenta),
            ("lightcyan", Color::LightCyan),
            ("white", Color::White),
        ];
        for (name, expected) in cases {
            let theme = Theme::from_settings(&settings_with_banner_override(
                Style::default().fg(Color::from_str(name).unwrap()),
            ));
            assert_eq!(
                theme.banner.fg,
                Some(*expected),
                "named color '{name}' did not resolve to {expected:?}",
            );
        }
    }

    // ── ThemeOverride / ColorSpec deserialization ──────────────────────────

    #[test]
    fn theme_override_default_has_no_fields_set() {
        let o = ThemeOverride::default();
        assert!(o.status_running.is_none());
        assert!(o.table_header.is_none());
        assert!(o.banner.is_none());
        assert!(o.text_error.is_none());
    }

    #[test]
    fn color_spec_named_deserializes() {
        let o: ThemeOverride =
            serde_json::from_str(r#"{"banner": {"fg": "Cyan"}}"#).unwrap();
        assert_eq!(
            o.banner,
            Some(Style::default().fg(Color::Cyan)),
            "expected Named(\"Cyan\")"
        );
    }

    #[test]
    fn color_spec_indexed_deserializes() {
        let o: ThemeOverride =
            serde_json::from_str(r#"{"row_selected": {"fg": "108"}}"#).unwrap();
        assert_eq!(
            o.row_selected,
            Some(Style::default().fg(Color::Indexed(108))),
            "expected Indexed(108)"
        );
    }

    #[test]
    fn color_spec_rgb_deserializes() {
        let o: ThemeOverride =
            serde_json::from_str(r##"{"banner": {"fg": "#1005FF"}}"##).unwrap();
        assert_eq!(
            o.banner,
            Some(Style::default().fg(Color::Rgb(16, 5, 255))),
            "expected Rgb(16, 5, 255)"
        );
    }

    #[test]
    fn theme_name_dark_deserializes() {
        let name: ThemeName = serde_json::from_str(r#""dark""#).unwrap();
        assert!(matches!(name, ThemeName::Dark));
    }

    #[test]
    fn theme_name_light_deserializes() {
        let name: ThemeName = serde_json::from_str(r#""light""#).unwrap();
        assert!(matches!(name, ThemeName::Light));
    }

    #[test]
    fn theme_override_unspecified_fields_remain_none() {
        let o: ThemeOverride =
            serde_json::from_str(r#"{"banner": {"fg": "Red"}}"#).unwrap();
        assert!(o.banner.is_some());
        assert!(o.status_running.is_none());
        assert!(o.table_header.is_none());
        assert!(o.row_selected.is_none());
    }
}
