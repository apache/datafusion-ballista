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
use ratatui::text::Span;
use std::sync::OnceLock;
use std::time::Duration;

/// Generates a vector of `Span` objects with a shimmer effect applied to the given text.
///
/// This function utilizes the `tui_shimmer` crate to create a shimmering animation
/// effect across the text. The shimmer effect's phase is dynamically calculated
/// based on the elapsed time.
///
/// # Parameters
///
/// - `text`: A string slice (`&str`) representing the text to which the shimmer effect
///   will be applied.
/// - `style`: A `Style` object that defines the visual styling (e.g., color, modifiers)
///   to be applied to the shimmering text.
///
/// # Returns
///
/// A `Vec<Span>` containing the text with the shimmering effect applied. Each
/// `Span` encapsulates a portion of the text along with the calculated shimmering
/// style for that segment.
///
/// # Dependencies
///
/// This function relies on:
/// - `tui_shimmer::shimmer_spans_with_style_at_phase`: A utility function that performs
///   the shimmer effect at a specific phase.
/// - `shimmer_phase_from_elapsed()`: A helper function used to determine the current
///   shimmer phase based on elapsed time.
///
/// # Example
///
/// ```ignore
/// use ratatui::style::{Color, Style};
/// use ratatui::text::Span;
///
/// let text = "Shimmering Example";
/// let style = Style::default().fg(Color::Yellow);
///
/// let shimmered_spans = shimmer_spans_with_style(text, style);
///
/// // `shimmered_spans` now contains the shimmering text as a vector of `Span` objects.
/// ```
pub(crate) fn shimmer_spans_with_style(text: &str, style: Style) -> Vec<Span<'_>> {
    tui_shimmer::shimmer_spans_with_style_at_phase(
        text,
        style,
        shimmer_phase_from_elapsed(),
    )
}

#[cfg(not(feature = "web"))]
fn elapsed_since_start() -> Duration {
    use std::time::Instant;
    static PROCESS_START: OnceLock<Instant> = OnceLock::new();
    let start = PROCESS_START.get_or_init(Instant::now);
    start.elapsed()
}

#[cfg(feature = "web")]
fn elapsed_since_start() -> Duration {
    static PROCESS_START: OnceLock<f64> = OnceLock::new();
    let start = PROCESS_START.get_or_init(js_sys::Date::now);
    let millis = js_sys::Date::now() - start;
    Duration::from_millis(millis as u64)
}

// copied from tui-shimmer and adapted for WASM32 build to use
// js_sys::Date::now() instead of std::time::Instant::now()
fn shimmer_phase_from_elapsed() -> f32 {
    let elapsed = elapsed_since_start().as_secs_f32() / 2.0;
    elapsed.rem_euclid(1.0)
}
