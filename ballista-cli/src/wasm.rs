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

use wasm_bindgen::prelude::*;

/// WASM entry point. Registered as the `start` function so the browser calls it
/// automatically when the WASM module is loaded.
#[wasm_bindgen(start)]
pub fn wasm_start() {
    console_error_panic_hook::set_once();
    if let Err(e) = crate::tui::tui_web_main() {
        // unwrap_throw produces a JS exception visible in the browser console
        wasm_bindgen::throw_str(&format!("Ballista TUI failed to start: {e}"));
    }
}
