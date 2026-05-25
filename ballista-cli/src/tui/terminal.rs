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

// Native (crossterm) terminal backend
#[cfg(not(feature = "web"))]
mod native {
    use crate::tui::TuiResult;
    use crossterm::{
        execute,
        terminal::{
            EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
        },
    };
    use ratatui::{Terminal, backend::CrosstermBackend};
    use std::io::{self, Stdout};

    pub type Tui = Terminal<CrosstermBackend<Stdout>>;

    fn init() -> TuiResult<Tui> {
        execute!(io::stdout(), EnterAlternateScreen)?;
        enable_raw_mode()?;
        let backend = CrosstermBackend::new(io::stdout());
        let mut terminal = Terminal::new(backend)?;
        terminal.hide_cursor()?;
        terminal.clear()?;

        let original_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            let _ = restore();
            original_hook(panic_info);
        }));

        Ok(terminal)
    }

    pub fn restore() -> TuiResult<()> {
        let mut first_err: Option<std::io::Error> = None;

        if let Err(e) = disable_raw_mode() {
            tracing::error!("Failed to disable raw mode: {e:?}");
            first_err = Some(e);
        }

        if let Err(e) = execute!(io::stdout(), LeaveAlternateScreen) {
            tracing::error!("Failed to leave alternate screen: {e:?}");
            if first_err.is_none() {
                first_err = Some(e);
            }
        }

        if let Some(e) = first_err {
            return Err(e.into());
        }
        Ok(())
    }

    pub struct TuiWrapper {
        pub terminal: Tui,
    }

    impl TuiWrapper {
        pub fn new() -> TuiResult<Self> {
            Ok(Self { terminal: init()? })
        }
    }

    impl Drop for TuiWrapper {
        fn drop(&mut self) {
            let _ = restore();
        }
    }
}

// Web (Ratzilla) terminal backend
#[cfg(feature = "web")]
mod web {
    use crate::tui::TuiResult;
    use ratatui::Terminal;
    use ratzilla::CanvasBackend;

    pub type WebTui = Terminal<CanvasBackend>;

    pub struct TuiWrapper {
        pub terminal: WebTui,
    }

    impl TuiWrapper {
        pub fn new() -> TuiResult<Self> {
            let backend = CanvasBackend::new().map_err(|e| {
                crate::tui::error::TuiError::IO(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
            Ok(Self {
                terminal: Terminal::new(backend)?,
            })
        }
    }
}

#[cfg(not(feature = "web"))]
pub use native::TuiWrapper;

#[cfg(feature = "web")]
pub use web::TuiWrapper;
