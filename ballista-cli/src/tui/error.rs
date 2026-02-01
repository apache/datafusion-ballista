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

use tokio::sync::mpsc::error::SendError;

use crate::tui::event::Event;

#[derive(Debug)]
#[allow(dead_code)]
pub enum TuiError {
    Reqwest(reqwest::Error),
    Json(serde_json::Error),
    SendError(SendError<Event>),
}

impl std::fmt::Display for TuiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TuiError::Reqwest(err) => write!(f, "Reqwest error: {}", err),
            TuiError::Json(err) => write!(f, "JSON error: {}", err),
            TuiError::SendError(err) => write!(f, "Send error: {}", err),
        }
    }
}

impl std::error::Error for TuiError {}

impl From<reqwest::Error> for TuiError {
    fn from(err: reqwest::Error) -> Self {
        TuiError::Reqwest(err)
    }
}

impl From<serde_json::Error> for TuiError {
    fn from(err: serde_json::Error) -> Self {
        TuiError::Json(err)
    }
}

impl From<SendError<Event>> for TuiError {
    fn from(err: SendError<Event>) -> Self {
        TuiError::SendError(err)
    }
}
