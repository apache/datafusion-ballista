use tokio::sync::mpsc::error::SendError;

use crate::event::Event;

pub enum TuiError {
    Reqwest(reqwest::Error),
    Json(serde_json::Error),
    SendError(SendError<Event>),
}

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
