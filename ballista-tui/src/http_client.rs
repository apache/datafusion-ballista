use reqwest::Response;
use serde::de::DeserializeOwned;

use crate::{
    TuiResult,
    domain::{ExecutorsData, SchedulerState},
    error::TuiError,
};

pub struct HttpClient {
    scheduler_url: String,
}

impl HttpClient {
    pub fn new(scheduler_url: String) -> Self {
        Self { scheduler_url }
    }

    pub fn scheduler_url(&self) -> &str {
        &self.scheduler_url
    }

    pub async fn get_scheduler_state(&self) -> TuiResult<SchedulerState> {
        let url = self.url("state");
        self.json::<SchedulerState>(&url).await
    }

    pub async fn get_executors(&self) -> TuiResult<Vec<ExecutorsData>> {
        let url = self.url("executors");
        self.json::<Vec<ExecutorsData>>(&url).await
    }

    async fn json<R>(&self, url: &str) -> TuiResult<R>
    where
        R: std::fmt::Debug + DeserializeOwned,
    {
        let response = self.get(&url).await?;
        response
            .json::<R>()
            .await
            .map_err(TuiError::Reqwest)
            .inspect(|data| tracing::trace!("Loaded: {data:?}"))
            .inspect_err(|err| tracing::error!("The http request failed: {err:?}"))
    }

    async fn get(&self, url: &str) -> TuiResult<Response> {
        tracing::trace!("Going to make a request to {}", &url);
        reqwest::get(url).await.map_err(TuiError::Reqwest)
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/{}", self.scheduler_url, path)
    }
}
