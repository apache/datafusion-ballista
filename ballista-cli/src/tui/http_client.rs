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

use std::time::Duration;

use color_eyre::eyre::Result;
use reqwest::{Client, Response};
use serde::de::DeserializeOwned;

use crate::tui::{
    TuiResult,
    domain::{ExecutorsData, SchedulerState},
    error::TuiError,
    infrastructure::Settings,
};

pub struct HttpClient {
    scheduler_url: String,
    client: reqwest::Client,
}

impl HttpClient {
    pub fn new(config: Settings) -> Result<Self> {
        Ok(Self {
            scheduler_url: config.scheduler.url,
            client: Client::builder()
                .timeout(Duration::from_millis(config.http.timeout))
                .build()?,
        })
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
        let response = self.get(url).await?;
        let response = response
            .error_for_status()
            .map_err(TuiError::Reqwest)
            .inspect_err(|err| tracing::error!("HTTP error status: {err:?}"))?;

        response
            .json::<R>()
            .await
            .map_err(TuiError::Reqwest)
            .inspect(|data| tracing::trace!("Loaded: {data:?}"))
            .inspect_err(|err| tracing::error!("The HTTP request failed: {err:?}"))
    }

    async fn get(&self, url: &str) -> TuiResult<Response> {
        tracing::trace!("Going to make a request to {}", &url);
        self.client
            .get(url)
            .send()
            .await
            .inspect(|data| tracing::trace!("Got: {data:?}"))
            .inspect_err(|err| tracing::error!("The HTTP GET request failed: {err:?}"))
            .map_err(TuiError::Reqwest)
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/{}", self.scheduler_url, path)
    }
}
