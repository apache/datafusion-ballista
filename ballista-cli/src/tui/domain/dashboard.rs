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

use crate::tui::domain::Job;
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct ExecutorsData {
    pub host: String,
    pub port: u16,
    pub id: String,
    pub last_seen: i64,
}

#[derive(Clone, Debug)]
pub struct DashboardData {
    pub scheduler_state: Option<super::SchedulerState>,
    pub executors_data: Vec<ExecutorsData>,
    pub jobs_data: Vec<Job>,
}

impl DashboardData {
    pub fn new() -> Self {
        Self {
            scheduler_state: None,
            executors_data: Vec::new(),
            jobs_data: Vec::new(),
        }
    }
}
