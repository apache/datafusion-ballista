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

mod dashboard;
mod jobs;
mod metrics;

pub(crate) use dashboard::*;
pub(crate) use jobs::*;
pub(crate) use metrics::*;

use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct SchedulerState {
    pub started: i64,
    pub version: String,
    pub datafusion_version: String,
    pub substrait_support: bool,
    pub keda_support: bool,
    pub prometheus_support: bool,
    pub graphviz_support: bool,
    pub spark_support: bool,
    pub scheduling_policy: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}
