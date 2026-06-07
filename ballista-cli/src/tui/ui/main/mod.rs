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

mod executors;
mod jobs;
mod metrics;

pub use executors::{executor_details_popup, render_executors};
#[cfg(not(feature = "web"))]
pub use executors::{load_executor_details_popup, load_executors_data};
#[cfg(not(feature = "web"))]
pub use jobs::{
    load_job_details, load_job_dot, load_job_stages_popup, load_jobs_data,
    load_stage_plan,
};

#[cfg(feature = "web")]
pub(crate) use jobs::dot_parser;
pub use jobs::{
    job_dot_popup, job_plan_popup, job_stages_popup, render_jobs, stage_plan_popup,
    stage_tasks_popup,
};
#[cfg(not(feature = "web"))]
pub use metrics::load_metrics_data;
pub use metrics::render_metrics;
