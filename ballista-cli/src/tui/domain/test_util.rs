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

//! Fixtures shared by the unit tests of the domain types and the app.

use crate::tui::domain::executors::{Executor, Metric, OsInfo, Specification};
use crate::tui::domain::jobs::Job;
use crate::tui::domain::jobs::stages::{
    JobStageResponse, JobStagesPopup, JobStagesResponse,
};
use ballista_api_types::dto::Percentiles;

/// An executor on Ubuntu with four physical cores and the given memory metrics.
pub(crate) fn make_executor(
    host: &str,
    port: u16,
    id: &str,
    vcores: u32,
    proc_physical_memory_usage: u64,
    peak_physical_memory_usage: u64,
    last_seen: Option<u64>,
) -> Executor {
    Executor {
        host: host.to_string(),
        port,
        id: id.to_string(),
        last_seen,
        specification: Specification { vcores },
        metrics: vec![
            Metric {
                typ: "proc_physical_memory".to_string(),
                value: proc_physical_memory_usage,
            },
            Metric {
                typ: "peak_physical_memory".to_string(),
                value: peak_physical_memory_usage,
            },
        ],
        os_info: OsInfo {
            kernel_ver: "5.15".to_string(),
            num_disks: 1,
            open_files_limit: 1024,
            os_ver: "Ubuntu 22.04".to_string(),
            os_ver_long: "Ubuntu 22.04.1 LTS".to_string(),
            physical_cores: 4,
            system_name: "Linux".to_string(),
            total_available_disk_space: 50_000_000_000,
            total_disk_space: 100_000_000_000,
        },
    }
}

/// A job with the given status and progress, and no plans.
#[expect(clippy::too_many_arguments)]
pub(crate) fn make_job(
    id: &str,
    name: &str,
    status: &str,
    start_time: u64,
    end_time: u64,
    num_stages: usize,
    completed_stages: usize,
    percent_complete: u8,
) -> Job {
    Job {
        job_id: id.to_string(),
        job_name: name.to_string(),
        status: status.to_string(),
        job_status: status.to_string(),
        start_time,
        end_time,
        num_stages,
        completed_stages,
        percent_complete,
        logical_plan: None,
        physical_plan: None,
        stage_plan: None,
    }
}

fn make_percentiles() -> Percentiles {
    Percentiles {
        min: 0,
        max: 100,
        median: 50,
        p25: 25,
        p75: 75,
    }
}

pub(crate) fn make_stage(id: &str) -> JobStageResponse {
    JobStageResponse {
        stage_id: id.to_string(),
        stage_status: "Completed".to_string(),
        stage_plan: None,
        input_rows: 0,
        output_rows: 0,
        elapsed_compute: Some("1ns".to_string()),
        task_duration_percentiles: Some(make_percentiles()),
        task_input_percentiles: Some(make_percentiles()),
        tasks: Vec::new(),
    }
}

fn make_stages_response(n: usize) -> JobStagesResponse {
    JobStagesResponse {
        stages: (0..n).map(|i| make_stage(&i.to_string())).collect(),
    }
}

/// A popup for job `job1` over `n` completed stages.
pub(crate) fn make_popup(n: usize) -> JobStagesPopup {
    JobStagesPopup::new("job1".to_string(), make_stages_response(n))
}
