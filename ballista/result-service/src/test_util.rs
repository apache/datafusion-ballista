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

//! Fixtures shared by the unit tests of the result service.

use ballista_core::JobId;
use ballista_core::serde::scheduler::Action;

/// A request for partition 0 of stage 1 of job `job`, served by the executor
/// `executor:50051`.
pub(crate) fn fetch_partition_action() -> Action {
    Action::FetchPartition {
        job_id: JobId::new("job"),
        stage_id: 1,
        partition_id: 0,
        host: "executor".to_owned(),
        port: 50051,
        file_id: None,
        layout: Default::default(),
        file_kind: Default::default(),
        byte_ranges: vec![],
    }
}
