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

//! kapot Prelude (common imports)

pub use kapot_core::{
    config::{
        KapotConfig, KAPOT_COLLECT_STATISTICS, KAPOT_DEFAULT_BATCH_SIZE,
        KAPOT_DEFAULT_SHUFFLE_PARTITIONS, KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE,
        KAPOT_JOB_NAME, KAPOT_PARQUET_PRUNING, KAPOT_PLUGIN_DIR,
        KAPOT_REPARTITION_AGGREGATIONS, KAPOT_REPARTITION_JOINS,
        KAPOT_REPARTITION_WINDOWS, KAPOT_WITH_INFORMATION_SCHEMA,
    },
    error::{KapotError, Result},
};

pub use futures::StreamExt;

pub use crate::context::KapotContext;
