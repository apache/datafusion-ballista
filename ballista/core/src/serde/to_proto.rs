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

use crate::execution_plans::BallistaPlanType;
use crate::serde::protobuf;
use std::sync::Arc;

/// Convert Ballista BallistaPlanType to protobuf BallistaPlanType
impl From<&BallistaPlanType> for protobuf::BallistaPlanType {
    fn from(ballista_type: &BallistaPlanType) -> Self {
        let plan_type = match ballista_type {
            BallistaPlanType::DataFusionPlanType(df_type) => {
                // Create a dummy StringifiedPlan to leverage DataFusion's conversion
                let dummy_plan = datafusion::common::display::StringifiedPlan {
                    plan_type: df_type.clone(),
                    plan: Arc::new("".to_string()),
                };
                let proto_plan: datafusion_proto::protobuf::StringifiedPlan =
                    (&dummy_plan).into();
                Some(protobuf::ballista_plan_type::PlanType::DatafusionPlanType(
                    proto_plan.plan_type.unwrap(),
                ))
            }
            BallistaPlanType::DistributedPlan => {
                Some(protobuf::ballista_plan_type::PlanType::DistributedPlan(
                    datafusion_proto_common::EmptyMessage {},
                ))
            }
        };

        Self { plan_type }
    }
}
