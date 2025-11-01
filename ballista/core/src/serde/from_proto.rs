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

/// Convert protobuf BallistaPlanType to Ballista BallistaPlanType
impl From<&protobuf::BallistaPlanType> for BallistaPlanType {
    fn from(proto_type: &protobuf::BallistaPlanType) -> Self {
        match &proto_type.plan_type {
            Some(protobuf::ballista_plan_type::PlanType::DatafusionPlanType(df_type)) => {
                // Create a dummy protobuf StringifiedPlan to leverage DataFusion's conversion
                let dummy_proto_plan = datafusion_proto::protobuf::StringifiedPlan {
                    plan_type: Some(df_type.clone()),
                    plan: "".to_string(),
                };
                let df_plan: datafusion::common::display::StringifiedPlan =
                    (&dummy_proto_plan).into();
                BallistaPlanType::DataFusionPlanType(df_plan.plan_type)
            }
            Some(protobuf::ballista_plan_type::PlanType::DistributedPlan(_)) => {
                BallistaPlanType::DistributedPlan
            }
            None => panic!("Missing plan_type variant in BallistaPlanType"),
        }
    }
}
