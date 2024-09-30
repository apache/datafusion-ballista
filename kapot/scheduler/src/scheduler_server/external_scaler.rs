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

use crate::scheduler_server::externalscaler::{
    external_scaler_server::ExternalScaler, GetMetricSpecResponse, GetMetricsRequest,
    GetMetricsResponse, IsActiveResponse, MetricSpec, MetricValue, ScaledObjectRef,
};
use crate::scheduler_server::SchedulerServer;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;

use tonic::{Request, Response};

const PENDING_JOBS_METRIC_NAME: &str = "pending_jobs";
const RUNNING_JOBS_METRIC_NAME: &str = "running_jobs";

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExternalScaler
    for SchedulerServer<T, U>
{
    async fn is_active(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<IsActiveResponse>, tonic::Status> {
        Ok(Response::new(IsActiveResponse { result: true }))
    }

    async fn get_metric_spec(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<GetMetricSpecResponse>, tonic::Status> {
        Ok(Response::new(GetMetricSpecResponse {
            metric_specs: vec![MetricSpec {
                metric_name: PENDING_JOBS_METRIC_NAME.to_string(),
                target_size: 0,
            }],
        }))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, tonic::Status> {
        Ok(Response::new(GetMetricsResponse {
            metric_values: vec![
                MetricValue {
                    metric_name: PENDING_JOBS_METRIC_NAME.to_string(),
                    metric_value: self.pending_job_number() as i64,
                },
                MetricValue {
                    metric_name: RUNNING_JOBS_METRIC_NAME.to_string(),
                    metric_value: self.running_job_number() as i64,
                },
            ],
        }))
    }
}
