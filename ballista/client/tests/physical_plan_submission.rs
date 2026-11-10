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

//! End-to-end test for submitting an already-built physical plan directly to a
//! Ballista scheduler, bypassing logical plan creation on the scheduler side.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod physical_plan_submission_tests {
    use crate::common::{setup_test_cluster, two_stage_physical_plan};
    use ballista_core::config::BallistaConfig;
    use ballista_core::execution_plans::execute_physical_plan;
    use ballista_core::extension::SessionConfigExt;
    use datafusion::common::test_util::batches_to_sort_string;
    use datafusion::physical_plan;
    use datafusion::prelude::SessionConfig;
    use datafusion_proto::protobuf::PhysicalPlanNode;

    /// Submits a hand-built physical plan (scan -> hash repartition) directly to
    /// a standalone Ballista cluster via the new physical-plan submission path,
    /// and checks that the rows returned match what executing the same plan
    /// locally produces. The plan's `RepartitionExec(Hash)` boundary means the
    /// scheduler must have split it into two shuffle stages for this to work.
    #[tokio::test]
    async fn should_execute_submitted_physical_plan_across_shuffle_stages() {
        let (host, port) = setup_test_cluster().await;
        let scheduler_url = format!("http://{host}:{port}");

        let (plan, ctx) = two_stage_physical_plan().await;

        let expected = physical_plan::collect(plan.clone(), ctx.task_ctx())
            .await
            .unwrap();

        let session_config = SessionConfig::new_with_ballista();
        let codec = session_config.ballista_physical_extension_codec();
        let session_id = uuid::Uuid::new_v4().to_string();

        let stream = execute_physical_plan::<PhysicalPlanNode>(
            scheduler_url,
            &BallistaConfig::default(),
            plan,
            codec.as_ref(),
            session_id,
            session_config,
        )
        .await
        .expect("physical plan job should be submitted and executed");

        let actual = physical_plan::common::collect(stream)
            .await
            .expect("collecting results from the submitted physical plan");

        assert_eq!(
            batches_to_sort_string(&expected),
            batches_to_sort_string(&actual)
        );
    }
}
