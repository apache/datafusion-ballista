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

//! Job-failure lifecycle tests for the adaptive execution graph.

use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::execution_graph::ExecutionGraph;
use crate::state::execution_stage::ExecutionStage;
use crate::test_utils::mock_executor;
use ballista_core::error::Result;
use ballista_core::serde::protobuf::{
    FailedTask, JobStatus, failed_task, job_status, task_status,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::functions_aggregate::sum::sum;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::{JoinType, col};
use datafusion::test_util::scan_empty_with_partitions;

/// Builds an adaptive graph for a join (two concurrent leaf stages).
async fn test_join_plan(partition: usize) -> AdaptiveExecutionGraph {
    let mut config = SessionConfig::new().with_target_partitions(partition);
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    let ctx = SessionContext::new_with_config(config);

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("gmv", DataType::UInt64, false),
    ]);

    let left_plan = scan_empty_with_partitions(Some("left"), &schema, None, 2).unwrap();
    let right_plan = scan_empty_with_partitions(Some("right"), &schema, None, 2)
        .unwrap()
        .build()
        .unwrap();
    let sort_expr = SortExpr::new(col("id"), false, false);
    let logical_plan = left_plan
        .join(right_plan, JoinType::Inner, (vec!["id"], vec!["id"]), None)
        .unwrap()
        .aggregate(vec![col("left.id")], vec![sum(col("left.gmv"))])
        .unwrap()
        .sort(vec![sort_expr])
        .unwrap()
        .build()
        .unwrap();

    AdaptiveExecutionGraph::try_new(
        "localhost:50050",
        &"job".into(),
        "",
        &ctx,
        &logical_plan,
        0,
    )
    .await
    .unwrap()
}

// Same contract as the static graph's test: aborting transitions every running
// stage to Failed and returns its in-flight tasks for cancellation.
#[tokio::test]
async fn test_abort_running_cancels_stages_and_returns_inflight_tasks() -> Result<()> {
    let executor = mock_executor("executor-id1".to_string());
    let mut graph = test_join_plan(2).await;

    // Call revive to move the two leaf Resolved stages to Running
    graph.revive();
    assert!(
        graph.running_stages().len() >= 2,
        "expected two concurrently running leaf stages, found {:?}",
        graph.running_stages()
    );

    // Dispatch a task so there is an in-flight task to cancel
    let _task = graph.pop_next_task(&executor.id)?.unwrap();

    // Aborting cancels every running stage and returns its in-flight tasks
    let cancelled = graph.abort_running("job aborted".to_string());

    assert!(
        !cancelled.is_empty(),
        "abort_running must return the in-flight tasks to cancel"
    );
    assert!(
        graph.running_stages().is_empty(),
        "every running stage must be cancelled, found {:?}",
        graph.running_stages()
    );
    assert!(
        matches!(
            graph.status(),
            JobStatus {
                status: Some(job_status::Status::Failed(_)),
                ..
            }
        ),
        "the job must be Failed after abort"
    );

    // In-flight tasks of the cancelled stage are recorded as Failed(TaskKilled)
    let has_killed_task = graph.stages.values().any(|stage| match stage {
        ExecutionStage::Failed(failed) => {
            failed.task_infos.iter().flatten().any(|info| {
                matches!(
                    &info.task_status,
                    task_status::Status::Failed(FailedTask {
                        failed_reason: Some(failed_task::FailedReason::TaskKilled(_)),
                        ..
                    })
                )
            })
        }
        _ => false,
    });
    assert!(
        has_killed_task,
        "in-flight tasks must be recorded as Failed(TaskKilled) after abort"
    );

    Ok(())
}
