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

use super::*;
use crate::cluster::bind_task_bias;
use crate::test_utils::{test_aggregation_plan, test_cluster_context};
use ballista_core::serde::protobuf::{
    AvailableVcores, FailedTask, RunningTask, SuccessfulTask, task_status,
};
use ballista_core::serde::scheduler::TaskKey;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion_proto::protobuf::LogicalPlanNode;

type TestTaskManager = TaskManager<LogicalPlanNode, PhysicalPlanNode>;

fn manager() -> TestTaskManager {
    TaskManager::new(
        test_cluster_context().job_state(),
        BallistaCodec::default(),
        "test-scheduler".to_string(),
        "localhost:50050".to_string(),
        Arc::new(SchedulerConfig::default()),
    )
}

fn task(task_id: usize, stage_attempt_num: usize, vcores: u32) -> TaskDescription {
    TaskDescription {
        session_id: "session".to_string(),
        key: TaskKey {
            job_id: "job".into(),
            stage_id: 1,
            task_id,
        },
        stage_attempt_num,
        task_attempt: 0,
        global_input_partition_ids: (0..vcores as usize).collect(),
        vcores_consumed: vcores,
        plan: Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        session_config: Arc::new(SessionConfig::new_with_ballista()),
    }
}

fn completed(task: &TaskDescription) -> TaskStatus {
    TaskStatus {
        job_id: task.key.job_id.to_string(),
        stage_id: task.key.stage_id as u32,
        stage_attempt_num: task.stage_attempt_num as u32,
        task_id: task.key.task_id as u32,
        status: Some(task_status::Status::Successful(SuccessfulTask {
            executor_id: "executor-1".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[tokio::test]
async fn terminal_statuses_refund_weighted_reservations_only_once() {
    let manager = manager();
    let first = task(0, 0, 3);
    let second = task(1, 0, 5);
    manager.record_task_reservations(&[
        ("executor-1".to_string(), first.clone()),
        ("executor-1".to_string(), second.clone()),
    ]);

    let successful = completed(&first);
    let mut failed = completed(&second);
    failed.status = Some(task_status::Status::Failed(FailedTask::default()));
    let statuses = vec![successful.clone(), failed.clone(), successful, failed];

    assert_eq!(manager.take_vcores_for_statuses("executor-1", &statuses), 8);
    assert_eq!(manager.take_vcores_for_statuses("executor-1", &statuses), 0);
    assert!(manager.outstanding_task_reservations.is_empty());
}

#[tokio::test]
async fn nonterminal_unknown_and_wrong_executor_statuses_do_not_refund() {
    let manager = manager();
    let task = task(0, 0, 4);
    manager.record_task_reservations(&[("executor-1".to_string(), task.clone())]);
    let terminal = completed(&task);
    let mut running = terminal.clone();
    running.status = Some(task_status::Status::Running(RunningTask {
        executor_id: "executor-1".to_string(),
    }));
    let mut missing_status = terminal.clone();
    missing_status.status = None;
    let mut unknown_task = terminal.clone();
    unknown_task.task_id += 1;
    let mut unknown_stage = terminal.clone();
    unknown_stage.stage_id += 1;
    let mut unknown_job = terminal.clone();
    unknown_job.job_id = "unknown-job".to_string();

    assert_eq!(
        manager.take_vcores_for_statuses(
            "executor-1",
            &[
                running,
                missing_status,
                unknown_task,
                unknown_stage,
                unknown_job
            ],
        ),
        0
    );
    assert_eq!(
        manager.take_vcores_for_statuses("executor-2", std::slice::from_ref(&terminal)),
        0
    );
    assert_eq!(manager.outstanding_task_reservations.len(), 1);
    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[terminal]),
        4
    );
}

#[tokio::test]
async fn stale_stage_attempt_cannot_consume_reused_task_id() {
    let manager = manager();
    let old = task(0, 0, 2);
    let current = task(0, 1, 5);
    manager.record_task_reservations(&[
        ("executor-1".to_string(), old.clone()),
        ("executor-1".to_string(), current.clone()),
    ]);

    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[completed(&old)]),
        2
    );
    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[completed(&old)]),
        0
    );
    assert_eq!(manager.outstanding_task_reservations.len(), 1);
    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[completed(&current)]),
        5
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_duplicate_statuses_release_one_reservation() {
    let manager = manager();
    let task = task(0, 0, 7);
    manager.record_task_reservations(&[("executor-1".to_string(), task.clone())]);
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut reports = Vec::new();
    for _ in 0..8 {
        let manager = manager.clone();
        let barrier = barrier.clone();
        let status = completed(&task);
        reports.push(tokio::spawn(async move {
            barrier.wait().await;
            manager.take_vcores_for_statuses("executor-1", &[status])
        }));
    }

    let mut refunded = 0;
    for report in reports {
        refunded += report.await.expect("report should not panic");
    }
    assert_eq!(refunded, 7);
    assert!(manager.outstanding_task_reservations.is_empty());
}

#[tokio::test]
async fn launch_rejection_and_status_updates_share_exactly_once_accounting() {
    let manager = manager();
    let rejected = task(0, 0, 6);
    let running = task(1, 0, 3);
    manager.record_task_reservations(&[
        ("executor-1".to_string(), rejected.clone()),
        ("executor-1".to_string(), running.clone()),
    ]);

    assert_eq!(
        manager.take_task_reservations("executor-1", std::slice::from_ref(&rejected)),
        6
    );
    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[completed(&rejected)]),
        0
    );
    assert_eq!(manager.take_task_reservations("executor-1", &[rejected]), 0);
    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[completed(&running)]),
        3
    );
    assert_eq!(manager.take_task_reservations("executor-1", &[running]), 0);
}

#[tokio::test]
async fn executor_loss_discards_only_lost_executor_reservations() -> Result<()> {
    let manager = manager();
    let lost = task(0, 0, 6);
    let healthy = task(1, 0, 2);
    manager.record_task_reservations(&[
        ("executor-1".to_string(), lost.clone()),
        ("executor-2".to_string(), healthy.clone()),
    ]);
    // Neither job has a cached graph: executor cleanup must use the ledger.
    assert!(manager.active_job_cache.is_empty());
    assert!(manager.executor_lost("executor-1").await?.is_empty());
    assert_eq!(manager.outstanding_task_reservations.len(), 1);
    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[completed(&lost)]),
        0
    );
    assert_eq!(manager.take_task_reservations("executor-1", &[lost]), 0);
    assert_eq!(
        manager.take_vcores_for_statuses("executor-2", &[completed(&healthy)]),
        2
    );
    assert!(manager.outstanding_task_reservations.is_empty());
    Ok(())
}

#[tokio::test]
async fn abort_refunds_only_current_running_attempt() -> Result<()> {
    let manager = manager();
    let mut graph: ExecutionGraphBox = Box::new(test_aggregation_plan(4).await);
    graph.revive();
    graph
        .fetch_running_stage(&[])
        .expect("job should have a runnable stage")
        .stage_attempt_num = 1;
    let job_id = graph.job_id().to_owned();
    manager.state.accept_job(&job_id, graph.job_name(), 0)?;
    manager
        .state
        .submit_job(job_id.clone(), &graph, None)
        .await?;
    manager
        .active_job_cache
        .insert(job_id.clone(), JobInfoCache::new(graph));

    let mut budget = AvailableVcores {
        executor_id: "executor-1".to_string(),
        vcores: 8,
    };
    let bound =
        bind_task_bias(vec![&mut budget], manager.get_running_job_cache(), |_| {
            false
        })
        .await;
    assert!(!bound.is_empty());
    let reserved: u32 = bound.iter().map(|(_, task)| task.vcores_consumed).sum();
    assert!(reserved > 0, "cancellation must release a reservation");
    manager.record_task_reservations(&bound);

    // A previous attempt can still be finishing when the current attempt is
    // cancelled. Its reused task id must not be refunded by that cancellation.
    let mut old_attempt = bound[0].1.clone();
    old_attempt.stage_attempt_num = 0;
    old_attempt.vcores_consumed = 5;
    manager.record_task_reservations(&[("executor-1".to_string(), old_attempt.clone())]);

    // An AQE-retired stage no longer appears in the graph's running_tasks.
    // Its outstanding reservation must survive the job's abort and eviction.
    let mut retired = task(0, 0, 3);
    retired.key.job_id = job_id.clone();
    retired.key.stage_id = u32::MAX as usize;
    manager.record_task_reservations(&[("executor-1".to_string(), retired.clone())]);

    let expected_tasks = bound.len();
    manager
        .abort_job(
            &job_id,
            "cancelled".to_string(),
            move |tasks, slots| async move {
                assert_eq!(tasks.len(), expected_tasks);
                assert_eq!(slots, vec![("executor-1".to_string(), reserved)]);
                Ok(())
            },
        )
        .await?;

    assert!(manager.get_active_execution_graph(&job_id).is_none());
    let statuses: Vec<_> = bound.iter().map(|(_, task)| completed(task)).collect();
    assert_eq!(manager.take_vcores_for_statuses("executor-1", &statuses), 0);
    assert_eq!(manager.outstanding_task_reservations.len(), 2);
    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[completed(&old_attempt)]),
        5
    );
    assert_eq!(
        manager.take_vcores_for_statuses("executor-1", &[completed(&retired)]),
        3
    );
    assert!(manager.outstanding_task_reservations.is_empty());
    Ok(())
}
