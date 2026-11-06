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

//! Tests for the rules a scheduler embedder adds to adaptive planning.

use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{
    Embedder, Seen, mock_batch, mock_context, mock_partitions_with_size,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Two stages: one shuffling on `(a, c)`, and one reading it and shuffling on
/// `c`, then the final stage.
async fn two_stage_planner(
    embedder: &Embedder,
) -> datafusion::common::Result<AdaptivePlanner> {
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;
    AdaptivePlanner::from_logical_sql(
        &ctx,
        "select c, count(*) from (select a, c from t group by a, c) group by c",
        Some(embedder),
    )
    .await
}

/// An embedder's preparation rules run once, and its replan rules run when
/// the job is planned and again after each stage completes, seeing the
/// finished stage's measured statistics.
#[tokio::test]
async fn should_run_an_embedders_rules_and_show_them_measured_stages()
-> datafusion::common::Result<()> {
    let seen = Arc::new(Seen::default());
    let embedder = Embedder {
        seen: Arc::clone(&seen),
        hold: false,
    };
    let mut planner = two_stage_planner(&embedder).await?;
    assert_eq!(seen.preparation_runs.load(Ordering::Relaxed), 1);
    assert_eq!(seen.replan_runs.load(Ordering::Relaxed), 1);

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(stages.len(), 1);
    planner.finalise_stage_internal(0, mock_partitions_with_size(42, 1024))?;

    assert_eq!(seen.preparation_runs.load(Ordering::Relaxed), 1);
    assert_eq!(seen.replan_runs.load(Ordering::Relaxed), 2);
    assert_eq!(*seen.measured_rows.lock().unwrap(), vec![(0, 42)]);
    Ok(())
}

/// A replan rule sees each stage before it can start, so it can hold one back,
/// and a held stage doesn't run even though its inputs are ready.
#[tokio::test]
async fn should_not_run_a_stage_an_embedders_rule_holds() -> datafusion::common::Result<()>
{
    let embedder = Embedder {
        seen: Arc::new(Seen::default()),
        hold: true,
    };
    let mut planner = two_stage_planner(&embedder).await?;
    let stages = planner.runnable_stages()?.unwrap();
    assert!(stages.is_empty(), "a held stage must not run");
    Ok(())
}
