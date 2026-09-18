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

//! Run one stage where the plan asks for the same stage twice.
//!
//! A query that reads a table twice the same way plans two exchanges over
//! identical inputs, and each becomes a stage: TPC-H q18 scans lineitem's
//! `[l_orderkey, l_quantity]` in two stages, q21 scans
//! `[l_orderkey, l_suppkey, l_commitdate, l_receiptdate]` in two more. Both
//! stages read the same bytes and write the same shuffle output.
//!
//! Where the inputs are structurally identical and the exchanges want the same
//! partitioning, the duplicates are pointed at the first one's stage — Spark's
//! `ReusedExchange`. `output_links` is already a list, so a stage may feed
//! several consumers.

use crate::state::aqe::execution_plan::ExchangeExec;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct ReuseIdenticalStagesRule {
    plan_id_generator: Arc<AtomicUsize>,
}

impl ReuseIdenticalStagesRule {
    pub(crate) fn new(plan_id_generator: Arc<AtomicUsize>) -> Self {
        Self { plan_id_generator }
    }
}

/// What makes two exchanges interchangeable: the same input plan, encoded the
/// way the executor will receive it, plus the partitioning and broadcast shape
/// the consumer reads.
///
/// The encoding is the identity check. Rendered plan text is not: two different
/// in-memory tables both display as
/// `DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]`, so matching on
/// text merges unrelated scans and turns a join into a self-join. The serialized
/// plan is what a task actually runs, so equal bytes mean either copy computes
/// the same rows — and a plan the codec cannot encode is one this rule leaves
/// alone.
fn fingerprint(exchange: &ExchangeExec) -> Option<Vec<u8>> {
    if !reads_files(exchange.input().as_ref()) {
        return None;
    }
    let codec = BallistaPhysicalExtensionCodec::default();
    let encoded =
        physical_plan_to_bytes_with_extension_codec(exchange.input().clone(), &codec)
            .ok()?;
    let mut key =
        format!("{:?}|{}|", exchange.partitioning, exchange.broadcast).into_bytes();
    key.extend_from_slice(&encoded);
    Some(key)
}

/// Restricts reuse to pipelines whose scans read files.
///
/// Not a correctness requirement — the encoded plan already establishes identity
/// — but it keeps this rule to the case it was written for. Lifting it would also
/// merge in-memory scans of identical content, which is sound and would need the
/// stage counts in two AQE tests updated.
fn reads_files(plan: &dyn ExecutionPlan) -> bool {
    let mut scans = 0;
    let mut file_scans = 0;
    let rendered = datafusion::physical_plan::displayable(plan)
        .indent(false)
        .to_string();
    for line in rendered.lines() {
        if line.contains("DataSourceExec") {
            scans += 1;
            if line.contains("file_groups={") {
                file_scans += 1;
            }
        }
    }
    scans > 0 && scans == file_scans
}

impl PhysicalOptimizerRule for ReuseIdenticalStagesRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // First occurrence of each fingerprint becomes the stage the others read.
        let mut canonical: HashMap<Vec<u8>, Arc<dyn ExecutionPlan>> = HashMap::new();
        plan.apply(|node| {
            if let Some(exchange) = node.downcast_ref::<ExchangeExec>()
                && let Some(key) = fingerprint(exchange)
            {
                canonical.entry(key).or_insert_with(|| Arc::clone(node));
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;

        if canonical.is_empty() {
            return Ok(plan);
        }

        let result = plan.transform_up(|node| {
            let Some(exchange) = node.downcast_ref::<ExchangeExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(first) = fingerprint(exchange).and_then(|k| canonical.get(&k))
            else {
                return Ok(Transformed::no(node));
            };
            let first = first
                .downcast_ref::<ExchangeExec>()
                .expect("canonical entries are exchanges");
            // The canonical exchange itself, or one already pointed at it.
            if std::ptr::eq(first as *const _, exchange as *const _)
                || exchange.shares_stage_with(first)
            {
                return Ok(Transformed::no(node));
            }
            // A stage that has already been assigned cannot be redirected: its
            // output may be materialized under its own id already.
            if exchange.stage_id().is_some() {
                return Ok(Transformed::no(node));
            }
            let plan_id = self.plan_id_generator.fetch_add(1, Ordering::Relaxed);
            Ok(Transformed::yes(
                Arc::new(exchange.sharing_stage_with(first, plan_id))
                    as Arc<dyn ExecutionPlan>,
            ))
        })?;

        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ReuseIdenticalStagesRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
