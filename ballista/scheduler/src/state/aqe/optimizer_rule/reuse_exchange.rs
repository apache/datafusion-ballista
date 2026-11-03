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

//! Runs a repeated subplan's stage once (Spark's `ReuseExchange`).
//!
//! A query that reads one subplan twice, such as a common table expression
//! joined to itself or TPC-H q15's revenue view, plans two identical
//! [`ExchangeExec`]s. Without this rule each becomes its own stage and the
//! subplan runs twice. This rule finds exchanges whose subtrees are identical
//! and makes every one of them a reader of a single stage: the stage runs
//! once, and each consumer reads the same shuffle output within the job.
//!
//! # Matching
//!
//! An exchange's key is the protobuf encoding of its subtree under the job's
//! physical extension codec, the encoding that ships stages to executors, so
//! two subtrees match only when an executor could not tell them apart. Inside
//! a subtree a nested exchange is encoded by what it writes, its partitioning
//! and whether it is a broadcast, never by its stage id. A subtree the codec
//! cannot encode gets no key and is left alone.
//!
//! # Sharing
//!
//! A duplicate is rewritten with [`ExchangeExec::share_stage_of`]: it takes
//! the canonical exchange's input and write-side state, and keeps its own
//! reader-side state (coalesce and skew-join decisions), since each consuming
//! stage decides those for itself. The canonical exchange of a key is one
//! whose shuffle is already resolved, else one already running, else the
//! first met bottom-up. An exchange already running a stage of its own is
//! never re-pointed at another.
//!
//! # What is never merged
//!
//! - A subtree with a volatile expression such as `random()`: it encodes like
//!   its twin but has to produce values of its own.
//! - Range repartitioning and the window rewrites built on it. The scheduler
//!   finishes those stages by reading state off the one consumer of the
//!   stage, so a second consumer is not supported.
//!
//! The rule runs last in the replan chain, after an embedder's rules, so keys
//! cover each stage as it will run.

use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::is_volatile;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalProtoConverterExtension;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use log::debug;
use prost::Message;

use crate::state::aqe::execution_plan::ExchangeExec;

/// Operators whose stage the scheduler finishes by reading state off the
/// stage's single consumer. See the module docs.
const SINGLE_CONSUMER_OPERATORS: &[&str] = &[
    "OrderedRangeRepartitionExec",
    "UnorderedRangeRepartitionExec",
    "RangeFilterExec",
    "PerPartitionFilterExec",
    "PrefixMergeExec",
    "PartitionedBoundedWindowAggExec",
    "RuntimeStatsExec",
];

/// Makes every exchange in a set of identical exchanges a reader of one
/// stage. See the module docs.
#[derive(Debug)]
pub struct ReuseExchangeRule {
    codec: Arc<dyn PhysicalExtensionCodec>,
}

impl ReuseExchangeRule {
    /// A rule that matches subtrees by their encoding under `codec`, the job's
    /// physical extension codec.
    pub fn new(codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { codec }
    }

    /// The key `node` is matched on, or `None` when `node` is not an exchange
    /// that may be merged.
    fn key(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Vec<u8>> {
        let exchange = node.downcast_ref::<ExchangeExec>()?;
        if matches!(exchange.partitioning(), Some(Partitioning::Range(_)))
            || !mergeable(node)
        {
            return None;
        }
        let codec = KeyCodec {
            inner: Arc::clone(&self.codec),
        };
        physical_plan_to_bytes_with_extension_codec(Arc::clone(node), &codec)
            .ok()
            .map(Vec::from)
    }
}

/// How far along an exchange's stage is, ordering the candidates for the
/// canonical exchange of a key.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Progress {
    Pending,
    Running,
    Resolved,
}

fn progress(exchange: &ExchangeExec) -> Progress {
    if exchange.shuffle_created() {
        Progress::Resolved
    } else if exchange.stage_id().is_some() {
        Progress::Running
    } else {
        Progress::Pending
    }
}

impl PhysicalOptimizerRule for ReuseExchangeRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let enabled = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default()
            .adaptive_reuse_exchange_enabled();
        if !enabled {
            return Ok(plan);
        }

        // First pass: key every exchange, and find for each key the exchange
        // furthest along, which the others will read from.
        let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut furthest: HashMap<Vec<u8>, Arc<dyn ExecutionPlan>> = HashMap::new();
        // Keys of the exchanges behind each stage handle, to find a pending
        // handle that exchanges of different subtrees ended up sharing.
        let mut handle_keys: HashMap<*const (), Vec<Vec<u8>>> = HashMap::new();
        plan.apply(|node| {
            if let Some(exchange) = node.downcast_ref::<ExchangeExec>()
                && let Some(key) = self.key(node)
            {
                let seen = handle_keys.entry(exchange.stage_handle()).or_default();
                if !seen.contains(&key) {
                    seen.push(key.clone());
                }
                *counts.entry(key.clone()).or_default() += 1;
                let best = furthest.entry(key).or_insert_with(|| Arc::clone(node));
                if progress(exchange) > progress(as_exchange(best)) {
                    *best = Arc::clone(node);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let has_conflict = handle_keys.values().any(|keys| keys.len() > 1);
        if !has_conflict && counts.values().all(|count| *count < 2) {
            return Ok(plan);
        }

        // Second pass, bottom-up so a nested repeat is merged before the
        // subtrees above it. Keys are taken again rather than looked up: a
        // rewritten child reads from a subtree that encodes the same, so a
        // node's key is the same as in the first pass.
        let mut canonical: HashMap<Vec<u8>, Arc<dyn ExecutionPlan>> = HashMap::new();
        let transformed = plan.transform_up(|node| {
            let Some(key) = self.key(&node) else {
                return Ok(Transformed::no(node));
            };
            let exchange = as_exchange(&node);

            // A pending stage handle shared by exchanges of different
            // subtrees, as `to_broadcast` can leave behind. Neither may write
            // for the other, so this one starts again with a stage of its own.
            let conflicted = progress(exchange) == Progress::Pending
                && handle_keys
                    .get(&exchange.stage_handle())
                    .is_some_and(|keys| keys.len() > 1);
            let node: Arc<dyn ExecutionPlan> = if conflicted {
                Arc::new(exchange.with_own_stage())
            } else {
                node
            };
            let exchange = as_exchange(&node);

            let target = match canonical.get(&key) {
                Some(target) => Arc::clone(target),
                None => {
                    let best = &furthest[&key];
                    if progress(as_exchange(best)) == Progress::Pending
                        || exchange.shares_stage_with(as_exchange(best))
                    {
                        // Nothing has run for this subtree yet, or this is a
                        // reader of the stage that has: it is the one the
                        // rest read from.
                        canonical.insert(key, Arc::clone(&node));
                        return Ok(Transformed::new_transformed(node, conflicted));
                    }
                    // The exchange furthest along comes later in the walk.
                    // Read from it as it is: its stage has started, so its
                    // subtree no longer changes what it writes.
                    Arc::clone(best)
                }
            };

            let target_exchange = as_exchange(&target);
            if exchange.shares_stage_with(target_exchange)
                || progress(exchange) != Progress::Pending
            {
                // Already a reader of that stage, or running a stage of its
                // own, which is never re-pointed.
                return Ok(Transformed::new_transformed(node, conflicted));
            }
            debug!(
                "exchange reuse: exchange {} reads the stage of exchange {}",
                exchange.plan_id(),
                target_exchange.plan_id()
            );
            Ok(Transformed::yes(Arc::new(
                exchange.share_stage_of(target_exchange),
            )))
        })?;
        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "ReuseExchangeRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn as_exchange(plan: &Arc<dyn ExecutionPlan>) -> &ExchangeExec {
    plan.downcast_ref::<ExchangeExec>()
        .expect("only exchanges are keyed")
}

/// Whether nothing in `plan` rules out sharing its stage: no volatile
/// expression and no single-consumer operator.
fn mergeable(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut mergeable = true;
    let _ = plan.apply(|node| {
        if SINGLE_CONSUMER_OPERATORS.contains(&node.name()) {
            mergeable = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        node.apply_expressions(&mut |expr| {
            if is_volatile(expr) {
                mergeable = false;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
    });
    mergeable
}

/// Encodes a subtree for matching only, never for decoding. An
/// [`ExchangeExec`] is encoded by what it writes, so a nested exchange's stage
/// id never reaches the key; everything else goes to the job's codec.
#[derive(Debug)]
struct KeyCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
}

/// Marks an exchange in a key, so it can't collide with another node's bytes.
const EXCHANGE_TAG: &[u8] = b"ballista.aqe.ExchangeExec";

impl PhysicalExtensionCodec for KeyCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        datafusion::common::internal_err!("exchange reuse keys are never decoded")
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(exchange) = node.downcast_ref::<ExchangeExec>() else {
            return self.inner.try_encode(node, buf, proto_converter);
        };
        buf.extend_from_slice(EXCHANGE_TAG);
        buf.push(exchange.is_broadcast() as u8);
        match exchange.partitioning() {
            Some(partitioning) => {
                buf.push(1);
                serialize_partitioning(partitioning, self, proto_converter)?
                    .encode(buf)
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Internal(e.to_string())
                    })?;
            }
            None => buf.push(0),
        }
        Ok(())
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        self.inner.try_encode_expr(node, buf, ctx)
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_higher_order_function(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}
