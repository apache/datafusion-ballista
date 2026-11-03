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

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Write};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{
    OrderedRangeRepartitionExec, PartitionedBoundedWindowAggExec, PerPartitionFilterExec,
    PrefixMergeExec, RangeFilterExec, RuntimeStatsExec, UnorderedRangeRepartitionExec,
};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::is_volatile;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, displayable};
use datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalProtoConverterExtension;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use log::debug;
use prost::Message;

use crate::state::aqe::execution_plan::ExchangeExec;

/// Makes every exchange in a set of identical exchanges a reader of one
/// stage. See the module docs.
#[derive(Debug)]
pub struct ReuseExchangeRule {
    codec: Arc<dyn PhysicalExtensionCodec>,
    read_finished_stage: Arc<AtomicBool>,
}

impl ReuseExchangeRule {
    /// A rule that matches subtrees by their encoding under `codec`, the job's
    /// physical extension codec. It raises `read_finished_stage` whenever it
    /// points a pending exchange at a stage that has already finished: that
    /// resolves the exchange after the rules that react to a resolved exchange
    /// have run, so the caller must run the optimizer chain again.
    pub fn new(
        codec: Arc<dyn PhysicalExtensionCodec>,
        read_finished_stage: Arc<AtomicBool>,
    ) -> Self {
        Self {
            codec,
            read_finished_stage,
        }
    }

    /// The key `node` is matched on, or `None` when `node` is not an exchange
    /// that may be merged.
    fn key(&self, node: &Arc<dyn ExecutionPlan>) -> Option<Vec<u8>> {
        let exchange = node.downcast_ref::<ExchangeExec>()?;
        if matches!(exchange.partitioning(), Some(Partitioning::Range(_)))
            || !mergeable(node).ok()?
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

/// The exchanges of one key: how many there are, and the one furthest along.
struct Group {
    exchanges: usize,
    furthest: Arc<dyn ExecutionPlan>,
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
            .map(BallistaConfig::adaptive_reuse_exchange_enabled)
            .unwrap_or_else(|| {
                BallistaConfig::default().adaptive_reuse_exchange_enabled()
            });
        if !enabled {
            return Ok(plan);
        }

        // Encoding a subtree is costly, and most plans repeat nothing. Only
        // exchanges a cheap fingerprint can't tell apart, or that already
        // share a stage handle, are keyed.
        let mut exchanges = Vec::new();
        fingerprints(&plan, &mut exchanges);
        let mut fingerprint_counts: HashMap<u64, usize> = HashMap::new();
        let mut handle_counts: HashMap<*const (), usize> = HashMap::new();
        for (node, fingerprint) in &exchanges {
            *fingerprint_counts.entry(*fingerprint).or_default() += 1;
            *handle_counts
                .entry(as_exchange(node).stage_handle())
                .or_default() += 1;
        }
        let candidates = exchanges.iter().filter(|(node, fingerprint)| {
            fingerprint_counts[fingerprint] > 1
                || handle_counts[&as_exchange(node).stage_handle()] > 1
        });

        // Key each candidate, and find for each key the exchange furthest
        // along, which the others will read from. Keys are interned so the
        // second pass looks them up by exchange instead of encoding again;
        // an exchange's plan id and stage handle survive the rewrites below.
        let mut key_ids: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut groups: Vec<Group> = Vec::new();
        let mut exchange_keys: HashMap<(usize, *const ()), usize> = HashMap::new();
        let mut handle_key: HashMap<*const (), usize> = HashMap::new();
        // Pending stage handles that exchanges of different subtrees share,
        // as `to_broadcast` or an embedder's rewrite of one reader can leave
        // behind. Neither may write for the other.
        let mut conflicted: HashSet<*const ()> = HashSet::new();
        for (node, _) in candidates {
            let Some(key) = self.key(node) else {
                continue;
            };
            let exchange = as_exchange(node);
            let next_id = groups.len();
            let id = *key_ids.entry(key).or_insert(next_id);
            if id == next_id {
                groups.push(Group {
                    exchanges: 0,
                    furthest: Arc::clone(node),
                });
            }
            let group = &mut groups[id];
            group.exchanges += 1;
            if progress(exchange) > progress(as_exchange(&group.furthest)) {
                group.furthest = Arc::clone(node);
            }
            let handle = exchange.stage_handle();
            exchange_keys.insert((exchange.plan_id(), handle), id);
            if handle_key
                .insert(handle, id)
                .is_some_and(|other| other != id)
                && progress(exchange) == Progress::Pending
            {
                conflicted.insert(handle);
            }
        }
        if conflicted.is_empty() && groups.iter().all(|group| group.exchanges < 2) {
            return Ok(plan);
        }

        // Bottom-up, so a nested repeat is merged before the subtrees above.
        let mut canonical: HashMap<usize, Arc<dyn ExecutionPlan>> = HashMap::new();
        let transformed = plan.transform_up(|node| {
            let Some(exchange) = node.downcast_ref::<ExchangeExec>() else {
                return Ok(Transformed::no(node));
            };
            let handle = exchange.stage_handle();
            let Some(&id) = exchange_keys.get(&(exchange.plan_id(), handle)) else {
                return Ok(Transformed::no(node));
            };

            let split = conflicted.contains(&handle);
            let node: Arc<dyn ExecutionPlan> = if split {
                Arc::new(exchange.with_own_stage())
            } else {
                node
            };
            let exchange = as_exchange(&node);

            // The exchange furthest along is read from, even when the walk
            // meets it later: its stage has started, so its subtree no longer
            // changes what it writes. If nothing has run yet, the first met is.
            let target = Arc::clone(canonical.entry(id).or_insert_with(|| {
                let furthest = &groups[id].furthest;
                if progress(as_exchange(furthest)) == Progress::Pending
                    || exchange.shares_stage_with(as_exchange(furthest))
                {
                    Arc::clone(&node)
                } else {
                    Arc::clone(furthest)
                }
            }));
            let target = as_exchange(&target);

            // Already a reader of that stage, or running a stage of its own,
            // which is never re-pointed.
            if exchange.shares_stage_with(target)
                || progress(exchange) != Progress::Pending
            {
                return Ok(Transformed::new_transformed(node, split));
            }
            debug!(
                "exchange reuse: exchange {} reads the stage of exchange {}",
                exchange.plan_id(),
                target.plan_id()
            );
            if progress(target) == Progress::Resolved {
                self.read_finished_stage.store(true, Ordering::Relaxed);
            }
            Ok(Transformed::yes(Arc::new(exchange.share_stage_of(target))))
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

/// Whether nothing in `plan` rules out sharing its stage: no volatile
/// expression and no single-consumer operator.
fn mergeable(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    let excluded = plan.exists(|node| {
        if requires_single_consumer(node.as_ref()) {
            return Ok(true);
        }
        let mut volatile = false;
        node.apply_expressions(&mut |expr| {
            volatile = is_volatile(expr);
            Ok(if volatile {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(volatile)
    })?;
    Ok(!excluded)
}

/// Operators whose stage the scheduler finishes by reading state off the
/// stage's single consumer. See the module docs.
fn requires_single_consumer(plan: &dyn ExecutionPlan) -> bool {
    plan.is::<OrderedRangeRepartitionExec>()
        || plan.is::<UnorderedRangeRepartitionExec>()
        || plan.is::<RangeFilterExec>()
        || plan.is::<PerPartitionFilterExec>()
        || plan.is::<PrefixMergeExec>()
        || plan.is::<PartitionedBoundedWindowAggExec>()
        || plan.is::<RuntimeStatsExec>()
}

/// Pushes every exchange in `plan` onto `exchanges`, children first, with a
/// hash of its subtree that two subtrees with one key always share, though
/// different subtrees may share it too. Returns the hash of `plan`. Each node
/// contributes its one-line display, except that an exchange contributes only
/// what it writes, so stage ids never reach the hash.
fn fingerprints(
    plan: &Arc<dyn ExecutionPlan>,
    exchanges: &mut Vec<(Arc<dyn ExecutionPlan>, u64)>,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    match plan.downcast_ref::<ExchangeExec>() {
        Some(exchange) => {
            "ExchangeExec".hash(&mut hasher);
            exchange.is_broadcast().hash(&mut hasher);
            exchange
                .partitioning()
                .map(|partitioning| partitioning.to_string())
                .hash(&mut hasher);
        }
        None => {
            let _ = write!(
                HashWriter(&mut hasher),
                "{}",
                displayable(plan.as_ref()).one_line()
            );
        }
    }
    for child in plan.children() {
        fingerprints(child, exchanges).hash(&mut hasher);
    }
    let fingerprint = hasher.finish();
    if plan.is::<ExchangeExec>() {
        exchanges.push((Arc::clone(plan), fingerprint));
    }
    fingerprint
}

/// Feeds formatted text straight into a hasher.
struct HashWriter<'a>(&'a mut DefaultHasher);

impl fmt::Write for HashWriter<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.write(s.as_bytes());
        Ok(())
    }
}

fn as_exchange(plan: &Arc<dyn ExecutionPlan>) -> &ExchangeExec {
    plan.downcast_ref::<ExchangeExec>()
        .expect("only exchanges are keyed")
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
