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
//! [`ExchangeExec`]s. Without reuse each becomes its own stage and the subplan
//! runs twice. Reuse happens when a stage is created, as Spark's AQE does it
//! with its stage cache: [`AdaptivePlanner`] keys each exchange it is about to
//! launch with [`reuse_key`] and, when an earlier stage has the same key, makes
//! the exchange another reader of that stage with
//! [`ExchangeExec::share_stage_of`] instead of launching a new one.
//!
//! Matching at launch keys each stage once, on exactly the plan that runs,
//! after every rule of the replan chain, an embedder's included. Readers can't
//! diverge afterwards, since what a stage writes is fixed once it launches.
//!
//! # Matching
//!
//! An exchange's key is a digest of the protobuf encoding of its subtree
//! under the job's physical extension codec, the encoding that ships stages to executors, so
//! two subtrees match only when an executor could not tell them apart. An
//! exchange being launched reads only finished stages, and a nested exchange
//! is encoded by the stage it reads, so two subtrees match only when they read
//! the same stages. A subtree the codec cannot encode gets no key.
//!
//! # What is never merged
//!
//! - A subtree with a volatile expression such as `random()`: it encodes like
//!   its twin but has to produce values of its own.
//! - Range repartitioning and the window rewrites built on it. The scheduler
//!   finishes those stages by reading state off the one consumer of the
//!   stage, so a second consumer is not supported.
//!
//! [`AdaptivePlanner`]: crate::state::aqe::planner::AdaptivePlanner

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use ballista_core::execution_plans::{
    OrderedRangeRepartitionExec, PartitionedBoundedWindowAggExec, PerPartitionFilterExec,
    PrefixMergeExec, RangeFilterExec, RuntimeStatsExec, UnorderedRangeRepartitionExec,
};
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::is_volatile;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, replace_children_if_necessary,
};
use datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalProtoConverterExtension;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use prost::Message;

use crate::state::aqe::execution_plan::ExchangeExec;

/// What a stage is matched on: a digest of its encoding. See the module docs.
pub(crate) type ReuseKey = [u8; 32];

/// Why the stage of an exchange may not be shared. See the module docs.
#[derive(Debug)]
pub(crate) enum Unshareable {
    /// The exchange range-partitions its output.
    RangePartitioned,
    /// The named operator of the stage has a volatile expression.
    Volatile(String),
    /// The named operator of the stage needs the stage to have one consumer.
    SingleConsumer(String),
    /// The stage could not be keyed, such as when the codec can't encode it.
    NoKey(DataFusionError),
}

impl Display for Unshareable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::RangePartitioned => write!(f, "it range-partitions its output"),
            Self::Volatile(name) => write!(f, "{name} has a volatile expression"),
            Self::SingleConsumer(name) => {
                write!(f, "{name} needs its stage to have one consumer")
            }
            Self::NoKey(e) => write!(f, "its stage could not be keyed: {e}"),
        }
    }
}

/// The key `node`, an exchange about to launch its stage, is matched on, or
/// why its stage may not be shared. See the module docs.
pub(crate) fn reuse_key(
    node: &Arc<dyn ExecutionPlan>,
    codec: &Arc<dyn PhysicalExtensionCodec>,
) -> std::result::Result<ReuseKey, Unshareable> {
    let exchange = node.downcast_ref::<ExchangeExec>().ok_or_else(|| {
        Unshareable::NoKey(internal_datafusion_err!(
            "{} is not an exchange",
            node.name()
        ))
    })?;
    if matches!(exchange.partitioning(), Some(Partitioning::Range(_))) {
        return Err(Unshareable::RangePartitioned);
    }
    // One walk over the stage rules out what may not be shared, and swaps
    // each exchange it reads for one over an empty input, so the stages below
    // are encoded by their stage ids alone instead of being encoded again.
    let mut unshareable = None;
    let input = Arc::clone(exchange.input())
        .transform_down(|node| {
            if let Some(read) = node.downcast_ref::<ExchangeExec>() {
                let empty: Arc<dyn ExecutionPlan> =
                    Arc::new(EmptyExec::new(read.input().schema()));
                let leaf = replace_children_if_necessary(Arc::clone(&node), vec![empty])?;
                return Ok(Transformed::new(leaf, true, TreeNodeRecursion::Jump));
            }
            if requires_single_consumer(node.as_ref()) {
                unshareable = Some(Unshareable::SingleConsumer(node.name().to_owned()));
            } else if has_volatile_expr(node.as_ref())? {
                unshareable = Some(Unshareable::Volatile(node.name().to_owned()));
            } else {
                return Ok(Transformed::no(node));
            }
            Ok(Transformed::new(node, false, TreeNodeRecursion::Stop))
        })
        .map_err(Unshareable::NoKey)?
        .data;
    if let Some(unshareable) = unshareable {
        return Err(unshareable);
    }
    let node = replace_children_if_necessary(Arc::clone(node), vec![input])
        .map_err(Unshareable::NoKey)?;
    let codec = KeyCodec {
        inner: Arc::clone(codec),
    };
    // Encodings run to the size of the stage plan, file lists included, and
    // a key is kept for every stage of the job, so only a 256-bit digest of
    // it is kept.
    let bytes = physical_plan_to_bytes_with_extension_codec(node, &codec)
        .map_err(Unshareable::NoKey)?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

/// Whether `node` has a volatile expression such as `random()`.
fn has_volatile_expr(node: &dyn ExecutionPlan) -> Result<bool> {
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

/// Encodes a subtree for matching only, never for decoding. An
/// [`ExchangeExec`] is encoded by what it writes and the stage that writes
/// it; everything else goes to the job's codec.
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
        // The stage a nested exchange reads. The exchange being keyed has
        // none yet.
        buf.extend_from_slice(
            &exchange.stage_id().map_or(-1, |id| id as i64).to_le_bytes(),
        );
        match exchange.partitioning() {
            Some(partitioning) => {
                buf.push(1);
                serialize_partitioning(partitioning, self, proto_converter)?
                    .encode(buf)
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?;
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

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}
