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

//! Bounded, mergeable runtime filters transported through task completion.
//!
//! A build tap publishes only after every local input partition reaches EOF.
//! Missing/partial reports are never interpreted as empty filters. The scheduler
//! unions all successful producer tasks before specializing a probe stage.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::hash_utils::{RandomState, with_hashes};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;

use crate::serde::protobuf::RuntimeFilterPayload;

const BLOOM_BYTES: usize = 8192;
const MAX_VALUES: usize = 128;
const MAX_BOUND_BYTES: usize = 128;
const VERSION: u32 = 2;
const HASH_STATE: RandomState = RandomState::with_seed(0);

/// Supported keys have identical equality and ordering on both sides of a join.
/// Floats (NaN/signed zero), dictionaries, nested and extension types are excluded.
pub fn supports_key(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// A Bloom filter with a bounded IN hash set and optional range summary.
/// Hash collisions retain extra candidates; the join verifies actual equality.
#[derive(Clone, Debug)]
pub struct RuntimeFilter {
    bloom: Vec<u8>,
    has_null: bool,
    in_hashes: Option<BTreeSet<u64>>,
    bounds: Option<(ScalarValue, ScalarValue)>,
    bounds_disabled: bool,
}

impl Default for RuntimeFilter {
    fn default() -> Self {
        Self {
            bloom: vec![0; BLOOM_BYTES],
            has_null: false,
            in_hashes: Some(BTreeSet::new()),
            bounds: None,
            bounds_disabled: false,
        }
    }
}

fn bloom_bits(hash: u64) -> impl Iterator<Item = usize> {
    let step = hash.rotate_left(31) | 1;
    (0..4u64).map(move |i| {
        hash.wrapping_add(i.wrapping_mul(step)) as usize % (BLOOM_BYTES * 8)
    })
}

impl RuntimeFilter {
    fn update(&mut self, keys: &ArrayRef) -> Result<()> {
        self.has_null |= keys.null_count() > 0;
        with_hashes([keys], &HASH_STATE, |hashes| {
            for (row, &hash) in hashes.iter().enumerate() {
                if keys.is_null(row) {
                    continue;
                }
                for bit in bloom_bits(hash) {
                    self.bloom[bit / 8] |= 1 << (bit % 8);
                }
                if let Some(values) = &mut self.in_hashes {
                    values.insert(hash);
                    if values.len() > MAX_VALUES {
                        self.in_hashes = None;
                    }
                }
            }
            Ok(())
        })?;
        if !self.bounds_disabled && keys.null_count() != keys.len() {
            let mut min = MinAccumulator::try_new(keys.data_type())?;
            let mut max = MaxAccumulator::try_new(keys.data_type())?;
            min.update_batch(std::slice::from_ref(keys))?;
            max.update_batch(std::slice::from_ref(keys))?;
            let (min, max) = (min.evaluate()?, max.evaluate()?);
            if min.size() > MAX_BOUND_BYTES || max.size() > MAX_BOUND_BYTES {
                self.bounds = None;
                self.bounds_disabled = true;
            } else {
                self.merge_bounds(&min, &max);
            }
        }
        Ok(())
    }

    fn merge_bounds(&mut self, lo: &ScalarValue, hi: &ScalarValue) {
        match &mut self.bounds {
            Some((min, max)) => {
                if lo < min {
                    *min = lo.clone();
                }
                if hi > max {
                    *max = hi.clone();
                }
            }
            None => self.bounds = Some((lo.clone(), hi.clone())),
        }
    }

    // Hash collisions are harmless: the join still verifies equality. NULL is
    // retained for null-safe equality whenever a producer observed one.
    fn contains(&self, hash: u64, is_null: bool) -> bool {
        if is_null {
            return self.has_null;
        }
        if let Some(values) = &self.in_hashes {
            return values.contains(&hash);
        }
        bloom_bits(hash).all(|bit| self.bloom[bit / 8] & (1 << (bit % 8)) != 0)
    }

    /// Union another complete producer summary into this filter.
    pub fn merge(&mut self, other: &Self) {
        for (a, b) in self.bloom.iter_mut().zip(&other.bloom) {
            *a |= b;
        }
        self.has_null |= other.has_null;
        match (&mut self.in_hashes, &other.in_hashes) {
            (Some(values), Some(more)) => {
                values.extend(more);
                if values.len() > MAX_VALUES {
                    self.in_hashes = None;
                }
            }
            _ => self.in_hashes = None,
        }
        self.bounds_disabled |= other.bounds_disabled;
        if self.bounds_disabled {
            self.bounds = None;
        } else if let Some((lo, hi)) = &other.bounds {
            self.merge_bounds(lo, hi);
        }
    }

    /// Encode the bounded summary with its job-local identity.
    pub fn to_proto(&self, filter_id: u64) -> Result<RuntimeFilterPayload> {
        Ok(RuntimeFilterPayload {
            filter_id,
            version: VERSION,
            bloom: self.bloom.clone(),
            has_null: self.has_null,
            has_in_set: self.in_hashes.is_some(),
            in_hashes: self
                .in_hashes
                .iter()
                .flat_map(|hashes| hashes.iter().copied())
                .collect(),
            min: self
                .bounds
                .as_ref()
                .map(|(v, _)| v.try_into())
                .transpose()?,
            max: self
                .bounds
                .as_ref()
                .map(|(_, v)| v.try_into())
                .transpose()?,
        })
    }

    /// Decode a task report, checking its wire version and fixed Bloom layout.
    pub fn from_proto(payload: &RuntimeFilterPayload) -> Result<Self> {
        if payload.version != VERSION || payload.bloom.len() != BLOOM_BYTES {
            return internal_err!("Invalid runtime filter payload");
        }
        let bounds = match (&payload.min, &payload.max) {
            (Some(min), Some(max)) => Some((min.try_into()?, max.try_into()?)),
            (None, None) => None,
            _ => return internal_err!("Incomplete runtime filter bounds"),
        };
        Ok(Self {
            bloom: payload.bloom.clone(),
            has_null: payload.has_null,
            in_hashes: payload
                .has_in_set
                .then(|| payload.in_hashes.iter().copied().collect()),
            // Empty/all-NULL producers are the identity for range union. A
            // nonempty Bloom without bounds means a large bound was omitted.
            bounds_disabled: bounds.is_none()
                && payload.bloom.iter().any(|&byte| byte != 0),
            bounds,
        })
    }
}

/// Pass-through tap on the build input, below its shuffle writer.
#[derive(Debug)]
pub struct RuntimeFilterBuildExec {
    input: Arc<dyn ExecutionPlan>,
    /// Identity assigned by the scheduler, unique within the job.
    pub filter_id: u64,
    /// Join key evaluated in the input schema.
    pub key: Arc<dyn PhysicalExpr>,
    completed: Arc<Mutex<BTreeMap<usize, RuntimeFilter>>>,
}

impl RuntimeFilterBuildExec {
    /// Tap a supported join key; the planner selects supported types.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        filter_id: u64,
        key: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            input,
            filter_id,
            key,
            completed: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn report(&self) -> Result<Option<RuntimeFilterPayload>> {
        let completed = self.completed.lock();
        if completed.len() != self.input.output_partitioning().partition_count() {
            return Ok(None);
        }
        let mut merged = RuntimeFilter::default();
        for filter in completed.values() {
            merged.merge(filter);
        }
        merged.to_proto(self.filter_id).map(Some)
    }
}

impl DisplayAs for RuntimeFilterBuildExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RuntimeFilterBuildExec: id={}, key={}",
            self.filter_id, self.key
        )
    }
}

impl ExecutionPlan for RuntimeFilterBuildExec {
    fn name(&self) -> &str {
        "RuntimeFilterBuildExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        f(&self.key)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.filter_id,
            self.key.clone(),
        )))
    }
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        datafusion::physical_plan::statistics::StatisticsContext::new().compute(
            self.input.as_ref(),
            &datafusion::physical_plan::StatisticsArgs::new().with_partition(partition),
        )
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input = self.input.execute(partition, ctx)?;
        let key = self.key.clone();
        let completed = self.completed.clone();
        let stream = futures::stream::try_unfold(
            (input, RuntimeFilter::default()),
            move |(mut input, mut filter)| {
                let key = key.clone();
                let completed = completed.clone();
                async move {
                    match input.try_next().await? {
                        Some(batch) => {
                            let keys =
                                key.evaluate(&batch)?.into_array(batch.num_rows())?;
                            filter.update(&keys)?;
                            Ok(Some((batch, (input, filter))))
                        }
                        None => {
                            completed.lock().insert(partition, filter);
                            Ok(None)
                        }
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Probe-side filtering. A missing payload passes through without evaluating
/// the key; a complete payload is fixed for the lifetime of the task.
#[derive(Debug)]
pub struct RuntimeFilterApplyExec {
    input: Arc<dyn ExecutionPlan>,
    /// Identity assigned by the scheduler, unique within the job.
    pub filter_id: u64,
    /// Join key evaluated in the input schema.
    pub key: Arc<dyn PhysicalExpr>,
    /// Complete merged summary, or None for pass-through.
    pub payload: Option<RuntimeFilterPayload>,
    filter: Option<Arc<RuntimeFilter>>,
    metrics: ExecutionPlanMetricsSet,
}

impl RuntimeFilterApplyExec {
    /// Create an operator for a supported equality key.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        filter_id: u64,
        key: Arc<dyn PhysicalExpr>,
        payload: Option<RuntimeFilterPayload>,
    ) -> Result<Self> {
        let filter = payload
            .as_ref()
            .map(|payload| RuntimeFilter::from_proto(payload).map(Arc::new))
            .transpose()?;
        Ok(Self {
            input,
            filter_id,
            key,
            payload,
            filter,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for RuntimeFilterApplyExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RuntimeFilterApplyExec: id={}, key={}, ready={}",
            self.filter_id,
            self.key,
            self.filter.is_some()
        )
    }
}

impl ExecutionPlan for RuntimeFilterApplyExec {
    fn name(&self) -> &str {
        "RuntimeFilterApplyExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        f(&self.key)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.filter_id,
            self.key.clone(),
            self.payload.clone(),
        )?))
    }
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let stats = datafusion::physical_plan::statistics::StatisticsContext::new()
            .compute(
                self.input.as_ref(),
                &datafusion::physical_plan::StatisticsArgs::new()
                    .with_partition(partition),
            )?;
        Ok(if self.filter.is_some() {
            Arc::new(stats.as_ref().clone().to_inexact())
        } else {
            stats
        })
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, ctx)?;
        let Some(filter) = self.filter.clone() else {
            return Ok(input);
        };
        let key = self.key.clone();
        let pruned = MetricBuilder::new(&self.metrics).counter("rows_pruned", partition);
        let stream = input.map(move |batch| {
            let batch = batch?;
            let keys = key.evaluate(&batch)?.into_array(batch.num_rows())?;
            let keep = with_hashes([&keys], &HASH_STATE, |hashes| {
                Ok(BooleanArray::from_iter(hashes.iter().enumerate().map(
                    |(row, &hash)| Some(filter.contains(hash, keys.is_null(row))),
                )))
            })?;
            let filtered = filter_record_batch(&batch, &keep)?;
            pruned.add(batch.num_rows() - filtered.num_rows());
            Ok(filtered)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Harvest only complete build taps from the actually executed task plan.
pub fn collect_reports(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<RuntimeFilterPayload>> {
    let mut reports = Vec::new();
    plan.apply(|node| {
        if let Some(build) = node.downcast_ref::<RuntimeFilterBuildExec>()
            && let Some(report) = build.report()?
        {
            reports.push(report);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(reports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::BallistaPhysicalExtensionCodec;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringViewArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::{collect, common::collect as collect_stream};
    use datafusion::prelude::SessionContext;
    use datafusion_proto::physical_plan::{
        DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    };
    use prost::Message;

    fn input(partitions: Vec<Vec<Option<&str>>>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Utf8View,
            true,
        )]));
        let batches = partitions
            .into_iter()
            .map(|values| {
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(StringViewArray::from(values))],
                    )
                    .unwrap(),
                ]
            })
            .collect::<Vec<_>>();
        MemorySourceConfig::try_new_exec(&batches, schema, None).unwrap()
    }

    fn roundtrip(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let codec = BallistaPhysicalExtensionCodec::default();
        let mut bytes = Vec::new();
        codec
            .try_encode(plan.clone(), &mut bytes, &DefaultPhysicalProtoConverter {})
            .unwrap();
        codec
            .try_decode(
                &bytes,
                &[plan.children()[0].clone()],
                &SessionContext::new().task_ctx(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap()
    }

    #[tokio::test]
    async fn runtime_filter_complete_partitions_survive_wire_and_prune() {
        let ctx = SessionContext::new().task_ctx();
        let long_key = "z".repeat(256);
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            input(vec![
                vec![Some("b"), None],
                vec![Some(long_key.as_str()), Some("b")],
            ]),
            7,
            Arc::new(Column::new("key", 0)),
        ));
        let build = roundtrip(&build);
        collect_stream(build.execute(0, ctx.clone()).unwrap())
            .await
            .unwrap();
        assert!(
            collect_reports(&build).unwrap().is_empty(),
            "partial build must never publish"
        );
        collect_stream(build.execute(1, ctx.clone()).unwrap())
            .await
            .unwrap();
        let payload = collect_reports(&build).unwrap().pop().unwrap();
        let payload =
            RuntimeFilterPayload::decode(payload.encode_to_vec().as_slice()).unwrap();
        let probe: Arc<dyn ExecutionPlan> = Arc::new(
            RuntimeFilterApplyExec::try_new(
                input(vec![vec![
                    Some("a"),
                    Some("b"),
                    Some(long_key.as_str()),
                    Some("zz"),
                    None,
                ]]),
                7,
                Arc::new(Column::new("key", 0)),
                Some(payload),
            )
            .unwrap(),
        );
        let probe = roundtrip(&probe);
        let result = collect(probe.clone(), ctx).await.unwrap();
        let keys = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(
            keys,
            &StringViewArray::from(vec![Some("b"), Some(long_key.as_str()), None])
        );
        assert_eq!(
            probe
                .metrics()
                .unwrap()
                .sum_by_name("rows_pruned")
                .unwrap()
                .as_usize(),
            2
        );
    }

    #[test]
    fn runtime_filter_union_switches_to_bloom_without_false_negatives() {
        let mut merged = RuntimeFilter::default();
        for partition in 0..3 {
            let mut filter = RuntimeFilter::default();
            filter
                .update(
                    &(Arc::new(Int64Array::from_iter_values(
                        partition * 100..(partition + 1) * 100,
                    )) as ArrayRef),
                )
                .unwrap();
            merged
                .merge(&RuntimeFilter::from_proto(&filter.to_proto(4).unwrap()).unwrap());
        }
        let empty = RuntimeFilter::default().to_proto(4).unwrap();
        merged.merge(&RuntimeFilter::from_proto(&empty).unwrap());
        let proto = merged.to_proto(4).unwrap();
        assert_eq!(
            ScalarValue::try_from(proto.min.as_ref().unwrap()).unwrap(),
            ScalarValue::Int64(Some(0))
        );
        assert_eq!(
            ScalarValue::try_from(proto.max.as_ref().unwrap()).unwrap(),
            ScalarValue::Int64(Some(299))
        );
        assert!(!proto.has_in_set);
        assert!(proto.encode_to_vec().len() < 9000);
        let decoded = RuntimeFilter::from_proto(&proto).unwrap();
        let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(0..300));
        with_hashes([&keys], &HASH_STATE, |hashes| {
            assert!(hashes.iter().all(|&hash| decoded.contains(hash, false)));
            assert!(
                hashes
                    .iter()
                    .all(|&hash| !RuntimeFilter::default().contains(hash, false))
            );
            Ok(())
        })
        .unwrap();
    }
}
