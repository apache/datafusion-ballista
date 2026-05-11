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

//! Hash-partition (bucketing) metadata for distributed query optimization.
//!
//! Pinot-style colocated joins, sub-partitioning, and small-side broadcast
//! all need to know whether a table's data is already hash-bucketed by some
//! column. This module defines the contract by which a [`TableProvider`] can
//! declare that bucketing, plus a thin wrapper that lets users attach the
//! metadata to any existing provider without writing a custom one.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Constraints, Statistics};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr,
    PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

/// Hash function used to bucket a table's rows on disk.
///
/// The optimizer uses this to verify that two co-located inputs were bucketed
/// by the same function before eliding a shuffle. Crucially, it does **not**
/// have to match DataFusion's internal `RepartitionExec` hasher — the
/// declaration is a promise about the on-disk layout, and the colocated-join
/// rule trusts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HashFn {
    /// Murmur3 (32-bit), as used by Spark/Hive bucketing.
    Murmur3,
    /// xxHash64.
    XxHash64,
}

impl fmt::Display for HashFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HashFn::Murmur3 => write!(f, "murmur3"),
            HashFn::XxHash64 => write!(f, "xxhash64"),
        }
    }
}

/// Declares that a table is hash-bucketed across `num_buckets` partitions on
/// the columns named in `keys`, using `hash_fn`.
///
/// Two tables are *co-located* for a join when they share the same `keys`
/// (matching join keys positionally), the same `hash_fn`, and the same
/// `num_buckets` — bucket *k* of one matches bucket *k* of the other.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HashDistribution {
    /// Column names used as partition keys.
    pub keys: Vec<String>,
    /// Hash function used to assign rows to buckets.
    pub hash_fn: HashFn,
    /// Number of buckets.
    pub num_buckets: usize,
}

impl HashDistribution {
    /// Construct a new hash distribution descriptor.
    pub fn new(keys: Vec<String>, hash_fn: HashFn, num_buckets: usize) -> Self {
        Self {
            keys,
            hash_fn,
            num_buckets,
        }
    }
}

/// Optional trait that a [`TableProvider`] can implement to advertise its
/// hash-partition layout to the Ballista physical optimizer.
///
/// The optimizer downcasts each scan's underlying provider to this trait via
/// [`std::any::Any`]. If the downcast succeeds and the returned distribution
/// matches a join's required hash partitioning, the planner can elide the
/// shuffle.
pub trait BallistaPartitionMetadata: Send + Sync {
    /// Returns the hash distribution of this table, or `None` if the table is
    /// not bucketed.
    fn hash_distribution(&self) -> Option<HashDistribution>;
}

/// Convenience wrapper that attaches a [`HashDistribution`] to any existing
/// [`TableProvider`].
///
/// All `TableProvider` methods delegate to the inner provider; the only
/// extras are (a) the [`BallistaPartitionMetadata`] impl and (b) an
/// adapter that re-advertises the scan's `output_partitioning()` as
/// [`Partitioning::Hash`] so downstream optimizer rules can see it.
///
/// The caller is responsible for ensuring that the inner provider produces
/// exactly `distribution.num_buckets` partitions, with file group *k*
/// containing bucket *k*. The standard Spark/Hive convention of
/// `part-NNNNN-…` filenames satisfies this when files are sorted by name.
#[derive(Debug)]
pub struct PartitionedTableProvider {
    inner: Arc<dyn TableProvider>,
    distribution: HashDistribution,
}

impl PartitionedTableProvider {
    /// Wrap an existing provider and declare its hash distribution.
    pub fn new(
        inner: Arc<dyn TableProvider>,
        distribution: HashDistribution,
    ) -> Self {
        Self {
            inner,
            distribution,
        }
    }

    /// Returns the wrapped provider.
    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

impl BallistaPartitionMetadata for PartitionedTableProvider {
    fn hash_distribution(&self) -> Option<HashDistribution> {
        Some(self.distribution.clone())
    }
}

#[async_trait]
impl TableProvider for PartitionedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_column_default(
        &self,
        column: &str,
    ) -> Option<&datafusion::logical_expr::Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner_plan = self.inner.scan(state, projection, filters, limit).await?;
        HashDistributedScanExec::try_new(inner_plan, self.distribution.clone())
            .map(|exec| Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
}

/// A passthrough [`ExecutionPlan`] adapter that overrides `output_partitioning`
/// to advertise a known [`HashDistribution`].
///
/// The inner plan is executed unchanged — this node only edits its
/// [`PlanProperties`] so optimizer rules looking at `output_partitioning()`
/// see `Partitioning::Hash(keys, num_buckets)`.
///
/// Resolving the hash key column names to physical [`Column`] expressions
/// requires the inner schema, which is why construction can fail if a
/// declared key is not present after projection.
#[derive(Debug)]
pub struct HashDistributedScanExec {
    inner: Arc<dyn ExecutionPlan>,
    distribution: HashDistribution,
    properties: Arc<PlanProperties>,
}

impl HashDistributedScanExec {
    /// Wrap an inner scan plan and re-advertise its partitioning.
    ///
    /// Returns an error if any column in `distribution.keys` is missing from
    /// the inner plan's output schema (e.g., projected away).
    pub fn try_new(
        inner: Arc<dyn ExecutionPlan>,
        distribution: HashDistribution,
    ) -> Result<Self> {
        let schema = inner.schema();
        let key_exprs = resolve_key_columns(&schema, &distribution.keys)?;

        let inner_props = inner.properties();
        let eq_properties = EquivalenceProperties::new(schema.clone());

        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::Hash(key_exprs, distribution.num_buckets),
            inner_props.emission_type,
            inner_props.boundedness,
        ));

        Ok(Self {
            inner,
            distribution,
            properties,
        })
    }

    /// The hash distribution this adapter declares.
    pub fn distribution(&self) -> &HashDistribution {
        &self.distribution
    }
}

impl BallistaPartitionMetadata for HashDistributedScanExec {
    fn hash_distribution(&self) -> Option<HashDistribution> {
        Some(self.distribution.clone())
    }
}

impl DisplayAs for HashDistributedScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "HashDistributedScanExec: keys=[{}], hash_fn={}, buckets={}",
                    self.distribution.keys.join(","),
                    self.distribution.hash_fn,
                    self.distribution.num_buckets,
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "buckets={} hash_fn={}",
                    self.distribution.num_buckets, self.distribution.hash_fn,
                )
            }
        }
    }
}

impl ExecutionPlan for HashDistributedScanExec {
    fn name(&self) -> &str {
        "HashDistributedScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Plan(
                "HashDistributedScanExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::try_new(child, self.distribution.clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
    }
}

fn resolve_key_columns(
    schema: &SchemaRef,
    keys: &[String],
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    keys.iter()
        .map(|name| {
            Column::new_with_schema(name, schema)
                .map(|col| Arc::new(col) as Arc<dyn PhysicalExpr>)
        })
        .collect::<Result<Vec<_>>>()
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "hash distribution key not found in scan output: {e}"
            ))
        })
}

/// Per-partition concat that re-buckets an already-bucketed plan into a
/// smaller (divisor) bucket count without a network shuffle.
///
/// When two co-located inputs were bucketed by the same key and same hash
/// function but different bucket counts (e.g., 16 vs 8), the larger side can
/// be locally coalesced so its partitioning matches the smaller side. This is
/// safe because, for any row key `k`, `(hash(k) % 16) % 8 == hash(k) % 8` —
/// so input partitions `[i, i+8]` of the 16-bucket side both belong in
/// output partition `i` of the 8-bucket projection.
///
/// The exec is a pure remapping: it does not move data across executors, it
/// just chains record batches from the relevant input partitions.
#[derive(Debug)]
pub struct BucketSubPartitionExec {
    inner: Arc<dyn ExecutionPlan>,
    output_distribution: HashDistribution,
    properties: Arc<PlanProperties>,
}

impl BucketSubPartitionExec {
    /// Wrap `inner` to emit `output_distribution.num_buckets` partitions.
    ///
    /// `output_distribution.num_buckets` must evenly divide
    /// `inner.output_partitioning().partition_count()`.
    pub fn try_new(
        inner: Arc<dyn ExecutionPlan>,
        output_distribution: HashDistribution,
    ) -> Result<Self> {
        let inner_count = inner.properties().output_partitioning().partition_count();
        let out_count = output_distribution.num_buckets;
        if out_count == 0 || inner_count == 0 || !inner_count.is_multiple_of(out_count) {
            return Err(DataFusionError::Plan(format!(
                "BucketSubPartitionExec requires output buckets to divide input \
                 partitions; got input={inner_count}, output={out_count}",
            )));
        }

        let schema = inner.schema();
        let key_exprs = resolve_key_columns(&schema, &output_distribution.keys)?;
        let inner_props = inner.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::Hash(key_exprs, out_count),
            inner_props.emission_type,
            inner_props.boundedness,
        ));
        Ok(Self {
            inner,
            output_distribution,
            properties,
        })
    }

    /// The hash distribution this exec advertises.
    pub fn output_distribution(&self) -> &HashDistribution {
        &self.output_distribution
    }

    /// Number of input partitions per output partition.
    pub fn coalesce_factor(&self) -> usize {
        self.inner.properties().output_partitioning().partition_count()
            / self.output_distribution.num_buckets
    }
}

impl BallistaPartitionMetadata for BucketSubPartitionExec {
    fn hash_distribution(&self) -> Option<HashDistribution> {
        Some(self.output_distribution.clone())
    }
}

impl DisplayAs for BucketSubPartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "BucketSubPartitionExec: out_buckets={}, factor={}",
                    self.output_distribution.num_buckets,
                    self.coalesce_factor(),
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "out_buckets={} factor={}",
                    self.output_distribution.num_buckets,
                    self.coalesce_factor(),
                )
            }
        }
    }
}

impl ExecutionPlan for BucketSubPartitionExec {
    fn name(&self) -> &str {
        "BucketSubPartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Plan(
                "BucketSubPartitionExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::try_new(child, self.output_distribution.clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let inner_count =
            self.inner.properties().output_partitioning().partition_count();
        let stride = self.output_distribution.num_buckets;
        let mut input_streams = Vec::with_capacity(self.coalesce_factor());
        let mut idx = partition;
        while idx < inner_count {
            input_streams.push(self.inner.execute(idx, Arc::clone(&context))?);
            idx += stride;
        }
        let chained = futures::stream::iter(input_streams).flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            chained,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    fn sample_table() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        // Two batches → two partitions, mimicking a 2-bucket layout.
        Arc::new(
            MemTable::try_new(schema, vec![vec![batch.clone()], vec![batch]]).unwrap(),
        )
    }

    #[tokio::test]
    async fn provider_advertises_hash_distribution() {
        let inner = sample_table();
        let dist = HashDistribution::new(vec!["id".into()], HashFn::Murmur3, 2);
        let provider = PartitionedTableProvider::new(inner, dist.clone());

        assert_eq!(provider.hash_distribution(), Some(dist.clone()));

        let ctx = SessionContext::new();
        let plan = provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should succeed");

        match plan.properties().output_partitioning() {
            Partitioning::Hash(keys, n) => {
                assert_eq!(*n, 2);
                assert_eq!(keys.len(), 1);
                let col = keys[0]
                    .as_any()
                    .downcast_ref::<Column>()
                    .expect("expected Column expression");
                assert_eq!(col.name(), "id");
            }
            other => panic!("expected Hash partitioning, got {other:?}"),
        }

        let any_provider: &dyn Any = &provider;
        assert!(any_provider.downcast_ref::<PartitionedTableProvider>().is_some());

        let scan_meta = plan
            .as_any()
            .downcast_ref::<HashDistributedScanExec>()
            .expect("scan should be wrapped in HashDistributedScanExec");
        assert_eq!(scan_meta.hash_distribution().as_ref(), Some(&dist));
    }

    #[tokio::test]
    async fn missing_key_column_is_an_error() {
        let inner = sample_table();
        let dist = HashDistribution::new(vec!["nope".into()], HashFn::XxHash64, 4);
        let provider = PartitionedTableProvider::new(inner, dist);
        let ctx = SessionContext::new();
        let err = provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect_err("scan should fail when key column missing");
        assert!(
            err.to_string().contains("hash distribution key not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn projection_removing_key_is_an_error() {
        let inner = sample_table();
        let dist = HashDistribution::new(vec!["id".into()], HashFn::Murmur3, 2);
        let provider = PartitionedTableProvider::new(inner, dist);
        let ctx = SessionContext::new();
        // Project away `id`; only `name` remains.
        let err = provider
            .scan(&ctx.state(), Some(&vec![1]), &[], None)
            .await
            .expect_err("scan should fail when key projected away");
        assert!(err.to_string().contains("hash distribution key not found"));
    }

    async fn multi_partition_table(
        partitions: usize,
        rows_per_partition: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let parts: Vec<Vec<RecordBatch>> = (0..partitions)
            .map(|p| {
                let arr = Arc::new(Int32Array::from_iter_values(
                    (0..rows_per_partition).map(|r| (p * 1000 + r) as i32),
                ));
                vec![RecordBatch::try_new(schema.clone(), vec![arr]).unwrap()]
            })
            .collect();
        let provider = Arc::new(MemTable::try_new(schema, parts).unwrap());
        let ctx = SessionContext::new();
        provider.scan(&ctx.state(), None, &[], None).await.unwrap()
    }

    #[tokio::test]
    async fn sub_partition_chains_input_partitions() {
        use datafusion::execution::context::TaskContext;
        use futures::TryStreamExt;

        // 6 input partitions, chain into 3 outputs (factor=2).
        // Output 0 reads inputs [0, 3]; output 1 reads [1, 4]; output 2 reads [2, 5].
        let inner = multi_partition_table(6, 2).await;
        let dist = HashDistribution::new(vec!["id".into()], HashFn::Murmur3, 3);
        let exec = BucketSubPartitionExec::try_new(inner, dist).unwrap();

        match exec.properties().output_partitioning() {
            Partitioning::Hash(_, n) => assert_eq!(*n, 3),
            other => panic!("expected Hash, got {other:?}"),
        }
        assert_eq!(exec.coalesce_factor(), 2);

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 2, "output 0 should chain inputs 0 and 3");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4); // 2 + 2
    }

    #[tokio::test]
    async fn sub_partition_rejects_non_divisor() {
        let inner = multi_partition_table(5, 1).await;
        let dist = HashDistribution::new(vec!["id".into()], HashFn::Murmur3, 3);
        let err =
            BucketSubPartitionExec::try_new(inner, dist).expect_err("should reject");
        assert!(
            err.to_string().contains("divide"),
            "unexpected error: {err}"
        );
    }
}
