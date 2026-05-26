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

use crate::planner::create_shuffle_writer_with_config;
use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::planner::AdaptiveStageInfo;
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::common::exec_err;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::Partitioning;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    physical_plan::ExecutionPlan,
};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(crate) struct BallistaAdapter {
    inputs: Vec<usize>,
}

///
/// Used to transform plan nodes used in adaptive planning
/// to ballista specific nodes such as
/// ShuffleWriterExec/SortShuffleWriterExec and [ShuffleReaderExec]
///
impl BallistaAdapter {
    fn transform_children(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>() {
            let schema = exchange.schema().clone();
            let partitions = exchange.shuffle_partitions().ok_or_else(|| {
                DataFusionError::Execution(
                    "partitions have to be resolved at this point".to_string(),
                )
            })?;
            let stage_id = exchange.stage_id().ok_or_else(|| {
                DataFusionError::Execution(
                    "stage ID has to be generated at this point".to_string(),
                )
            })?;
            self.inputs.push(stage_id);
            let partitioning = exchange.properties().partitioning.clone();

            let reader = match (exchange.coalesce(), exchange.skew_join()) {
                (Some(_), Some(_)) => {
                    // Mutually exclusive by construction — the rules
                    // short-circuit when the other slot is already set. If
                    // we ever see both, that's a rule bug; surface it loudly.
                    return exec_err!(
                        "ExchangeExec has both coalesce and skew_join decisions set; \
                         these are mutually exclusive"
                    );
                }
                (Some(cp), None) => {
                    // Concatenate M-shape locations into K-shape per CoalescePlan.groups.
                    let k_shape: Vec<Vec<_>> = cp
                        .groups
                        .iter()
                        .map(|pg| {
                            let mut concat = Vec::new();
                            for &idx in &pg.upstream_indices {
                                if let Some(inner) = partitions.get(idx as usize) {
                                    concat.extend_from_slice(inner);
                                }
                            }
                            concat
                        })
                        .collect();
                    let new_partitioning = match &partitioning {
                        Partitioning::Hash(keys, _m) => {
                            Partitioning::Hash(keys.clone(), cp.groups.len())
                        }
                        _ => Partitioning::UnknownPartitioning(cp.groups.len()),
                    };
                    ShuffleReaderExec::try_new_coalesced(
                        stage_id,
                        k_shape,
                        (*cp).clone(),
                        schema,
                        new_partitioning,
                    )?
                }
                (None, Some(sp)) => {
                    // One entry per shard; slice each upstream partition's
                    // PartitionLocation list to the shard's [start_map_idx,
                    // end_map_idx) window. Passthrough shards (full mapper
                    // range) take the whole inner Vec.
                    let mut k_shape: Vec<Vec<PartitionLocation>> =
                        Vec::with_capacity(sp.shards.len());
                    for shard in &sp.shards {
                        let inner = partitions
                            .get(shard.upstream_idx as usize)
                            .ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "SkewJoinPlan references upstream_idx {} but \
                                     only {} upstream partitions exist",
                                    shard.upstream_idx,
                                    partitions.len(),
                                ))
                            })?;
                        let start = shard.start_map_idx as usize;
                        let end = (shard.end_map_idx as usize).min(inner.len());
                        let assigned: Vec<_> = if start < end {
                            inner[start..end].to_vec()
                        } else {
                            Vec::new()
                        };
                        k_shape.push(assigned);
                    }
                    // Preserve the hash partitioning width at K'; the
                    // OptimizeSkewedJoinRule's join-side fix (`is_skew_join`
                    // in C4) is what relaxes the "same key in one partition"
                    // invariant downstream.
                    let new_partitioning = match &partitioning {
                        Partitioning::Hash(keys, _m) => {
                            Partitioning::Hash(keys.clone(), sp.shards.len())
                        }
                        _ => Partitioning::UnknownPartitioning(sp.shards.len()),
                    };
                    ShuffleReaderExec::try_new_skew_join(
                        stage_id,
                        k_shape,
                        (*sp).clone(),
                        schema,
                        new_partitioning,
                    )?
                }
                (None, None) => ShuffleReaderExec::try_new(
                    stage_id,
                    partitions,
                    schema,
                    partitioning,
                )?,
            };

            Ok(Transformed::yes(Arc::new(reader)))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    /// Converts Adaptive plan to plan which ballista expects
    /// This is to be used to convert [ExchangeExec] to
    /// ShuffleWriterExec/SortShuffleWriterExec and [ShuffleReaderExec]
    pub fn adapt_to_ballista(
        plan: Arc<dyn ExecutionPlan>,
        job_id: &str,
        config: &ConfigOptions,
    ) -> datafusion::error::Result<AdaptiveStageInfo> {
        if let Some(root) = plan.as_any().downcast_ref::<ExchangeExec>() {
            let mut adapter = BallistaAdapter::default();
            let plan = root
                .input()
                .clone()
                .transform_down(|e| adapter.transform_children(e))?
                .data;
            let stage_id = root.stage_id().ok_or_else(|| {
                DataFusionError::Execution(
                    "shuffle partitions have to be resolved at this point".to_string(),
                )
            })?;
            let partitioning = root.partitioning.clone();

            let writer = create_shuffle_writer_with_config(
                job_id,
                stage_id,
                plan,
                partitioning,
                config,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(AdaptiveStageInfo {
                plan: writer,
                inputs: adapter.inputs,
            })
        } else if let Some(root) = plan.as_any().downcast_ref::<AdaptiveDatafusionExec>()
        {
            let mut adapter = BallistaAdapter::default();
            let plan = root
                .input()
                .clone()
                .transform_down(|e| adapter.transform_children(e))?
                .data;
            let stage_id = root.stage_id().ok_or_else(|| {
                DataFusionError::Execution(
                    "shuffle partitions have to be resolved at this point".to_string(),
                )
            })?;

            let writer =
                create_shuffle_writer_with_config(job_id, stage_id, plan, None, config)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(AdaptiveStageInfo {
                plan: writer,
                inputs: adapter.inputs,
            })
        } else {
            exec_err!(
                "Root exec expected to be either ExchangeExec or AdaptiveDatafusionExec"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::execution_plans::{
        ShuffleReaderExec, SkewJoinPlan, SkewJoinShard,
    };
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
        PartitionId, PartitionLocation, PartitionStats,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use parking_lot::Mutex;
    use std::sync::atomic::AtomicI64;

    // Build a synthetic PartitionLocation tagged with (upstream_partition,
    // map_partition) so the assertions below can verify which mapper output
    // each shard ended up reading.
    fn synthetic_loc(partition_id: usize, map_partition_id: usize) -> PartitionLocation {
        PartitionLocation {
            map_partition_id,
            partition_id: PartitionId {
                job_id: "test_job".to_string(),
                stage_id: 0,
                partition_id,
            },
            executor_meta: ExecutorMetadata {
                id: "".to_string(),
                host: "".to_string(),
                port: 0,
                grpc_port: 0,
                specification: ExecutorSpecification::default().with_task_slots(0),
                os_info: ExecutorOperatingSystemSpecification::default(),
            },
            partition_stats: PartitionStats::new(Some(0), None, Some(0)),
            file_id: None,
            is_sort_shuffle: false,
        }
    }

    // Builds an `ExchangeExec` whose `shuffle_partitions` is already resolved
    // to `partitions` and whose `stage_id` is set. The input is a trivial
    // `EmptyExec` because the adapter never descends past the Exchange itself
    // when transforming a leaf shuffle.
    fn synthetic_exchange(
        partitions: Vec<Vec<PartitionLocation>>,
        m: usize,
    ) -> Arc<ExchangeExec> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let empty = Arc::new(EmptyExec::new(schema));
        let stage_id = Arc::new(AtomicI64::new(7));
        let shuffle_parts = Arc::new(Mutex::new(Some(partitions)));
        let exchange = ExchangeExec::new_with_details(
            empty,
            Some(Partitioning::Hash(vec![], m)),
            1,
            stage_id,
            shuffle_parts,
        );
        Arc::new(exchange)
    }

    /// The adapter, when given an Exchange with a `SkewJoinPlan` attached,
    /// produces a `ShuffleReaderExec` whose `partition` vector has one entry
    /// per shard, each holding the per-mapper slice
    /// `[start_map_idx, end_map_idx)` of the named upstream partition.
    #[test]
    fn adapter_slices_skew_join_shards_by_map_range() {
        // 3 upstream partitions, each with 4 mapper outputs. The
        // `map_partition_id` tag is `[0..4)` within each upstream so the
        // assertions below can recover which mapper a shard read.
        let partitions: Vec<Vec<PartitionLocation>> = (0..3)
            .map(|p| (0..4).map(|m| synthetic_loc(p, m)).collect())
            .collect();

        // Mix of split shapes:
        //   upstream 0 → 2-way split   ([0..2), [2..4))
        //   upstream 1 → 4-way split   ([0..1), [1..2), [2..3), [3..4))
        //   upstream 2 → passthrough   ([0..4))
        // K' = 2 + 4 + 1 = 7.
        let skew_join = Arc::new(SkewJoinPlan {
            upstream_partition_count: 3,
            shards: vec![
                SkewJoinShard { upstream_idx: 0, start_map_idx: 0, end_map_idx: 2 },
                SkewJoinShard { upstream_idx: 0, start_map_idx: 2, end_map_idx: 4 },
                SkewJoinShard { upstream_idx: 1, start_map_idx: 0, end_map_idx: 1 },
                SkewJoinShard { upstream_idx: 1, start_map_idx: 1, end_map_idx: 2 },
                SkewJoinShard { upstream_idx: 1, start_map_idx: 2, end_map_idx: 3 },
                SkewJoinShard { upstream_idx: 1, start_map_idx: 3, end_map_idx: 4 },
                SkewJoinShard { upstream_idx: 2, start_map_idx: 0, end_map_idx: 4 },
            ],
        });

        let exchange = synthetic_exchange(partitions, 3);
        exchange.set_skew_join(skew_join.clone());

        let mut adapter = BallistaAdapter::default();
        let transformed = adapter
            .transform_children(exchange as Arc<dyn ExecutionPlan>)
            .expect("transform_children should succeed");
        let reader_arc = transformed.data;
        let reader = reader_arc
            .as_any()
            .downcast_ref::<ShuffleReaderExec>()
            .expect("transform_children must produce ShuffleReaderExec");

        assert!(reader.skew_join.is_some(), "skew_join slot must be threaded");
        assert!(reader.coalesce.is_none(), "coalesce slot must be empty");

        // K' = 7 — one output partition per shard.
        assert_eq!(reader.partition.len(), 7);
        assert_eq!(reader.properties().partitioning.partition_count(), 7);

        // Each output partition's (upstream_partition_id, map_partition_id)
        // pairs must match the requested [start_map_idx, end_map_idx) slice
        // of the named upstream.
        let actual: Vec<Vec<(usize, usize)>> = reader
            .partition
            .iter()
            .map(|inner| {
                inner
                    .iter()
                    .map(|l| (l.partition_id.partition_id, l.map_partition_id))
                    .collect()
            })
            .collect();
        let expected: Vec<Vec<(usize, usize)>> = vec![
            vec![(0, 0), (0, 1)],         // upstream 0 [0..2)
            vec![(0, 2), (0, 3)],         // upstream 0 [2..4)
            vec![(1, 0)],                  // upstream 1 [0..1)
            vec![(1, 1)],                  // upstream 1 [1..2)
            vec![(1, 2)],                  // upstream 1 [2..3)
            vec![(1, 3)],                  // upstream 1 [3..4)
            vec![(2, 0), (2, 1), (2, 2), (2, 3)], // upstream 2 [0..4)
        ];
        assert_eq!(actual, expected);
    }

    /// When both slots are set the adapter must error rather than silently
    /// picking one — this catches rule bugs (the rules themselves
    /// short-circuit when the other slot is set, so a violation here means
    /// something went wrong upstream of the adapter).
    #[test]
    fn adapter_errors_when_both_coalesce_and_skew_join_set() {
        use ballista_core::execution_plans::{CoalescePlan, PartitionGroup};

        let partitions = vec![vec![synthetic_loc(0, 0)]];
        let exchange = synthetic_exchange(partitions, 1);
        exchange.set_coalesce(Arc::new(CoalescePlan {
            upstream_partition_count: 1,
            groups: vec![PartitionGroup { upstream_indices: vec![0] }],
        }));
        exchange.set_skew_join(Arc::new(SkewJoinPlan {
            upstream_partition_count: 1,
            shards: vec![SkewJoinShard {
                upstream_idx: 0,
                start_map_idx: 0,
                end_map_idx: 1,
            }],
        }));

        let mut adapter = BallistaAdapter::default();
        let err = adapter
            .transform_children(exchange as Arc<dyn ExecutionPlan>)
            .expect_err("must reject both-slots-set");
        let msg = err.to_string();
        assert!(
            msg.contains("mutually exclusive"),
            "error should explain the mutual-exclusion invariant, got: {msg}"
        );
    }

    /// Out-of-bounds `start_map_idx` clamps to an empty shard rather than
    /// panicking; out-of-bounds `upstream_idx` returns a clear error.
    #[test]
    fn adapter_guards_out_of_bounds_skew_join_indices() {
        // upstream_idx beyond the M=2 partitions → error.
        let partitions: Vec<Vec<PartitionLocation>> = (0..2)
            .map(|p| vec![synthetic_loc(p, 0), synthetic_loc(p, 1)])
            .collect();
        let exchange = synthetic_exchange(partitions, 2);
        exchange.set_skew_join(Arc::new(SkewJoinPlan {
            upstream_partition_count: 2,
            shards: vec![SkewJoinShard {
                upstream_idx: 5, // > M-1
                start_map_idx: 0,
                end_map_idx: 1,
            }],
        }));
        let mut adapter = BallistaAdapter::default();
        let err = adapter
            .transform_children(exchange as Arc<dyn ExecutionPlan>)
            .expect_err("must reject upstream_idx > M-1");
        assert!(
            err.to_string().contains("upstream_idx 5"),
            "error should name the offending idx, got: {err}"
        );

        // end_map_idx past inner.len() clamps; start past end yields an
        // empty shard — both safe.
        let partitions: Vec<Vec<PartitionLocation>> =
            vec![vec![synthetic_loc(0, 0), synthetic_loc(0, 1)]];
        let exchange = synthetic_exchange(partitions, 1);
        exchange.set_skew_join(Arc::new(SkewJoinPlan {
            upstream_partition_count: 1,
            shards: vec![
                SkewJoinShard {
                    upstream_idx: 0,
                    start_map_idx: 0,
                    end_map_idx: 99, // > inner.len()=2
                },
                SkewJoinShard {
                    upstream_idx: 0,
                    start_map_idx: 5, // > end
                    end_map_idx: 5,
                },
            ],
        }));
        let mut adapter = BallistaAdapter::default();
        let transformed = adapter
            .transform_children(exchange as Arc<dyn ExecutionPlan>)
            .expect("clamping path should not error");
        let reader = transformed
            .data
            .as_any()
            .downcast_ref::<ShuffleReaderExec>()
            .unwrap();
        assert_eq!(reader.partition[0].len(), 2, "end clamps to inner.len()");
        assert_eq!(reader.partition[1].len(), 0, "start>=end yields empty shard");
    }
}
