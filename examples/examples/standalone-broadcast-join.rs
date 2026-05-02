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

//! Standalone broadcast-join correctness example.
//!
//! Builds two Parquet-backed tables, runs a hash join in a standalone Ballista
//! cluster with the broadcast-join threshold enabled, and asserts the result
//! matches a plain DataFusion reference run.
//!
//! Parquet files are used (not CSV) because `DataSourceExec` for Parquet
//! reports `total_byte_size` from row-group metadata, which is required for
//! `maybe_promote_to_broadcast` to fire. CSV sources report `Absent` for
//! `total_byte_size` and would silently skip promotion.

use std::sync::Arc;

use ballista::datafusion::{
    arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    common::Result,
    execution::{SessionStateBuilder, options::ParquetReadOptions},
    prelude::{SessionConfig, SessionContext},
};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::config::BALLISTA_BROADCAST_JOIN_THRESHOLD_BYTES;

#[tokio::main]
async fn main() -> Result<()> {
    // ── Build shared in-memory batches ─────────────────────────────────────

    let big_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let small_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("name", DataType::Int32, false),
    ]));

    let big_batch = RecordBatch::try_new(
        big_schema.clone(),
        vec![
            Arc::new(Int32Array::from((0..1000).collect::<Vec<i32>>())),
            Arc::new(Int32Array::from(
                (0..1000).map(|i: i32| i * 2).collect::<Vec<i32>>(),
            )),
        ],
    )?;

    let small_batch = RecordBatch::try_new(
        small_schema,
        vec![
            Arc::new(Int32Array::from(vec![1i32, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![10i32, 20, 30, 40, 50])),
        ],
    )?;

    const QUERY: &str = "select count(*) from big join small on big.k = small.k";

    // ── Reference run: plain DataFusion ───────────────────────────────────

    let ref_ctx = SessionContext::new();
    ref_ctx.register_batch("big", big_batch.clone())?;
    ref_ctx.register_batch("small", small_batch.clone())?;

    let ref_batches = ref_ctx.sql(QUERY).await?.collect().await?;
    let ref_count: i64 = ref_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ballista::datafusion::arrow::array::Int64Array>()
        .expect("count(*) should produce Int64Array")
        .value(0);

    // ── Write Parquet files so Ballista can serialize the table references ─
    //
    // Parquet is required here (not CSV) because DataFusion's Parquet reader
    // populates `Statistics::total_byte_size` from row-group metadata.
    // The Ballista broadcast-join optimizer (`supports_collect_by_thresholds`)
    // checks `total_byte_size` first; when it is `Absent` (as with CSV) the
    // check falls back to `num_rows`, and when that is also unavailable the
    // promotion is silently skipped.

    let tmp = tempfile::tempdir().expect("tmp dir");
    let big_path = tmp.path().join("big.parquet");
    let small_path = tmp.path().join("small.parquet");

    // Use a plain (non-Ballista) context to write the Parquet files.
    let write_ctx = SessionContext::new();
    write_ctx
        .read_batch(big_batch.clone())?
        .write_parquet(
            big_path.to_str().expect("valid path"),
            Default::default(),
            None,
        )
        .await?;
    write_ctx
        .read_batch(small_batch.clone())?
        .write_parquet(
            small_path.to_str().expect("valid path"),
            Default::default(),
            None,
        )
        .await?;

    // ── Ballista standalone run ────────────────────────────────────────────

    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(2)
        .with_ballista_standalone_parallelism(2)
        // Enable the Ballista broadcast-join promotion.
        // new_with_ballista() already disables DataFusion's own single-partition
        // threshold via ballista_restricted_configuration(), so we only need
        // to ensure the Ballista threshold is set to a value large enough that
        // the small table (5 rows, ~300 B as Parquet) is promoted.
        .set_usize(BALLISTA_BROADCAST_JOIN_THRESHOLD_BYTES, 10 * 1024 * 1024);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::standalone_with_state(state).await?;

    ctx.register_parquet(
        "big",
        big_path.to_str().expect("valid path"),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "small",
        small_path.to_str().expect("valid path"),
        ParquetReadOptions::default(),
    )
    .await?;

    // Print the physical plan so we can confirm broadcast lowering.
    // Because DataFusion's hash_join_single_partition_threshold is set to 0
    // by new_with_ballista(), the plan arrives at Ballista as Partitioned.
    // The Ballista JoinSelection optimizer then sees the Parquet byte stats
    // for the small table and promotes it to a broadcast (CollectLeft) join.
    let physical_plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
    println!(
        "Physical plan:\n{}",
        ballista::datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(true)
    );

    let result_batches = ctx.sql(QUERY).await?.collect().await?;
    let result_count: i64 = result_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ballista::datafusion::arrow::array::Int64Array>()
        .expect("count(*) should produce Int64Array")
        .value(0);

    assert_eq!(
        result_count, ref_count,
        "Ballista result ({result_count}) does not match reference ({ref_count})"
    );

    println!(
        "ok: broadcast join produced {result_count} rows (matches reference); \
         threshold=10 MiB, small table written as Parquet so byte stats are present"
    );

    Ok(())
}
