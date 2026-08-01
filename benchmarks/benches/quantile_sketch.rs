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

//! Ingest throughput: TDigest (production RuntimeStatsExec path) vs KLL
//! over `OwnedRow` (production-swap path).
//!
//! Merge and quantile query are excluded: for N=1M rows at P=64 partitions
//! and K=64 cuts, both are 3+ orders of magnitude cheaper than ingest and
//! would not move the swap decision.
//!
//! Sketch sizing is analytical — TDigest at `TDIGEST_MAX_SIZE=100` (what
//! `runtime_stats.rs` uses today), KLL at `k=800` (roughly `1/√k ≈ 0.035`
//! normalized error, comparable to TDigest's default compression at
//! extreme quantiles). Empirical parity check is future work if the
//! throughput gap turns out to be borderline.

use std::sync::Arc;

use ballista_core::kll::KllSketch;
use criterion::{
    BatchSize, Criterion, Throughput, criterion_group, criterion_main,
};
use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_functions_aggregate_common::tdigest::TDigest;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Matches `runtime_stats::TDIGEST_MAX_SIZE` — production sizing.
const TDIGEST_MAX_SIZE: usize = 100;

/// KLL top-level compactor capacity chosen for analytical error parity with
/// TDigest at `TDIGEST_MAX_SIZE=100`. KLL normalized error scales as
/// `~1/√k`; k=800 gives ε ≈ 0.035, which is in the same neighborhood as
/// TDigest's default compression at the tails.
const KLL_K: usize = 800;

/// RuntimeStatsExec observes one batch at a time from the upstream
/// operator. 8192 is DataFusion's default target batch size.
const BATCH_SIZE: usize = 8192;

/// Fixed PRNG seed so the harness is deterministic across runs.
const SEED: u64 = 0xDEC0DE;

/// Total row counts benched. 100K represents a small stage; 1M represents
/// a per-partition slice of a large TPC-H stage.
const ROW_COUNTS: &[usize] = &[100_000, 1_000_000];

/// Build a `Vec<Float64Array>` of `BATCH_SIZE`-length batches whose row
/// count sums to `n`. Uniform on [0, 1), seeded.
fn build_batches(n: usize) -> Vec<Float64Array> {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut batches = Vec::with_capacity(n.div_ceil(BATCH_SIZE));
    let mut remaining = n;
    while remaining > 0 {
        let take = remaining.min(BATCH_SIZE);
        let vals: Vec<f64> = (0..take).map(|_| rng.random::<f64>()).collect();
        batches.push(Float64Array::from(vals));
        remaining -= take;
    }
    batches
}

/// Production TDigest ingest path — one `Vec<f64>` per batch through
/// `merge_unsorted_f64`. This mirrors `runtime_stats::StreamState::record`
/// modulo the null-flatten and partition-slot lookup.
fn ingest_tdigest(batches: &[Float64Array]) -> TDigest {
    let mut sketch = TDigest::new(TDIGEST_MAX_SIZE);
    for arr in batches {
        // Uniform-f64 batches have no nulls, so `values()` matches the
        // shape the sketch consumes after RuntimeStatsExec's flatten step.
        let values: Vec<f64> = arr.values().to_vec();
        sketch = sketch.merge_unsorted_f64(values);
    }
    sketch
}

/// Production-swap KLL ingest path — encode each batch through the arrow
/// row converter, then push per-row `OwnedRow` into a `KllSketch<OwnedRow>`.
/// The heap alloc per `owned()` is what the level-0 borrowed-row
/// optimization would eliminate.
fn ingest_kll(batches: &[Float64Array], converter: &RowConverter) -> KllSketch<OwnedRow> {
    let mut sketch = KllSketch::<OwnedRow>::new(KLL_K);
    for arr in batches {
        let col: ArrayRef = Arc::new(arr.clone());
        let rows = converter.convert_columns(&[col]).unwrap();
        for row in rows.iter() {
            sketch.insert(row.owned());
        }
    }
    sketch
}

fn bench_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_stats_ingest");
    // Ingest is CPU-bound and single-threaded; ten samples is enough for
    // the ratios we care about and keeps the harness under a minute.
    group.sample_size(10);

    let converter =
        RowConverter::new(vec![SortField::new(DataType::Float64)]).unwrap();

    for &n in ROW_COUNTS {
        let batches = build_batches(n);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_function(format!("tdigest/{n}"), |b| {
            b.iter_batched(
                || batches.clone(),
                |bs| ingest_tdigest(&bs),
                BatchSize::LargeInput,
            );
        });

        group.bench_function(format!("kll/{n}"), |b| {
            b.iter_batched(
                || batches.clone(),
                |bs| ingest_kll(&bs, &converter),
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_ingest);
criterion_main!(benches);
