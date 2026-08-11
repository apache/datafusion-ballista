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
//! over `OwnedRow` (multi-column-capable swap path) vs KLL over
//! `OrderedFloat<f64>` (single-column-native-Ord fast path).
//!
//! The three variants isolate the cost of the arrow-row encoding: TDigest
//! is the incumbent baseline, KLL<OwnedRow> pays for `RowConverter` +
//! per-row `owned()` heap alloc, KLL<OrderedFloat<f64>> skips both and
//! measures the sketch algorithm on its own.
//!
//! Merge and quantile query are excluded: for N=1M rows at P=64 partitions
//! and K=64 cuts, both are 3+ orders of magnitude cheaper than ingest and
//! would not move the swap decision.
//!
//! Sketch sizing is empirical, not analytical. TDigest is held at
//! `TDIGEST_MAX_SIZE=100` (production), and `KLL_K` is picked so KLL's
//! worst-case normalized rank error on the same uniform 1M stream is
//! within a whisker of TDigest's — a fair race. The parity check itself
//! prints alongside the throughput results when the env var
//! `KLL_PARITY_CHECK=1` is set, so anyone can rerun it.

use std::sync::Arc;

use ballista_core::kll::KllSketch;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_functions_aggregate_common::tdigest::TDigest;
use ordered_float::OrderedFloat;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Matches `runtime_stats::TDIGEST_MAX_SIZE` — production sizing.
const TDIGEST_MAX_SIZE: usize = 100;

/// KLL top-level compactor capacity picked empirically for worst-case
/// rank-error parity with TDigest at `TDIGEST_MAX_SIZE=100`. Measured on
/// the uniform 1M stream this bench feeds:
///
/// | sketch                       | worst rank err (9 deciles) |
/// |------------------------------|---------------------------:|
/// | TDigest max_size=100         |                     0.0021 |
/// | KLL k=800                    |                     0.0016 |
///
/// Rerun via `KLL_PARITY_CHECK=1 cargo bench --bench quantile_sketch`.
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

/// Same underlying stream as `build_batches`, sorted globally before
/// batching. Consecutive batches are monotonically increasing — matches
/// what a `SortExec` upstream of `RuntimeStatsExec` would feed the sketch
/// in the ORRE plan shape.
fn build_globally_sorted_batches(n: usize) -> Vec<Float64Array> {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut all: Vec<f64> = (0..n).map(|_| rng.random::<f64>()).collect();
    all.sort_by(f64::total_cmp);
    all.chunks(BATCH_SIZE)
        .map(|chunk| Float64Array::from(chunk.to_vec()))
        .collect()
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

/// Sorted-input TDigest ingest — same shape as `ingest_tdigest` but hits
/// `merge_sorted_f64` on each pre-sorted batch. Answers the apples-to-
/// apples question against `absorb_sorted_slice` on the ORRE-plan-shape
/// arm: TDigest also benefits from sorted input (it skips its own
/// pre-sort), so a matched race lines them up on equal footing.
fn ingest_tdigest_sorted(batches: &[Float64Array]) -> TDigest {
    let mut sketch = TDigest::new(TDIGEST_MAX_SIZE);
    for arr in batches {
        sketch = sketch.merge_sorted_f64(arr.values());
    }
    sketch
}

/// Multi-column-capable KLL ingest path — encode each batch through the
/// arrow row converter, then push per-row `OwnedRow` into a
/// `KllSketch<OwnedRow>`. The heap alloc per `owned()` is what the
/// level-0 borrowed-row optimization would eliminate.
fn ingest_kll_row(
    batches: &[Float64Array],
    converter: &RowConverter,
) -> KllSketch<OwnedRow> {
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

/// Single-column native-`Ord` KLL fast path — bypasses arrow-row entirely
/// by using `OrderedFloat<f64>` as the sketch item type. Answers "what
/// does KLL cost when we dispatch by column type instead of routing
/// everything through OwnedRow?"
fn ingest_kll_ordered_float(batches: &[Float64Array]) -> KllSketch<OrderedFloat<f64>> {
    let mut sketch = KllSketch::<OrderedFloat<f64>>::new(KLL_K);
    for arr in batches {
        for v in arr.values() {
            sketch.insert(OrderedFloat(*v));
        }
    }
    sketch
}

/// Same as `ingest_kll_ordered_float` but batches each Arrow batch into
/// `absorb_slice` — measures the batch-oriented ingest optimization
/// (amortized compact_all + batch min/max + memcpy-style extend).
fn ingest_kll_ordered_float_absorb_slice(
    batches: &[Float64Array],
) -> KllSketch<OrderedFloat<f64>> {
    let mut sketch = KllSketch::<OrderedFloat<f64>>::new(KLL_K);
    for arr in batches {
        let vals: Vec<OrderedFloat<f64>> =
            arr.values().iter().copied().map(OrderedFloat).collect();
        sketch.absorb_slice(&vals);
    }
    sketch
}

/// Same shape as `ingest_kll_ordered_float_absorb_slice`, but consumes
/// pre-sorted batches via `absorb_sorted_slice` — measures the sketch
/// side of the ORRE-plan-shape follow-up, where a `SortExec` upstream of
/// `RuntimeStatsExec` guarantees sorted input and lets the sketch skip
/// every compaction sort (not just those above level 0).
fn ingest_kll_ordered_float_absorb_sorted_slice(
    batches: &[Float64Array],
) -> KllSketch<OrderedFloat<f64>> {
    let mut sketch = KllSketch::<OrderedFloat<f64>>::new(KLL_K);
    for arr in batches {
        let vals: Vec<OrderedFloat<f64>> =
            arr.values().iter().copied().map(OrderedFloat).collect();
        sketch.absorb_sorted_slice(&vals);
    }
    sketch
}

/// Same as `ingest_kll_row` but batches each Arrow batch into `absorb`
/// (owned-iter variant) — measures the compact_all amortization win on the
/// `OwnedRow` path without the extra clone that `absorb_slice` would
/// force.
fn ingest_kll_row_absorb(
    batches: &[Float64Array],
    converter: &RowConverter,
) -> KllSketch<OwnedRow> {
    let mut sketch = KllSketch::<OwnedRow>::new(KLL_K);
    for arr in batches {
        let col: ArrayRef = Arc::new(arr.clone());
        let rows = converter.convert_columns(&[col]).unwrap();
        sketch.absorb(rows.iter().map(|r| r.owned()));
    }
    sketch
}

/// Print worst-case quantile-inversion error at deciles for TDigest at
/// `TDIGEST_MAX_SIZE=100` and KLL at a sweep of `k`. Called from
/// `bench_ingest` when `KLL_PARITY_CHECK=1` is set. Reproducibility
/// artifact for the `KLL_K` choice — a reviewer can rerun and confirm
/// the two sketches sit at matched accuracy at the settings the bench
/// uses.
///
/// Error metric: at each decile `q`, ask the sketch for `quantile(q)`,
/// look up what fraction of the true stream is below the returned value,
/// and take `|true_fraction - q|`. That's normalized rank error via
/// quantile inversion — the same units for both sketches even though
/// TDigest lacks a public rank API.
fn print_parity_table(n: usize) {
    let batches = build_batches(n);
    let all: Vec<f64> = batches
        .iter()
        .flat_map(|b| b.values().iter().copied())
        .collect();
    let mut sorted = all.clone();
    sorted.sort_by(f64::total_cmp);

    let true_rank_frac =
        |probe: f64| -> f64 { sorted.partition_point(|x| *x < probe) as f64 / n as f64 };
    // Nine interior deciles; hits TDigest's median hump (worst-case for it)
    // and both tails (KLL uniform, TDigest tightest).
    let qs: Vec<f64> = (1..10).map(|i| i as f64 / 10.0).collect();

    let td = ingest_tdigest(&batches);
    let td_worst = qs
        .iter()
        .map(|&q| (true_rank_frac(td.estimate_quantile(q)) - q).abs())
        .fold(0.0_f64, f64::max);

    println!("\n=== quantile_sketch parity @ n={n} (uniform f64) ===");
    println!("TDigest max_size={TDIGEST_MAX_SIZE:>4} → worst rank err = {td_worst:.4}");
    for &k in &[50usize, 100, 200, 400, 800] {
        let mut sk = KllSketch::<OrderedFloat<f64>>::new(k);
        for arr in &batches {
            for v in arr.values() {
                sk.insert(OrderedFloat(*v));
            }
        }
        let kll_worst = qs
            .iter()
            .map(|&q| {
                let guess = sk.quantile(q).map(|of| of.0).unwrap_or(f64::NAN);
                (true_rank_frac(guess) - q).abs()
            })
            .fold(0.0_f64, f64::max);
        let marker = if k == KLL_K { " ← KLL_K" } else { "" };
        println!("KLL     k       ={k:>4} → worst rank err = {kll_worst:.4}{marker}");
    }
    println!();
}

fn bench_ingest(c: &mut Criterion) {
    if std::env::var_os("KLL_PARITY_CHECK").is_some() {
        for &n in ROW_COUNTS {
            print_parity_table(n);
        }
    }

    let mut group = c.benchmark_group("runtime_stats_ingest");
    // Ingest is CPU-bound and single-threaded; ten samples is enough for
    // the ratios we care about and keeps the harness under a minute.
    group.sample_size(10);

    let converter = RowConverter::new(vec![SortField::new(DataType::Float64)]).unwrap();

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

        group.bench_function(format!("kll_row/{n}"), |b| {
            b.iter_batched(
                || batches.clone(),
                |bs| ingest_kll_row(&bs, &converter),
                BatchSize::LargeInput,
            );
        });

        group.bench_function(format!("kll_row_absorb/{n}"), |b| {
            b.iter_batched(
                || batches.clone(),
                |bs| ingest_kll_row_absorb(&bs, &converter),
                BatchSize::LargeInput,
            );
        });

        group.bench_function(format!("kll_ordered_float/{n}"), |b| {
            b.iter_batched(
                || batches.clone(),
                |bs| ingest_kll_ordered_float(&bs),
                BatchSize::LargeInput,
            );
        });

        group.bench_function(format!("kll_ordered_float_absorb_slice/{n}"), |b| {
            b.iter_batched(
                || batches.clone(),
                |bs| ingest_kll_ordered_float_absorb_slice(&bs),
                BatchSize::LargeInput,
            );
        });

        let sorted_batches = build_globally_sorted_batches(n);
        group.bench_function(format!("tdigest_sorted/{n}"), |b| {
            b.iter_batched(
                || sorted_batches.clone(),
                |bs| ingest_tdigest_sorted(&bs),
                BatchSize::LargeInput,
            );
        });

        group.bench_function(format!("kll_ordered_float_absorb_sorted_slice/{n}"), |b| {
            b.iter_batched(
                || sorted_batches.clone(),
                |bs| ingest_kll_ordered_float_absorb_sorted_slice(&bs),
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_ingest);
criterion_main!(benches);
