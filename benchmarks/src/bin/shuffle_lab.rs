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

//! Diagnostics for the sort-based shuffle write and read paths.
//!
//! `shuffle_bench` times an end-to-end shuffle write against real Parquet
//! input. This binary complements it by isolating the individual costs that
//! end-to-end number is made of, on synthetic data, so a change to one of them
//! can be attributed:
//!
//! * open file descriptors held during a spilling write, against the output
//!   partition count
//! * write time split into hash assignment, spill and finalize
//! * compression codec cost, on both the write and the read side
//! * per-partition schema overhead in the consolidated file
//! * index lookup cost, whole-index read against a positioned read
//! * batch granularity the writer produces, and what coalescing at finalize
//!   time would recover
//!
//! Run with `cargo run --release --bin shuffle_lab`. It takes no arguments;
//! edit `main` to change the partition counts and schemas swept.

use ballista_core::execution_plans::sort_shuffle::{
    SortShuffleConfig, SortShuffleWriterExec, get_index_path,
    stream_sort_shuffle_partition,
};
use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::create_hashes;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const BATCH_ROWS: usize = 8192;

fn schema_with(num_int: usize, num_str: usize) -> SchemaRef {
    let mut fields = Vec::new();
    for i in 0..num_int {
        fields.push(Field::new(format!("i{i}"), DataType::Int64, true));
    }
    for i in 0..num_str {
        fields.push(Field::new(format!("s{i}"), DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn build_batch(schema: &SchemaRef, seed: i64) -> RecordBatch {
    let mut cols: Vec<Arc<dyn Array>> = Vec::new();
    for f in schema.fields() {
        match f.data_type() {
            DataType::Int64 => {
                let v: Vec<i64> = (0..BATCH_ROWS)
                    .map(|r| (r as i64).wrapping_mul(2654435761).wrapping_add(seed))
                    .collect();
                cols.push(Arc::new(Int64Array::from(v)));
            }
            DataType::Utf8 => {
                let v: Vec<String> = (0..BATCH_ROWS)
                    .map(|r| format!("val-{}-{}", seed, r % 977))
                    .collect();
                cols.push(Arc::new(StringArray::from(v)));
            }
            other => panic!("unsupported {other}"),
        }
    }
    RecordBatch::try_new(schema.clone(), cols).unwrap()
}

fn make_input(schema: &SchemaRef, batches: usize) -> Arc<dyn ExecutionPlan> {
    let part: Vec<RecordBatch> = (0..batches)
        .map(|i| build_batch(schema, i as i64))
        .collect();
    let src = MemorySourceConfig::try_new(&[part], schema.clone(), None).unwrap();
    Arc::new(DataSourceExec::new(Arc::new(src)))
}

/// Write a sort-shuffle file and return (data_path, index_path).
async fn write_shuffle(
    schema: &SchemaRef,
    batches: usize,
    partitions: usize,
    work_dir: &Path,
    memory_limit: Option<usize>,
) -> (PathBuf, PathBuf) {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    let ctx = match memory_limit {
        Some(l) => {
            let rt = Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_limit(l, 1.0)
                    .build()
                    .unwrap(),
            );
            SessionContext::new_with_config_rt(Default::default(), rt)
        }
        None => SessionContext::new(),
    };
    let config = SortShuffleConfig::new(true, BATCH_ROWS)
        .with_memory_limit_per_task_bytes(memory_limit.unwrap_or(0));
    let writer = SortShuffleWriterExec::try_new(
        "lab_job".into(),
        1,
        make_input(schema, batches),
        work_dir.to_str().unwrap().to_string(),
        Partitioning::Hash(vec![Arc::new(Column::new("i0", 0))], partitions),
        config,
    )
    .unwrap();
    let task_ctx = ctx.task_ctx();

    let mut handles = Vec::new();
    let plan = Arc::new(writer);
    for k in 0..partitions {
        let p = plan.clone();
        let c = task_ctx.clone();
        handles.push(tokio::spawn(async move {
            let mut s = p.execute(k, c).unwrap();
            while (s.try_next().await.unwrap()).is_some() {}
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let data = work_dir
        .join("lab_job")
        .join("1")
        .join("0")
        .join("data.arrow");
    let index = get_index_path(&data);
    (data, index)
}

// ---------------------------------------------------------------------------
// Experiment 9: what an in-memory shuffle hand-off would save.
//
// The writer buffers rows in memory and, at finalize, always materializes
// them, IPC-encodes them, compresses them and writes a file — even when
// nothing ever spilled and the consumer runs on the same node. That consumer
// then reopens the file, decompresses and decodes it back into the batches the
// writer already had.
//
// This measures the ceiling on removing that round trip: the same hash
// partitioning and the same `interleave`-based materialization, stopping once
// the per-partition batches exist in memory. Anything between this number and
// the write+read total is what an in-memory hand-off could give back.
// ---------------------------------------------------------------------------

/// Materializes every partition's batches in memory, exactly as the writer's
/// finalize step does, but without encoding or writing anything. Returns
/// (batches, rows, resident bytes).
fn materialize_in_memory(
    batches: &[RecordBatch],
    assignments: &[Vec<(u32, u32)>],
    batch_size: usize,
) -> (usize, usize, usize) {
    use datafusion::arrow::compute::interleave_record_batch;
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let mut out_batches = 0usize;
    let mut out_rows = 0usize;
    let mut resident = 0usize;
    let mut scratch: Vec<(usize, usize)> = Vec::with_capacity(batch_size);
    for indices in assignments {
        let mut pos = 0usize;
        while pos < indices.len() {
            let end = (pos + batch_size).min(indices.len());
            scratch.clear();
            scratch.extend(
                indices[pos..end]
                    .iter()
                    .map(|&(b, r)| (b as usize, r as usize)),
            );
            pos = end;
            let batch = interleave_record_batch(&refs, &scratch).unwrap();
            out_rows += batch.num_rows();
            resident += batch.get_array_memory_size();
            out_batches += 1;
            // Dropped immediately: an in-memory hand-off would retain these,
            // but retaining them here would measure the allocator, not the
            // materialization the writer already performs.
        }
    }
    (out_batches, out_rows, resident)
}

async fn exp_in_memory(
    label: &str,
    schema: &SchemaRef,
    num_batches: usize,
    partitions: usize,
) {
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("i0", 0))];
    let input: Vec<RecordBatch> = (0..num_batches)
        .map(|i| build_batch(schema, i as i64))
        .collect();

    // Hash every row and build the same (batch_idx, row_idx) index lists the
    // writer's buffer holds.
    let mut hashes: Vec<u64> = Vec::new();
    let mut assignments: Vec<Vec<(u32, u32)>> = vec![Vec::new(); partitions];
    let t_hash = Instant::now();
    for (bi, batch) in input.iter().enumerate() {
        let arrays = evaluate_expressions_to_arrays(&exprs, batch).unwrap();
        hashes.clear();
        hashes.resize(batch.num_rows(), 0);
        create_hashes(
            &arrays,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )
        .unwrap();
        for (row, &h) in hashes.iter().enumerate() {
            assignments[(h % partitions as u64) as usize].push((bi as u32, row as u32));
        }
    }
    let hash_time = t_hash.elapsed();

    let t_mem = Instant::now();
    let (mem_batches, mem_rows, resident) =
        materialize_in_memory(&input, &assignments, BATCH_ROWS);
    let materialize_time = t_mem.elapsed();

    // The shipped path, for comparison: full write then full read back.
    let dir = tempfile::TempDir::new().unwrap();
    let t_w = Instant::now();
    let (data, index) =
        write_shuffle(schema, num_batches, partitions, dir.path(), None).await;
    let write_time = t_w.elapsed();
    let file_len = std::fs::metadata(&data).unwrap().len();

    let t_r = Instant::now();
    let mut read_rows = 0usize;
    for k in 0..partitions {
        let mut s = stream_sort_shuffle_partition(&data, &index, k).unwrap();
        while let Some(b) = s.try_next().await.unwrap() {
            read_rows += b.num_rows();
        }
    }
    let read_time = t_r.elapsed();
    assert_eq!(
        read_rows, mem_rows,
        "in-memory and on-disk row counts differ"
    );

    let in_mem_total = hash_time + materialize_time;
    let on_disk_total = write_time + read_time;
    println!(
        "[in-mem] {label:<22} K={partitions:<5} write={write_time:>8.2?} read={read_time:>8.2?} \
         (total {on_disk_total:>8.2?}) | in-memory: hash={hash_time:>7.2?} materialize={materialize_time:>8.2?} \
         (total {in_mem_total:>8.2?})",
    );
    println!(
        "{:<9} {:<22}         cache-local-reads-only saves {:>5.0}% | full in-memory saves {:>5.0}% \
         | file={:>9} resident={:>9} batches={mem_batches}",
        "",
        "",
        100.0 * read_time.as_secs_f64() / on_disk_total.as_secs_f64(),
        100.0 * (1.0 - in_mem_total.as_secs_f64() / on_disk_total.as_secs_f64()),
        human(file_len),
        human(resident as u64),
    );
}

// ---------------------------------------------------------------------------
// Experiment 1: read path — cost and batch granularity of reading every
// partition of a sort-shuffle file back through the shipped reader.
// ---------------------------------------------------------------------------

fn read_index(index_path: &Path) -> Vec<i64> {
    let mut buf = Vec::new();
    File::open(index_path)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    buf.chunks_exact(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

async fn exp_read_path(
    label: &str,
    schema: &SchemaRef,
    batches: usize,
    partitions: usize,
    memory_limit: Option<usize>,
) {
    let dir = tempfile::TempDir::new().unwrap();
    let (data, index) =
        write_shuffle(schema, batches, partitions, dir.path(), memory_limit).await;
    let offsets = read_index(&index);
    let data_len = std::fs::metadata(&data).unwrap().len();

    // Read every partition through the shipped reader. Batch count relative to
    // row count is the interesting number: it shows how finely the writer
    // fragmented each partition, which drives IPC overhead and compression
    // ratio far more than raw I/O does.
    let t0 = Instant::now();
    let mut rows = 0usize;
    let mut batches_read = 0usize;
    for k in 0..partitions {
        let mut s = stream_sort_shuffle_partition(&data, &index, k).unwrap();
        while let Some(b) = s.try_next().await.unwrap() {
            rows += b.num_rows();
            batches_read += 1;
        }
    }
    let elapsed = t0.elapsed();
    // Sanity: the index must cover the whole file.
    assert_eq!(*offsets.last().unwrap() as u64, data_len);

    println!(
        "[read] {label:<34} K={partitions:<5} file={:>9} batches={batches_read:<6} rows={rows:<9} avg_rows/batch={:<6} read={elapsed:>10.2?}",
        human(data_len),
        rows / batches_read.max(1),
    );
}

// ---------------------------------------------------------------------------
// Experiment 2: per-partition schema overhead in the consolidated file.
// ---------------------------------------------------------------------------

fn ipc_header_bytes(schema: &SchemaRef) -> usize {
    let mut out: Vec<u8> = Vec::new();
    let w =
        StreamWriter::try_new_with_options(&mut out, schema, IpcWriteOptions::default())
            .unwrap();
    w.into_inner().unwrap();
    out.len()
}

async fn exp_schema_overhead(
    label: &str,
    schema: &SchemaRef,
    batches: usize,
    partitions: usize,
) {
    let dir = tempfile::TempDir::new().unwrap();
    let (data, _index) =
        write_shuffle(schema, batches, partitions, dir.path(), None).await;
    let data_len = std::fs::metadata(&data).unwrap().len();
    let header = ipc_header_bytes(schema) as u64;
    // One schema-header stream + one stream per non-empty partition.
    let overhead = header * (partitions as u64 + 1);
    println!(
        "[schema] {label:<32} K={partitions:<5} file={:>9} schema_msg={:>8} est_overhead={:>9} ({:.1}% of file)",
        human(data_len),
        human(header),
        human(overhead),
        100.0 * overhead as f64 / data_len as f64
    );
}

// ---------------------------------------------------------------------------
// Experiment 3: hash-partition index computation, Vec<Vec<u32>> vs flat CSR.
// ---------------------------------------------------------------------------

fn indices_nested(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    k: usize,
    hashes: &mut Vec<u64>,
) -> Vec<Vec<u32>> {
    let arrays = evaluate_expressions_to_arrays(exprs, batch).unwrap();
    hashes.clear();
    hashes.resize(batch.num_rows(), 0);
    create_hashes(&arrays, REPARTITION_RANDOM_STATE.random_state(), hashes).unwrap();
    let mut out: Vec<Vec<u32>> = (0..k).map(|_| Vec::new()).collect();
    for (row, &h) in hashes.iter().enumerate() {
        out[(h % k as u64) as usize].push(row as u32);
    }
    out
}

/// Counting-sort layout: one `rows` buffer plus `offsets` (CSR), no per-
/// partition Vec allocation.
fn indices_csr(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    k: usize,
    hashes: &mut Vec<u64>,
    part_of_row: &mut Vec<u32>,
    offsets: &mut Vec<u32>,
    rows: &mut Vec<u32>,
) {
    let arrays = evaluate_expressions_to_arrays(exprs, batch).unwrap();
    let n = batch.num_rows();
    hashes.clear();
    hashes.resize(n, 0);
    create_hashes(&arrays, REPARTITION_RANDOM_STATE.random_state(), hashes).unwrap();

    part_of_row.clear();
    part_of_row.reserve(n);
    offsets.clear();
    offsets.resize(k + 1, 0);
    for &h in hashes.iter() {
        let p = (h % k as u64) as u32;
        part_of_row.push(p);
        offsets[p as usize + 1] += 1;
    }
    for i in 0..k {
        offsets[i + 1] += offsets[i];
    }
    rows.clear();
    rows.resize(n, 0);
    let mut cursor: Vec<u32> = offsets[..k].to_vec();
    for (row, &p) in part_of_row.iter().enumerate() {
        let slot = &mut cursor[p as usize];
        rows[*slot as usize] = row as u32;
        *slot += 1;
    }
}

fn exp_partition_indices(schema: &SchemaRef, k: usize, iters: usize) {
    let batch = build_batch(schema, 7);
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("i0", 0))];

    let mut hashes = Vec::new();
    // warm
    let _ = indices_nested(&batch, &exprs, k, &mut hashes);

    let t0 = Instant::now();
    let mut sink = 0usize;
    for _ in 0..iters {
        let v = indices_nested(&batch, &exprs, k, &mut hashes);
        sink += v.len();
    }
    let nested = t0.elapsed();

    let mut part_of_row = Vec::new();
    let mut offsets = Vec::new();
    let mut rows = Vec::new();
    let t1 = Instant::now();
    for _ in 0..iters {
        indices_csr(
            &batch,
            &exprs,
            k,
            &mut hashes,
            &mut part_of_row,
            &mut offsets,
            &mut rows,
        );
        sink += rows.len();
    }
    let csr = t1.elapsed();

    println!(
        "[partition-idx] K={k:<5} iters={iters:<5} nested={:>10.2?} csr={:>10.2?} speedup={:.2}x (sink={sink})",
        nested / iters as u32,
        csr / iters as u32,
        nested.as_secs_f64() / csr.as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Experiment 4: index lookup — whole-file read (current) vs pread of the two
// offsets the caller actually needs.
// ---------------------------------------------------------------------------

fn index_lookup_pread(path: &Path, partition_id: usize) -> (i64, i64) {
    use std::os::unix::fs::FileExt;
    let f = File::open(path).unwrap();
    let mut buf = [0u8; 16];
    f.read_exact_at(&mut buf, (partition_id * 8) as u64)
        .unwrap();
    (
        i64::from_le_bytes(buf[..8].try_into().unwrap()),
        i64::from_le_bytes(buf[8..].try_into().unwrap()),
    )
}

fn exp_index_lookup(index_path: &Path, partitions: usize) {
    let iters = 200;
    let t0 = Instant::now();
    let mut sink = 0i64;
    for _ in 0..iters {
        for k in 0..partitions {
            let offs = read_index(index_path);
            sink += offs[k] + offs[k + 1];
        }
    }
    let whole = t0.elapsed();

    let t1 = Instant::now();
    for _ in 0..iters {
        for k in 0..partitions {
            let (a, b) = index_lookup_pread(index_path, k);
            sink += a + b;
        }
    }
    let pread = t1.elapsed();

    println!(
        "[index] K={partitions:<5} lookups={:<8} whole_file={:>10.2?} pread={:>10.2?} speedup={:.2}x (sink={sink})",
        iters * partitions,
        whole,
        pread,
        whole.as_secs_f64() / pread.as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Experiment 5: write-side breakdown (repart vs spill vs write).
// ---------------------------------------------------------------------------

async fn exp_write_breakdown(
    label: &str,
    schema: &SchemaRef,
    batches: usize,
    partitions: usize,
    memory_limit: Option<usize>,
) {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    let dir = tempfile::TempDir::new().unwrap();
    let ctx = match memory_limit {
        Some(l) => {
            let rt = Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_limit(l, 1.0)
                    .build()
                    .unwrap(),
            );
            SessionContext::new_with_config_rt(Default::default(), rt)
        }
        None => SessionContext::new(),
    };
    let config = SortShuffleConfig::new(true, BATCH_ROWS)
        .with_memory_limit_per_task_bytes(memory_limit.unwrap_or(0));
    let writer = Arc::new(
        SortShuffleWriterExec::try_new(
            "lab_job".into(),
            1,
            make_input(schema, batches),
            dir.path().to_str().unwrap().to_string(),
            Partitioning::Hash(vec![Arc::new(Column::new("i0", 0))], partitions),
            config,
        )
        .unwrap(),
    );
    let task_ctx = ctx.task_ctx();
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for k in 0..partitions {
        let p = writer.clone();
        let c = task_ctx.clone();
        handles.push(tokio::spawn(async move {
            let mut s = p.execute(k, c).unwrap();
            while (s.try_next().await.unwrap()).is_some() {}
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let total = t0.elapsed();

    let m = writer.metrics().unwrap();
    let get = |name: &str| -> f64 {
        m.iter()
            .filter(|v| v.value().name() == name)
            .map(|v| v.value().as_usize() as f64)
            .sum()
    };
    let repart_ms = get("repart_time") / 1e6;
    let spill_ms = get("spill_time") / 1e6;
    let write_ms = get("write_time") / 1e6;
    println!(
        "[write] {label:<24} K={partitions:<5} total={total:>9.2?} repart={repart_ms:>8.1}ms spill={spill_ms:>8.1}ms finalize={write_ms:>8.1}ms  repart_share={:.0}%",
        100.0 * repart_ms / total.as_secs_f64() / 1000.0
    );
}

// ---------------------------------------------------------------------------
// Experiment 6: coalescing spill runs at finalize time. Models replacing the
// verbatim spill-file byte copy with decode -> coalesce -> re-encode, by
// rewriting an already-produced shuffle file partition by partition.
// ---------------------------------------------------------------------------

async fn exp_finalize_coalesce(
    label: &str,
    schema: &SchemaRef,
    batches: usize,
    partitions: usize,
    memory_limit: Option<usize>,
) {
    use datafusion::arrow::compute::concat_batches;
    let dir = tempfile::TempDir::new().unwrap();
    let (data, index) =
        write_shuffle(schema, batches, partitions, dir.path(), memory_limit).await;
    let raw_len = std::fs::metadata(&data).unwrap().len();

    let opts = IpcWriteOptions::default()
        .try_with_compression(Some(datafusion::arrow::ipc::CompressionType::LZ4_FRAME))
        .unwrap();

    let out_path = dir.path().join("coalesced.arrow");
    let t0 = Instant::now();
    let mut out = std::io::BufWriter::new(File::create(&out_path).unwrap());
    let mut in_batches = 0usize;
    let mut out_batches = 0usize;
    for k in 0..partitions {
        let mut s = stream_sort_shuffle_partition(&data, &index, k).unwrap();
        let mut pending: Vec<RecordBatch> = Vec::new();
        let mut pending_rows = 0usize;
        let mut w =
            StreamWriter::try_new_with_options(&mut out, schema, opts.clone()).unwrap();
        while let Some(b) = s.try_next().await.unwrap() {
            in_batches += 1;
            pending_rows += b.num_rows();
            pending.push(b);
            if pending_rows >= BATCH_ROWS {
                let merged = concat_batches(schema, &pending).unwrap();
                w.write(&merged).unwrap();
                out_batches += 1;
                pending.clear();
                pending_rows = 0;
            }
        }
        if !pending.is_empty() {
            let merged = concat_batches(schema, &pending).unwrap();
            w.write(&merged).unwrap();
            out_batches += 1;
        }
        w.finish().unwrap();
    }
    drop(out);
    let elapsed = t0.elapsed();
    let new_len = std::fs::metadata(&out_path).unwrap().len();

    println!(
        "[coalesce] {label:<26} K={partitions:<5} verbatim={:>9} coalesced={:>9} ({:+.0}%) batches {in_batches}->{out_batches}  rewrite_cost={elapsed:>9.2?}",
        human(raw_len),
        human(new_len),
        100.0 * (new_len as f64 - raw_len as f64) / raw_len as f64,
    );
}

// ---------------------------------------------------------------------------
// Experiment 7: what is inside finalize? compression codec sweep.
// ---------------------------------------------------------------------------

async fn exp_codec(
    schema: &SchemaRef,
    batches: usize,
    partitions: usize,
    codec: &str,
    memory_limit: Option<usize>,
) {
    use ballista_core::config::BALLISTA_SHUFFLE_COMPRESSION_CODEC;
    use ballista_core::extension::SessionConfigExt;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = datafusion::execution::config::SessionConfig::new_with_ballista();
    cfg = cfg.set_str(BALLISTA_SHUFFLE_COMPRESSION_CODEC, codec);
    let rt = Arc::new(
        match memory_limit {
            Some(l) => RuntimeEnvBuilder::new().with_memory_limit(l, 1.0),
            None => RuntimeEnvBuilder::new(),
        }
        .build()
        .unwrap(),
    );
    let ctx = SessionContext::new_with_config_rt(cfg, rt);

    let config = SortShuffleConfig::new(true, BATCH_ROWS)
        .with_memory_limit_per_task_bytes(memory_limit.unwrap_or(0));
    let writer = Arc::new(
        SortShuffleWriterExec::try_new(
            "lab_job".into(),
            1,
            make_input(schema, batches),
            dir.path().to_str().unwrap().to_string(),
            Partitioning::Hash(vec![Arc::new(Column::new("i0", 0))], partitions),
            config,
        )
        .unwrap(),
    );
    let task_ctx = ctx.task_ctx();
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for k in 0..partitions {
        let p = writer.clone();
        let c = task_ctx.clone();
        handles.push(tokio::spawn(async move {
            let mut s = p.execute(k, c).unwrap();
            while (s.try_next().await.unwrap()).is_some() {}
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let total = t0.elapsed();
    let data = dir
        .path()
        .join("lab_job")
        .join("1")
        .join("0")
        .join("data.arrow");
    let len = std::fs::metadata(&data).unwrap().len();
    let index = get_index_path(&data);

    // Read every partition back so the codec's decode cost is visible too.
    let t1 = Instant::now();
    let mut rows = 0usize;
    for k in 0..partitions {
        let mut s = stream_sort_shuffle_partition(&data, &index, k).unwrap();
        while let Some(b) = s.try_next().await.unwrap() {
            rows += b.num_rows();
        }
    }
    let read = t1.elapsed();
    println!(
        "[codec] {codec:<5} K={partitions:<5} spill={:<5} write={total:>9.2?} read={read:>9.2?} file={:>9} rows={rows}",
        memory_limit.is_some(),
        human(len)
    );
}

// ---------------------------------------------------------------------------
// Experiment 8: spill-file / file-descriptor pressure. The spill manager keeps
// one open StreamWriter per output partition for the lifetime of the write.
// ---------------------------------------------------------------------------

async fn exp_spill_files(schema: &SchemaRef, batches: usize, partitions: usize) {
    let dir = tempfile::TempDir::new().unwrap();
    // Keep the spill dir alive by writing into a path we control, then count
    // the spill files created before cleanup by watching the peak fd count.
    let before = count_open_fds();
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(before));
    let peak_c = peak.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_c = stop.clone();
    let sampler = std::thread::spawn(move || {
        while !stop_c.load(std::sync::atomic::Ordering::Relaxed) {
            let n = count_open_fds();
            peak_c.fetch_max(n, std::sync::atomic::Ordering::Relaxed);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });

    let _ = write_shuffle(schema, batches, partitions, dir.path(), Some(4 << 20)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    sampler.join().unwrap();

    println!(
        "[fds] K={partitions:<5} open_fds before={before:<5} peak={:<6} delta={}",
        peak.load(std::sync::atomic::Ordering::Relaxed),
        peak.load(std::sync::atomic::Ordering::Relaxed) as i64 - before as i64
    );
}

fn count_open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

fn human(bytes: u64) -> String {
    const U: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < 3 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1}{}", U[i])
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let wide = schema_with(30, 5);
    let narrow = schema_with(4, 1);

    println!("== experiment 9: in-memory hand-off ceiling ==");
    for k in [200usize, 1000, 4000] {
        exp_in_memory("narrow(5 cols)", &narrow, 40, k).await;
    }
    for k in [200usize, 1000] {
        exp_in_memory("wide(35 cols)", &wide, 40, k).await;
    }

    println!("\n== experiment 8: spill file-descriptor pressure ==");
    for k in [200usize, 1000, 4000] {
        exp_spill_files(&narrow, 40, k).await;
    }

    println!("\n== experiment 7: compression codec ==");
    for codec in ["lz4", "zstd", "none"] {
        exp_codec(&narrow, 40, 1000, codec, None).await;
    }
    for codec in ["lz4", "none"] {
        exp_codec(&wide, 40, 1000, codec, None).await;
    }
    for codec in ["lz4", "none"] {
        exp_codec(&narrow, 60, 1000, codec, Some(8 << 20)).await;
    }

    println!("\n== experiment 5: write-side breakdown ==");
    for k in [200usize, 1000, 4000] {
        exp_write_breakdown("narrow, no spill", &narrow, 40, k, None).await;
    }
    exp_write_breakdown("wide, no spill", &wide, 40, 1000, None).await;
    exp_write_breakdown("narrow, spill 8MiB", &narrow, 60, 1000, Some(8 << 20)).await;

    println!("\n== experiment 6: coalesce at finalize ==");
    exp_finalize_coalesce("narrow, no spill", &narrow, 40, 200, None).await;
    exp_finalize_coalesce("narrow, spill 8MiB", &narrow, 60, 200, Some(8 << 20)).await;
    exp_finalize_coalesce("narrow, spill 2MiB", &narrow, 60, 200, Some(2 << 20)).await;
    exp_finalize_coalesce("narrow, spill 2MiB", &narrow, 60, 1000, Some(2 << 20)).await;
    exp_finalize_coalesce("wide, spill 8MiB", &wide, 40, 200, Some(8 << 20)).await;

    println!("\n== experiment 3: hash-partition index layout ==");
    for k in [64usize, 200, 1000, 4000] {
        exp_partition_indices(&narrow, k, 200);
    }

    println!("\n== experiment 2: schema overhead per partition ==");
    for k in [200usize, 1000, 4000] {
        exp_schema_overhead("wide(35 cols)", &wide, 40, k).await;
    }
    for k in [200usize, 1000, 4000] {
        exp_schema_overhead("narrow(5 cols)", &narrow, 40, k).await;
    }

    println!("\n== experiment 4: index lookup ==");
    for k in [200usize, 1000, 4000] {
        let dir = tempfile::TempDir::new().unwrap();
        let (_d, index) = write_shuffle(&narrow, 8, k, dir.path(), None).await;
        exp_index_lookup(&index, k);
    }

    println!("\n== experiment 1: sort-shuffle read path ==");
    exp_read_path("narrow, no spill", &narrow, 40, 200, None).await;
    exp_read_path("narrow, no spill", &narrow, 40, 1000, None).await;
    exp_read_path("wide, no spill", &wide, 40, 200, None).await;
    exp_read_path("narrow, spilling(8MiB)", &narrow, 60, 200, Some(8 << 20)).await;
    exp_read_path("narrow, spilling(2MiB)", &narrow, 60, 200, Some(2 << 20)).await;
}
