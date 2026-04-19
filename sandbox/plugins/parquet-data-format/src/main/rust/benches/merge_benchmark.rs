/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Criterion benchmarks: original `merge_sorted` vs `merge_sorted_optimized`.
//!
//! V4 optimizations over baseline:
//!   - Precomputed `ColumnMapping` per cursor (no per-batch name lookups)
//!   - Single-chunk `concat_batches` skip in flush
//!   - Dedicated 2-thread prefetch pool (separate from 4-thread encoding pool)
//!
//! Noise reduction:
//!   - Page cache pre-warmed before measurement
//!   - Output directory reused across iterations
//!   - 5s warm-up, 20s measurement, 20 samples
//!
//! Run:  cargo bench --bench merge_benchmark

use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use opensearch_parquet_format::merge::{merge_sorted, merge_sorted_optimized};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

fn bench_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn write_sorted_file(dir: &TempDir, file_idx: usize, num_rows: usize, num_files: usize) -> String {
    let schema = bench_schema();
    let path = dir.path().join(format!("input_{}.parquet", file_idx))
        .to_string_lossy().to_string();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    let timestamps: Vec<i64> = (0..num_rows)
        .map(|row| (row * num_files + file_idx) as i64 * 1000)
        .collect();
    let user_ids: Vec<i64> = (0..num_rows).map(|r| (r % 1000) as i64).collect();
    let messages: Vec<Option<&str>> = (0..num_rows)
        .map(|r| if r % 10 == 0 { None } else { Some("benchmark_message_payload") })
        .collect();
    let values: Vec<f64> = (0..num_rows).map(|r| r as f64 * 0.01).collect();

    let batch_size = 10_000;
    for chunk_start in (0..num_rows).step_by(batch_size) {
        let chunk_end = (chunk_start + batch_size).min(num_rows);
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(TimestampMillisecondArray::from(timestamps[chunk_start..chunk_end].to_vec())),
            Arc::new(Int64Array::from(user_ids[chunk_start..chunk_end].to_vec())),
            Arc::new(StringArray::from(messages[chunk_start..chunk_end].to_vec())),
            Arc::new(Float64Array::from(values[chunk_start..chunk_end].to_vec())),
        ]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn generate_test_files(dir: &TempDir, num_files: usize, rows_per_file: usize) -> Vec<String> {
    (0..num_files).map(|i| write_sorted_file(dir, i, rows_per_file, num_files)).collect()
}

fn warm_page_cache(files: &[String]) {
    let mut buf = vec![0u8; 64 * 1024];
    for path in files {
        let mut f = File::open(path).unwrap();
        while f.read(&mut buf).unwrap() > 0 {}
    }
}

// ─── Original vs V4 merge benchmark ─────────────────────────────────────────

fn bench_merge_sorted(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_sorted");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(5));
    group.measurement_time(Duration::from_secs(20));

    let configs = vec![
        (3, 50_000),
        (5, 100_000),
        (10, 100_000),
    ];

    for (num_files, rows_per_file) in &configs {
        let total_rows = num_files * rows_per_file;
        let label = format!("{}f_{}r", num_files, total_rows);

        let input_dir = TempDir::new().unwrap();
        let input_files = generate_test_files(&input_dir, *num_files, *rows_per_file);
        warm_page_cache(&input_files);

        let sort_cols = vec!["timestamp".to_string()];
        let reverse = vec![false];
        let nulls_first = vec![false];

        let out_dir_orig = TempDir::new().unwrap();
        let out_orig = out_dir_orig.path().join("merged.parquet").to_string_lossy().to_string();
        let out_dir_v4 = TempDir::new().unwrap();
        let out_v4 = out_dir_v4.path().join("merged.parquet").to_string_lossy().to_string();

        group.throughput(Throughput::Elements(total_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("original", &label),
            &(&input_files, &sort_cols, &reverse, &nulls_first, &out_orig),
            |b, (files, sc, rev, nf, out)| {
                b.iter(|| { merge_sorted(files, out, "bench-index", sc, rev, nf).unwrap(); });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("optimized", &label),
            &(&input_files, &sort_cols, &reverse, &nulls_first, &out_v4),
            |b, (files, sc, rev, nf, out)| {
                b.iter(|| { merge_sorted_optimized(files, out, "bench-index", sc, rev, nf).unwrap(); });
            },
        );
    }

    group.finish();
}

// ─── Schema padding micro-benchmark (pure CPU) ─────────────────────────────

fn bench_schema_padding(c: &mut Criterion) {
    use opensearch_parquet_format::merge::schema::pad_batch_to_schema;
    use opensearch_parquet_format::merge::schema_optimized::ColumnMapping;

    let mut group = c.benchmark_group("schema_padding");
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));

    let source_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Float64, false),
        Field::new("c", DataType::Utf8, true),
        Field::new("d", DataType::Int32, false),
        Field::new("e", DataType::Int64, false),
    ]));
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Float64, false),
        Field::new("c", DataType::Utf8, true),
        Field::new("d", DataType::Int32, false),
        Field::new("e", DataType::Int64, false),
        Field::new("f", DataType::Float64, true),
        Field::new("g", DataType::Utf8, true),
        Field::new("h", DataType::Int64, true),
        Field::new("i", DataType::Int32, true),
        Field::new("j", DataType::Float64, true),
        Field::new("k", DataType::Utf8, true),
        Field::new("l", DataType::Int64, true),
    ]));

    let num_rows = 100_000;
    let batch = RecordBatch::try_new(source_schema.clone(), vec![
        Arc::new(Int64Array::from_iter_values(0..num_rows as i64)),
        Arc::new(Float64Array::from_iter_values((0..num_rows).map(|i| i as f64))),
        Arc::new(StringArray::from_iter_values((0..num_rows).map(|i| format!("v{}", i)))),
        Arc::new(Int32Array::from_iter_values(0..num_rows as i32)),
        Arc::new(Int64Array::from_iter_values(0..num_rows as i64)),
    ]).unwrap();

    group.throughput(Throughput::Elements(num_rows as u64));

    group.bench_function("original_pad_batch", |b| {
        b.iter(|| { pad_batch_to_schema(&batch, &target_schema).unwrap(); });
    });

    let mapping = ColumnMapping::new(&source_schema, &target_schema);
    group.bench_function("optimized_pad_batch", |b| {
        b.iter(|| { mapping.pad_batch(&batch).unwrap(); });
    });

    group.finish();
}

criterion_group!(benches, bench_schema_padding, bench_merge_sorted);
criterion_main!(benches);
