/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Standalone binary for CPU and lock profiling of Parquet merge.
//!
//! Usage:
//!   # CPU flamegraph (requires sudo for dtrace on macOS):
//!   cargo flamegraph --example profile_merge -- --variant optimized --iters 10
//!
//!   # Or build and profile manually:
//!   cargo build --release --example profile_merge
//!   sample <PID> 10 -f /tmp/merge_sample.txt
//!
//! Arguments:
//!   --variant original|optimized  (default: optimized)
//!   --iters N                     (default: 5)
//!   --files N                     (default: 5)
//!   --rows N                      (default: 100000)

use std::env;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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

fn write_sorted_file(dir: &TempDir, idx: usize, rows: usize, nfiles: usize) -> String {
    let schema = bench_schema();
    let path = dir.path().join(format!("input_{}.parquet", idx)).to_string_lossy().to_string();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    let ts: Vec<i64> = (0..rows).map(|r| (r * nfiles + idx) as i64 * 1000).collect();
    let uids: Vec<i64> = (0..rows).map(|r| (r % 1000) as i64).collect();
    let msgs: Vec<Option<&str>> = (0..rows)
        .map(|r| if r % 10 == 0 { None } else { Some("payload") })
        .collect();
    let vals: Vec<f64> = (0..rows).map(|r| r as f64 * 0.01).collect();

    for start in (0..rows).step_by(10_000) {
        let end = (start + 10_000).min(rows);
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(TimestampMillisecondArray::from(ts[start..end].to_vec())),
            Arc::new(Int64Array::from(uids[start..end].to_vec())),
            Arc::new(StringArray::from(msgs[start..end].to_vec())),
            Arc::new(Float64Array::from(vals[start..end].to_vec())),
        ]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn parse_arg(args: &[String], flag: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let variant = parse_arg(&args, "--variant", "optimized");
    let iters: usize = parse_arg(&args, "--iters", "5").parse().unwrap();
    let nfiles: usize = parse_arg(&args, "--files", "5").parse().unwrap();
    let rows: usize = parse_arg(&args, "--rows", "100000").parse().unwrap();

    eprintln!("Profiling: variant={}, iters={}, files={}, rows_per_file={}", variant, iters, nfiles, rows);
    eprintln!("PID: {} — attach profiler now if needed", std::process::id());

    // Generate input files
    let input_dir = TempDir::new().unwrap();
    let input_files: Vec<String> = (0..nfiles)
        .map(|i| write_sorted_file(&input_dir, i, rows, nfiles))
        .collect();

    let sort_cols = vec!["timestamp".to_string()];
    let reverse = vec![false];
    let nulls_first = vec![false];
    let out_dir = TempDir::new().unwrap();
    let out_path = out_dir.path().join("merged.parquet").to_string_lossy().to_string();

    // Warm up
    match variant.as_str() {
        "original" => merge_sorted(&input_files, &out_path, "profile", &sort_cols, &reverse, &nulls_first).unwrap(),
        _ => merge_sorted_optimized(&input_files, &out_path, "profile", &sort_cols, &reverse, &nulls_first).unwrap(),
    }

    // Profiled iterations
    let start = Instant::now();
    for i in 0..iters {
        let iter_start = Instant::now();
        match variant.as_str() {
            "original" => merge_sorted(&input_files, &out_path, "profile", &sort_cols, &reverse, &nulls_first).unwrap(),
            _ => merge_sorted_optimized(&input_files, &out_path, "profile", &sort_cols, &reverse, &nulls_first).unwrap(),
        }
        eprintln!("  iter {}: {:.1}ms", i, iter_start.elapsed().as_millis());
    }
    let total = start.elapsed();
    eprintln!("Total: {:.1}ms ({:.1}ms/iter)", total.as_millis(), total.as_millis() as f64 / iters as f64);
}
