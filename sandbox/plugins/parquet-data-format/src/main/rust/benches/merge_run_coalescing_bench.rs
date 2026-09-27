/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Benchmark for run coalescing in the sorted merge (`merge/sorted.rs::RunCoalescer`).
//!
//! A sorted k-way merge over segments whose sort keys interleave emits one Tier-3 run per
//! heap pop, often a handful of rows, and every run used to be one
//! `MergeContext::push_batch` call. Two changes target that:
//!
//! - the parallel-write **gate** (`merge_parallel_write_min_rows`) stops fanning tiny
//!   batches across the rayon pool, removing scheduler spin but leaving one push per run;
//! - **coalescing** (`merge_coalesce_rows`) accumulates runs and pushes once per
//!   `merge_batch_size` rows, removing the per-push fixed cost (`append_row_id`,
//!   `compute_leaves`, per-column level buffers and `ColumnPath` lookups, `memory_size`)
//!   and giving every push enough rows to make the parallel path worthwhile.
//!
//! Four variants are compared per interleave pattern:
//!
//! | variant            | gate    | coalesce |
//! |--------------------|---------|----------|
//! | `old`              | off (0) | off (1)  |
//! | `gate only`        | default | off (1)  |
//! | `coalesce only`    | off (0) | default  |
//! | `gate + coalesce`  | default | default  |
//!
//! Every variant's output is checked for identical rows against `old`. The bytes may differ:
//! parquet-rs decides page boundaries per write call (`should_add_data_page` after each
//! mini-batch), so grouping rows into fewer, larger calls can shift where pages split. The
//! gate alone is byte-identical; coalescing is content-identical.
//!
//! Run:
//!   cargo bench -p opensearch-parquet-format --bench merge_run_coalescing_bench

use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use opensearch_parquet_format::{NativeSettings, SETTINGS_STORE};

const NUM_FILES: usize = 6;
const ROWS_PER_FILE: usize = 100_000;
/// Mirrors a log mapping: one sort key, many low-cardinality keywords, a few ints.
const STRING_COLUMNS: usize = 40;
const INT_COLUMNS: usize = 5;
/// Distinct values per string column; keeps output dictionary-encoded and small so the
/// merge's fixed 20 MB/s output throttle does not dominate wall time.
const STRING_CARDINALITY: usize = 512;
const WRITE_BATCH: usize = 8192;
/// Threads for the shared merge pool (process-wide OnceLock, first caller wins).
const MERGE_POOL_THREADS: usize = 4;
const MEASURED_ITERATIONS: usize = 2;

/// How input sort keys interleave across files, which sets the Tier-3 run length.
#[derive(Clone, Copy)]
enum Pattern {
    /// Files alternate every `run` rows: key = (i / run) * NUM_FILES * run + f * run + i % run.
    Interleaved { run: usize },
    /// Each file owns a disjoint key range; the merge drains whole batches (Tier 1/2).
    Disjoint,
}

impl Pattern {
    fn label(&self) -> String {
        match self {
            Pattern::Interleaved { run } => format!("interleaved run={}", run),
            Pattern::Disjoint => "disjoint (whole batches)".to_string(),
        }
    }

    fn tag(&self) -> String {
        match self {
            Pattern::Interleaved { run } => format!("il{}", run),
            Pattern::Disjoint => "disjoint".to_string(),
        }
    }

    fn key(&self, file_id: usize, row: usize) -> i64 {
        match self {
            Pattern::Interleaved { run } => {
                ((row / run) * NUM_FILES * run + file_id * run + (row % run)) as i64
            }
            Pattern::Disjoint => (file_id * ROWS_PER_FILE + row) as i64,
        }
    }
}

fn schema() -> Arc<ArrowSchema> {
    let mut fields = vec![Field::new("time", DataType::Int64, false)];
    for i in 0..STRING_COLUMNS {
        fields.push(Field::new(format!("kw_{}", i), DataType::Utf8, true));
    }
    for i in 0..INT_COLUMNS {
        fields.push(Field::new(format!("num_{}", i), DataType::Int32, true));
    }
    Arc::new(ArrowSchema::new(fields))
}

fn generate_file(path: &str, file_id: usize, pattern: Pattern) {
    let schema = schema();
    let file = fs::File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_FILE))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut written = 0;
    while written < ROWS_PER_FILE {
        let len = WRITE_BATCH.min(ROWS_PER_FILE - written);
        let mut columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(schema.fields().len());

        let keys: Vec<i64> = (0..len)
            .map(|i| pattern.key(file_id, written + i))
            .collect();
        columns.push(Arc::new(Int64Array::from(keys)));

        for col in 0..STRING_COLUMNS {
            let values: Vec<String> = (0..len)
                .map(|i| {
                    let v = ((written + i) * 31 + col * 7 + file_id * 13) % STRING_CARDINALITY;
                    format!("value-{:04}", v)
                })
                .collect();
            columns.push(Arc::new(StringArray::from(values)));
        }
        for col in 0..INT_COLUMNS {
            let values: Vec<i32> = (0..len)
                .map(|i| ((written + i) as i32).wrapping_mul(17 + col as i32) % 100_000)
                .collect();
            columns.push(Arc::new(Int32Array::from(values)));
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        writer.write(&batch).unwrap();
        written += len;
    }
    writer.close().unwrap();
}

/// Process CPU time (user + system) so far.
fn cpu_time() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    assert_eq!(rc, 0, "getrusage failed");
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, (t.tv_usec as u32) * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// Reads a Parquet file back as one Arrow batch (row-id column included) for a logical
/// content comparison that is independent of page layout.
fn read_all(path: &str) -> RecordBatch {
    let file = fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(1 << 20)
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

struct Variant {
    label: &'static str,
    /// `None` leaves the setting unset so the crate default applies.
    parallel_min_rows: Option<usize>,
    /// `None` leaves the setting unset so the crate default applies (= merge batch size).
    coalesce_rows: Option<usize>,
}

const VARIANTS: [Variant; 4] = [
    Variant {
        label: "old",
        parallel_min_rows: Some(0),
        coalesce_rows: Some(1),
    },
    Variant {
        label: "gate only",
        parallel_min_rows: None,
        coalesce_rows: Some(1),
    },
    Variant {
        label: "coalesce only",
        parallel_min_rows: Some(0),
        coalesce_rows: None,
    },
    Variant {
        label: "gate + coalesce",
        parallel_min_rows: None,
        coalesce_rows: None,
    },
];

struct Sample {
    wall: Duration,
    cpu: Duration,
    output_rows: i64,
    row_groups: usize,
    crc32: u32,
}

fn run_once(inputs: &[String], output: &str, index_name: &str) -> Sample {
    let sort_columns = vec!["time".to_string()];
    let cpu0 = cpu_time();
    let t0 = Instant::now();
    let result = opensearch_parquet_format::merge::merge_sorted(
        inputs,
        output,
        index_name,
        &sort_columns,
        &[false],
        &[false],
        &[],
        1,
        &[],
    )
    .expect("merge failed");
    let wall = t0.elapsed();
    let cpu = cpu_time() - cpu0;
    Sample {
        wall,
        cpu,
        output_rows: result.metadata.file_metadata().num_rows(),
        row_groups: result.metadata.num_row_groups(),
        crc32: result.crc32,
    }
}

fn bench_pattern(pattern: Pattern) {
    let tmp = tempfile::tempdir().unwrap();
    let inputs: Vec<String> = (0..NUM_FILES)
        .map(|f| {
            let p = tmp.path().join(format!("in_{}.parquet", f));
            let s = p.to_str().unwrap().to_string();
            generate_file(&s, f, pattern);
            s
        })
        .collect();
    let input_bytes: u64 = inputs
        .iter()
        .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();
    let expected_rows = (NUM_FILES * ROWS_PER_FILE) as i64;

    println!(
        "┌──────────────────────────────────────────────────────────────────────────────────────"
    );
    println!(
        "│ {}  ({} files × {} rows, {} cols, {:.1} MB input)",
        pattern.label(),
        NUM_FILES,
        ROWS_PER_FILE,
        1 + STRING_COLUMNS + INT_COLUMNS,
        input_bytes as f64 / 1_048_576.0
    );
    println!(
        "├──────────────────┬──────────┬──────────┬──────────┬──────────────────────┬───────────"
    );
    println!("│ variant          │ wall     │ cpu      │ cpu/wall │ vs old               │ crc32");
    println!(
        "├──────────────────┼──────────┼──────────┼──────────┼──────────────────────┼───────────"
    );

    let mut baseline: Option<(Duration, Duration, u32)> = None;
    for variant in VARIANTS.iter() {
        let index_name = format!("bench_coalesce_{}_{}", pattern.tag(), variant.label);
        SETTINGS_STORE.insert(
            index_name.clone(),
            NativeSettings {
                merge_parallel_write_min_rows: variant.parallel_min_rows,
                merge_coalesce_rows: variant.coalesce_rows,
                merge_rayon_threads: Some(MERGE_POOL_THREADS),
                ..Default::default()
            },
        );
        let output = tmp
            .path()
            .join(format!("out_{}.parquet", variant.label.replace(' ', "_")));
        let output = output.to_str().unwrap();

        // Warm-up (page cache, pool threads, allocator), then measured runs; keep the
        // fastest wall sample and its CPU reading.
        let _ = run_once(&inputs, output, &index_name);
        let mut best: Option<Sample> = None;
        for _ in 0..MEASURED_ITERATIONS {
            let s = run_once(&inputs, output, &index_name);
            assert_eq!(s.output_rows, expected_rows, "row count mismatch");
            if best.as_ref().map_or(true, |b| s.wall < b.wall) {
                best = Some(s);
            }
        }
        let s = best.unwrap();
        let rel = match baseline {
            None => {
                baseline = Some((s.wall, s.cpu, s.crc32));
                "baseline".to_string()
            }
            Some((bw, bc, bcrc)) => {
                // Page boundaries depend on write-call granularity (parquet-rs checks
                // `should_add_data_page` per write mini-batch), so coalesced output may
                // differ in bytes from per-run output. The rows must be identical.
                if s.crc32 != bcrc {
                    let baseline_out = tmp.path().join(format!(
                        "out_{}.parquet",
                        VARIANTS[0].label.replace(' ', "_")
                    ));
                    assert!(
                        read_all(baseline_out.to_str().unwrap()) == read_all(output),
                        "output rows differ from baseline for variant '{}'",
                        variant.label
                    );
                }
                format!(
                    "{:+.0}% wall {:+.0}% cpu",
                    (s.wall.as_secs_f64() / bw.as_secs_f64() - 1.0) * 100.0,
                    (s.cpu.as_secs_f64() / bc.as_secs_f64() - 1.0) * 100.0
                )
            }
        };
        println!(
            "│ {:<16} │ {:>7.2}s │ {:>7.2}s │ {:>6.2}x  │ {:<20} │ {:#010x} ({} RG)",
            variant.label,
            s.wall.as_secs_f64(),
            s.cpu.as_secs_f64(),
            s.cpu.as_secs_f64() / s.wall.as_secs_f64(),
            rel,
            s.crc32,
            s.row_groups
        );
    }
    println!(
        "└──────────────────┴──────────┴──────────┴──────────┴──────────────────────┴───────────"
    );
    println!();
}

fn main() {
    println!();
    println!(
        "═══════════════════════════════════════════════════════════════════════════════════════"
    );
    println!(" sorted-merge run coalescing benchmark");
    println!(
        " merge pool threads = {}, host cores = {}",
        MERGE_POOL_THREADS,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)
    );
    println!(" cpu = process user+sys (getrusage); cpu/wall > 1 means other cores were busy");
    println!(
        "═══════════════════════════════════════════════════════════════════════════════════════"
    );
    println!();

    for pattern in [
        Pattern::Interleaved { run: 1 },
        Pattern::Interleaved { run: 8 },
        Pattern::Interleaved { run: 256 },
        Pattern::Disjoint,
    ] {
        bench_pattern(pattern);
    }
}
