/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Benchmark for the `push_batch` parallel-write gate in the sorted merge.
//!
//! A sorted k-way merge over segments whose sort keys interleave emits many short runs
//! (Tier 3 in `merge/sorted.rs`), and every run is one `MergeContext::push_batch` call.
//! Before the gate, each call fanned one rayon job per column across the merge pool
//! regardless of row count, so a 3-row run paid a ~N_columns-job dispatch plus worker
//! spin. This bench drives the sorted merge over inputs with controlled run lengths and
//! compares three settings of `merge_parallel_write_min_rows`:
//!
//! - `parallel-always` (0): the pre-gate behaviour.
//! - `gated` (crate default, see `NativeSettings::get_merge_parallel_write_min_rows`):
//!   small batches inline, large batches parallel.
//! - `inline-always` (usize::MAX): never fan out, to show the parallel path still pays
//!   for full batches.
//!
//! The headline number is process CPU seconds (user+sys via getrusage), because rayon
//! spin burns cores without necessarily lengthening wall time on an idle machine.
//!
//! Run:
//!   cargo bench -p opensearch-parquet-format --bench merge_push_batch_gate_bench

use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
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

struct Variant {
    label: &'static str,
    /// `None` leaves the setting unset so the crate default applies.
    min_rows: Option<usize>,
}

const VARIANTS: [Variant; 3] = [
    Variant {
        label: "parallel-always",
        min_rows: Some(0),
    },
    Variant {
        label: "gated (default)",
        min_rows: None,
    },
    Variant {
        label: "inline-always",
        min_rows: Some(usize::MAX),
    },
];

struct Sample {
    wall: Duration,
    cpu: Duration,
    output_rows: i64,
    row_groups: usize,
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

    println!("┌──────────────────────────────────────────────────────────────────────────────");
    println!(
        "│ {}  ({} files × {} rows, {} cols, {:.1} MB input)",
        pattern.label(),
        NUM_FILES,
        ROWS_PER_FILE,
        1 + STRING_COLUMNS + INT_COLUMNS,
        input_bytes as f64 / 1_048_576.0
    );
    println!("├──────────────────────┬──────────┬──────────┬──────────────┬─────────────────");
    println!("│ variant              │ wall     │ cpu      │ cpu/wall     │ output");
    println!("├──────────────────────┼──────────┼──────────┼──────────────┼─────────────────");

    let mut baseline_cpu: Option<Duration> = None;
    for variant in VARIANTS.iter() {
        let index_name = format!("bench_gate_{}_{}", pattern.tag(), variant.label);
        SETTINGS_STORE.insert(
            index_name.clone(),
            NativeSettings {
                merge_parallel_write_min_rows: variant.min_rows,
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
        let rel = match baseline_cpu {
            None => {
                baseline_cpu = Some(s.cpu);
                "baseline".to_string()
            }
            Some(b) => format!(
                "{:+.0}% cpu",
                (s.cpu.as_secs_f64() / b.as_secs_f64() - 1.0) * 100.0
            ),
        };
        println!(
            "│ {:<20} │ {:>7.2}s │ {:>7.2}s │ {:>5.2}x       │ {} rows/{} RG  {}",
            variant.label,
            s.wall.as_secs_f64(),
            s.cpu.as_secs_f64(),
            s.cpu.as_secs_f64() / s.wall.as_secs_f64(),
            s.output_rows,
            s.row_groups,
            rel
        );
    }
    println!("└──────────────────────┴──────────┴──────────┴──────────────┴─────────────────");
    println!();
}

fn main() {
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!(" push_batch parallel-write gate benchmark");
    println!(
        " merge pool threads = {}, host cores = {}",
        MERGE_POOL_THREADS,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)
    );
    println!(" cpu = process user+sys (getrusage); cpu/wall > 1 means other cores were busy");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!();

    for pattern in [
        Pattern::Interleaved { run: 1 },
        Pattern::Interleaved { run: 8 },
        Pattern::Interleaved { run: 256 },
        Pattern::Interleaved { run: 1024 },
        Pattern::Interleaved { run: 4096 },
        Pattern::Disjoint,
    ] {
        bench_pattern(pattern);
    }
}
