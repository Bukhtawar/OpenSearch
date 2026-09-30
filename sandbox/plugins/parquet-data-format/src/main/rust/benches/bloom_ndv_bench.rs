//! Benchmark: bloom filter self-sizing (unset NDV + fold) vs the old pinned NDV=100k.
//!
//! Writes the same data twice through the plugin's `WriterPropertiesBuilder`:
//!   * `pinned_100k` : `index.parquet.bloom_filter_ndv = 100000` (the previous default)
//!   * `self_sized`  : NDV unset -> parquet sizes each filter for a full row group and folds
//!                     it down to the target FPP at row-group close (arrow-rs #9628).
//!
//! Columns (row groups of ROWS_PER_RG rows, NUM_RGS row groups):
//!   * `low_card`  : Utf8, LOW_CARD_DISTINCT distinct values shared by every row group
//!   * `high_card` : Utf8, unique per row (trace/request-id shape), each value in exactly one RG
//!   * `ts`        : Int64, no bloom filter (control column)
//!
//! Reports, per variant and bloomed column:
//!   * bloom filter bytes per row group (from the footer's bloom_filter_length)
//!   * measured FPP against values known to be absent from the probed row group
//!   * row groups that a point lookup on `high_card` cannot prune (ideal = 1 of NUM_RGS)
//!   * write wall time (fold cost is included)
//!
//! Run:
//!   cargo bench -p opensearch-parquet-format --bench bloom_ndv_bench
//!
//! Env overrides: BLOOM_BENCH_ROWS_PER_RG, BLOOM_BENCH_NUM_RGS, BLOOM_BENCH_FPP.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};

use opensearch_parquet_format::{FieldConfig, NativeSettings, WriterPropertiesBuilder};

const DEFAULT_ROWS_PER_RG: usize = 250_000;
const DEFAULT_NUM_RGS: usize = 4;
const DEFAULT_FPP: f64 = 0.1;
const BATCH_SIZE: usize = 8192;
const LOW_CARD_DISTINCT: usize = 50;
const PROBES: usize = 10_000;
const POINT_LOOKUPS: usize = 2_000;

const COL_LOW: usize = 0;
const COL_HIGH: usize = 1;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn low_card_value(row: usize) -> String {
    format!("service-{:03}", row % LOW_CARD_DISTINCT)
}

/// Unique per row; encodes the row group so we know exactly which RG holds it.
fn high_card_value(rg: usize, row_in_rg: usize) -> String {
    format!(
        "trace-{:02x}-{:016x}",
        rg,
        (row_in_rg as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
    )
}

fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("low_card", DataType::Utf8, false),
        Field::new("high_card", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

struct Variant {
    name: &'static str,
    ndv: Option<u64>,
}

struct Outcome {
    write_ms: u128,
    file_bytes: u64,
    low_bytes_per_rg: Vec<u64>,
    high_bytes_per_rg: Vec<u64>,
    low_fpp: f64,
    high_fpp: f64,
    /// Average number of row groups a point lookup on `high_card` must still read.
    high_rgs_per_lookup: f64,
}

fn write_file(path: &str, variant: &Variant, rows_per_rg: usize, num_rgs: usize, fpp: f64) -> u128 {
    // The control column carries no bloom filter in either variant.
    let mut field_configs = HashMap::new();
    field_configs.insert(
        "ts".to_string(),
        FieldConfig {
            bloom_filter_enabled: Some(false),
            ..Default::default()
        },
    );
    let settings = NativeSettings {
        bloom_filter_enabled: Some(true),
        bloom_filter_fpp: Some(fpp),
        bloom_filter_ndv: variant.ndv,
        row_group_max_rows: Some(rows_per_rg),
        field_configs: Some(field_configs),
        ..Default::default()
    };
    let schema = schema();
    let props = WriterPropertiesBuilder::build(&settings, &schema).unwrap();

    let start = Instant::now();
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    for rg in 0..num_rgs {
        let mut written = 0;
        while written < rows_per_rg {
            let n = BATCH_SIZE.min(rows_per_rg - written);
            let low: Vec<String> = (0..n).map(|i| low_card_value(written + i)).collect();
            let high: Vec<String> = (0..n).map(|i| high_card_value(rg, written + i)).collect();
            let ts: Vec<i64> = (0..n)
                .map(|i| (rg * rows_per_rg + written + i) as i64)
                .collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(low)),
                    Arc::new(StringArray::from(high)),
                    Arc::new(Int64Array::from(ts)),
                ],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            written += n;
        }
        // Rows are counted by the writer against max_row_group_row_count; flush explicitly so
        // each RG holds exactly rows_per_rg regardless of batch alignment.
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    start.elapsed().as_millis()
}

fn measure(
    path: &str,
    num_rgs: usize,
    rows_per_rg: usize,
) -> (u64, Vec<u64>, Vec<u64>, f64, f64, f64) {
    let file = fs::File::open(path).unwrap();
    let options = ReadOptionsBuilder::new()
        .with_reader_properties(
            ReaderProperties::builder()
                .set_read_bloom_filter(true)
                .build(),
        )
        .build();
    let reader = SerializedFileReader::new_with_options(file, options).unwrap();
    let meta = reader.metadata();
    assert_eq!(meta.num_row_groups(), num_rgs, "unexpected row group count");

    let mut low_bytes = Vec::with_capacity(num_rgs);
    let mut high_bytes = Vec::with_capacity(num_rgs);
    for rg in 0..num_rgs {
        let rgm = meta.row_group(rg);
        assert_eq!(rgm.num_rows() as usize, rows_per_rg);
        low_bytes.push(rgm.column(COL_LOW).bloom_filter_length().unwrap_or(0) as u64);
        high_bytes.push(rgm.column(COL_HIGH).bloom_filter_length().unwrap_or(0) as u64);
    }

    // low_card FPP: probe values that never occur anywhere.
    let mut low_fp = 0usize;
    let mut low_total = 0usize;
    for rg in 0..num_rgs {
        let rg_reader = reader.get_row_group(rg).unwrap();
        let sbbf = rg_reader
            .get_column_bloom_filter(COL_LOW)
            .expect("low_card bloom filter missing");
        for i in 0..PROBES {
            let absent = format!("absent-{:06}", i);
            if sbbf.check(absent.as_str()) {
                low_fp += 1;
            }
            low_total += 1;
        }
    }

    // high_card FPP: probe each RG with values that live in a DIFFERENT RG (realistic point
    // lookup shape), and count how many RGs a lookup cannot prune.
    let mut high_fp = 0usize;
    let mut high_total = 0usize;
    let mut rgs_read = 0usize;
    for lookup in 0..POINT_LOOKUPS {
        let home_rg = lookup % num_rgs;
        let row = (lookup * 7919) % rows_per_rg;
        let value = high_card_value(home_rg, row);
        let mut maybe_present = 0usize;
        for rg in 0..num_rgs {
            let rg_reader = reader.get_row_group(rg).unwrap();
            let sbbf = rg_reader
                .get_column_bloom_filter(COL_HIGH)
                .expect("high_card bloom filter missing");
            let hit = sbbf.check(value.as_str());
            if rg == home_rg {
                assert!(hit, "bloom filter returned a false negative");
            } else {
                high_total += 1;
                if hit {
                    high_fp += 1;
                }
            }
            if hit {
                maybe_present += 1;
            }
        }
        rgs_read += maybe_present;
    }

    (
        fs::metadata(path).unwrap().len(),
        low_bytes,
        high_bytes,
        low_fp as f64 / low_total as f64,
        high_fp as f64 / high_total as f64,
        rgs_read as f64 / POINT_LOOKUPS as f64,
    )
}

fn human(bytes: u64) -> String {
    if bytes >= 1 << 20 {
        format!("{:.1} MiB", bytes as f64 / (1u64 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn main() {
    let rows_per_rg = env_usize("BLOOM_BENCH_ROWS_PER_RG", DEFAULT_ROWS_PER_RG);
    let num_rgs = env_usize("BLOOM_BENCH_NUM_RGS", DEFAULT_NUM_RGS);
    let fpp = env_f64("BLOOM_BENCH_FPP", DEFAULT_FPP);

    let dir = std::env::temp_dir().join(format!("bloom_ndv_bench_{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();

    println!(
        "bloom NDV bench: {} row groups x {} rows, target fpp {}, low_card distinct {}",
        num_rgs, rows_per_rg, fpp, LOW_CARD_DISTINCT
    );
    println!();

    let variants = [
        Variant {
            name: "pinned_100k",
            ndv: Some(100_000),
        },
        Variant {
            name: "self_sized",
            ndv: None,
        },
    ];

    let mut outcomes = Vec::new();
    for v in &variants {
        let path = dir.join(format!("{}.parquet", v.name));
        let path = path.to_str().unwrap();
        let write_ms = write_file(path, v, rows_per_rg, num_rgs, fpp);
        let (
            file_bytes,
            low_bytes_per_rg,
            high_bytes_per_rg,
            low_fpp,
            high_fpp,
            high_rgs_per_lookup,
        ) = measure(path, num_rgs, rows_per_rg);
        outcomes.push(Outcome {
            write_ms,
            file_bytes,
            low_bytes_per_rg,
            high_bytes_per_rg,
            low_fpp,
            high_fpp,
            high_rgs_per_lookup,
        });
    }

    println!(
        "{:<12} {:>9} {:>11} {:>16} {:>10} {:>16} {:>10} {:>14}",
        "variant",
        "write_ms",
        "file",
        "low_card bf/RG",
        "low fpp",
        "high_card bf/RG",
        "high fpp",
        "RGs/lookup"
    );
    for (v, o) in variants.iter().zip(outcomes.iter()) {
        println!(
            "{:<12} {:>9} {:>11} {:>16} {:>10.4} {:>16} {:>10.4} {:>9.2}/{:<3}",
            v.name,
            o.write_ms,
            human(o.file_bytes),
            human(o.low_bytes_per_rg[0]),
            o.low_fpp,
            human(o.high_bytes_per_rg[0]),
            o.high_fpp,
            o.high_rgs_per_lookup,
            num_rgs,
        );
    }

    let pinned = &outcomes[0];
    let folded = &outcomes[1];
    let low_total_pinned: u64 = pinned.low_bytes_per_rg.iter().sum();
    let low_total_folded: u64 = folded.low_bytes_per_rg.iter().sum();
    let high_total_pinned: u64 = pinned.high_bytes_per_rg.iter().sum();
    let high_total_folded: u64 = folded.high_bytes_per_rg.iter().sum();
    println!();
    println!("key benefits (self_sized vs pinned_100k):");
    println!(
        "  low_card  filter bytes: {} -> {} ({:.1}x smaller), fpp {:.4} -> {:.4}",
        human(low_total_pinned),
        human(low_total_folded),
        low_total_pinned as f64 / low_total_folded.max(1) as f64,
        pinned.low_fpp,
        folded.low_fpp
    );
    println!(
        "  high_card fpp: {:.4} -> {:.4} (target {}), filter bytes {} -> {}",
        pinned.high_fpp,
        folded.high_fpp,
        fpp,
        human(high_total_pinned),
        human(high_total_folded)
    );
    println!(
        "  high_card point lookup reads {:.2} -> {:.2} of {} row groups (ideal 1.00)",
        pinned.high_rgs_per_lookup, folded.high_rgs_per_lookup, num_rgs
    );
    println!(
        "  write time {} ms -> {} ms (fold cost included)",
        pinned.write_ms, folded.write_ms
    );

    let _ = fs::remove_dir_all(&dir);
}
