# Parquet Merge Benchmarks

Criterion benchmarks comparing the baseline `merge_sorted` against the optimized `merge_sorted_optimized` for the native Rust k-way Parquet merge.

## Prerequisites

- Rust 1.70+ with `cargo`
- First run compiles in release+LTO mode (~90s). Subsequent runs reuse cached artifacts.

## Quick Start

```bash
cd sandbox/plugins/parquet-data-format/src/main/rust

# Run all benchmarks
cargo bench --bench merge_benchmark

# Run only the merge comparison (baseline vs optimized)
cargo bench --bench merge_benchmark -- "merge_sorted/"

# Run only the schema padding micro-benchmark
cargo bench --bench merge_benchmark -- "schema_padding"
```

## What Gets Benchmarked

### `merge_sorted` — End-to-End K-Way Merge

Generates synthetic sorted Parquet files with interleaving timestamps, then measures wall-clock time for the full merge pipeline (read → sort-merge → encode → write).

Three configurations:
- 3 files × 50k rows (150k total)
- 5 files × 100k rows (500k total)
- 10 files × 100k rows (1M total)

Each configuration runs both `original` (baseline) and `optimized` variants.

### `schema_padding` — Isolated CPU Micro-Benchmark

Measures `pad_batch_to_schema` (original name-based lookup) vs `ColumnMapping::pad_batch` (precomputed index mapping) on 100k rows with a 5→12 column schema gap.

## Noise Reduction

The benchmarks apply several techniques to minimize variance:
- 5s warm-up period before measurement
- 20s measurement window with 20 samples
- Page cache pre-warmed by reading all input files before measurement
- Output directory reused across iterations (no TempDir creation in hot loop)

## Interpreting Results

Criterion outputs three numbers per benchmark:

```
time:   [lower_bound  median  upper_bound]
```

The 95% confidence interval. If intervals don't overlap between original and optimized, the difference is statistically significant.

Criterion also reports `change` relative to the last saved baseline:
- "Performance has improved" = statistically significant speedup
- "No change in performance detected" = within noise

Results are saved to `target/criterion/` and compared across runs automatically.

## Optimizations in the Optimized Path

1. **Zero-allocation row comparison** (`row_compare.rs`, `cursor_optimized.rs`) — `arrow::row::RowConverter` pre-computes comparable byte representations per batch. Row comparison is a memcmp instead of per-row `Vec<SortKey>` allocation.

2. **Precomputed column mapping** (`schema_optimized.rs`) — `ColumnMapping` builds source→target column index mapping once per cursor, replacing per-batch `schema.index_of()` name lookups.

3. **Tokio prefetch pool separation** (`cursor_optimized.rs`) — Batch prefetch uses `tokio::spawn_blocking` instead of the Rayon encoding pool, keeping Rayon 100% available for CPU-bound column encoding.

4. **Single-chunk flush skip** (`context.rs`) — Skips `concat_batches` memcpy when only one batch is buffered before flush.

## CPU Profiling

A standalone binary is provided for attaching profilers:

```bash
# Build
cargo build --release --example profile_merge

# Run (prints PID for profiler attachment)
./target/release/examples/profile_merge \
  --variant optimized \
  --iters 20 \
  --files 10 \
  --rows 200000

# macOS: attach sampler to running process
sample <PID> 10 -f /tmp/merge_cpu_sample.txt

# Extract hot functions from sample output
grep -oE "[0-9]+ [a-zA-Z_].*\(in profile_merge\)" /tmp/merge_cpu_sample.txt \
  | sed 's/ (in profile_merge)//' \
  | awk '{count=$1; $1=""; name=$0; a[name]+=count} END {for(k in a) print a[k], k}' \
  | sort -rn | head -20
```

Arguments:
- `--variant original|optimized` (default: optimized)
- `--iters N` — number of merge iterations (default: 5)
- `--files N` — number of input files (default: 5)
- `--rows N` — rows per file (default: 100000)

## Benchmark Results

Measured on Apple M-series, 5s warm-up, 20s measurement, 20 samples:

| Config | Baseline | Optimized | Improvement |
|--------|----------|-----------|-------------|
| 3 files × 50k (150k rows) | 295 ms | 236 ms | 19.9% faster |
| 5 files × 100k (500k rows) | 869 ms | 676 ms | 22.2% faster |
| 10 files × 100k (1M rows) | 1586 ms | 1368 ms | 13.7% faster |

All results have non-overlapping 95% confidence intervals.
