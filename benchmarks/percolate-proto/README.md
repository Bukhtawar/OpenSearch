# percolate-proto: standing-rule evaluation cost on Parquet rows

A standalone harness that measures what it costs to evaluate a Security Analytics
sized Sigma rule set (782 `process_creation` rules) against rows of a Parquet
index, comparing row-at-a-time evaluators (Java and Rust) with the same rules
expressed as DataFusion predicates. It was built to size the matcher inside a
materialized-view round for detection workloads on the pluggable data-format
engine; the write-up and results tables live in the design doc
"Monitors on Mustang: percolate today vs. materialized-view shards".

## Layout

| Path | What |
| --- | --- |
| `compile_sigma.py` | Sigma YAML -> predicate IR (`rules.json`) and DataFusion SQL (`rules.sql`). `NO_RE=1` replaces regex leaves with `FALSE`. |
| `rust/src/gen.rs` | synthetic Sysmon-style dataset: Parquet (writer settings matching the 3.7 data-format defaults: LZ4_RAW, 1M-row groups, 1 MiB pages, 2 MiB dictionaries) plus a TSV copy; injects rule tokens into a fraction of rows with randomised case |
| `rust/src/dfscan.rs` | query-time leg: DataFusion 54, all rules as `LIKE`/`regexp_like` predicates (`where` = `_id` only, `project` = one boolean column per rule) |
| `rust/src/rsmatch.rs` | row-time leg a4: per-field Aho-Corasick DFA for `contains`, anchored DFA walks for prefix/suffix, memmem for sparse fields and glob segments, ahash for `eq`, linear-time regex; leaf bits -> candidate rules -> tree evaluation. Optional Vectorscan path (`VS=1` per row, `VS=batch` over the Arrow values buffer) when built with `VS_LIB_DIR` |
| `java/Harness.java`, `java/LeafIndex.java` | row-time legs a1 (per-row tree walk), a1cse (shared leaves memoised), a2 (per-batch columnar), a3 (leaf-indexed: Aho-Corasick + tries) |
| `run_matrix.sh` | runs every leg at N = 50, 200, 782 |

## Running

```bash
export WORK=/path/to/scratch
# 1. rules (Security Analytics bundled Sigma rules, windows/ category)
git clone --filter=blob:none --no-checkout --depth 1 https://github.com/opensearch-project/security-analytics.git $WORK/sa
git -C $WORK/sa sparse-checkout set src/main/config/rules && git -C $WORK/sa checkout
python3 compile_sigma.py $WORK/sa/src/main/config/rules/windows $WORK/rules          # all leaves
NO_RE=1 python3 compile_sigma.py $WORK/sa/src/main/config/rules/windows $WORK/rules_nore
# 2. binaries
(cd rust && CARGO_TARGET_DIR=$WORK/target cargo build --release)
javac java/*.java
# 3. dataset: 10 files x 1M rows, 3% injected positives
$WORK/target/release/gen $WORK/rules/rules.json $WORK/data 10 1000000 0.03 tsv
# 4. matrix
./run_matrix.sh
```

Single runs:

```bash
AC_KIND=dfa $WORK/target/release/rsmatch $WORK/data2 $WORK/rules_nore/rules.json 782 per_rule.txt
$WORK/target/release/dfscan $WORK/data $WORK/rules_nore/rules.json $WORK/rules_nore/rules.sql 782 project 16 2
java -Xmx12g -cp java Harness $WORK/data2 $WORK/rules_nore/rules.json 782 a3 250000
```

Every leg prints `matched_rows` and `rule_hits`; `rsmatch` and `Harness` also write
per-rule counts when given an output path, so legs can be diffed rule by rule.

## Results (aarch64, 16 cores, single thread unless stated, 782 rules, 2M rows)

| Leg | us per row | ns per row per rule |
| --- | --- | --- |
| DataFusion 54, 1 partition | 225 | 288 |
| DataFusion 54, 16 partitions (wall clock) | 22.7 | 29 |
| Java a1cse | 125 | 160 |
| Java a3 | 85 | 109 |
| Rust a4 | 9.7 | 12.4 |
| Rust a4 + 28 regex leaves | 13.2 | 16.9 |
| Rust a4 with Vectorscan 5.4.8 (NEON) | 11.1 | 14.2 |

Per-rule cost of a4 falls with the rule count (37 ns at 50 rules, 21 at 200, 12 at
782) because each field is scanned once for all its patterns. All legs agree on
per-rule counts; DataFusion diverges on 2 rules whose `NOT field = x` hits SQL
three-valued logic on an absent field (fix: `NOT COALESCE(expr, FALSE)`).

## Things learned the hard way

- `aho-corasick` refuses anchored + overlapping iteration; walk the DFA by hand and
  accept a match only when `pattern_len == pos + 1`.
- Reverse suffix patterns by bytes, not chars (one Sigma value contains a non-ASCII character).
- A backtracking regex engine (java.util.regex) hung for >17 minutes on 28 Sigma regexes;
  user-supplied rules need a linear-time engine.
- Globs as regex were most of the Java a3 cost; segment `memmem` with an anchored first
  or last segment is cheap.

## Vectorscan build (optional)

Vectorscan 5.4.8 needs g++ >= 9 upstream; with g++ 7 it builds after lowering
`GNUCXX_MINVER`, replacing `-std=c17` with `-std=c11`, forcing `GNUCC_ARCH=armv8-a`
on aarch64, and building with `-fPIC` so Rust can link it into a PIE binary.
Then `VS_LIB_DIR=<build>/lib cargo build --release --bin rsmatch`.
