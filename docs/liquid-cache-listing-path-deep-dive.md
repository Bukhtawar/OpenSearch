# Liquid Cache on the Listing Table Path — Deep Dive

## How Queries Reach the Listing Table Path

The analytics engine routes queries based on whether the Substrait plan contains an `index_filter` UDF:
- **`index_filter` present** → Indexed path (BoolNode evaluator, bloom filters, collectors)
- **No `index_filter`** → Listing table path (DataFusion full pipeline)

Most ClickBench queries go through the listing table path because the OpenSearch SQL plugin generates plans that DataFusion's optimizer handles directly.

## The LC Engagement Decision (Physical Optimizer)

When `LocalModeLiquidCacheOptimizer` is registered on the session, it runs as a physical optimizer rule AFTER DataFusion converts the logical plan to a physical plan.

### What the optimizer sees:

```
AggregateExec (sum, count, avg, dc, etc.)
  └─ CoalesceBatchesExec (optional)
      └─ FilterExec (optional — if predicate wasn't pushed into ParquetSource)
          └─ DataSourceExec
               └─ FileScanConfig
                    └─ ParquetSource (with optional predicate, projection)
```

The optimizer looks for `DataSourceExec` nodes containing `ParquetSource`. It checks:

1. **`plan.schema()` (output schema)** — what columns does this DataSourceExec produce?
2. **`parquet_source.filter()`** — does it have a pushed-down predicate?

### Gate conditions (skip if ANY is true):

```rust
// 1. Empty projection (COUNT(*) — just row count from metadata)
if output_schema.fields().is_empty() → SKIP

// 2. Any output column is string/binary
if output_schema.fields().iter().any(|f| is_uncacheable_type(f)) → SKIP

// 3. Predicate references a string column
if predicate references string column → SKIP
```

If all gates pass → **WRAP**: replace `ParquetSource` with `LiquidParquetSource`

---

## Case 1: Pure Numeric Projection, No Predicate

**Examples:** Q3 (`stats sum(AdvEngineID), count(), avg(ResolutionWidth)`), Q4 (`stats avg(UserID)`)

### Physical Plan:
```
AggregateExec [sum(col0), count(), avg(col1)]
  └─ DataSourceExec [ParquetSource]
       output: [AdvEngineID: Int16, ResolutionWidth: Int16]
       predicate: None
       files: [_merged_310.parquet, _merged_1eb.parquet, ...]
```

### Optimizer Decision:
- output_schema: `[AdvEngineID: Int16, ResolutionWidth: Int16]` — all numeric ✓
- predicate: None — no string check needed ✓
- **WRAP** → `LiquidParquetSource`

### Execution Flow:

**Iteration 1 (cold cache):**
```
LiquidParquetOpener.open(file):
  - predicate = None → no row_filter built
  - No RG pruning (no predicate to prune with)
  - row_selection = all rows
  - Selectivity gate: no row_filter to apply → N/A
  - Builds LiquidStream with all row groups

LiquidStream.plan_row_group(rg=0):
  - filter = None → predicate_projection = None
  - predicate_column_ids = cache_column_ids (our fix)
    = [col_20 (AdvEngineID), col_86 (ResolutionWidth)]
  - All projected columns marked is_predicate_column = true

LiquidCacheReader per batch:
  - get_arrow_array_with_filter(batch_id=0, col=20):
    - is_predicate_column = true ✓
    - is_string_type = false ✓
    - cache lookup → NOT FOUND (cold)
    - return None
  - get_arrow_array_with_filter(batch_id=0, col=86):
    - same → NOT FOUND → None
  - ALL misses → read_parquet_batch_and_fill_cache:
    - fetch_batch from parquet (decode both columns)
    - tokio::spawn(insert col_20, insert col_86 into cache) ← async
    - return RecordBatch
  - Extract columns, filter, return
```

**Iteration 2+ (warm cache):**
```
LiquidCacheReader per batch:
  - get_arrow_array_with_filter(batch_id=0, col=20):
    - cache lookup → FOUND (MemoryArrow)
    - arrow::compute::filter(cached_array, selection) ← zero-copy filter
    - return Some(filtered_array)
  - get_arrow_array_with_filter(batch_id=0, col=86):
    - cache lookup → FOUND → return filtered array
  - ALL hits → no parquet I/O needed
  - Assemble RecordBatch from cached arrays
```

**Performance:**
- Iter 1: ~2000ms (decode all parquet + cache fill)
- Iter 2+: ~85ms (serve from MemoryArrow, no parquet decode)
- LC OFF: ~125ms (decode from OS page cache every time)
- **Speedup: 1.5x on warm**

---

## Case 2: Numeric Predicate + Numeric Projection

**Examples:** Q2 (`WHERE AdvEngineID != 0 | stats count()`), Q8 (`WHERE AdvEngineID != 0 | stats count() by AdvEngineID`)

### Physical Plan:
```
AggregateExec [count()]
  └─ DataSourceExec [ParquetSource]
       output: [AdvEngineID: Int16]
       predicate: Some(AdvEngineID != 0)  ← pushed down by DataFusion
```

### Optimizer Decision:
- output_schema: `[AdvEngineID: Int16]` — numeric ✓
- predicate: `AdvEngineID != 0` → collect_columns → [AdvEngineID: Int16] — not string ✓
- **WRAP**

### Execution Flow:

```
LiquidParquetOpener.open(file):
  - predicate = Some(AdvEngineID != 0)
  - Builds pruning_predicate from predicate
  - Builds row_filter from predicate (for per-row evaluation during decode)
  
  RG Pruning (statistics-based):
    - For each row group, check min/max of AdvEngineID
    - If min=0, max=0 → prune this RG (all zeros, none match != 0)
    - Most RGs have min=0, max=large → survive (could have non-zero)
    - Result: most RGs survive → estimated_selectivity ≈ 0.95
  
  Selectivity gate:
    - estimated_selectivity = 0.95 (> 0.5)
    - row_filter SKIPPED (high selectivity = most rows match = pushdown overhead > savings)
    - LC acts as pure cache (same as Case 1)

LiquidStream + LiquidCacheReader:
  - Same as Case 1 — pure cache mode
  - Iter 1: decode + cache
  - Iter 2+: serve from MemoryArrow
```

**If selectivity WERE low (e.g., Q20 WHERE UserID = specific_value):**
```
  RG Pruning:
    - Data sorted by UserID → most RGs have min/max that exclude this value
    - Only 1-2 RGs survive → estimated_selectivity = 0.02
  
  Selectivity gate:
    - estimated_selectivity = 0.02 (< 0.5)
    - row_filter APPLIED
    
  During decode (with row_filter):
    - Parquet reader decodes AdvEngineID column first
    - Evaluates predicate per-row: AdvEngineID != 0?
    - Only rows where predicate = true have remaining columns decoded
    - Massive savings: skip 98% of row decode
    
  Cache behavior:
    - Predicate column (AdvEngineID) cached after first decode
    - On repeat: predicate evaluation still runs (on cached data = fast)
    - Other columns: only matching rows decoded and cached
```

---

## Case 3: String in Output (SKIPPED)

**Examples:** Q11 (`WHERE MobilePhoneModel != '' | stats dc(UserID) by MobilePhoneModel`)

### Physical Plan:
```
AggregateExec [dc(UserID) by MobilePhoneModel]
  └─ FilterExec [MobilePhoneModel != '']
      └─ DataSourceExec [ParquetSource]
           output: [UserID: Int64, MobilePhoneModel: Utf8View]
           predicate: Some(MobilePhoneModel != '')
```

### Optimizer Decision:
- output_schema: `[UserID: Int64, MobilePhoneModel: Utf8View]`
- has_string_output: Utf8View → **true** ✗
- **SKIP** → plan unchanged, standard ParquetSource executes

### Execution Flow:
```
Standard DataFusion ParquetSource:
  - Decodes all columns in one vectorized pass
  - FilterExec applies predicate post-decode
  - No LC overhead whatsoever
```

**Performance:** Identical to LC OFF. Zero regression.

---

## Case 4: String in Predicate Only (SKIPPED)

**Examples:** Q13 (`WHERE SearchPhrase != '' | stats count() by SearchPhrase`)

### Physical Plan:
```
AggregateExec [count() by SearchPhrase]
  └─ DataSourceExec [ParquetSource]
       output: [SearchPhrase: Utf8View]
       predicate: Some(SearchPhrase != '')
```

### Optimizer Decision:
- output_schema: `[SearchPhrase: Utf8View]` — string ✗
- **SKIP** (caught by has_string_output check)

Even if output were numeric, the predicate check would catch it:
- predicate: `SearchPhrase != ''` → collect_columns → [SearchPhrase: Utf8View] → string ✗
- **SKIP**

---

## Case 5: Empty Projection — COUNT(*) (SKIPPED)

**Examples:** Q1 (`stats count()`)

### Physical Plan:
```
AggregateExec [count()]
  └─ DataSourceExec [ParquetSource]
       output: []  ← empty! Just needs row count
       predicate: None
```

### Optimizer Decision:
- output_schema.fields().is_empty() → **true**
- **SKIP** (COUNT(*) only needs row count from parquet metadata, no decode)

---

## Summary: When LC Helps vs Hurts

| Scenario | Gate Decision | LC Mode | Benefit |
|----------|--------------|---------|---------|
| Pure numeric, no filter (Q3,Q4) | WRAP | Pure cache | **1.5-2.6x on warm** |
| Numeric predicate, high selectivity (Q2,Q8) | WRAP | Pure cache (filter skipped) | **Same or slight gain** |
| Numeric predicate, low selectivity (Q20) | WRAP | Cache + filter pushdown | **Significant if RG pruning effective** |
| String in output (Q11-Q15, Q21-Q43) | SKIP | N/A | **Zero regression** |
| String in predicate (Q13-Q15) | SKIP | N/A | **Zero regression** |
| Empty projection COUNT(*) (Q1) | SKIP | N/A | **Zero regression** |

## The Selectivity Gate Inside the Opener

After the optimizer decides to WRAP, the opener has a second gate:

```
After RG pruning + page index pruning:
  estimated_selectivity = selected_rows / total_rows

If selectivity >= 0.5:
  → Skip row_filter (most rows match, pushdown overhead > savings)
  → LC acts as pure cache

If selectivity < 0.5:
  → Apply row_filter (few rows match, pushdown skips significant decode)
  → LC does filter pushdown + cache
```

This prevents the case where LC applies expensive per-row filter evaluation
on queries where most rows pass anyway.
