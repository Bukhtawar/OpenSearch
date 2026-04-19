/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Optimized merge with zero-allocation row comparison.
//!
//! Uses `arrow::row::RowConverter` for O(1) memcmp-based row comparison,
//! precomputed `ColumnMapping` for schema padding, Tokio prefetch, and
//! single-chunk concat_batches skip.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::row::OwnedRow;
use parquet::schema::types::SchemaDescriptor;

use crate::log_info;

use super::context::MergeContext;
use super::cursor_optimized::FileCursorOptimized;
use super::io_task::{BATCH_SIZE, OUTPUT_FLUSH_ROWS};
use super::schema_optimized::ColumnMapping;

/// Heap entry using `OwnedRow` — zero per-comparison allocation.
/// `OwnedRow` is a self-contained byte buffer that implements `Ord` via memcmp.
/// Sort direction and null ordering are baked into the byte layout by `RowConverter`.
struct HeapEntry {
    row: OwnedRow,
    file_id: usize,
}

impl Eq for HeapEntry {}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool { self.row.row() == other.row.row() }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed for min-heap on BinaryHeap (max-heap)
        other.row.row().cmp(&self.row.row())
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

pub fn merge_sorted_optimized(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    sort_columns: &[String],
    reverse_sorts: &[bool],
    nulls_first: &[bool],
) -> super::MergeResult<()> {
    if input_files.is_empty() { return Ok(()); }
    if sort_columns.is_empty() {
        return Err(super::MergeError::Logic("merge_sorted_optimized: empty sort_columns".into()));
    }

    log_info!(
        "[RUST] Starting optimized merge: {} files, sort={:?}, output='{}'",
        input_files.len(), sort_columns, output_path
    );

    // Phase 1: Open cursors (with RowConverter + Tokio prefetch)
    let mut cursors: Vec<FileCursorOptimized> = Vec::with_capacity(input_files.len());
    let mut arrow_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());
    let mut parquet_descriptors: Vec<SchemaDescriptor> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        let (cursor, projected_schema, parquet_descr) =
            FileCursorOptimized::new(path, file_id, sort_columns, nulls_first, reverse_sorts, BATCH_SIZE)?;
        cursors.push(cursor);
        arrow_schemas.push(projected_schema.as_ref().clone());
        parquet_descriptors.push(parquet_descr);
    }

    // Phase 2: MergeContext + precomputed column mappings
    let mut ctx = MergeContext::new(
        arrow_schemas.clone(), &parquet_descriptors, output_path, index_name, OUTPUT_FLUSH_ROWS,
    )?;
    let col_mappings: Vec<ColumnMapping> = arrow_schemas.iter()
        .map(|s| ColumnMapping::new(s, ctx.data_schema()))
        .collect();

    // Phase 3: Seed heap with OwnedRow (zero-alloc comparison)
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(cursors.len());
    for cursor in &cursors {
        heap.push(HeapEntry {
            row: cursor.current_row().owned(),
            file_id: cursor.file_id,
        });
    }

    // Phase 4: K-way merge loop — all comparisons are memcmp via Row::cmp
    while let Some(item) = heap.pop() {
        let file_id = item.file_id;
        let mapping = &col_mappings[file_id];

        // TIER 1: Single cursor remaining — drain
        if heap.is_empty() {
            let cursor = &mut cursors[file_id];
            loop {
                let remaining = cursor.batch_height() - cursor.row_idx;
                if remaining > 0 {
                    ctx.push_batch(mapping.pad_batch(&cursor.take_slice(cursor.row_idx, remaining))?)?;
                }
                if !cursor.advance_past_batch()? { break; }
            }
            break;
        }

        let cursor = &mut cursors[file_id];
        loop {
            let heap_top = &heap.peek().unwrap().row;

            // TIER 2: Entire remaining batch fits before heap top
            let last = cursor.last_row();
            if last.cmp(&heap_top.row()) != Ordering::Greater {
                let remaining = cursor.batch_height() - cursor.row_idx;
                ctx.push_batch(mapping.pad_batch(&cursor.take_slice(cursor.row_idx, remaining))?)?;
                if !cursor.advance_past_batch()? { break; }
                continue;
            }

            // TIER 3: Binary search for exact boundary
            let run_start = cursor.row_idx;
            let batch_h = cursor.batch_height();
            let mut lo = run_start;
            let mut hi = batch_h - 1;
            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                if cursor.row_at(mid).cmp(&heap_top.row()) != Ordering::Greater {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let run_end = lo;
            let run_len = run_end - run_start + 1;
            if run_len > 0 {
                ctx.push_batch(mapping.pad_batch(&cursor.take_slice(run_start, run_len))?)?;
            }

            cursor.row_idx = run_end;
            if !cursor.advance()? { break; }

            let next = cursor.current_row();
            if next.cmp(&heap_top.row()) == Ordering::Greater {
                heap.push(HeapEntry {
                    row: next.owned(),
                    file_id,
                });
                break;
            }
        }
    }

    let _metadata = ctx.finish()?;
    log_info!("[RUST] Optimized merge complete: {} rows in {} row groups",
        _metadata.file_metadata().num_rows(), _metadata.num_row_groups());
    Ok(())
}
