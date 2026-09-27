/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use crate::log_debug;

use super::context::MergeContext;
use super::cursor::FileCursor;
use super::heap::{cmp_sort_values, get_sort_values, HeapItem};
use super::io_task::get_merge_pool;
use super::schema::ColumnMapping;

use crate::memory::merge_pool;
use native_bridge_common::memory_pool::{MemoryReservation, PoolBehavior};

/// Accumulates the slices the k-way merge emits and hands them to
/// [`MergeContext::push_batch`] in batches of at least `target_rows` rows.
///
/// A sorted merge over inputs whose sort keys interleave (adjacent segments of one
/// time-sorted log stream) emits one Tier-3 run per heap pop, often a handful of rows.
/// Every `push_batch` pays a per-call cost that does not shrink with row count
/// (`append_row_id`, `compute_leaves`, a level-buffer allocation and a `ColumnPath` hash
/// lookup per column, the writer `memory_size` sum), so millions of tiny pushes per merge
/// dominate the encode work. Concatenating pending runs into one batch first makes an
/// interleaved merge cost the same number of pushes as a disjoint one, and every push is
/// then large enough to take the parallel write path in `push_batch`.
///
/// Slices are zero-copy views into a cursor's current batch, so holding them pins that
/// batch after the cursor has released its memory tracking. `flush` is therefore called
/// before any cursor drops a batch (`advance_past_batch`, and `advance` at the last row),
/// which bounds pinned memory to the batches the cursors already hold. Those flushes are
/// rare (once per source batch) so they do not undo the coalescing.
///
/// Output ordering and row ids are unchanged: rows reach the writer in emission order,
/// and `push_batch` assigns row ids sequentially over whatever it receives.
struct RunCoalescer {
    target_rows: usize,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    /// `MergeContext::data_schema()`: the union schema every padded slice already has.
    schema: Arc<ArrowSchema>,
    pushes: usize,
}

impl RunCoalescer {
    fn new(target_rows: usize, schema: Arc<ArrowSchema>) -> Self {
        Self {
            target_rows: target_rows.max(1),
            pending: Vec::new(),
            pending_rows: 0,
            schema,
            pushes: 0,
        }
    }

    /// Queues a padded slice, pushing to the writer once `target_rows` are pending.
    #[inline]
    fn push(&mut self, slice: RecordBatch, ctx: &mut MergeContext) -> super::MergeResult<()> {
        if slice.num_rows() == 0 {
            return Ok(());
        }
        self.pending_rows += slice.num_rows();
        self.pending.push(slice);
        if self.pending_rows >= self.target_rows {
            self.flush(ctx)?;
        }
        Ok(())
    }

    /// Pushes whatever is pending. A single pending slice goes through untouched (no copy);
    /// several are concatenated into one owned batch first.
    fn flush(&mut self, ctx: &mut MergeContext) -> super::MergeResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batch = if self.pending.len() == 1 {
            self.pending.pop().unwrap()
        } else {
            let merged = concat_batches(&self.schema, &self.pending)?;
            self.pending.clear();
            merged
        };
        self.pending_rows = 0;
        self.pushes += 1;
        ctx.push_batch(batch)
    }
}

/// Checked write of a surviving row's new id into the flat mapping. A row id at or beyond
/// the file's footer-declared span would silently corrupt the next file's slots, so it is
/// rejected with a descriptive error (diagnosable across the FFM boundary, unlike a panic).
#[inline]
fn record_survivor(
    mapping: &mut [i64],
    file_offset: usize,
    file_rows: usize,
    file_id: usize,
    abs: u64,
    new_row_id: i64,
) -> super::MergeResult<()> {
    let rel = abs as usize;
    if rel >= file_rows || file_offset + rel >= mapping.len() {
        return Err(super::MergeError::Logic(format!(
            "row-id mapping overflow: file {} produced absolute row id {} but its footer declares only {} rows (mapping len {})",
            file_id,
            abs,
            file_rows,
            mapping.len()
        )));
    }
    mapping[file_offset + rel] = new_row_id;
    Ok(())
}

/// Performs a streaming k-way merge with an explicit sort direction per column.
/// `live_docs_per_input` is parallel to `input_files`; a `None` (or absent) entry
/// disables filtering for that file (zero-copy fast path).
pub fn merge_sorted(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    sort_columns: &[String],
    reverse_sorts: &[bool],
    nulls_first: &[bool],
    max_sort_modes: &[bool],
    output_writer_generation: i64,
    live_docs_per_input: &[Option<Vec<u64>>],
) -> super::MergeResult<super::MergeOutput> {
    let mut reservation =
        MemoryReservation::new(merge_pool(), "merge_sorted", PoolBehavior::Reject);
    merge_sorted_with_pool(
        input_files,
        output_path,
        index_name,
        sort_columns,
        reverse_sorts,
        nulls_first,
        max_sort_modes,
        output_writer_generation,
        &mut reservation,
        live_docs_per_input,
    )
}

/// Performs a streaming k-way merge using the provided memory reservation.
/// `live_docs_per_input` is parallel to `input_files`; a `None` (or absent) entry
/// disables filtering for that file (zero-copy fast path).
pub fn merge_sorted_with_pool(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    sort_columns: &[String],
    reverse_sorts: &[bool],
    nulls_first: &[bool],
    max_sort_modes: &[bool],
    output_writer_generation: i64,
    reservation: &mut MemoryReservation,
    live_docs_per_input: &[Option<Vec<u64>>],
) -> super::MergeResult<super::MergeOutput> {
    let config = crate::writer::SETTINGS_STORE
        .get(index_name)
        .map(|r| r.clone())
        .unwrap_or_default();
    let batch_size = config.get_merge_batch_size();
    let output_flush_rows = config.get_row_group_max_rows();
    let rayon_threads = config.get_merge_rayon_threads();
    let io_threads = config.get_merge_io_threads();
    let deferred_threshold = config.get_merge_deferred_column_threshold();
    let coalesce_rows = config.get_merge_coalesce_rows();
    if input_files.is_empty() {
        return Err(super::MergeError::Logic(
            "merge_sorted called with empty input_files".into(),
        ));
    }

    if sort_columns.is_empty() {
        return Err(super::MergeError::Logic(
            "merge_sorted called with empty sort_columns; use merge_unsorted instead".into(),
        ));
    }

    let pool = get_merge_pool(rayon_threads);
    let direction_label = if reverse_sorts.iter().all(|&r| !r) {
        "ascending"
    } else if reverse_sorts.iter().all(|&r| r) {
        "descending"
    } else {
        "mixed"
    };

    log_debug!(
        "[RUST] Starting streaming merge ({}): {} input files, sort_columns={:?}, \
         batch_size={}, flush_rows={}, coalesce_rows={}, merge_threads={}, output='{}'",
        direction_label,
        input_files.len(),
        sort_columns,
        batch_size,
        output_flush_rows,
        coalesce_rows,
        pool.current_num_threads(),
        output_path
    );

    // ── Phase 1: Initialize cursors and collect schemas ─────────────────
    let mut cursors: Vec<FileCursor> = Vec::with_capacity(input_files.len());
    let mut arrow_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());
    let mut file_generations: Vec<i64> = Vec::with_capacity(input_files.len());
    let mut file_row_counts: Vec<usize> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        log_debug!("[RUST] Opening cursor {} for file: {}", file_id, path);
        let live_bits = live_docs_per_input
            .get(file_id)
            .and_then(|opt| opt.as_ref())
            .map(|bits| Arc::new(bits.clone()));
        let (cursor, projected_schema, _parquet_descr, generation, row_count) = FileCursor::new(
            path,
            file_id,
            sort_columns,
            nulls_first,
            max_sort_modes,
            batch_size,
            deferred_threshold,
            reservation,
            live_bits,
        )?;
        cursors.push(cursor);
        arrow_schemas.push(projected_schema.as_ref().clone());
        file_generations.push(generation);
        file_row_counts.push(row_count);
    }

    let num_cursors = cursors.len();

    // ── Phase 2: Create MergeContext (union schemas, writer, IO task) ───
    let ctx_reservation = reservation.child("merge:flush");
    let mut ctx = MergeContext::new(
        arrow_schemas.clone(),
        output_path,
        index_name,
        output_flush_rows,
        rayon_threads,
        io_threads,
        output_writer_generation,
        ctx_reservation,
    )?;

    // Precompute column mappings per cursor (avoids per-batch name lookups)
    let col_mappings: Vec<ColumnMapping> = arrow_schemas
        .iter()
        .map(|s| ColumnMapping::new(s, ctx.data_schema()))
        .collect();

    let mut runs = RunCoalescer::new(coalesce_rows, Arc::clone(ctx.data_schema()));

    // Row-ID mapping: pre-allocate the flat mapping array and compute offsets
    // from file metadata row counts (known before reading any data). The mapping
    // is indexed by absolute source row id; dead (deleted) rows keep the -1
    // sentinel since they are never emitted.
    let total_rows: usize = file_row_counts.iter().sum();
    let mapping_bytes = total_rows * std::mem::size_of::<i64>();
    // Reserve for row-ID mapping Vec<i64> — total_rows × 8 bytes, allocated next line
    reservation
        .request(mapping_bytes)
        .map_err(|e| super::MergeError::Logic(format!("Merge pool exceeded (mapping): {}", e)))?;
    let mut mapping: Vec<i64> = vec![-1i64; total_rows];
    let mut gen_keys: Vec<i64> = Vec::with_capacity(num_cursors);
    let mut gen_offsets: Vec<i32> = Vec::with_capacity(num_cursors);
    let mut gen_sizes: Vec<i32> = Vec::with_capacity(num_cursors);

    let mut offset = 0i32;
    for file_id in 0..num_cursors {
        gen_keys.push(file_generations[file_id]);
        gen_offsets.push(offset);
        let size = file_row_counts[file_id] as i32;
        gen_sizes.push(size);
        offset += size;
    }

    let mut new_row_id: i64 = 0;

    log_debug!(
        "[RUST] Merge initialized ({}): {} cursors",
        direction_label,
        num_cursors
    );

    // ── Phase 3: Seed the heap ──────────────────────────────────────────
    let reverse_sorts_arc = Arc::new(reverse_sorts.to_vec());
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(num_cursors);
    for cursor in &cursors {
        // Cursors with all-dead data are already exhausted — skip.
        if cursor.sort_batch.is_none() {
            continue;
        }
        let sv = cursor.current_sort_values()?;
        heap.push(HeapItem {
            sort_values: sv,
            file_id: cursor.file_id,
            reverse_sorts: Arc::clone(&reverse_sorts_arc),
        });
    }

    // ── Phase 4: K-way merge loop — three-tier cascade ──────────────────
    while let Some(item) = heap.pop() {
        let file_id = item.file_id;

        // TIER 1: Single cursor remaining — drain it
        if heap.is_empty() {
            let cursor = &mut cursors[file_id];
            let col_mapping = &col_mappings[file_id];
            let file_offset = gen_offsets[file_id] as usize;
            let file_rows = file_row_counts[file_id];
            loop {
                let start_idx = cursor.row_idx;
                let base_row_id = cursor.base_row_id;
                let remaining = cursor.batch_height() - start_idx;
                if remaining > 0 {
                    let slice = cursor.take_slice(start_idx, remaining, reservation)?;
                    // Build mapping for surviving rows (indexed by absolute source row id).
                    for i in 0..remaining {
                        let abs = base_row_id + (start_idx + i) as u64;
                        if cursor.is_row_id_alive(abs) {
                            record_survivor(
                                &mut mapping,
                                file_offset,
                                file_rows,
                                file_id,
                                abs,
                                new_row_id,
                            )?;
                            new_row_id += 1;
                        }
                    }
                    if slice.num_rows() > 0 {
                        runs.push(col_mapping.pad_batch(&slice)?, &mut ctx)?;
                    }
                }
                // The next call drops this cursor's batch; pending slices view into it.
                runs.flush(&mut ctx)?;
                if !cursor.advance_past_batch(reservation)? {
                    break;
                }
            }
            break;
        }

        // TIER 2 & 3: Multiple cursors active
        let cursor = &mut cursors[file_id];
        let col_mapping = &col_mappings[file_id];
        let file_offset = gen_offsets[file_id] as usize;
        let file_rows = file_row_counts[file_id];

        loop {
            let heap_top = &heap.peek().unwrap().sort_values;

            // TIER 2: Entire remaining batch fits before heap top
            let last_val = cursor.last_sort_values()?;
            if cmp_sort_values(&last_val, heap_top, reverse_sorts) != Ordering::Greater {
                let start_idx = cursor.row_idx;
                let base_row_id = cursor.base_row_id;
                let remaining = cursor.batch_height() - start_idx;
                let slice = cursor.take_slice(start_idx, remaining, reservation)?;
                for i in 0..remaining {
                    let abs = base_row_id + (start_idx + i) as u64;
                    if cursor.is_row_id_alive(abs) {
                        record_survivor(
                            &mut mapping,
                            file_offset,
                            file_rows,
                            file_id,
                            abs,
                            new_row_id,
                        )?;
                        new_row_id += 1;
                    }
                }
                if slice.num_rows() > 0 {
                    runs.push(col_mapping.pad_batch(&slice)?, &mut ctx)?;
                }

                // The next call drops this cursor's batch; pending slices view into it.
                runs.flush(&mut ctx)?;
                if !cursor.advance_past_batch(reservation)? {
                    break;
                }
                // Check if cursor should yield after loading new batch
                let val = cursor.current_sort_values()?;
                if cmp_sort_values(&val, heap_top, reverse_sorts) == Ordering::Greater {
                    heap.push(HeapItem {
                        sort_values: val,
                        file_id,
                        reverse_sorts: Arc::clone(&reverse_sorts_arc),
                    });
                    break;
                }
                continue;
            }

            // TIER 3: Binary search for the exact boundary
            let run_start = cursor.row_idx;
            let base_row_id = cursor.base_row_id;
            let batch_h = cursor.batch_height();
            let batch = cursor.sort_batch.as_ref().unwrap();

            let mut lo = run_start;
            let mut hi = batch_h - 1;

            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                let mid_val = get_sort_values(
                    batch,
                    mid,
                    &cursor.sort_col_indices,
                    &cursor.sort_col_types,
                    &cursor.nulls_first,
                    &cursor.max_sort_modes,
                )?;

                if cmp_sort_values(&mid_val, heap_top, reverse_sorts) != Ordering::Greater {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let run_end = lo;

            let run_len = run_end - run_start + 1;
            if run_len > 0 {
                let slice = cursor.take_slice(run_start, run_len, reservation)?;
                for i in 0..run_len {
                    let abs = base_row_id + (run_start + i) as u64;
                    if cursor.is_row_id_alive(abs) {
                        record_survivor(
                            &mut mapping,
                            file_offset,
                            file_rows,
                            file_id,
                            abs,
                            new_row_id,
                        )?;
                        new_row_id += 1;
                    }
                }
                if slice.num_rows() > 0 {
                    runs.push(col_mapping.pad_batch(&slice)?, &mut ctx)?;
                }
            }

            cursor.row_idx = run_end;
            // `advance` drops the current batch when the run ended on its last row;
            // pending slices view into it, so hand them to the writer first. Runs that
            // end mid-batch (the common case) keep accumulating.
            if run_end + 1 >= batch_h {
                runs.flush(&mut ctx)?;
            }
            if !cursor.advance(reservation)? {
                break;
            }

            // Binary search invariant guarantees cursor.current_value > heap_top
            // so we always yield here (no need for conditional check)
            let val = cursor.current_sort_values()?;
            heap.push(HeapItem {
                sort_values: val,
                file_id,
                reverse_sorts: Arc::clone(&reverse_sorts_arc),
            });
            break;
        }
    }

    // ── Phase 5: Close ──────────────────────────────────────────────────
    runs.flush(&mut ctx)?;
    let stats = ctx.finish()?;

    log_debug!(
        "[RUST] Merge complete ({}): {} total rows written to '{}' in {} row groups \
         via {} push_batch calls, crc32={:#010x}",
        direction_label,
        stats.metadata.file_metadata().num_rows(),
        output_path,
        stats.metadata.num_row_groups(),
        runs.pushes,
        stats.crc32
    );

    // Detach mapping from reservation — FFI layer will track via merge_pool().grow
    reservation.shrink(mapping_bytes);

    Ok(super::MergeOutput {
        mapping,
        gen_keys,
        gen_offsets,
        gen_sizes,
        metadata: stats.metadata,
        crc32: stats.crc32,
        flush_and_sort_chunk_count: stats.flush_and_sort_chunk_count,
        flush_and_sort_chunk_time_millis: stats.flush_and_sort_chunk_time_millis,
        row_id_mapping_max: stats.row_id_mapping_max,
    })
}
