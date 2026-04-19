/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Zero-allocation row comparison using `arrow::row::RowConverter`.
//!
//! Replaces the per-row `Vec<SortKey>` allocation in `heap.rs` with a
//! pre-computed byte representation that supports O(1) comparison via memcmp.
//!
//! The `RowConverter` encodes sort direction, null ordering, and type-specific
//! comparison logic into the byte layout at conversion time. At comparison time,
//! it's just a byte slice comparison — no allocation, no type dispatch.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::SortOptions;
use arrow::row::{RowConverter, Rows, SortField};

use super::error::{MergeError, MergeResult};

/// Builds a `RowConverter` from sort column specs. Created once per merge.
pub fn build_row_converter(
    batch: &RecordBatch,
    sort_col_indices: &[usize],
    reverse_sorts: &[bool],
    nulls_first: &[bool],
) -> MergeResult<RowConverter> {
    let fields: Vec<SortField> = sort_col_indices
        .iter()
        .enumerate()
        .map(|(i, &col_idx)| {
            let dt = batch.column(col_idx).data_type().clone();
            let opts = SortOptions {
                descending: reverse_sorts.get(i).copied().unwrap_or(false),
                nulls_first: nulls_first.get(i).copied().unwrap_or(false),
            };
            SortField::new_with_options(dt, opts)
        })
        .collect();

    RowConverter::new(fields).map_err(|e| MergeError::Arrow(e))
}

/// Convert sort columns of a batch into comparable `Rows`.
/// Each `Row` in the result can be compared with `Ord` (zero-alloc memcmp).
pub fn convert_batch_rows(
    converter: &RowConverter,
    batch: &RecordBatch,
    sort_col_indices: &[usize],
) -> MergeResult<Rows> {
    let cols: Vec<ArrayRef> = sort_col_indices
        .iter()
        .map(|&idx| batch.column(idx).clone())
        .collect();

    converter.convert_columns(&cols).map_err(|e| MergeError::Arrow(e))
}
