/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Optimized cursor with zero-allocation row comparison.
//!
//! Uses `arrow::row::RowConverter` to pre-compute a comparable byte
//! representation of sort columns per batch. Row comparison is a simple
//! memcmp — no per-row allocation, no type dispatch at comparison time.
//!
//! Prefetches on Tokio's blocking pool to keep Rayon free for encoding.

use std::fs::File;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::row::{Row, RowConverter, Rows};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::schema::types::SchemaDescriptor;

use super::error::{MergeError, MergeResult};
use super::io_task::get_io_runtime;
use super::row_compare::{build_row_converter, convert_batch_rows};
use super::schema::projection_indices_excluding_row_id;

pub struct FileCursorOptimized {
    reader: Arc<Mutex<parquet::arrow::arrow_reader::ParquetRecordBatchReader>>,
    prefetch_rx: std::sync::mpsc::Receiver<Option<MergeResult<RecordBatch>>>,
    prefetch_tx: std::sync::mpsc::SyncSender<Option<MergeResult<RecordBatch>>>,
    prefetch_pending: bool,
    pub current_batch: Option<RecordBatch>,
    /// Pre-computed comparable rows for the current batch's sort columns.
    current_rows: Option<Rows>,
    row_converter: RowConverter,
    pub row_idx: usize,
    pub file_id: usize,
    pub sort_col_indices: Vec<usize>,
}

impl FileCursorOptimized {
    pub fn new(
        path: &str, file_id: usize, sort_columns: &[String],
        nulls_first: &[bool], reverse_sorts: &[bool], batch_size: usize,
    ) -> MergeResult<(Self, Arc<ArrowSchema>, SchemaDescriptor)> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        let parquet_schema_descr = builder.parquet_schema().clone();
        let proj_indices = projection_indices_excluding_row_id(&schema);
        let projection = parquet::arrow::ProjectionMask::roots(&parquet_schema_descr, proj_indices);
        let mut reader = builder.with_batch_size(batch_size).with_projection(projection).build()?;

        let first_batch = match reader.next() {
            Some(Ok(b)) if b.num_rows() > 0 => b,
            Some(Err(e)) => return Err(e.into()),
            _ => return Err(MergeError::Logic(format!(
                "File '{}' (cursor {}) yielded no rows", path, file_id))),
        };

        let projected_schema = first_batch.schema();
        let mut sort_col_indices = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let idx = projected_schema.fields().iter()
                .position(|f| f.name() == col_name.as_str())
                .ok_or_else(|| MergeError::Logic(format!(
                    "Sort column '{}' not found after projection in file '{}'", col_name, path
                )))?;
            sort_col_indices.push(idx);
        }

        // Build RowConverter once — encodes sort direction + null ordering into byte layout
        let row_converter = build_row_converter(&first_batch, &sort_col_indices, reverse_sorts, nulls_first)?;
        let current_rows = convert_batch_rows(&row_converter, &first_batch, &sort_col_indices)?;

        let (prefetch_tx, prefetch_rx) = std::sync::mpsc::sync_channel(1);
        let reader = Arc::new(Mutex::new(reader));
        let mut cursor = Self {
            reader, prefetch_rx, prefetch_tx, prefetch_pending: false,
            current_batch: Some(first_batch),
            current_rows: Some(current_rows),
            row_converter,
            row_idx: 0, file_id, sort_col_indices,
        };
        cursor.start_prefetch();
        Ok((cursor, projected_schema, parquet_schema_descr))
    }

    fn start_prefetch(&mut self) {
        if self.prefetch_pending { return; }
        self.prefetch_pending = true;
        let reader = Arc::clone(&self.reader);
        let tx = self.prefetch_tx.clone();
        get_io_runtime().spawn_blocking(move || {
            let mut reader = reader.lock().unwrap();
            let result = match reader.next() {
                Some(Ok(batch)) if batch.num_rows() > 0 => Some(Ok(batch)),
                Some(Err(e)) => Some(Err(MergeError::Arrow(e))),
                _ => None,
            };
            let _ = tx.send(result);
        });
    }

    fn recompute_rows(&mut self) -> MergeResult<()> {
        if let Some(batch) = &self.current_batch {
            self.current_rows = Some(convert_batch_rows(&self.row_converter, batch, &self.sort_col_indices)?);
        } else {
            self.current_rows = None;
        }
        Ok(())
    }

    pub fn load_next_batch(&mut self) -> MergeResult<bool> {
        self.current_batch = None;
        self.current_rows = None;
        match self.prefetch_rx.recv() {
            Ok(Some(Ok(batch))) => {
                self.current_batch = Some(batch);
                self.row_idx = 0;
                self.prefetch_pending = false;
                self.recompute_rows()?;
                self.start_prefetch();
                Ok(true)
            }
            Ok(Some(Err(e))) => { self.prefetch_pending = false; Err(e) }
            Ok(None) | Err(_) => { self.prefetch_pending = false; Ok(false) }
        }
    }

    /// Zero-alloc: returns a reference to the pre-computed comparable row.
    #[inline]
    pub fn current_row(&self) -> Row<'_> {
        self.current_rows.as_ref().unwrap().row(self.row_idx)
    }

    /// Zero-alloc: returns the last row in the current batch.
    #[inline]
    pub fn last_row(&self) -> Row<'_> {
        let rows = self.current_rows.as_ref().unwrap();
        rows.row(rows.num_rows() - 1)
    }

    /// Get a row at a specific index (for binary search).
    #[inline]
    pub fn row_at(&self, idx: usize) -> Row<'_> {
        self.current_rows.as_ref().unwrap().row(idx)
    }

    #[inline]
    pub fn batch_height(&self) -> usize { self.current_batch.as_ref().map_or(0, |b| b.num_rows()) }

    #[inline]
    pub fn take_slice(&self, start: usize, len: usize) -> RecordBatch {
        self.current_batch.as_ref().unwrap().slice(start, len)
    }

    pub fn advance(&mut self) -> MergeResult<bool> {
        if self.current_batch.is_none() { return Ok(false); }
        self.row_idx += 1;
        if self.row_idx >= self.current_batch.as_ref().unwrap().num_rows() {
            self.current_batch = None;
            self.current_rows = None;
            return self.load_next_batch();
        }
        Ok(true)
    }

    pub fn advance_past_batch(&mut self) -> MergeResult<bool> {
        self.current_batch = None;
        self.current_rows = None;
        self.load_next_batch()
    }
}
