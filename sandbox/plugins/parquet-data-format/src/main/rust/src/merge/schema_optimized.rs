/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Optimized schema padding with precomputed column index mappings.
//!
//! The original `pad_batch_to_schema` performs a name-based lookup
//! (`schema.index_of(field.name())`) for every field on every batch.
//! This module precomputes the mapping once per cursor and reuses it,
//! turning O(fields²) per batch into O(fields).

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema as ArrowSchema;

use super::error::MergeResult;

/// Precomputed mapping from target schema field positions to source batch
/// column indices. `None` means the field is missing in the source and
/// should be filled with nulls.
pub struct ColumnMapping {
    /// For each field in the target schema: `Some(source_col_idx)` or `None`.
    mapping: Vec<Option<usize>>,
    target_schema: Arc<ArrowSchema>,
    /// True when source and target schemas are identical (fast path: no remapping needed).
    is_identity: bool,
}

impl ColumnMapping {
    /// Build a mapping from `source_schema` → `target_schema`.
    pub fn new(source_schema: &ArrowSchema, target_schema: &Arc<ArrowSchema>) -> Self {
        let mut mapping = Vec::with_capacity(target_schema.fields().len());
        let mut is_identity = source_schema.fields().len() == target_schema.fields().len();

        for (target_idx, field) in target_schema.fields().iter().enumerate() {
            match source_schema.index_of(field.name()) {
                Ok(src_idx) => {
                    if is_identity && src_idx != target_idx {
                        is_identity = false;
                    }
                    mapping.push(Some(src_idx));
                }
                Err(_) => {
                    is_identity = false;
                    mapping.push(None);
                }
            }
        }

        Self {
            mapping,
            target_schema: target_schema.clone(),
            is_identity,
        }
    }

    /// Remap a batch using the precomputed mapping. Zero-copy when schemas match.
    #[inline]
    pub fn pad_batch(&self, batch: &RecordBatch) -> MergeResult<RecordBatch> {
        if self.is_identity {
            return Ok(batch.clone());
        }

        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.mapping.len());

        for entry in &self.mapping {
            match entry {
                Some(src_idx) => columns.push(batch.column(*src_idx).clone()),
                None => {
                    let field = &self.target_schema.fields()[columns.len()];
                    columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
                }
            }
        }

        Ok(RecordBatch::try_new(self.target_schema.clone(), columns)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_identity_mapping() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let mapping = ColumnMapping::new(&schema, &schema);
        assert!(mapping.is_identity);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![3, 4])),
            ],
        )
        .unwrap();

        let result = mapping.pad_batch(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_missing_column_padded_with_nulls() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
        ]));
        let target_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));

        let mapping = ColumnMapping::new(&source_schema, &target_schema);
        assert!(!mapping.is_identity);

        let batch = RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let result = mapping.pad_batch(&batch).unwrap();
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.column(1).null_count(), 3);
    }
}
