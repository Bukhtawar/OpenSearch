/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use parquet::basic::{ConvertedType, Repetition};
use parquet::schema::types::Type;

use super::error::{MergeError, MergeResult};

/// Reserved column name for the synthetic row identifier added during merge.
pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

/// Whether a Parquet field is a LIST logical group.
fn is_parquet_list(t: &Type) -> bool {
    t.is_group() && t.get_basic_info().converted_type() == ConvertedType::LIST
}

/// Builds the output Parquet schema as the union of pre-read schema descriptors.
///
/// The output schema contains every column seen across all inputs, except:
/// - Any existing `__row_id__` column is removed.
/// - A fresh `__row_id__` INT64 REQUIRED column is appended at the end.
///
/// When the same column name appears both as a scalar and as a LIST across
/// inputs (a legacy scalar file merged with a list-encoded file), the LIST
/// shape wins: scalar batches are promoted to singleton lists at read time by
/// [`ColumnMapping::pad_batch`], never the other way around.
pub fn build_parquet_root_schema(
    schema_descriptors: &[parquet::schema::types::SchemaDescriptor],
) -> MergeResult<Arc<Type>> {
    let mut index_by_name: HashMap<String, usize> = HashMap::new();
    let mut parquet_fields: Vec<Arc<Type>> = Vec::new();

    for descr in schema_descriptors {
        let root = descr.root_schema();
        for field in root.get_fields() {
            if field.name() == ROW_ID_COLUMN_NAME {
                continue;
            }
            match index_by_name.get(field.name()) {
                None => {
                    index_by_name.insert(field.name().to_string(), parquet_fields.len());
                    parquet_fields.push(Arc::new(field.as_ref().clone()));
                }
                Some(&existing_idx) => {
                    // Prefer the LIST shape on a scalar/LIST collision.
                    if !is_parquet_list(&parquet_fields[existing_idx]) && is_parquet_list(field) {
                        parquet_fields[existing_idx] = Arc::new(field.as_ref().clone());
                    }
                }
            }
        }
    }

    let row_id_type = Type::primitive_type_builder(ROW_ID_COLUMN_NAME, parquet::basic::Type::INT64)
        .with_repetition(Repetition::REQUIRED)
        .build()?;
    parquet_fields.push(Arc::new(row_id_type));

    let parquet_root = Type::group_type_builder("schema")
        .with_fields(parquet_fields)
        .build()?;

    Ok(Arc::new(parquet_root))
}

/// Rewrites scalar fields to their LIST form wherever any input schema carries
/// the same column as `LIST<scalar>`, so `ArrowSchema::try_merge` sees one
/// consistent shape per column instead of failing on a scalar/LIST conflict.
pub fn unify_list_shapes(schemas: Vec<ArrowSchema>) -> Vec<ArrowSchema> {
    // First pass: collect the LIST form of every column that has one.
    let mut list_fields: HashMap<String, arrow::datatypes::FieldRef> = HashMap::new();
    for schema in &schemas {
        for field in schema.fields() {
            if matches!(field.data_type(), ArrowDataType::List(_)) {
                list_fields
                    .entry(field.name().clone())
                    .or_insert_with(|| Arc::clone(field));
            }
        }
    }
    if list_fields.is_empty() {
        return schemas;
    }
    // Second pass: replace scalar occurrences whose type matches the list's
    // element type. Anything else is left untouched and will surface the same
    // union error it always did.
    schemas
        .into_iter()
        .map(|schema| {
            let fields: Vec<arrow::datatypes::FieldRef> = schema
                .fields()
                .iter()
                .map(|field| match list_fields.get(field.name()) {
                    Some(list_field) if !matches!(field.data_type(), ArrowDataType::List(_)) => {
                        if let ArrowDataType::List(child) = list_field.data_type() {
                            if child.data_type() == field.data_type() {
                                return Arc::clone(list_field);
                            }
                        }
                        Arc::clone(field)
                    }
                    _ => Arc::clone(field),
                })
                .collect();
            ArrowSchema::new(fields)
        })
        .collect()
}

/// Returns column indices that exclude `__row_id__`, for use as a projection mask.
pub fn projection_indices_excluding_row_id(schema: &ArrowSchema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() != ROW_ID_COLUMN_NAME)
        .map(|(i, _)| i)
        .collect()
}

/// Appends a `__row_id__` column with sequential values `[start_id, start_id + N)`
/// to the given batch, producing a new batch with the output schema.
pub fn append_row_id(
    batch: &RecordBatch,
    start_id: i64,
    output_schema: &Arc<ArrowSchema>,
) -> MergeResult<RecordBatch> {
    let n = batch.num_rows() as i64;
    let row_ids = Int64Array::from_iter_values(start_id..start_id + n);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(row_ids));
    let result = RecordBatch::try_new(output_schema.clone(), columns)?;
    Ok(result)
}

// =============================================================================
// ColumnMapping — precomputed source→target index mapping
// =============================================================================

/// Per-target-column source resolution.
enum ColumnSource {
    /// Take the source column as-is.
    Direct(usize),
    /// Source column is the scalar element form of a target LIST column:
    /// wrap each value as a singleton list (legacy scalar file upgrade-on-read).
    PromoteToList(usize),
    /// Column absent in the source file: pad with nulls.
    NullPad,
}

/// Precomputed mapping from target schema field positions to source batch
/// column indices. Built once per cursor, reused for every batch from that cursor.
///
/// Replaces per-batch `schema.index_of(field.name())` name lookups with O(1)
/// indexed access.
pub struct ColumnMapping {
    mapping: Vec<ColumnSource>,
    target_schema: Arc<ArrowSchema>,
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
                    let src_type = source_schema.field(src_idx).data_type();
                    let needs_promotion = match field.data_type() {
                        ArrowDataType::List(child) => {
                            !matches!(src_type, ArrowDataType::List(_))
                                && child.data_type() == src_type
                        }
                        _ => false,
                    };
                    if needs_promotion {
                        is_identity = false;
                        mapping.push(ColumnSource::PromoteToList(src_idx));
                    } else {
                        if is_identity && src_idx != target_idx {
                            is_identity = false;
                        }
                        mapping.push(ColumnSource::Direct(src_idx));
                    }
                }
                Err(_) => {
                    is_identity = false;
                    mapping.push(ColumnSource::NullPad);
                }
            }
        }

        Self {
            mapping,
            target_schema: target_schema.clone(),
            is_identity,
        }
    }

    /// Remap a batch using the precomputed mapping. Zero-copy when schemas match;
    /// scalar columns of a target LIST shape are wrapped as singleton lists
    /// (offsets buffer is the only allocation — values are shared, not copied).
    #[inline]
    pub fn pad_batch(&self, batch: &RecordBatch) -> MergeResult<RecordBatch> {
        if self.is_identity {
            return Ok(batch.clone());
        }
        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.mapping.len());
        for (i, entry) in self.mapping.iter().enumerate() {
            match entry {
                ColumnSource::Direct(src_idx) => columns.push(batch.column(*src_idx).clone()),
                ColumnSource::PromoteToList(src_idx) => {
                    let field = &self.target_schema.fields()[i];
                    columns.push(wrap_as_singleton_list(batch.column(*src_idx), field)?);
                }
                ColumnSource::NullPad => {
                    let field = &self.target_schema.fields()[i];
                    columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
                }
            }
        }
        Ok(RecordBatch::try_new(self.target_schema.clone(), columns)?)
    }
}

/// Wraps a scalar array as a list array of singleton lists. A null scalar row
/// becomes a null list (absent field), matching how the writer represents an
/// absent field in a LIST column. The values buffer is shared zero-copy; only
/// the offsets buffer (one i32 per row) is allocated.
fn wrap_as_singleton_list(
    col: &ArrayRef,
    target_field: &arrow::datatypes::FieldRef,
) -> MergeResult<ArrayRef> {
    let ArrowDataType::List(child) = target_field.data_type() else {
        return Err(MergeError::Logic(format!(
            "wrap_as_singleton_list called for non-list target field [{}]",
            target_field.name()
        )));
    };
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat(1).take(col.len()));
    let nulls = col.nulls().cloned();
    let list = ListArray::try_new(Arc::clone(child), offsets, Arc::clone(col), nulls)
        .map_err(|e| MergeError::Logic(format!("singleton-list promotion failed: {}", e)))?;
    Ok(Arc::new(list))
}
